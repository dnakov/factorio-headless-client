use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fs::File;
use std::io::{self, BufRead};
use std::net::SocketAddr;
use std::time::Duration;
use std::path::PathBuf;

use crate::codec::{
    BinaryReader, BinaryWriter, InputAction as CodecInputAction, InputActionType,
    ChunkPosition, Direction, MapEntity, MapPosition, ShootingState, TilePosition,
    SynchronizerActionType, parse_map_data, map_transfer::MapData,
};
use crate::error::{Error, Result};
use crate::protocol::message::{
    ConnectionRequest, ConnectionRequestReply, ConnectionRequestReplyConfirm,
    ConnectionAcceptOrDeny, ModInfo, ServerInfo, TransferBlockRequest,
    InputAction,
};
use crate::protocol::packet::{PacketHeader, MessageType};
use crate::protocol::transport::Transport;
use crate::simulation::{TickExecutor, tick::TickClosureData, tick::TickAction};
use crate::state::{GameWorld, surface::Tile, entity::{Entity, entity_type_from_name, EntityData, EntityType}};
use crate::state::recipe::{Recipe, RecipeItem};
use crate::lua::prototype::Prototypes;

mod actions;
pub use actions::ConnectionActions;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

const MAP_DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(60);
const RECV_TIMEOUT: Duration = Duration::from_millis(500);
const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(16); // ~60 Hz
const MAX_CATCHUP_TICKS_PER_FLUSH: u32 = 60;
const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(3);
const CLIENT_TICK_LEAD_INITIAL: u32 = 32; // PCAP: client_tick=5066319, confirmed_tick=5066287, diff=32
const CLIENT_TICK_LEAD_BIAS: i32 = 0;
const CLIENT_TICK_LEAD_MIN: u32 = 32; // Must match PCAP observation of ~32 tick lead
const CLIENT_TICK_LEAD_MAX: u32 = 256;
const INITIAL_BLOCK_REQUEST_MAX: u32 = 8192;

/// Connection state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    Disconnected,
    QueryingServerInfo,
    Connecting,
    WaitingForReply,
    WaitingForAccept,
    DownloadingMap,
    Connected,     // Map downloaded, but not yet in-game
    InGame,        // Fully synced, can send InputActions
}

/// State for tracking other players
#[derive(Debug, Clone, Default)]
pub struct PlayerState {
    pub player_index: u16,
    pub username: Option<String>,
    pub connected: bool,
    pub x: f64,
    pub y: f64,
    pub walking: bool,
    pub walk_direction: (f64, f64),
    pub last_tick: u32,
    pub mining: bool,
    pub shooting: bool,
}

struct SimulationState {
    world: GameWorld,
    executor: TickExecutor,
}

#[derive(Debug, Clone, Copy)]
struct ReliableRng {
    state: u64,
    inc: u64,
}

impl ReliableRng {
    fn new(seed: u64, stream: u64) -> Self {
        let mut rng = Self {
            state: 0,
            inc: (stream << 1) | 1,
        };
        rng.next_u32();
        rng.state = rng.state.wrapping_add(seed);
        rng.next_u32();
        rng
    }

    fn reseed(&mut self, seed: u64, stream: u64) {
        *self = Self::new(seed, stream);
    }

    fn next_u32(&mut self) -> u32 {
        let old = self.state;
        self.state = old
            .wrapping_mul(6364136223846793005)
            .wrapping_add(self.inc);
        let xorshifted = (((old >> 18) ^ old) >> 27) as u32;
        let rot = (old >> 59) as u32;
        xorshifted.rotate_right(rot)
    }

    fn next_bool(&mut self) -> bool {
        (self.next_u32() & 1) != 0
    }
}

/// Factorio server connection
pub struct Connection {
    addr: SocketAddr,
    transport: Transport,
    state: ConnectionState,
    username: String,

    // Connection info
    client_request_id: u32,
    server_request_id: Option<u32>,
    server_mods: Vec<ModInfo>,

    // Player info
    peer_id: Option<u16>,
    player_index: Option<u16>,
    player_index_confirmed: bool,
    server_name: Option<String>,

    // Player position (client-side tracking)
    player_x: f64,
    player_y: f64,

    // Walking state for position tracking (server-tick based)
    walk_active: bool,
    walk_dir: (f64, f64),
    walk_last_tick: u32,

    // Tick synchronization
    client_tick: u32, // Last sent game tick (used inside tick closures).
    next_closure_tick: Option<u32>,
    client_seq: u32,         // Last sent heartbeat sequence number.
    client_seq_base: u32,    // Initial heartbeat sequence number from Accept.
    server_seq: u32,         // Server heartbeat sequence number (S2C tick_a).
    last_sent_server_seq: u32,
    server_seq_sample: Option<(u32, std::time::Instant)>,
    server_seq_rate_hz: f64,
    server_tick: u32,
    confirmed_tick: u32,
    client_tick_lead: u32,
    latency_value: Option<u8>,
    accept_latency: Option<u8>,
    start_sending_tick: Option<u32>,
    allow_actions: bool,
    seq_synced_with_s2c: bool,                // Whether we've synced C2S seq with first S2C heartbeat
    base_tick: u32,                           // Server tick when we entered InGame
    game_start: Option<std::time::Instant>,   // Time when we entered InGame
    msg_id: u16,          // Reliable message id counter for non-heartbeat packets.
    peer_constant: u16,   // Session constant from Accept (used for RNG/seeding/logging).
    tick_sync: u16,       // Legacy field (kept for debugging/compat).
    reliable_rng: ReliableRng, // RNG for reliable bit selection (pcap shows ~50/50)
    map_transfer_size: Option<u32>,
    map_tick: Option<u32>,

    // Map data
    map_data: Vec<u8>,
    entities: Vec<MapEntity>,
    pub(crate) parsed_map: Option<MapData>,

    // Pending confirmations for reliable messages we received
    pending_confirms: Vec<u32>,
    fragmented_heartbeats: HashMap<u16, FragmentAssembly>,

    // Chat sequence number (pcap shows incrementing u32 in chat actions)
    chat_seq: u32,

    // Pending input actions to send (one per gameplay tick)
    pending_actions: VecDeque<InputAction>,

    // Send init action once player_index is known
    pending_init_action: bool,
    // Send the first gameplay heartbeat with ClientChangedState(0x07)
    pending_start_gameplay: bool,
    // Pending IncreasedLatencyConfirm (latency increase amount to confirm)
    pending_latency_confirm: Option<u8>,
    pending_skipped_tick_confirms: VecDeque<u64>,
    /// Ticks that we should skip (from server's SkippedTickClosure)
    pending_skipped_ticks: std::collections::HashSet<u32>,

    // Track last gameplay send time to avoid multiple sends per tick
    last_gameplay_send_at: Option<std::time::Instant>,
    last_server_heartbeat_at: Option<std::time::Instant>,
    last_disconnect_reason: Option<String>,

    // Track last mining/cursor position for stop signals
    last_mine_position: Option<MapPosition>,

    // Debug counters for initial gameplay packets
    debug_poll_counter: u64,
    debug_current_poll_id: u64,
    debug_gameplay_heartbeats: u8,
    debug_action_packets: u8,
    debug_player_index_dumped: u8,
    debug_server_heartbeat_dumped: u8,
    debug_tick_closure_failures: u8,
    debug_confirm_failures: u8,
    last_action_player_index: u16,

    // Other player tracking
    other_players: HashMap<u16, PlayerState>,
    initial_player_positions: Vec<(f64, f64)>,
    /// Tracks which character positions from the map have been assigned
    assigned_position_indices: std::collections::HashSet<usize>,
    /// Character movement speed from prototype data (tiles per tick)
    character_speed: f64,

    // Deterministic simulation state (client-side)
    simulation: Option<SimulationState>,
}

struct FragmentAssembly {
    type_byte: u8,
    fragments: BTreeMap<u16, Vec<u8>>,
    max_len: usize,
    last_id: Option<u16>,
    created_at: std::time::Instant,
}

impl Connection {
    pub async fn new(addr: SocketAddr, username: String) -> Result<Self> {
        let transport = Transport::new(addr).await?;
        let client_request_id = super::rand_u32();
        let reliable_seed = (rand_u64() ^ ((client_request_id as u64) << 32)).max(1);
        let reliable_rng = ReliableRng::new(reliable_seed, 0x9e3779b97f4a7c15);

        Ok(Self {
            addr,
            transport,
            state: ConnectionState::Disconnected,
            username,
            client_request_id,
            server_request_id: None,
            server_mods: Vec::new(),
            peer_id: None,
            player_index: None,
            player_index_confirmed: false,
            server_name: None,
            player_x: 0.0,
            player_y: 0.0,
            walk_active: false,
            walk_dir: (0.0, 0.0),
            walk_last_tick: 0,
            client_tick: 0,
            next_closure_tick: None,
            client_seq: 0,
            client_seq_base: 0,
            server_seq: 0,
            last_sent_server_seq: 0,
            server_seq_sample: None,
            server_seq_rate_hz: 45.0,
            server_tick: 0,
            confirmed_tick: 0,
            client_tick_lead: CLIENT_TICK_LEAD_INITIAL,
            latency_value: None,
            accept_latency: None,
            start_sending_tick: None,
            allow_actions: false,
            seq_synced_with_s2c: false,
            base_tick: 0,
            game_start: None,
            msg_id: 1,
            peer_constant: 0x41ca, // Default, will be updated from ConnectionAcceptOrDeny
            tick_sync: 0xffff,   // Uses confirmed_tick low 16 bits; 0xffff until known
            reliable_rng,
            map_transfer_size: None,
            map_tick: None,
            map_data: Vec::new(),
            entities: Vec::new(),
            parsed_map: None,
            pending_confirms: Vec::new(),
            fragmented_heartbeats: HashMap::new(),
            chat_seq: 1,
            pending_actions: VecDeque::new(),
            pending_init_action: false,
            pending_start_gameplay: false,
            pending_latency_confirm: None,
            pending_skipped_tick_confirms: VecDeque::new(),
            pending_skipped_ticks: std::collections::HashSet::new(),
            last_gameplay_send_at: None,
            last_server_heartbeat_at: None,
            last_disconnect_reason: None,
            last_mine_position: None,
            debug_poll_counter: 0,
            debug_current_poll_id: 0,
            debug_gameplay_heartbeats: 0,
            debug_action_packets: 0,
            debug_player_index_dumped: 0,
            debug_server_heartbeat_dumped: 0,
            debug_tick_closure_failures: 0,
            debug_confirm_failures: 0,
            last_action_player_index: 0xFFFF,
            other_players: HashMap::new(),
            initial_player_positions: Vec::new(),
            assigned_position_indices: std::collections::HashSet::new(),
            character_speed: 0.15, // Default, updated from map data
            simulation: None,
        })
    }

    pub fn peer_constant(&self) -> u16 {
        self.peer_constant
    }

    pub fn state(&self) -> ConnectionState {
        self.state
    }

    pub fn last_disconnect_reason(&self) -> Option<&str> {
        self.last_disconnect_reason.as_deref()
    }

    pub fn last_server_heartbeat_age_ms(&self) -> Option<u64> {
        self.last_server_heartbeat_at
            .map(|t| t.elapsed().as_millis() as u64)
    }

    pub fn start_sending_tick(&self) -> Option<u32> {
        self.start_sending_tick
    }

    pub fn confirmed_tick(&self) -> u32 {
        self.confirmed_tick
    }

    pub fn server_tick(&self) -> u32 {
        self.server_tick
    }

    pub fn server_seq(&self) -> u32 {
        self.server_seq
    }

    pub fn client_tick(&self) -> u32 {
        self.client_tick
    }

    pub fn client_seq(&self) -> u32 {
        self.client_seq
    }

    pub fn client_seq_base(&self) -> u32 {
        self.client_seq_base
    }

    pub fn latency_value(&self) -> Option<u8> {
        self.latency_value
    }

    pub fn actions(&mut self) -> ConnectionActions<'_> {
        ConnectionActions::new(self)
    }

    pub fn player_index(&self) -> Option<u16> {
        self.player_index
    }

    pub fn peer_id(&self) -> Option<u16> {
        self.peer_id
    }

    pub fn server_name(&self) -> Option<&str> {
        self.server_name.as_deref()
    }

    pub fn map_data(&self) -> &[u8] {
        &self.map_data
    }

    pub fn sim_world(&self) -> Option<&GameWorld> {
        self.simulation.as_ref().map(|s| &s.world)
    }

    pub fn sim_world_mut(&mut self) -> Option<&mut GameWorld> {
        self.simulation.as_mut().map(|s| &mut s.world)
    }

    fn init_simulation_from_map(&mut self, map: &MapData) {
        let mut world = GameWorld::new();
        world.tick = map.ticks_played;
        world.seed = map.seed;
        world.character_speed = map.character_speed();
        world.spawn_position = MapPosition::from_tiles(map.player_spawn.0, map.player_spawn.1);
        if let Some(items) = map.prototype_mappings.tables.get("ItemPrototype") {
            world.item_id_map = items.clone();
        }
        if let Some(recipes) = map.prototype_mappings.tables.get("Recipe") {
            world.recipe_id_map = recipes.clone();
        }
        if let Some(entities) = map.prototype_mappings.tables.get("Entity") {
            world.entity_id_map = entities.clone();
        }
        if let Some(tiles) = map.prototype_mappings.tables.get("TilePrototype") {
            world.tile_id_map = tiles.clone();
        }
        if let Some(techs) = map.prototype_mappings.tables.get("Technology") {
            world.tech_id_map = techs.clone();
        }

        // Lua prototype loading is slow and blocks the main loop, causing heartbeat
        // timeout. Skip by default. Set FACTORIO_LOAD_LUA_PROTOS=1 to enable (for
        // rendering or when Lua data is needed). If prototypes are already loaded,
        // we use them without blocking.
        if std::env::var("FACTORIO_LOAD_LUA_PROTOS").is_ok() && Prototypes::global().is_none() {
            if let Some(path) = default_factorio_data_path() {
                let _ = Prototypes::init_global(&path);
            }
        }
        if let Some(protos) = Prototypes::global() {
            if let Some(character) = protos.entity("character") {
                if let Some(speed) = character.running_speed {
                    world.character_speed = speed;
                }
                if let Some(dpf) = character.distance_per_frame {
                    world.character_distance_per_frame = dpf;
                }
                if let Some(corner) = character.maximum_corner_sliding_distance {
                    world.character_max_corner_sliding_distance = corner;
                }
                world.character_collision_box = character.collision_box;
            }
            for (id, name) in &world.recipe_id_map {
                if let Some(proto) = protos.recipe(name) {
                    let mut recipe = Recipe::new(proto.name.clone());
                    recipe.category = proto.category.clone();
                    recipe.crafting_time = proto.energy_required;
                    recipe.ingredients = proto
                        .ingredients
                        .iter()
                        .map(|ing| RecipeItem::new(ing.name.clone(), ing.amount))
                        .collect();
                    recipe.products = proto
                        .results
                        .iter()
                        .map(|res| RecipeItem::new(res.name.clone(), res.amount))
                        .collect();
                    world.recipes.add(*id, recipe);
                }
            }
        }

        if let Some(surface) = world.nauvis_mut() {
            for tile in &map.tiles {
                let pos = TilePosition::new(tile.x, tile.y);
                let chunk_pos = ChunkPosition::from_tile(pos);
                let chunk = surface.get_or_create_chunk(chunk_pos);
                let local_x = pos.x.rem_euclid(32) as u8;
                let local_y = pos.y.rem_euclid(32) as u8;
                chunk.set_tile(local_x, local_y, Tile::new(tile.name.clone()));
                chunk.generated = true;
            }
        }

        for ent in &map.entities {
            let id = world.next_entity_id();
            let direction = Direction::from_u8((ent.direction / 2) % 8).unwrap_or(Direction::North);
            let entity_type = entity_type_from_name(&ent.name);
            let mut entity = Entity::new(id, ent.name.clone(), MapPosition::from_tiles(ent.x, ent.y))
                .with_direction(direction)
                .with_type(entity_type);
            entity.data = match entity_type {
                EntityType::Resource => {
                    let mut infinite = ent.resource_infinite;
                    if !infinite {
                        if let Some(proto) = Prototypes::global().and_then(|p| p.entity(&ent.name)) {
                            infinite = proto.resource_infinite;
                        }
                    }
                    let amount = ent.resource_amount.unwrap_or(0);
                    EntityData::Resource(crate::state::entity::ResourceData {
                        amount,
                        infinite,
                        mining_time: 0.0,
                    })
                }
                _ => crate::state::entity::default_entity_data_for_type(entity_type),
            };
            if entity_type == EntityType::TrainStop {
                if let EntityData::TrainStop(ref mut data) = entity.data {
                    if data.station_name.is_empty() {
                        data.station_name = format!("train-stop-{}", id);
                    }
                }
            }
            crate::state::entity::init_entity_inventories(&mut entity);
            crate::state::entity::init_belt_metadata(&mut entity);
            if let Some(proto) = Prototypes::global().and_then(|p| p.entity(&entity.name)) {
                crate::state::entity::apply_entity_prototype(&mut entity, proto);
            }
            if entity_type == EntityType::UndergroundBelt {
                if let (Some(belt_type), EntityData::TransportBelt(ref mut data)) =
                    (ent.underground_type, &mut entity.data)
                {
                    data.underground_type = Some(belt_type);
                }
            }
            if let Some(surface) = world.nauvis_mut() {
                surface.add_entity(entity);
            }
        }

        self.simulation = Some(SimulationState {
            world,
            executor: TickExecutor::new(),
        });
    }

    pub fn player_position(&self) -> (f64, f64) {
        (self.player_x, self.player_y)
    }

    pub fn update_position(&mut self) {
        if self.simulation.is_some() {
            self.sync_simulation_to_server_tick();
            if !self.walk_active {
                if let Some(player_index) = self.player_index {
                    if let Some(sim) = self.simulation.as_ref() {
                        if let Some(player) = sim.world.players.get(&player_index) {
                            let (x, y) = player.position.to_tiles();
                            self.player_x = x;
                            self.player_y = y;
                        }
                    }
                }
            }
        }
        if !self.walk_active {
            return;
        }
        let movement_tick = if self.server_tick >= self.confirmed_tick {
            self.server_tick
        } else {
            self.confirmed_tick
        };
        let ticks = movement_tick.saturating_sub(self.walk_last_tick);
        if ticks == 0 {
            return;
        }
        let dx_tick = (self.character_speed * self.walk_dir.0 * 256.0).trunc() / 256.0;
        let dy_tick = (self.character_speed * self.walk_dir.1 * 256.0).trunc() / 256.0;
        self.player_x += dx_tick * ticks as f64;
        self.player_y += dy_tick * ticks as f64;
        self.walk_last_tick = movement_tick;
        if let Some(player_index) = self.player_index {
            if let Some(sim) = self.simulation.as_mut() {
                if let Some(player) = sim.world.players.get_mut(&player_index) {
                    player.position = MapPosition::from_tiles(self.player_x, self.player_y);
                }
            }
        }
    }

    pub fn entities(&self) -> &[MapEntity] {
        &self.entities
    }

    pub(crate) fn apply_parsed_map(&mut self, parsed: MapData) {
        self.initial_player_positions = parsed.character_positions();
        self.character_speed = parsed.character_speed();
        self.entities = parsed.entities.clone();
        if parsed.player_spawn != (0.0, 0.0) && (self.player_x == 0.0 && self.player_y == 0.0) {
            self.player_x = parsed.player_spawn.0;
            self.player_y = parsed.player_spawn.1;
        }
        if self.simulation.is_none() {
            self.init_simulation_from_map(&parsed);
        }
        self.parsed_map = Some(parsed);
    }

    /// Get other players' states
    pub fn other_players(&self) -> &HashMap<u16, PlayerState> {
        &self.other_players
    }

    /// Get initial player positions from map
    pub fn initial_player_positions(&self) -> &[(f64, f64)] {
        &self.initial_player_positions
    }

    /// Assign an unassigned character position from the map to a player
    /// Returns the position if one was available, None otherwise
    fn assign_character_position(&mut self) -> Option<(f64, f64)> {
        // Find the first unassigned position
        for (idx, pos) in self.initial_player_positions.iter().enumerate() {
            if !self.assigned_position_indices.contains(&idx) {
                self.assigned_position_indices.insert(idx);
                return Some(*pos);
            }
        }
        None
    }

    /// Connect to the server (full handshake)
    pub async fn connect(&mut self) -> Result<()> {
        self.last_disconnect_reason = None;
        self.last_server_heartbeat_at = None;
        self.pending_actions.clear();
        self.pending_confirms.clear();
        self.start_sending_tick = None;
        self.allow_actions = false;
        self.pending_init_action = false;
        self.pending_start_gameplay = false;
        self.pending_latency_confirm = None;
        self.pending_skipped_tick_confirms.clear();
        self.pending_skipped_ticks.clear();

        // Step 1: Query server info to get mod list (optional - use defaults if it fails)
        self.state = ConnectionState::QueryingServerInfo;
        if let Err(e) = self.query_server_info().await {
            let debug = std::env::var("FACTORIO_DEBUG").is_ok();
            if debug {
                eprintln!("[DEBUG] query_server_info failed: {:?}, using default mods", e);
            }
            // Use default Space Age mods (from pcap analysis)
            self.server_mods = Self::default_space_age_mods();
        }

        // Create a fresh transport for the actual connection
        // (Factorio expects a fresh socket after server info query)
        self.transport = Transport::new(self.addr).await?;

        // Step 2: Send ConnectionRequest
        self.state = ConnectionState::Connecting;
        self.send_connection_request().await?;

        // Step 3: Wait for ConnectionRequestReply
        self.state = ConnectionState::WaitingForReply;
        let reply = self.wait_for_reply().await?;
        self.server_request_id = Some(reply.server_request_id);

        // Step 4: Send ConnectionRequestReplyConfirm
        self.send_confirm().await?;

        // Step 5: Wait for ConnectionAcceptOrDeny
        self.state = ConnectionState::WaitingForAccept;
        let accept = self.wait_for_accept().await?;

        if !accept.accepted {
            let reason = accept.denial_reason
                .map(|r| format!("{:?}", r))
                .unwrap_or_else(|| "unknown".into());
            return Err(Error::ConnectionRefused { reason });
        }

        // Accept payload carries both peer identifier and player index.
        if self.peer_id.is_none() {
            self.peer_id = accept.peer_id;
        }
        // In Factorio 2.0, the player_index from Accept packet is actually the peer_id,
        // not the true player_index which is assigned via PlayerJoinGame. Don't use it.
        // We'll get the real player_index from PlayerJoinGame or other methods.
        // self.player_index = accept.player_index;
        // self.player_index_confirmed = accept.player_index.is_some();
        // Log accept info to HB log for debugging
        if let Ok(mut file) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open("/tmp/factorio-client-hb.log")
        {
            use std::io::Write;
            let _ = writeln!(
                file,
                "ACCEPT peer_id={:?} player_index={:?}",
                self.peer_id, self.player_index
            );
        }
        self.server_name = accept.server_name;

        if let Some(msg_id) = accept.initial_msg_id {
            self.msg_id = msg_id;
        }
        if let Some(latency) = accept.latency {
            self.accept_latency = Some(latency);
            if self.latency_value.is_none() {
                self.latency_value = Some(latency);
            }
            // Keep client tick lead aligned with server latency unless updated later.
            let lead = ((latency as u32) + 15) / 16;
            self.client_tick_lead = lead.clamp(CLIENT_TICK_LEAD_MIN, CLIENT_TICK_LEAD_MAX);
        }
        if let Some(session_const) = accept.session_constant {
            let debug = std::env::var("FACTORIO_DEBUG").is_ok();
            if debug {
                eprintln!("[DEBUG] Setting peer_constant from Accept: 0x{:04x} -> 0x{:04x}",
                    self.peer_constant, session_const);
            }
            self.peer_constant = session_const;
        }
        // Use initial_tick as our starting heartbeat sequence.
        // The server's nextHeartbeatSequenceNumber starts at initial_tick and advances ~60Hz.
        // We start from here and will catch up to the current S2C sequence when we receive it.
        if let Some(initial_tick) = accept.initial_tick {
            self.client_seq_base = initial_tick;
            self.client_seq = initial_tick;
            if std::env::var("FACTORIO_DEBUG").is_ok() {
                eprintln!("[DEBUG] Starting heartbeat sequence from initial_tick: {}", initial_tick);
            }
        }
        self.seed_reliable_rng();

        self.state = ConnectionState::Connected;

        Ok(())
    }

    fn seed_reliable_rng(&mut self) {
        let server_req = self.server_request_id.unwrap_or(0);
        let seed = ((self.client_request_id as u64) << 32) | server_req as u64;
        let stream = ((self.peer_constant as u64) << 16) | (server_req as u64 & 0xffff);
        self.reliable_rng.reseed(seed, stream);
    }

    async fn query_server_info(&mut self) -> Result<()> {
        // Use the LAN discovery port (34196) if available, matching real client behavior.
        let bind_addr = match self.addr {
            SocketAddr::V4(_) => SocketAddr::from(([0, 0, 0, 0], 34196)),
            SocketAddr::V6(_) => SocketAddr::from(([0, 0, 0, 0, 0, 0, 0, 0], 34196)),
        };
        let mut transport = match Transport::new_with_bind(self.addr, bind_addr).await {
            Ok(t) => t,
            Err(_) => Transport::new(self.addr).await?,
        };

        // Send GameInformationRequest (0x10) and the reliable-flag variant (0x30).
        transport.send_raw(&[0x10]).await?;
        transport.send_raw(&[0x30]).await?;

        // Wait for response
        let start = std::time::Instant::now();
        while start.elapsed() < CONNECT_TIMEOUT {
            if let Some(data) = transport.recv_raw_timeout(RECV_TIMEOUT).await? {
                if !data.is_empty() && (data[0] & 0x1F) == MessageType::GameInformationRequestReply as u8 {
                    // Parse mod list from server info
                    let info = ServerInfo::parse(&data)?;
                    self.server_mods = info.mods;
                    return Ok(());
                }
            }
        }

        Err(Error::ConnectionTimeout)
    }

    /// Default Space Age mods with CRCs from pcap analysis
    fn default_space_age_mods() -> Vec<ModInfo> {
        use crate::protocol::message::ModVersion;
        vec![
            ModInfo {
                name: "base".to_string(),
                version: ModVersion::new(2, 0, 72),
                crc: 0x70059c86,
            },
            ModInfo {
                name: "elevated-rails".to_string(),
                version: ModVersion::new(2, 0, 72),
                crc: 0x31790248,
            },
            ModInfo {
                name: "quality".to_string(),
                version: ModVersion::new(2, 0, 72),
                crc: 0x441a8746,
            },
            ModInfo {
                name: "space-age".to_string(),
                version: ModVersion::new(2, 0, 72),
                crc: 0x5a0ae76b,
            },
        ]
    }

    async fn send_connection_request(&mut self) -> Result<()> {
        let request = ConnectionRequest::new(self.client_request_id);

        let mut writer = BinaryWriter::new();
        request.write(&mut writer);

        let mut packet = Vec::with_capacity(1 + 2 + writer.as_slice().len());
        packet.push(MessageType::ConnectionRequest as u8);
        packet.extend_from_slice(&0u16.to_le_bytes()); // msg_id = 0
        packet.extend_from_slice(writer.as_slice());

        self.transport.send_raw(&packet).await
    }

    async fn wait_for_reply(&mut self) -> Result<ConnectionRequestReply> {
        let start = std::time::Instant::now();

        while start.elapsed() < CONNECT_TIMEOUT {
            if let Some(data) = self.transport.recv_raw_timeout(RECV_TIMEOUT).await? {
                if data.is_empty() {
                    continue;
                }

                let (header, payload_start) = PacketHeader::parse(&data)?;

                if header.message_type == MessageType::ConnectionRequestReply {
                    let payload = &data[payload_start..];
                    let mut reader = crate::codec::BinaryReader::new(payload);
                    return ConnectionRequestReply::read(&mut reader);
                }
            }
        }

        Err(Error::ConnectionTimeout)
    }

    async fn send_confirm(&mut self) -> Result<()> {
        let server_req_id = self.server_request_id
            .ok_or_else(|| Error::InvalidPacket("no server request ID".into()))?;

        let mut confirm = ConnectionRequestReplyConfirm::new(
            self.client_request_id,
            server_req_id,
            self.username.clone(),
            self.server_mods.clone(),
        );
        if let Some((core_checksum, prototype_checksum)) = read_local_checksums() {
            let debug = std::env::var("FACTORIO_DEBUG").is_ok();
            if debug {
                eprintln!(
                    "[DEBUG] Using local checksums: core={} prototype={}",
                    core_checksum, prototype_checksum
                );
            }
            confirm.core_checksum = core_checksum;
            confirm.prototype_list_checksum = prototype_checksum;
        }

        let mut writer = BinaryWriter::new();
        confirm.write(&mut writer);

        let mut packet = Vec::with_capacity(1 + 2 + writer.as_slice().len());
        packet.push(MessageType::ConnectionRequestReplyConfirm as u8);
        packet.extend_from_slice(&1u16.to_le_bytes()); // msg_id = 1
        packet.extend_from_slice(writer.as_slice());

        self.transport.send_raw(&packet).await
    }

    async fn wait_for_accept(&mut self) -> Result<ConnectionAcceptOrDeny> {
        let start = std::time::Instant::now();

        while start.elapsed() < CONNECT_TIMEOUT {
            if let Some(data) = self.transport.recv_raw_timeout(RECV_TIMEOUT).await? {
                if data.is_empty() {
                    continue;
                }

                let (header, payload_start) = PacketHeader::parse(&data)?;

                match header.message_type {
                    MessageType::ConnectionAcceptOrDeny => {
                        let payload = &data[payload_start..];
                        return Ok(self.parse_accept_payload(payload));
                    }
                    MessageType::ServerToClientHeartbeat => {
                        // Just process the heartbeat, don't respond yet - we haven't received
                        // ConnectionAcceptOrDeny which sets the correct heartbeat sequence number!
                        let _ = self.process_server_heartbeat(&data);
                    }
                    MessageType::Empty => {
                        return Err(Error::ConnectionRefused {
                            reason: "received Empty message (possible protocol error)".into(),
                        });
                    }
                    _ => continue,
                }
            }
        }

        Err(Error::ConnectionTimeout)
    }

    /// Parse ConnectionAcceptOrDeny payload to extract player_index, server_name, etc.
    fn parse_accept_payload(&self, payload: &[u8]) -> ConnectionAcceptOrDeny {
        let debug = std::env::var("FACTORIO_DEBUG").is_ok();

        if debug {
            eprintln!("[DEBUG] Accept payload ({} bytes): {:02x?}", payload.len(), payload);
        }

        let parsed = (|| -> Result<ConnectionAcceptOrDeny> {
            let mut reader = BinaryReader::new(payload);

            let _client_req_id = reader.read_u32_le()?;
            let status = reader.read_u8()?;
            let server_name = reader.read_string().ok();
            let _server_key = reader.read_string()?;
            let _unused_auth = reader.read_string()?;
            let latency = reader.read_u8()?;
            let _max_updates_per_second = reader.read_opt_u32()?;
            let _game_id = reader.read_u32_le()?;
            let steam_id = reader.read_u64_le().ok();

            let peer_id = self.parse_clients_peer_info(&mut reader, debug).ok().flatten();

            let initial_tick = reader.read_u32_le()?;
            // packed_ids is u32: (session_constant << 16) | initial_msg_id
            let packed_ids = reader.read_u32_le()?;
            let initial_msg_id = (packed_ids & 0xFFFF) as u16;
            let session_constant = ((packed_ids >> 16) & 0xFFFF) as u16;
            let player_index = reader.read_u16_le()?;

            // Mod list (opt_u32 count + entries), safe to skip
            let mod_count = reader.read_opt_u32()? as usize;
            for _ in 0..mod_count {
                let _ = ModInfo::read(&mut reader)?;
            }

            let accepted = status == 0;
            let accept = ConnectionAcceptOrDeny {
                accepted,
                peer_id,
                player_index: Some(player_index),
                server_name,
                denial_reason: if accepted { None } else { Some(crate::protocol::message::DenialReason::Unknown) },
                initial_tick: Some(initial_tick),
                initial_msg_id: Some(initial_msg_id),
                session_constant: Some(session_constant),
                latency: Some(latency),
                peer_count: None,
                map_tick: None,
                steam_id,
                latency_window: None,
            };

            if debug {
                eprintln!(
                    "[DEBUG] Accept parsed: player_index={} peer_id={:?} initial_tick={:?} initial_msg_id={:?} session_constant={:?} latency={:?}",
                    player_index,
                    peer_id,
                    accept.initial_tick,
                    accept.initial_msg_id,
                    accept.session_constant,
                    accept.latency
                );
            }
            if std::env::var("FACTORIO_DEBUG_HB_FILE").is_ok() {
                if let Ok(mut file) = std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open("/tmp/factorio-accept.log")
                {
                    use std::io::Write;
                    let _ = writeln!(
                        file,
                        "accept player_index={} peer_id={:?} initial_tick={:?} initial_msg_id={:?} session_constant={:?} latency={:?}",
                        player_index,
                        peer_id,
                        accept.initial_tick,
                        accept.initial_msg_id,
                        accept.session_constant,
                        accept.latency
                    );
                }
            }

            Ok(accept)
        })();

        if let Ok(accept) = parsed {
            return accept;
        }

        // Fallback heuristic if the payload format shifts
        let mut server_name = None;
        let mut peer_id: Option<u16> = None;
        let mut player_index: Option<u16> = None;
        let mut initial_tick: Option<u32> = None;
        let mut initial_msg_id: Option<u16> = None;
        let mut session_constant: Option<u16> = None;

        if payload.len() > 5 {
            let name_start = 5;
            if let Some(name_len) = payload.get(name_start) {
                let len = *name_len as usize;
                if payload.len() > name_start + 1 + len {
                    if let Ok(name) = std::str::from_utf8(&payload[name_start + 1..name_start + 1 + len]) {
                        server_name = Some(name.to_string());
                    }
                }
            }
        }

        let username_bytes = self.username.as_bytes();
        if username_bytes.len() < 255 {
            for i in 0..payload.len().saturating_sub(username_bytes.len() + 2) {
                if payload[i + 1] as usize == username_bytes.len() {
                    let potential_start = i + 2;
                    if potential_start + username_bytes.len() <= payload.len() {
                        if &payload[potential_start..potential_start + username_bytes.len()] == username_bytes {
                            player_index = Some(payload[i] as u16);
                            let session_start = potential_start + username_bytes.len() + 1;
                            if session_start + 8 <= payload.len() {
                                initial_tick = Some(u32::from_le_bytes([
                                    payload[session_start],
                                    payload[session_start + 1],
                                    payload[session_start + 2],
                                    payload[session_start + 3],
                                ]));
                                // packed_ids is u32: (session_constant << 16) | initial_msg_id
                                let packed_ids = u32::from_le_bytes([
                                    payload[session_start + 4],
                                    payload[session_start + 5],
                                    payload[session_start + 6],
                                    payload[session_start + 7],
                                ]);
                                initial_msg_id = Some((packed_ids & 0xFFFF) as u16);
                                session_constant = Some(((packed_ids >> 16) & 0xFFFF) as u16);
                            }
                            break;
                        }
                    }
                }
            }
        }

        if player_index.is_none() {
            player_index = Some(1);
        }

        ConnectionAcceptOrDeny {
            accepted: true,
            peer_id,
            player_index,
            server_name,
            denial_reason: None,
            initial_tick,
            initial_msg_id,
            session_constant,
            latency: None,
            peer_count: None,
            map_tick: None,
            steam_id: None,
            latency_window: None,
        }
    }

    fn parse_clients_peer_info(
        &self,
        reader: &mut BinaryReader,
        debug: bool,
    ) -> Result<Option<u16>> {
        let _server_state_name = reader.read_string()?;
        let _server_state = reader.read_u8()?;
        let state_count = reader.read_opt_u16()? as usize;
        for _ in 0..state_count {
            let _ = reader.read_opt_u16()?;
        }

        let peer_count = reader.read_opt_u32()? as usize;
        let mut peer_id = None;
        for _ in 0..peer_count {
            let id = reader.read_opt_u16()?;
            let name = reader.read_string()?;
            let flags = reader.read_u8()?;
            for bit in 0..5 {
                if (flags & (1 << bit)) != 0 {
                    let _ = reader.read_u8()?;
                }
            }
            if name == self.username {
                peer_id = Some(id);
            }
        }

        if debug {
            eprintln!("[DEBUG] ClientsPeerInfo parsed: peer_id={:?}", peer_id);
        }

        Ok(peer_id)
    }


    /// Download map data from server
    /// Call this after connect() succeeds
    pub async fn download_map(&mut self) -> Result<usize> {
        self.download_map_with_parse(true).await
    }

    /// Download map data from server with optional map parsing.
    pub async fn download_map_with_parse(&mut self, parse_map: bool) -> Result<usize> {
        eprintln!("[TRACE] download_map_with_parse: entering function");
        self.state = ConnectionState::DownloadingMap;
        self.map_data.clear();

        let debug = std::env::var("FACTORIO_DEBUG").is_ok();
        let download_started = std::time::Instant::now();
        eprintln!("[TRACE] download_map_with_parse: initial_tick={:?} client_seq={}",
            self.client_seq_base, self.client_seq);
        if debug {
            eprintln!("[DEBUG] download_map: start");
        }
        


        // Transfer size detection - we look for this throughout the phases
        let mut transfer_size: Option<u32> = None;
        let mut max_block: Option<u32> = None;

        fn update_transfer_size(
            transfer_size: &mut Option<u32>,
            max_block: &mut Option<u32>,
            size: u32,
        ) -> Option<u32> {
            let update = transfer_size.map_or(true, |current| size > current);
            if !update {
                return None;
            }
            let mut blocks = (size + 502) / 503;
            if blocks == 0 {
                blocks = 1;
            }
            *transfer_size = Some(size);
            *max_block = Some(blocks.saturating_sub(1));
            Some(blocks)
        }

        // Phase 0: Wait for first server heartbeat BEFORE sending our heartbeat
        // PCAP shows: server sends heartbeat first, then client responds
        // This establishes the latency window on the server side
        // CRITICAL: Server's latency window is NOT active until it sends its first heartbeat to us.
        // If we send before receiving the server heartbeat, we get "heartbeat outside latency window".
        let wait_start = std::time::Instant::now();
        let wait_timeout = Duration::from_millis(500);
        let mut got_server_heartbeat = false;

        // WAIT for server heartbeat first to establish latency window
        eprintln!("[HEARTBEAT] Waiting for server heartbeat to establish latency window...");
        while wait_start.elapsed() < wait_timeout && !got_server_heartbeat {
            match self.transport.recv_raw_timeout(Duration::from_millis(10)).await {
                Ok(Some(data)) if !data.is_empty() => {
                    if let Some(size) = self.extract_transfer_size_from_packet(&data, debug) {
                        let _ = update_transfer_size(&mut transfer_size, &mut max_block, size);
                    }
                    let msg_type = data[0] & 0x1F;
                    if msg_type == MessageType::ServerToClientHeartbeat as u8 {
                        let _ = self.process_server_heartbeat(&data);
                        got_server_heartbeat = true;
                        eprintln!("[HEARTBEAT] Got server heartbeat! server_seq={} elapsed={:?}",
                            self.server_seq, wait_start.elapsed());
                    }
                }
                _ => {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            }
        }

        // Mark that we've received the first S2C heartbeat.
        // The actual sequence synchronization happens in compute_heartbeat_seq_u32()
        // which will catch up to the server's sequence.
        if got_server_heartbeat && !self.seq_synced_with_s2c {
            self.seq_synced_with_s2c = true;
            eprintln!("[HEARTBEAT] Got server heartbeat! server_seq={} elapsed={:?}",
                self.server_seq, wait_start.elapsed());
        }

        // NOW send our first heartbeat after receiving server's (establishes latency window)
        eprintln!("[HEARTBEAT] Sending first heartbeat AFTER receiving server HB");
        self.send_initial_state_heartbeat_with_ff().await?;
        eprintln!("[HEARTBEAT] First heartbeat sent with seq={}", self.client_seq_base.saturating_sub(1));

        // Sync for a short time with flags 0x00 heartbeats using 0xff tick values
        // Real client sends ~6-7 sync heartbeats with 0xff ticks before ready-for-map
        let sync_start = std::time::Instant::now();
        let sync_duration = Duration::from_millis(100);

        while sync_start.elapsed() < sync_duration {
            match self.transport.recv_raw_timeout(Duration::from_millis(10)).await {
                Ok(Some(data)) if !data.is_empty() => {
                    // Check for transfer size in any large packet
                    if let Some(size) = self.extract_transfer_size_from_packet(&data, debug) {
                        let _ = update_transfer_size(&mut transfer_size, &mut max_block, size);
                    }
                    let msg_type = data[0] & 0x1F;
                    if msg_type == MessageType::ServerToClientHeartbeat as u8 {
                        let _ = self.process_server_heartbeat(&data);
                    }
                }
                _ => {}
            }
            // Send flags 0x00 heartbeat with 0xff tick values during initial sync
            // We start from initial_tick and compute_heartbeat_seq_u32 will catch up to server
            // Note: send_initial_sync_heartbeat has internal rate limiting via wait_pre_game_heartbeat_slot
            self.send_initial_sync_heartbeat().await?;
        }

        // Phase 2: Signal ready for map with trailer 02 03 03 09 00
        self.send_state_heartbeat(&[0x02, 0x03, 0x03, 0x09, 0x00]).await?;

        // Continue looking for transfer size (MapReadyForDownload) before requesting blocks.
        if max_block.is_none() {
            let wait_start = std::time::Instant::now();
            let wait_timeout = Duration::from_millis(1200);
            let mut last_ready_heartbeat = std::time::Instant::now()
                .checked_sub(self.heartbeat_interval())
                .unwrap_or_else(std::time::Instant::now);
            let ready_progress_steps: [u8; 3] = [0x23, 0xa0, 0xfe];
            let mut ready_progress_idx = 0usize;
            while wait_start.elapsed() < wait_timeout && max_block.is_none() {
                match self.transport.recv_raw_timeout(Duration::from_millis(10)).await {
                    Ok(Some(data)) if !data.is_empty() => {
                        if let Some(size) = self.extract_transfer_size_from_packet(&data, debug) {
                            let _ = update_transfer_size(&mut transfer_size, &mut max_block, size);
                        }
                        let msg_type = data[0] & 0x1F;
                        if msg_type == MessageType::ServerToClientHeartbeat as u8 {
                            let _ = self.process_server_heartbeat(&data);
                        }
                    }
                    _ => {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                }
                if last_ready_heartbeat.elapsed() >= self.heartbeat_interval() {
                    let step = ready_progress_steps
                        .get(ready_progress_idx)
                        .copied()
                        .unwrap_or(0xfe);
                    let _ = self.send_state_heartbeat(&[0x01, 0x09, step]).await;
                    if ready_progress_idx + 1 < ready_progress_steps.len() {
                        ready_progress_idx += 1;
                    }
                    last_ready_heartbeat = std::time::Instant::now();
                }
            }
        }

        if max_block.is_none() {
            return Err(Error::InvalidPacket(
                "MapReadyForDownload not received (transfer size unknown)".to_string(),
            ));
        }

        if debug {
            eprintln!("[DEBUG] Starting map download, transfer_size={:?} max_block={:?}", transfer_size, max_block);
        }

        // Phase 3: Request and receive map blocks
        // From pcap: real client sends ALL block requests RAPIDLY (burst), not waiting for responses
        let start = std::time::Instant::now();
        let mut received_blocks: std::collections::HashSet<u32> = std::collections::HashSet::new();
        let mut blocks: Vec<Option<Vec<u8>>> = Vec::new();
        let mut last_heartbeat = std::time::Instant::now();
        let mut last_new_block_time = std::time::Instant::now();
        let mut last_resend = std::time::Instant::now();
        let mut got_all_blocks = false;
        // Keep progress markers aligned with observed official values.
        let progress_markers: [u8; 12] = [
            0x08, 0x1e, 0x23, 0x35, 0x44, 0x5c, 0x73, 0x8a, 0xa0, 0xbc, 0xd3, 0xfe,
        ];
        let mut last_progress = progress_markers[0];
        let progress_marker_for = |progress: u8, current: u8| -> u8 {
            let mut value = current;
            for marker in progress_markers {
                if progress >= marker {
                    value = marker;
                } else {
                    break;
                }
            }
            value
        };

        // Send block requests in rapid batches, but intersperse heartbeats to stay in latency window
        let initial_max = max_block.unwrap_or(INITIAL_BLOCK_REQUEST_MAX);
        let mut sent = 0u32;
        let mut burst_heartbeat = std::time::Instant::now();
        let mut requested_max = initial_max;
        let mut next_request_block = initial_max.saturating_add(1);
        let mut last_expand = std::time::Instant::now();
        let mut all_requested = max_block.map_or(false, |max| max <= initial_max);
        while sent <= initial_max {
            let batch_end = (sent + 200).min(initial_max + 1);
            for i in sent..batch_end {
                let reliable = false;
                let request = TransferBlockRequest::new(i, reliable);
                let _ = self.transport.send_raw(&request.to_bytes()).await;
            }
            sent = batch_end;
            if burst_heartbeat.elapsed() >= self.heartbeat_interval() {
                if let Some(max) = max_block {
                    let progress = ((received_blocks.len() as f64 / (max + 1) as f64) * 255.0).round() as u8;
                    last_progress = progress_marker_for(progress, last_progress);
                }
                let _ = self.send_state_heartbeat(&[0x01, 0x09, last_progress]).await;
                burst_heartbeat = std::time::Instant::now();
            }
            // Drain incoming blocks while sending to prevent socket buffer overflow
            while let Ok(Some(data)) = self.transport.try_recv_raw() {
                if data.is_empty() { break; }
                if (data[0] & 0x1F) == MessageType::TransferBlock as u8 && data.len() >= 5 {
                    let recv_block = u32::from_le_bytes([data[1], data[2], data[3], data[4]]);
                    if !received_blocks.contains(&recv_block) {
                        if blocks.len() <= recv_block as usize {
                            blocks.resize(recv_block as usize + 1, None);
                        }
                        blocks[recv_block as usize] = Some(data[5..].to_vec());
                        received_blocks.insert(recv_block);
                        last_new_block_time = std::time::Instant::now();
                    }
                }
            }
            if let Some(max) = max_block {
                if received_blocks.len() as u32 >= max + 1 {
                    got_all_blocks = true;
                    break;
                }
            }
        }
        if debug {
            eprintln!("[DEBUG] Burst done: sent {} requests, received {} blocks, max_block={:?} got_all={}", initial_max + 1, received_blocks.len(), max_block, got_all_blocks);
        }

        while start.elapsed() < MAP_DOWNLOAD_TIMEOUT {
            if last_heartbeat.elapsed() >= self.heartbeat_interval() {
                if let Some(max) = max_block {
                    let progress = ((received_blocks.len() as f64 / (max + 1) as f64) * 255.0).round() as u8;
                    last_progress = progress_marker_for(progress, last_progress);
                }
                let _ = self.send_state_heartbeat(&[0x01, 0x09, last_progress]).await;
                last_heartbeat = std::time::Instant::now();
            }

            if got_all_blocks {
                break;
            }

            // Timeout: if no new blocks for 1000ms, assume complete when size is known.
            if max_block.is_some() && !received_blocks.is_empty() && last_new_block_time.elapsed() > Duration::from_millis(1000) {
                break;
            }

            let data = match self.transport.try_recv_raw() {
                Ok(Some(d)) if !d.is_empty() => d,
                _ => match self.transport.recv_raw_timeout(Duration::from_millis(1)).await {
                    Ok(Some(d)) if !d.is_empty() => d,
                    _ => continue,
                },
            };

            let type_byte = data[0];
            let msg_type = type_byte & 0x1F;
            let is_reliable = (type_byte & 0x20) != 0;

            // Track reliable messages for confirmation
            if is_reliable && data.len() >= 3 {
                let msg_id = u16::from_le_bytes([data[1], data[2]]) & 0x7FFF;
                self.pending_confirms.push(msg_id as u32);
                if debug {
                    eprintln!("[DEBUG] Queued confirm for msg_id={} (type=0x{:02x}), pending={}",
                             msg_id, type_byte, self.pending_confirms.len());
                }
            }

            // Still look for transfer size if we haven't found it
            if let Some(size) = self.extract_transfer_size_from_packet(&data, debug) {
                if let Some(block_count) = update_transfer_size(&mut transfer_size, &mut max_block, size) {
                    if let Some(max) = max_block {
                        if max > requested_max {
                            for block_id in next_request_block..=max {
                                let reliable = false;
                                let request = TransferBlockRequest::new(block_id, reliable);
                                let _ = self.transport.send_raw(&request.to_bytes()).await;
                            }
                            requested_max = max;
                            next_request_block = max.saturating_add(1);
                        }
                        all_requested = true;
                    }
                    let block_count = block_count as usize;
                    if blocks.len() < block_count {
                        blocks.resize(block_count, None);
                    }
                }
            }

            if msg_type == MessageType::TransferBlock as u8 {
                if data.len() < 5 {
                    continue;
                }
                let recv_block = u32::from_le_bytes([data[1], data[2], data[3], data[4]]);

                if !received_blocks.contains(&recv_block) {
                    if blocks.len() <= recv_block as usize {
                        blocks.resize(recv_block as usize + 1, None);
                    }
                    blocks[recv_block as usize] = Some(data[5..].to_vec());
                    received_blocks.insert(recv_block);
                    last_new_block_time = std::time::Instant::now();
                }

                if let Some(max) = max_block {
                    if received_blocks.len() as u32 >= max + 1 {
                        got_all_blocks = true;
                        continue;
                    }
                } else if data.len() < 500 {
                    max_block = Some(recv_block);
                    requested_max = requested_max.max(recv_block);
                    got_all_blocks = received_blocks.len() as u32 >= recv_block + 1;
                    continue;
                }

                // If we discover more blocks are needed (max_block updated), request them.
                if !all_requested {
                    if let Some(max) = max_block {
                        if max > requested_max {
                            for i in next_request_block..=max {
                                let reliable = false;
                                let request = TransferBlockRequest::new(i, reliable);
                                let _ = self.transport.send_raw(&request.to_bytes()).await;
                            }
                            requested_max = max;
                            next_request_block = max.saturating_add(1);
                        }
                        all_requested = true;
                    }
                }
            } else if msg_type == MessageType::ServerToClientHeartbeat as u8 {
                let _ = self.process_server_heartbeat(&data);
            }

            if last_resend.elapsed() > Duration::from_millis(200)
                && all_requested
                && last_new_block_time.elapsed() > Duration::from_millis(80)
            {
                let resend_max = max_block.unwrap_or(requested_max);
                let mut resent = 0;
                for block_id in 0..=resend_max {
                    let idx = block_id as usize;
                    let missing = blocks.get(idx).map_or(true, |b| b.is_none());
                    if missing {
                        let reliable = false;
                        let request = TransferBlockRequest::new(block_id, reliable);
                        let _ = self.transport.send_raw(&request.to_bytes()).await;
                        resent += 1;
                        if resent >= 100 {
                            break;
                        }
                    }
                }
                last_resend = std::time::Instant::now();
            }

            if transfer_size.is_none()
                && max_block.is_none()
                && !got_all_blocks
                && received_blocks.len() as u32 >= requested_max.saturating_sub(32)
                && last_expand.elapsed() > Duration::from_millis(50)
            {
                let new_max = requested_max.saturating_add(256);
                for block_id in next_request_block..=new_max {
                    let reliable = false;
                    let request = TransferBlockRequest::new(block_id, reliable);
                    let _ = self.transport.send_raw(&request.to_bytes()).await;
                }
                if debug {
                    eprintln!(
                        "[DEBUG] Expanding block requests: {} -> {}",
                        requested_max, new_max
                    );
                }
                requested_max = new_max;
                next_request_block = new_max.saturating_add(1);
                last_expand = std::time::Instant::now();
            }
        }

        // No trailing marker flush during download; we only send a final 0xfe once we
        // know the map is complete.

        // If we got some data, parse it and transition to InGame
        if !blocks.is_empty() {
            let max_index = max_block
                .or_else(|| blocks.len().checked_sub(1).map(|v| v as u32))
                .unwrap_or(0);
            let mut map_blob = Vec::with_capacity(((max_index + 1) as usize) * 503);
            let mut missing = 0;
            for i in 0..=max_index {
                match blocks.get(i as usize).and_then(|b| b.as_ref()) {
                    Some(chunk) => map_blob.extend_from_slice(chunk),
                    None => missing += 1,
                }
            }
            if missing > 0 {
                return Err(Error::InvalidPacket(format!("missing {} map blocks", missing)));
            }
            if let Some(size) = transfer_size {
                map_blob.truncate(size as usize);
            }
            self.map_data = map_blob;
            if debug {
                eprintln!(
                    "[DEBUG] download_map: assembled {} bytes in {:?}",
                    self.map_data.len(),
                    download_started.elapsed()
                );
            }

            if max_block.is_some() && last_progress != 0xfe {
                let _ = self.send_state_heartbeat(&[0x01, 0x09, 0xfe]).await;
                if let Ok(Some(data)) = self.transport.recv_raw_timeout(Duration::from_millis(8)).await {
                    if !data.is_empty()
                        && (data[0] & 0x1F) == MessageType::ServerToClientHeartbeat as u8
                    {
                        let _ = self.process_server_heartbeat(&data);
                    }
                }
                tokio::time::sleep(self.heartbeat_interval()).await;
                last_progress = 0xfe;
            }

            let skip_parse = !parse_map || std::env::var("FACTORIO_SKIP_MAP_PARSE").is_ok();
            if !skip_parse {
                // Try to parse entities from the map
                if let Ok(parsed) = parse_map_data(&self.map_data) {
                    if debug {
                        eprintln!(
                            "[DEBUG] download_map: parse_map_data ok in {:?}",
                            download_started.elapsed()
                        );
                    }
                    self.apply_parsed_map(parsed);
                }
            } else if debug {
                eprintln!("[DEBUG] download_map: skipping parse_map_data due to FACTORIO_SKIP_MAP_PARSE");
            }

            // Send state transition to signal we're ready for gameplay
            // The server expects specific state change signals before we can use gameplay heartbeats
            self.send_state_transition().await?;
            if debug {
                eprintln!(
                    "[DEBUG] download_map: sent state transition in {:?}",
                    download_started.elapsed()
                );
            }

            // Sync to the latest confirmed tick before starting gameplay heartbeats.
            self.sync_gameplay_clock().await;

            // Per doc lines 272-285, 541-542: after state trailers, send init action then gameplay heartbeats
            let _ = self.maybe_send_start_gameplay_heartbeat().await?;
            if debug {
                eprintln!(
                    "[DEBUG] download_map: sent start gameplay heartbeat in {:?}",
                    download_started.elapsed()
                );
            }

            // Always wait for player index - it's needed for any gameplay actions
            self.pending_init_action = true;
            self.await_player_index(Duration::from_millis(2000)).await;

            if std::env::var("FACTORIO_SKIP_INIT_ACTION").is_err() {
                let _ = self.maybe_send_start_gameplay_heartbeat().await?;
                
                // Retry init action until it succeeds (may need to wait for rate limiting)
                // The init action MUST be the first tick closure after 0x1e.
                // We also need to wait for ClientShouldStartSendingTickClosures (sets start_sending_tick).
                let init_start = std::time::Instant::now();
                while self.pending_init_action && init_start.elapsed() < Duration::from_millis(2000) {
                    let _ = self.maybe_send_init_action().await?;
                    if self.pending_init_action {
                        // Still pending - wait a bit and try again
                        tokio::time::sleep(self.heartbeat_interval()).await;
                        // Process any server heartbeats while waiting
                        // This is critical to receive ClientShouldStartSendingTickClosures
                        if let Ok(Some(data)) = self.transport.recv_raw_timeout(Duration::from_millis(10)).await {
                            if !data.is_empty() && (data[0] & 0x1F) == MessageType::ServerToClientHeartbeat as u8 {
                                let _ = self.process_server_heartbeat(&data);
                                let _ = self.flush_pending_confirmations().await;
                            }
                        }
                    }
                }

                // Wait for server responses after init
                for _ in 0..5 {
                    if let Ok(Some(data)) = self.transport.recv_raw_timeout(Duration::from_millis(10)).await {
                        if !data.is_empty() && (data[0] & 0x1F) == MessageType::ServerToClientHeartbeat as u8 {
                            let _ = self.process_server_heartbeat(&data);
                            let _ = self.flush_pending_confirmations().await;
                        }
                    }
                }
            }

            let _ = self.maybe_send_start_gameplay_heartbeat().await?;

            // Only NOW start sending gameplay heartbeats with flags=0x0e
            // The init action should already have consumed start_tick+1, so this will use start_tick+2
            for _ in 0..1 {
                let _ = self.send_heartbeat_raw().await;
                if let Ok(Some(data)) = self.transport.recv_raw_timeout(Duration::from_millis(8)).await {
                    if !data.is_empty() && (data[0] & 0x1F) == MessageType::ServerToClientHeartbeat as u8 {
                        let _ = self.process_server_heartbeat(&data);
                        let _ = self.flush_pending_confirmations().await;
                    }
                }
                tokio::time::sleep(Duration::from_millis(16)).await;
            }

            // Ensure allow_actions is true now that we're entering InGame.
            // Without this, we stay in the limited sending mode and fall behind.
            self.pending_init_action = false;
            self.allow_actions = true;

            // Fallback: if we didn't receive ClientShouldStartSendingTickClosures,
            // derive start_sending_tick from confirmed_tick. This ensures we can
            // send proper gameplay heartbeats.
            // Only use confirmed_tick if it's a reasonable value (> 10000)
            if self.start_sending_tick.is_none() && self.confirmed_tick > 10_000 {
                let start = self.confirmed_tick.wrapping_add(2);
                if std::env::var("FACTORIO_DEBUG").is_ok() {
                    eprintln!("[DEBUG] Fallback: setting start_sending_tick={} from confirmed_tick={}", start, self.confirmed_tick);
                }
                self.start_sending_tick = Some(start);
                self.client_tick = start;
            }

            // Allow immediate heartbeat send when entering InGame
            self.mark_needs_immediate_heartbeat();
            self.state = ConnectionState::InGame;
            if std::env::var("FACTORIO_DEBUG").is_ok() {
                eprintln!("[DEBUG] *** STATE TRANSITION TO INGAME ***");
            }
            return Ok(self.map_data.len());
        }

        Err(Error::ConnectionTimeout)
    }

    /// Send state transition heartbeats to signal ready for gameplay.
    /// Per doc lines 189-197, 533-537, these trailers must be sent in order:
    /// - `03 03 04 06 00 09 ff` - ClientChangedState(4) + MapLoadingProgressUpdate(0) + MapDownloadingProgressUpdate(255)
    /// - `01 06 40` - MapLoadingProgressUpdate(64)
    /// - `01 06 a4` - MapLoadingProgressUpdate(164)
    /// - `01 06 fe` - MapLoadingProgressUpdate(254)
    /// - `03 06 ff 03 05 03 06` - MapLoadingProgressUpdate(255) + ClientShouldStartSendingTickClosures + ClientChangedState(6)
    async fn send_state_transition(&mut self) -> Result<()> {
        let debug = std::env::var("FACTORIO_DEBUG").is_ok();

        // State transition trailers from official 2.0 pcap (order matters).
        // Per pcap: CompoundState(030304060009ff), PostDownload(0x40), PostDownload(0xa4),
        // PostDownload(0xfe), CompoundState(0306ff03050306)
        let trailers: &[&[u8]] = &[
            &[0x03, 0x03, 0x04, 0x06, 0x00, 0x09, 0xff],
            &[0x01, 0x06, 0x40],  // PostDownload(0x40) - was missing!
            &[0x01, 0x06, 0xa4],  // PostDownload(0xa4) - was 0xa8
            &[0x01, 0x06, 0xfe],
            &[0x03, 0x06, 0xff, 0x03, 0x05, 0x03, 0x06],
        ];

        for (i, trailer) in trailers.iter().enumerate() {
            self.send_state_heartbeat(trailer).await?;
            if debug {
                eprintln!("[DEBUG] Sent state trailer {}: {:02x?}", i + 1, trailer);
            }
            // Process server response between trailers
            if let Ok(Some(data)) = self.transport.recv_raw_timeout(Duration::from_millis(16)).await {
                if !data.is_empty() && (data[0] & 0x1F) == MessageType::ServerToClientHeartbeat as u8 {
                    let _ = self.process_server_heartbeat(&data);
                    let _ = self.flush_pending_confirmations().await;
                }
            }
            tokio::time::sleep(self.heartbeat_interval()).await;
        }

        Ok(())
    }

    async fn sync_gameplay_clock(&mut self) {
        let debug = std::env::var("FACTORIO_DEBUG").is_ok();
        let start = std::time::Instant::now();
        let mut last_heartbeat = std::time::Instant::now()
            .checked_sub(self.heartbeat_interval())
            .unwrap_or_else(std::time::Instant::now);

        // After state transition, send empty heartbeats (flags=0x00) to stay connected
        // Per pcap: after state 4 transition, client sends flags=0x00 heartbeats
        while start.elapsed() < Duration::from_millis(120) {
            match self.transport.recv_raw_timeout(Duration::from_millis(10)).await {
                Ok(Some(data)) if !data.is_empty() && (data[0] & 0x1F) == MessageType::ServerToClientHeartbeat as u8 => {
                    let _ = self.process_server_heartbeat(&data);
                    let _ = self.flush_pending_confirmations().await;
                }
                _ => {
                    tokio::task::yield_now().await;
                }
            }
            if self.start_sending_tick.is_some() {
                break;
            }
            // Send empty heartbeat (flags=0x00) to stay connected
            if last_heartbeat.elapsed() >= self.heartbeat_interval() {
                let _ = self.send_heartbeat_raw().await;
                last_heartbeat = std::time::Instant::now();
            }
        }

        // If start_sending_tick was set during heartbeat processing, use that.
        // Otherwise, use confirmed_tick + lead as a fallback.
        // IMPORTANT: Do NOT overwrite client_tick if it was already set by handle_start_sending_tick,
        // since that value is authoritative.
        if let Some(start_tick) = self.start_sending_tick {
            self.base_tick = start_tick;
            // Only update client_tick if it hasn't been set to start_tick already
            if self.client_tick != start_tick && self.client_tick != start_tick.wrapping_add(1) {
                self.client_tick = start_tick;
            }
        } else {
            // We need to start AHEAD of the server, not behind. Use confirmed_tick + lead
            // to ensure by the time our packets arrive, we're still ahead.
            // The lead of 30+ ticks accounts for network latency.
            // IMPORTANT: Use confirmed_tick (game tick), not client_seq_base (heartbeat sequence)!
            // Only set start_sending_tick if we have a valid GAME tick.
            // client_seq_base is the heartbeat sequence (~570k), not game tick (~37M).
            let lead = CLIENT_TICK_LEAD_INITIAL;
            let base = if self.confirmed_tick > 10_000 {
                self.confirmed_tick
            } else if self.server_tick > 10_000 {
                self.server_tick
            } else {
                // No valid game tick available - wait for S2C heartbeats to set confirmed_tick
                if debug {
                    eprintln!("[DEBUG] sync_gameplay_clock: waiting for game tick (confirmed={} server={})",
                        self.confirmed_tick, self.server_tick);
                }
                return;
            };
            let start_tick = base.wrapping_add(lead);
            self.start_sending_tick = Some(start_tick);
            self.base_tick = start_tick;
            self.client_tick = start_tick;
            // Note: We do NOT set pending_start_gameplay here because the state transitions
            // (including ClientChangedState(0x06)) were already sent in send_state_transition().
            self.allow_actions = true;
            if debug {
                eprintln!("[DEBUG] sync_gameplay_clock: fallback start_sending_tick={} from base={} (confirmed={} server={}) + lead={}", start_tick, base, self.confirmed_tick, self.server_tick, lead);
            }
        }
        self.game_start = None;

        if debug {
            eprintln!(
                "[DEBUG] State transition complete, base_tick={}, client_tick={}, start_tick={:?}",
                self.base_tick, self.client_tick, self.start_sending_tick
            );
        }

        // If the server already sent ClientShouldStartSendingTickClosures, don't stall here.
        // Delaying the first gameplay heartbeat burns through the latency window and triggers
        // "skipped tick closures" on the server.
        if self.start_sending_tick.is_some() {
            if debug {
                eprintln!("[DEBUG] Start tick already set; skipping extra sync delay");
            }
            return;
        }

        // CRITICAL: After setting game_start, we need to receive a fresh server tick
        // before sending gameplay heartbeats. Otherwise server_tick_echo is stale and
        // server rejects with "heartbeat outside latency window".
        // Send a sync heartbeat to trigger server response, then wait for tick closure.
        let tick_before = self.server_tick;
        let sync_start = std::time::Instant::now();
        while sync_start.elapsed() < Duration::from_millis(500) {
            // Send a sync heartbeat (not gameplay) to stay connected.
            let _ = self.send_initial_sync_heartbeat().await;

            // Try to receive heartbeat with tick closure
            for _ in 0..5 {
                if let Ok(Some(data)) = self.transport.recv_raw_timeout(Duration::from_millis(10)).await {
                    if !data.is_empty() && (data[0] & 0x1F) == MessageType::ServerToClientHeartbeat as u8 {
                        let _ = self.process_server_heartbeat(&data);
                        // Check if server_tick was updated
                        if self.server_tick != tick_before && self.server_tick != 0 {
                            if debug {
                                eprintln!("[DEBUG] Got fresh server_tick {} (was {})", self.server_tick, tick_before);
                            }
                            return;
                        }
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(16)).await;
        }
        if debug {
            eprintln!("[DEBUG] Warning: did not receive fresh server_tick, using {}", self.server_tick);
        }
    }

    async fn await_player_index(&mut self, timeout: Duration) {
        let start = std::time::Instant::now();
        let mut last_heartbeat = std::time::Instant::now()
            .checked_sub(self.heartbeat_interval())
            .unwrap_or_else(std::time::Instant::now);
        // While waiting for player index, we must NOT send tick closures because
        // the init action must be the FIRST tick closure. Only send raw heartbeats
        // to keep the connection alive.
        while !self.player_index_confirmed && start.elapsed() < timeout {
            if let Ok(Some(data)) = self.transport.recv_raw_timeout(Duration::from_millis(5)).await {
                if !data.is_empty() && (data[0] & 0x1F) == MessageType::ServerToClientHeartbeat as u8 {
                    let _ = self.process_server_heartbeat(&data);
                    let _ = self.flush_pending_confirmations().await;
                }
            }
            // Send raw heartbeats (no tick closures) to stay alive
            if last_heartbeat.elapsed() >= self.heartbeat_interval() {
                let _ = self.send_heartbeat_raw().await;
                last_heartbeat = std::time::Instant::now();
            }
            tokio::time::sleep(Duration::from_millis(4)).await;
        }
    }

    /// Flush pending skipped tick confirmations and latency confirms without sending tick closures.
    /// Used during connection setup when we can't send gameplay heartbeats yet.
    async fn flush_pending_confirmations(&mut self) -> Result<()> {
        // TODO: sync action packet format is not yet correct, causing "garbage after message" errors.
        // For now, just clear the queues without sending.
        self.pending_skipped_tick_confirms.clear();
        self.pending_latency_confirm = None;
        Ok(())
    }

    fn build_init_action(&self, _player_index: u16) -> Vec<u8> {
        // The UpdateBlueprintShelf (0x91) format was causing the server to stop responding.
        // For now, send an empty action (count=0) to establish the connection without issues.
        // This allows the player to connect and receive actions from others.
        vec![0x00] // countAndSegments = 0 (opt_u32 encoding of 0)
    }

    async fn send_start_gameplay_heartbeat(&mut self) -> Result<()> {
        // ClientChangedState(ReadyForGameplay) sent with flags=0x10 (HasSyncActions only).
        // PCAP shows state transitions use 0x10 WITHOUT tick closures.
        // Tick closures start with the init action (flags=0x06).
        //
        // Sync action format: [count][type][data] = [0x01][0x03][0x06]
        // - 0x01 = 1 sync action
        // - 0x03 = ClientChangedState
        // - 0x06 = InGame state (based on pcap analysis)
        let trailer = [0x01, 0x03, 0x06];
        let tick = self.start_sending_tick.unwrap_or(self.server_tick);

        let debug = std::env::var("FACTORIO_DEBUG").is_ok();
        if debug {
            eprintln!(
                "[DEBUG] send_start_gameplay_heartbeat: tick={} start_sending_tick={:?} server_tick={} client_tick_before={} using flags=0x10",
                tick, self.start_sending_tick, self.server_tick, self.client_tick
            );
        }

        // Initialize client_tick to start_tick for the first tick closure (init action).
        // Don't advance it here since 0x10 heartbeat doesn't consume a tick.
        self.client_tick = tick;

        // Use flags=0x10 (HasSyncActions only, no tick closures)
        self.send_ingame_heartbeat_with_payload_force_at(0x10, None, Some(&trailer), tick)
            .await
    }

    async fn maybe_send_start_gameplay_heartbeat(&mut self) -> Result<()> {
        let debug = std::env::var("FACTORIO_DEBUG").is_ok();
        if debug && self.pending_start_gameplay {
            eprintln!(
                "[DEBUG] maybe_send_start_gameplay: pending={} start_tick={:?} confirmed={} server={}",
                self.pending_start_gameplay,
                self.start_sending_tick,
                self.confirmed_tick,
                self.server_tick
            );
        }
        if self.pending_start_gameplay
            && self.start_sending_tick.is_some()
            && (self.confirmed_tick != 0 || self.server_tick != 0)
        {
            if debug {
                eprintln!("[DEBUG] maybe_send_start_gameplay_heartbeat: sending 0x1e at start_tick={}", 
                    self.start_sending_tick.unwrap_or(0));
            }
            self.send_start_gameplay_heartbeat().await?;
            // The 0x1e heartbeat uses start_tick and the internal function sets client_tick = start_tick + 1.
            // The next tick closure (init action) should use start_tick + 1.
            if debug {
                eprintln!("[DEBUG] maybe_send_start_gameplay_heartbeat: client_tick now={}", self.client_tick);
            }
            self.pending_start_gameplay = false;
        }
        Ok(())
    }

    async fn send_init_action(&mut self) -> Result<()> {
        if !self.player_index_confirmed {
            return Ok(());
        }
        let player_index = self
            .player_index
            .ok_or_else(|| Error::InvalidPacket("player index unknown".into()))?;
        // The first tick closure should be at start_tick (NOT start_tick + 1).
        // The 0x10 state change heartbeat doesn't consume a tick, so the init action
        // is the first actual tick closure and should use start_tick.
        let tick_override = self.start_sending_tick;
        let data = self.build_init_action(player_index);
        let debug = std::env::var("FACTORIO_DEBUG").is_ok();
        if debug {
            eprintln!(
                "[DEBUG] Init action: len={} bytes={:02x?} tick_override={:?}",
                data.len(),
                &data[..data.len().min(24)],
                tick_override
            );
        }
        // PCAP shows flags=0x06 for init action (HasTickClosures | SingleTickClosure)
        if let Some(tick) = tick_override {
            return self
                .send_ingame_heartbeat_with_payload_force_at(0x06, Some(&data), None, tick)
                .await;
        }
        self.send_ingame_heartbeat_with_payload_force(0x06, Some(&data), None)
            .await
    }

    /// Returns true if the init action was sent (caller should not send more ticks this flush)
    async fn maybe_send_init_action(&mut self) -> Result<bool> {
        self.maybe_send_start_gameplay_heartbeat().await?;
        let debug = std::env::var("FACTORIO_DEBUG").is_ok();
        if self.pending_init_action
            && self.player_index_confirmed
            && self.confirmed_tick != 0
            && self.start_sending_tick.is_some()
        {
            if !self.can_send_heartbeat() {
                if debug {
                    eprintln!("[DEBUG] maybe_send_init_action: can't send heartbeat, skipping");
                }
                return Ok(false);
            }
            // Send init action by default. Set FACTORIO_SKIP_INIT_ACTION=1 to disable.
            let skip_init = std::env::var("FACTORIO_SKIP_INIT_ACTION").is_ok();
            if !skip_init {
                if debug {
                    eprintln!("[DEBUG] maybe_send_init_action: sending init action");
                }
                self.send_init_action().await?;
            } else if let Some(start_tick) = self.start_sending_tick {
                // Per architecture doc: init tick (start_tick+1) should use flags 0x06 if sending
                // init action, otherwise we skip it and go directly to start_tick + 2.
                // The first normal 0x0e should be at start_tick + 2.
                let new_tick = start_tick.wrapping_add(2);
                // Only set client_tick if it's still at 0 or lower than new_tick
                // to avoid resetting backwards (which causes server disconnect)
                if self.client_tick == 0 || self.client_tick < new_tick {
                    if debug {
                        eprintln!("[DEBUG] maybe_send_init_action: advancing client_tick to {}", new_tick);
                    }
                    self.client_tick = new_tick;
                } else if debug {
                    eprintln!("[DEBUG] maybe_send_init_action: keeping client_tick={} (already past start_tick+2={})", self.client_tick, new_tick);
                }
            }
            self.pending_init_action = false;
            self.allow_actions = true;
            // Don't clear start_sending_tick - it's still needed for min-tick clamping.
            // The throttle will use max(confirmed + lead, start + lead) which is correct.
            return Ok(true); // Init action was sent, caller should stop
        } else if debug && self.pending_init_action {
            eprintln!(
                "[DEBUG] maybe_send_init_action: conditions not met - pending={} player_index_confirmed={} confirmed_tick={} start_sending_tick={:?}",
                self.pending_init_action, self.player_index_confirmed, self.confirmed_tick, self.start_sending_tick
            );
        }
        Ok(false)
    }

    /// Compute the next client tick for a gameplay packet.
    /// Returns None when we'd be too far ahead of the server; caller should skip sending.
    fn compute_client_tick(&mut self) -> Option<u32> {
        // ENFORCED SEQUENTIAL TICKS: Never jump client_tick forward.
        // In Factorio's lockstep protocol, we must send EVERY tick closure in sequence.
        // If we fall behind, we catch up by sending multiple ticks per flush, NOT by skipping.
        //
        // The only time we set client_tick non-sequentially is:
        // 1. Initial startup (client_tick == 0) - set to start_sending_tick or confirmed_tick
        // 2. Server explicitly tells us to skip a tick via SkippedTickClosure
        
        let server_echo = if self.confirmed_tick != 0 {
            self.confirmed_tick
        } else {
            self.server_tick
        };
        let lead = self.desired_tick_lead();
        
        // Initialize client_tick if needed
        if self.client_tick == 0 {
            // Use start_sending_tick if available, otherwise use server_echo
            let initial_tick = if let Some(start_tick) = self.start_sending_tick {
                start_tick
            } else {
                server_echo.max(1)
            };
            self.client_tick = initial_tick;
        }
        
        // NOTE: Removed throttle check. Factorio lockstep requires continuous tick closures.
        // If we stop sending when "too far ahead", the server stops responding, causing timeout.
        let _ = (server_echo, lead); // Silence unused warnings

        if std::env::var("FACTORIO_DEBUG_HB_FILE").is_ok()
            && self.debug_gameplay_heartbeats < 50
        {
            if let Ok(mut file) = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open("/tmp/factorio-client-tick.log")
            {
                use std::io::Write;
                let _ = writeln!(
                    file,
                    "client_tick_before={} server_echo={} start_tick={:?} lead={} confirmed={} server={}",
                    self.client_tick,
                    server_echo,
                    self.start_sending_tick,
                    self.client_tick_lead,
                    self.confirmed_tick,
                    self.server_tick
                );
            }
        }
        
        // Skip over any ticks that the server told us to skip via SkippedTickClosure.
        // We need to skip these ticks and not send them, otherwise server disconnects with "wrong tick closure".
        while self.pending_skipped_ticks.remove(&self.client_tick) {
            if std::env::var("FACTORIO_DEBUG_HB_FILE").is_ok() {
                if let Ok(mut file) = std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open("/tmp/factorio-client-tick.log")
                {
                    use std::io::Write;
                    let _ = writeln!(
                        file,
                        "SKIP_TICK_APPLIED: skipping tick {} (was in pending_skipped_ticks)",
                        self.client_tick
                    );
                }
            }
            self.client_tick = self.client_tick.wrapping_add(1);
            
            // Re-check latency window after skipping
            if server_echo != 0 {
                let max_allowed = server_echo.saturating_add(lead);
                if self.client_tick > max_allowed {
                    return None;
                }
            }
        }
        
        let tick = self.client_tick;
        self.client_tick = self.client_tick.wrapping_add(1);
        if std::env::var("FACTORIO_DEBUG_HB_FILE").is_ok()
            && self.debug_gameplay_heartbeats < 50
        {
            if let Ok(mut file) = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open("/tmp/factorio-client-tick.log")
            {
                use std::io::Write;
                let _ = writeln!(
                    file,
                    "client_tick_after={} tick_sent={}",
                    self.client_tick,
                    tick
                );
            }
            self.debug_gameplay_heartbeats += 1;
        }
        Some(tick)
    }

    fn can_send_tick_closure(&self) -> bool {
        // Always allow sending tick closures. Factorio lockstep requires continuous sending.
        // Throttling causes deadlock: if we stop sending, server stops responding.
        true
    }

    /// Compute the next heartbeat sequence number for C2S heartbeats.
    ///
    /// PCAP analysis shows the C2S heartbeat sequence is structured as:
    ///   [2-3] msg_id (u16 LE) - starts from initial_msg_id, increments each heartbeat
    ///   [4-5] peer_constant (u16 LE) - constant from ConnectionAcceptOrDeny
    ///
    /// Combined as u32: (peer_constant << 16) | msg_id
    fn compute_heartbeat_seq_u32(&mut self) -> u32 {
        let msg_id = self.next_msg_id();
        let seq = ((self.peer_constant as u32) << 16) | (msg_id as u32);

        if std::env::var("FACTORIO_DEBUG_SEQ").is_ok() {
            eprintln!("[DEBUG] compute_seq: msg_id={} peer_constant=0x{:04x} seq=0x{:08x}",
                msg_id, self.peer_constant, seq);
        }
        seq
    }

    fn next_msg_id(&mut self) -> u16 {
        let id = self.msg_id;
        self.msg_id = self.msg_id.wrapping_add(1);
        id
    }

    fn server_tick_echo(&self) -> u32 {
        // Echo tells the server: "I've processed your tick closures up to this tick".
        // This should be based on server_tick (from S2C tick closures), NOT confirmed_tick
        // (which tracks what the server confirmed it received from US).
        if self.server_tick != 0 {
            return self.server_tick;
        }
        0
    }

    fn heartbeat_echo_u64(&self) -> u64 {
        if self.server_tick != 0 {
            self.server_tick as u64
        } else {
            // When not yet synced with server, use all 1s (per pcap analysis)
            0xffffffffffffffff
        }
    }

    fn heartbeat_echo_u32(&self) -> u32 {
        self.server_tick_echo()
    }

    fn heartbeat_interval(&self) -> Duration {
        HEARTBEAT_INTERVAL
    }

    fn desired_tick_lead(&self) -> u32 {
        let mut lead = if let Some(latency) = self.latency_value {
            // Latency appears to be in milliseconds; convert to ~tick units (~16ms).
            ((latency as u32) + 15) / 16
        } else {
            self.client_tick_lead
        };
        if lead == 0 {
            lead = CLIENT_TICK_LEAD_INITIAL;
        }
        lead = lead.saturating_add(CLIENT_TICK_LEAD_BIAS.max(0) as u32);
        lead.clamp(CLIENT_TICK_LEAD_MIN, CLIENT_TICK_LEAD_MAX)
    }

    fn can_send_heartbeat(&self) -> bool {
        match self.last_gameplay_send_at {
            Some(last) => last.elapsed() >= self.heartbeat_interval(),
            None => true,
        }
    }

    fn mark_sent_heartbeat(&mut self) {
        self.last_gameplay_send_at = Some(std::time::Instant::now());
    }

    /// Mark that we need to send a heartbeat immediately (bypass rate limiting).
    /// This is called when we receive a server heartbeat and need to respond promptly.
    fn mark_needs_immediate_heartbeat(&mut self) {
        // Set last_gameplay_send_at to a time far in the past so can_send_heartbeat returns true
        self.last_gameplay_send_at = Some(
            std::time::Instant::now()
                .checked_sub(std::time::Duration::from_secs(1))
                .unwrap_or_else(std::time::Instant::now),
        );
    }

    /// Pre-game tick for 0x00/0x10 heartbeats.
    /// Use confirmed_tick or server_tick if available, otherwise u64::MAX during early sync.
    fn pre_game_tick_u64(&self) -> u64 {
        // Always use confirmed_tick if available (most authoritative)
        if self.confirmed_tick != 0 {
            return self.confirmed_tick as u64;
        }
        // Fall back to server_tick if available
        if self.server_tick != 0 {
            return self.server_tick as u64;
        }
        // Only use u64::MAX during very early sync before any tick data
        u64::MAX
    }

    async fn wait_pre_game_heartbeat_slot(&mut self) {
        if self.state == ConnectionState::InGame {
            return;
        }
        if let Some(last) = self.last_gameplay_send_at {
            let elapsed = last.elapsed();
            let interval = self.heartbeat_interval();
            if elapsed < interval {
                tokio::time::sleep(interval - elapsed).await;
            }
        }
    }

    /// Send a heartbeat with state data (sync actions).
    /// C2S pre-game heartbeat format (sync):
    /// [type][flags][heartbeat_sequence u32][tick u64][state_data...]
    async fn send_state_heartbeat_with_ticks(&mut self, state_data: &[u8], force_ff: bool) -> Result<()> {
        self.send_state_heartbeat_with_ticks_reliable(state_data, force_ff, None).await
    }
    
    /// Send a heartbeat with optional reliable flag override.
    /// If reliable is None, uses next_reliable(). If Some(true), forces reliable (0x26).
    /// If Some(false), forces unreliable (0x06).
    async fn send_state_heartbeat_with_ticks_reliable(&mut self, state_data: &[u8], force_ff: bool, reliable_override: Option<bool>) -> Result<()> {
        self.wait_pre_game_heartbeat_slot().await;
        if !self.can_send_heartbeat() {
            return Ok(());
        }
        let reliable = reliable_override.unwrap_or_else(|| self.next_reliable());
        let type_byte = if reliable { 0x26 } else { 0x06 };
        let seq = self.compute_heartbeat_seq_u32();
        let debug = std::env::var("FACTORIO_DEBUG_HB").is_ok();
        // Pre-game format: 8-byte tick (u64 LE). Use confirmed_tick or server_tick if available.
        let tick = if force_ff {
            u64::MAX
        } else if self.confirmed_tick != 0 {
            self.confirmed_tick as u64
        } else if self.server_tick != 0 {
            self.server_tick as u64
        } else if self.client_tick != 0 {
            self.pre_game_tick_u64()
        } else {
            u64::MAX
        };
        let mut packet = Vec::with_capacity(1 + 1 + 4 + 8 + state_data.len());
        packet.push(type_byte);
        packet.push(0x10); // has sync actions
        packet.extend_from_slice(&seq.to_le_bytes());
        packet.extend_from_slice(&tick.to_le_bytes());
        packet.extend_from_slice(state_data);

        if debug {
            eprintln!(
                "[DEBUG] C2S hb(state) flags=0x10 seq={} tick={} len={} hex={:02x?}",
                seq, tick, packet.len(), &packet[..packet.len().min(20)]
            );
        }
        self.pending_confirms.clear();
        self.last_gameplay_send_at = Some(std::time::Instant::now());
        self.mark_sent_heartbeat();
        self.transport.send_raw(&packet).await
    }

    async fn send_state_heartbeat(&mut self, state_data: &[u8]) -> Result<()> {
        self.send_state_heartbeat_with_ticks(state_data, false).await
    }

    async fn send_gameplay_sync_action(
        &mut self,
        action: SynchronizerActionType,
        build_payload: impl FnOnce(&mut BinaryWriter),
        force: bool,
    ) -> Result<()> {
        let mut writer = BinaryWriter::with_capacity(24);
        writer.write_opt_u32(1); // count
        writer.write_u8(action as u8);
        // Per RE: sync actions include player_index immediately after action_type (VarShort).
        let player_index = self.player_index.unwrap_or(0);
        writer.write_opt_u16(player_index);
        build_payload(&mut writer);
        let data = writer.into_vec();
        // Use 0x10 (HasSyncActions only) without tick closures
        if force {
            self.send_ingame_heartbeat_with_payload_force(0x10, None, Some(&data))
                .await
        } else {
            self.send_ingame_heartbeat_with_payload(0x10, None, Some(&data))
                .await
        }
    }

    /// Send IncreasedLatencyConfirm with the latency increase amount (not total).
    async fn send_latency_confirm(&mut self, increase: u8) -> Result<()> {
        let tick = self.server_tick as u64;
        self.send_gameplay_sync_action(
            SynchronizerActionType::IncreasedLatencyConfirm,
            |writer| {
                writer.write_u64_le(tick);
                writer.write_u8(increase);
            },
            true,
        )
        .await
    }

    async fn send_skipped_tick_confirm(&mut self, tick: u64) -> Result<()> {
        self.send_gameplay_sync_action(
            SynchronizerActionType::SkippedTickClosureConfirm,
            |writer| {
                writer.write_u64_le(tick);
            },
            true,
        )
        .await
    }

    /// Send the initial state heartbeat (state 01 03 02).
    async fn send_initial_state_heartbeat_with_ff(&mut self) -> Result<()> {
        // Force unreliable (0x06) for the first heartbeat to match official client behavior
        // IMPORTANT: Official client uses 0xff..ff tick in initial pre-game heartbeats
        let trailer = [0x01, 0x03, 0x02];
        self.send_state_heartbeat_with_ticks_reliable(&trailer, true, Some(false)).await
    }

    /// Send a plain sync heartbeat (no state data) during initial sync phase.
    async fn send_initial_sync_heartbeat(&mut self) -> Result<()> {
        self.wait_pre_game_heartbeat_slot().await;
        if !self.can_send_heartbeat() {
            return Ok(());
        }
        let reliable = self.next_reliable();
        let type_byte = if reliable { 0x26 } else { 0x06 };
        let debug = std::env::var("FACTORIO_DEBUG_HB").is_ok();
        let seq = self.compute_heartbeat_seq_u32();

        // Use 0xff only if we haven't received a tick from the server yet
        let tick = if self.confirmed_tick != 0 {
            self.confirmed_tick as u64
        } else if self.server_tick != 0 {
            self.server_tick as u64
        } else {
            u64::MAX
        };
        let mut packet = Vec::with_capacity(1 + 1 + 4 + 8);
        packet.push(type_byte);
        packet.push(0x00);
        packet.extend_from_slice(&seq.to_le_bytes());
        packet.extend_from_slice(&tick.to_le_bytes());

        if debug {
            eprintln!(
                "[DEBUG] C2S hb(sync) flags=0x00 seq={} tick=0x{:x} len={} hex={:02x?}",
                seq, tick, packet.len(), &packet
            );
        }
        self.pending_confirms.clear();
        self.last_gameplay_send_at = Some(std::time::Instant::now());
        self.mark_sent_heartbeat();
        self.transport.send_raw(&packet).await
    }

    /// Download map and return raw bytes (for analysis)
    pub async fn download_map_raw(&mut self) -> Result<Vec<u8>> {
        self.download_map().await?;
        Ok(self.map_data.clone())
    }

    fn extract_transfer_size_from_packet(&mut self, data: &[u8], debug: bool) -> Option<u32> {
        if let Some(size) = self.map_transfer_size.take() {
            return Some(size);
        }
        if data.len() < 16 {
            return None;
        }
        let type_byte = data[0];
        let msg_type = type_byte & 0x1F;
        if msg_type != MessageType::ServerToClientHeartbeat as u8 {
            return None;
        }
        if (type_byte & 0x40) == 0 {
            return None;
        }

        // Fragmented S2C heartbeat: transfer size lives at fixed offset 11-14 in fragment 0 (pcap).
        if let Ok((header, payload_start)) = PacketHeader::parse(data) {
            if header.fragment_id.unwrap_or(1) == 0 {
                let offset = payload_start + 7;
                if data.len() >= offset + 4 {
                    let transfer_size = u32::from_le_bytes([
                        data[offset],
                        data[offset + 1],
                        data[offset + 2],
                        data[offset + 3],
                    ]);
                    if transfer_size > 500_000 && transfer_size < 50_000_000 {
                        if debug {
                            let blocks = ((transfer_size as u64 + 502) / 503).max(1);
                            eprintln!(
                                "[DEBUG] MapReady(fragment0) transfer size: {} bytes ({} blocks)",
                                transfer_size, blocks
                            );
                        }
                        return Some(transfer_size);
                    }
                }
            }
        }

        self.parse_map_ready_fragment(data, debug)
            .or_else(|| self.scan_map_ready_for_download(data, debug))
    }

    fn scan_map_ready_for_download(&mut self, data: &[u8], debug: bool) -> Option<u32> {
        let (header, payload_start) = PacketHeader::parse(data).ok()?;
        if header.message_type != MessageType::ServerToClientHeartbeat {
            return None;
        }
        if payload_start >= data.len() {
            return None;
        }
        let payload = &data[payload_start..];
        let min_size = 500_000u64;
        let max_size = 50_000_000u64;
        let needed = 1 + 8 + 8 + 4 + 8;
        for i in 0..payload.len().saturating_sub(needed) {
            if payload[i] != SynchronizerActionType::MapReadyForDownload as u8 {
                continue;
            }
            let size = u64::from_le_bytes([
                payload[i + 1],
                payload[i + 2],
                payload[i + 3],
                payload[i + 4],
                payload[i + 5],
                payload[i + 6],
                payload[i + 7],
                payload[i + 8],
            ]);
            if size < min_size || size > max_size {
                continue;
            }
            let map_tick = u64::from_le_bytes([
                payload[i + 1 + 8 + 8 + 4],
                payload[i + 2 + 8 + 8 + 4],
                payload[i + 3 + 8 + 8 + 4],
                payload[i + 4 + 8 + 8 + 4],
                payload[i + 5 + 8 + 8 + 4],
                payload[i + 6 + 8 + 8 + 4],
                payload[i + 7 + 8 + 8 + 4],
                payload[i + 8 + 8 + 8 + 4],
            ]);
            if map_tick > 0 && map_tick <= u32::MAX as u64 {
                let tick_u32 = map_tick as u32;
                self.map_tick = Some(tick_u32);
                if self.client_tick == 0 {
                    self.client_tick = tick_u32;
                }
            }
            if debug {
                let blocks = ((size + 502) / 503).max(1);
                eprintln!(
                    "[DEBUG] MapReady scan transfer size: {} bytes ({} blocks)",
                    size, blocks
                );
            }
            return Some(size as u32);
        }
        None
    }

    fn parse_map_ready_fragment(&mut self, data: &[u8], debug: bool) -> Option<u32> {
        let (header, payload_start) = PacketHeader::parse(data).ok()?;
        if header.message_type != MessageType::ServerToClientHeartbeat {
            return None;
        }
        if header.fragment_id.unwrap_or(0) != 0 {
            return None;
        }
        let mut pos = payload_start;
        if data.len() <= pos {
            return None;
        }
        let flags = data[pos];
        pos += 1;
        if flags != 0x10 {
            return None;
        }
        // S2C heartbeat header after flags: heartbeat_sequence(u32)
        if data.len() < pos + 4 {
            return None;
        }
        pos += 4;

        if data.len() <= pos {
            return None;
        }

        let mut reader = BinaryReader::new(&data[pos..]);
        let count = match reader.read_opt_u32() {
            Ok(v) => v,
            Err(_) => return None,
        };
        if count == 0 {
            return None;
        }

        for _ in 0..count {
            let action_type = match reader.read_u8() {
                Ok(v) => v,
                Err(_) => return None,
            };
            if action_type == SynchronizerActionType::MapReadyForDownload as u8 {
                let transfer_size = match reader.read_u64_le() {
                    Ok(v) => v,
                    Err(_) => return None,
                };
                let _aux = reader.read_u64_le().ok()?;
                let _crc = reader.read_u32_le().ok()?;
                let map_tick = reader.read_u64_le().ok()?;
                if map_tick > 0 && map_tick <= u32::MAX as u64 {
                    let tick_u32 = map_tick as u32;
                    self.map_tick = Some(tick_u32);
                    if self.client_tick == 0 {
                        self.client_tick = tick_u32;
                    }
                }

                if transfer_size > 500_000 && transfer_size < 50_000_000 {
                    if debug {
                        let blocks = ((transfer_size + 502) / 503).max(1);
                        eprintln!(
                            "[DEBUG] MapReadyForDownload transfer size: {} bytes ({} blocks)",
                            transfer_size, blocks
                        );
                    }
                    return Some(transfer_size as u32);
                }
                return None;
            }

            let action = match SynchronizerActionType::from_u8(action_type) {
                Some(v) => v,
                None => return None,
            };
            if self
                .skip_sync_action_data(&mut reader, action, false, true)
                .is_err()
            {
                return None;
            }
            if reader.read_u16_le().is_err() {
                return None;
            }
        }
        None
    }

    /// Sync to game state after map download
    /// Transitions from Connected to InGame by using in-game heartbeat format
    #[allow(dead_code)]
    async fn sync_to_game(&mut self) -> Result<()> {
        if self.state != ConnectionState::Connected {
            return Ok(());
        }

        // Phase 3: Post-download sync
        // Switch to in-game heartbeat format and wait for server to recognize us
        let sync_start = std::time::Instant::now();
        let sync_duration = Duration::from_millis(500);

        while sync_start.elapsed() < sync_duration {
            // Process incoming packets
            match self.transport.recv_raw_timeout(Duration::from_millis(10)).await {
                Ok(Some(data)) if !data.is_empty() => {
                    let msg_type = data[0] & 0x1F;
                    if msg_type == MessageType::ServerToClientHeartbeat as u8 {
                        let _ = self.process_server_heartbeat(&data);
                    }
                }
                _ => {}
            }

            // Send in-game format heartbeat (22-byte)
            let _ = self.send_ingame_heartbeat().await;
            tokio::time::sleep(Duration::from_millis(16)).await;
        }

        self.state = ConnectionState::InGame;
        Ok(())
    }

    /// Send in-game format heartbeat using the Space Age gameplay wire format
    #[allow(dead_code)]
    async fn send_ingame_heartbeat(&mut self) -> Result<()> {
        self.send_heartbeat_raw().await
    }

    /// Send empty heartbeat in C2S heartbeat format.
    async fn send_heartbeat_raw(&mut self) -> Result<()> {
        let reliable = self.next_reliable();
        let type_byte = if reliable { 0x26 } else { 0x06 };
        let debug = std::env::var("FACTORIO_DEBUG_HB").is_ok();

        // Once the server has told us to start sending tick closures, never fall back to the
        // legacy 14-byte pre-game heartbeat format. If we're still waiting on the startup
        // handshake, just skip sending until the init action is ready.
        if self.start_sending_tick.is_some() && self.pending_start_gameplay {
            return Ok(());
        }

        // Only send gameplay heartbeats after the initial gameplay start sequence has completed.
        // We need either start_sending_tick (from ClientShouldStartSendingTickClosures) OR
        // we can use confirmed_tick as fallback if we're already InGame
        let can_gameplay = (self.start_sending_tick.is_some() || self.state == ConnectionState::InGame) 
            && (self.server_tick != 0 || self.confirmed_tick != 0);
        
        if debug {
            eprintln!("[DEBUG] send_heartbeat_raw: start_tick={:?} server_tick={} confirmed_tick={} state={:?} can_gameplay={}", 
                self.start_sending_tick, self.server_tick, self.confirmed_tick, self.state, can_gameplay);
        }

        if can_gameplay {
            if !self.can_send_heartbeat() {
                return Ok(());
            }
            // PCAP: regular empty tick closures use 0x0e (HasTickClosures + SingleMode + AllEmpty)
            return self.send_ingame_heartbeat_with_payload(0x0e, None, None).await;
        }

        self.wait_pre_game_heartbeat_slot().await;
        if !self.can_send_heartbeat() {
            return Ok(());
        }
        let seq = self.compute_heartbeat_seq_u32();
        let pre_game = self.start_sending_tick.is_none();
        let base = if pre_game {
            self.client_tick
        } else if self.confirmed_tick != 0 {
            self.confirmed_tick
        } else {
            self.server_tick
        };
        
        // Pre-game sync heartbeat format (Section 3.1 of heartbeat-architecture.md):
        // [type][flags][heartbeat_sequence u32][tick u64]
        let tick = if base == 0 { u64::MAX } else { self.pre_game_tick_u64() };

        let mut packet = Vec::with_capacity(1 + 1 + 4 + 8);
        packet.push(type_byte);
        packet.push(0x00); // flags
        packet.extend_from_slice(&seq.to_le_bytes());
        packet.extend_from_slice(&tick.to_le_bytes());

        if debug {
            eprintln!(
                "[DEBUG] C2S hb(raw) flags=0x00 seq={} tick={} len={}",
                seq, tick, packet.len()
            );
        }
        self.pending_confirms.clear();
        self.last_gameplay_send_at = Some(std::time::Instant::now());
        self.mark_sent_heartbeat();
        self.transport.send_raw(&packet).await
    }

    /// Send a heartbeat to keep connection alive (14-byte format)
    pub async fn send_heartbeat(&mut self) -> Result<()> {
        self.send_heartbeat_raw().await
    }

    /// Send heartbeat with action data (TickClosure payload only).
    /// C2S heartbeat format: flags + heartbeat_sequence(u32) + tick closures + echo_tick(u64).
    async fn send_action_packet(&mut self, flags: u8, action_data: &[u8]) -> Result<()> {
        if self.start_sending_tick.is_none() {
            return self.send_heartbeat_raw().await;
        }
        // Actions must be sent at the next sequential tick (client_tick).
        // Server expects tick closures in order - skipping ticks causes disconnect.
        // Empty ticks are capped at confirmed+lead, so actions at client_tick will
        // also be within that range.
        self.send_ingame_heartbeat_with_payload_force(flags, Some(action_data), None)
            .await
    }

    async fn send_ingame_heartbeat_with_payload(
        &mut self,
        flags: u8,
        payload: Option<&[u8]>,
        sync_actions: Option<&[u8]>,
    ) -> Result<()> {
        self.send_ingame_heartbeat_with_payload_internal(flags, payload, sync_actions, false, None)
            .await
    }

    async fn send_ingame_heartbeat_with_payload_force(
        &mut self,
        flags: u8,
        payload: Option<&[u8]>,
        sync_actions: Option<&[u8]>,
    ) -> Result<()> {
        self.send_ingame_heartbeat_with_payload_internal(flags, payload, sync_actions, true, None)
            .await
    }

    async fn send_ingame_heartbeat_with_payload_force_at(
        &mut self,
        flags: u8,
        payload: Option<&[u8]>,
        sync_actions: Option<&[u8]>,
        tick: u32,
    ) -> Result<()> {
        self.send_ingame_heartbeat_with_payload_internal(
            flags,
            payload,
            sync_actions,
            true,
            Some(tick),
        )
        .await
    }

    async fn send_ingame_heartbeat_with_payload_internal(
        &mut self,
        flags: u8,
        payload: Option<&[u8]>,
        sync_actions: Option<&[u8]>,
        force: bool,
        tick_override: Option<u32>,
    ) -> Result<()> {
        let debug = std::env::var("FACTORIO_DEBUG_FLUSH").is_ok();
        if !force && !self.can_send_heartbeat() {
            if debug {
                eprintln!("[DEBUG] send_ingame_hb: can_send_heartbeat=false, returning early");
            }
            return Ok(());
        }
        // PCAP analysis shows all C2S gameplay heartbeats should be unreliable.
        // Force unreliable for: state transitions (0x10), tick closures (HasTickClosures = 0x02)
        let has_tick_closures = (flags & 0x02) != 0;
        let force_unreliable = flags == 0x10 || has_tick_closures;
        let reliable = if force_unreliable { false } else { self.next_reliable() };
        let type_byte = if reliable { 0x26 } else { 0x06 };
        let seq = self.compute_heartbeat_seq_u32();
        let debug = std::env::var("FACTORIO_DEBUG_HB").is_ok();

        // C2S gameplay heartbeat format (Space Age 2.0, per r2 + official PCAP):
        // [flags][heartbeat_sequence u32]
        // [tick_closures?] (if flags & 0x02)
        // [echo_tick u64]  (always present; server tick echo / latency tracking)
        // [sync actions?] (if flags & 0x10)
        //
        // Tick closures:
        // - Single mode (flags & 0x04): [tick u64] + [action_data?]
        // - Multi mode  (flags & 0x02, no 0x04): [count opt_u32] + N * ([tick u64] + action_data?)
        // - all_empty (flags & 0x08): omit action_data
        //
        // If HasTickClosures (0x02) is set, this heartbeat consumes a tick closure and we
        // should update client_tick.
        let has_tick_closures = (flags & 0x02) != 0;
        let single = (flags & 0x04) != 0;
        let all_empty = (flags & 0x08) != 0;
        let mut tick = if let Some(tick) = tick_override {
            if has_tick_closures {
                self.client_tick = tick.wrapping_add(1);
            }
            tick
        } else {
            match self.compute_client_tick() {
                Some(tick) => tick,
                None => {
                    // Throttled: client_tick > server_echo + lead. Wait for confirmations.
                    if debug {
                        eprintln!("[DEBUG] send_ingame_hb: compute_client_tick=None, waiting for server");
                    }
                    return Ok(());
                }
            }
        };
        // When start_sending_tick is set, that's the minimum tick we should use.
        // The server expects tick closures starting at start_tick, so never go below it.
        // Only apply minimum clamping for HasTickClosures heartbeats.
        // Maximum clamping is now handled by compute_client_tick() returning None.
        if has_tick_closures {
            if let Some(start_tick) = self.start_sending_tick {
                if tick < start_tick {
                    tick = start_tick;
                    self.client_tick = tick.wrapping_add(1);
                }
            }
        }
        let payload_len = payload.map_or(0, |p| p.len());
        let extra_len = 4 + 8 + payload_len + sync_actions.map_or(0, |s| s.len());
        let mut packet = Vec::with_capacity(1 + 1 + extra_len);
        packet.push(type_byte);
        packet.push(flags);
        packet.extend_from_slice(&seq.to_le_bytes());

        if has_tick_closures {
            if single {
                packet.extend_from_slice(&(tick as u64).to_le_bytes());
                if !all_empty {
                    if let Some(action_data) = payload {
                        packet.extend_from_slice(action_data);
                    } else {
                        let mut writer = BinaryWriter::with_capacity(5);
                        writer.write_opt_u32(0);
                        packet.extend_from_slice(&writer.into_vec());
                    }
                }
            } else {
                let mut writer = BinaryWriter::with_capacity(8 + payload_len + 5);
                let count = if payload.is_some() { 1u32 } else { 0u32 };
                writer.write_opt_u32(count);
                if count > 0 {
                    writer.write_u64_le(tick as u64);
                    if !all_empty {
                        if let Some(action_data) = payload {
                            writer.write_bytes(action_data);
                        }
                    }
                }
                packet.extend_from_slice(&writer.into_vec());
            }
        }

        // echo_tick is ALWAYS present in InGame heartbeats (per PCAP analysis)
        let echo_tick = self.heartbeat_echo_u64();
        packet.extend_from_slice(&echo_tick.to_le_bytes());

        if (flags & 0x10) != 0 {
            if let Some(actions) = sync_actions {
                packet.extend_from_slice(actions);
            }
        }

        if debug {
            eprintln!(
                "[DEBUG] C2S hb(gameplay) flags=0x{:02x} seq={} tick={} echo_tick={} len={} hex={:02x?}",
                flags,
                seq,
                tick,
                echo_tick,
                packet.len(),
                &packet[..packet.len().min(32)]
            );
        }
        if std::env::var("FACTORIO_DEBUG_HB_FILE").is_ok() {
            let payload_len = payload.map_or(0, |p| p.len());
            let sync_len = sync_actions.map_or(0, |s| s.len());
            if let Ok(mut file) = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open("/tmp/factorio-client-hb.log")
            {
                use std::io::Write;
                    let _ = writeln!(
                        file,
                        "flags=0x{:02x} seq={} tick={} echo_tick={} payload={} sync={} len={} pending_start={} pending_init={} allow_actions={} confirmed_tick={} server_tick={} start_tick={:?} lead={}",
                        flags,
                        seq,
                        tick,
                        echo_tick,
                        payload_len,
                        sync_len,
                        packet.len(),
                        self.pending_start_gameplay,
                        self.pending_init_action,
                        self.allow_actions,
                        self.confirmed_tick,
                        self.server_tick,
                        self.start_sending_tick,
                        self.client_tick_lead
                    );
                }
            }
        self.pending_confirms.clear();
        self.last_gameplay_send_at = Some(std::time::Instant::now());
        self.mark_sent_heartbeat();
        self.transport.send_raw(&packet).await
    }

    async fn send_empty_action_tick(&mut self) -> Result<()> {
        // PCAP shows regular empty tick closures use 0x0e (HasTickClosures + SingleMode + AllEmpty)
        // The AllEmpty flag indicates no action data, so no action count is needed.
        self.send_ingame_heartbeat_with_payload(0x0e, None, None)
            .await
    }


    fn action_player_delta(player_index: u16) -> u16 {
        // For tick closures, player deltas are relative to the previous action.
        // The first action starts from 0xFFFF, so delta = player_index - 0xFFFF
        // which equals player_index + 1 using wrapping arithmetic.
        player_index.wrapping_add(1)
    }

    fn encode_codec_action_payload(action: &CodecInputAction, player_index: u16) -> Vec<u8> {
        let mut writer = BinaryWriter::with_capacity(32);
        let count_and_segments = 2u32; // count=1, hasSegments=0
        writer.write_opt_u32(count_and_segments);
        let player_delta = Self::action_player_delta(player_index);
        action.write_protocol_order(&mut writer, player_delta);
        let data = writer.into_vec();
        // Always log walk actions to file for debugging
        if let CodecInputAction::StartWalking { direction_x, direction_y } = action {
            if let Ok(mut file) = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open("/tmp/factorio-client-hb.log")
            {
                use std::io::Write;
                let _ = writeln!(
                    file,
                    "WALK_ACTION player_idx={} delta={} len={} dir=({:.6},{:.6}) bytes={:02x?}",
                    player_index,
                    player_delta,
                    data.len(),
                    direction_x,
                    direction_y,
                    &data[..data.len().min(32)]
                );
            }
        }
        data
    }

    async fn send_codec_action(&mut self, action: CodecInputAction) -> Result<()> {
        if self.state != ConnectionState::InGame {
            if std::env::var("FACTORIO_DEBUG").is_ok() {
                eprintln!("[DEBUG] send_codec_action: state={:?} (expected InGame)", self.state);
            }
            return Err(Error::InvalidPacket("must be in game".into()));
        }
        let player_index = self
            .player_index
            .ok_or_else(|| Error::InvalidPacket("player index unknown".into()))?;
        // Always log player_index to HB log for debugging
        if let Ok(mut file) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open("/tmp/factorio-client-hb.log")
        {
            use std::io::Write;
            let _ = writeln!(
                file,
                "ACTION_SEND player_index={} peer_id={:?} action={:?}",
                player_index, self.peer_id, action.action_type()
            );
        }
        let data = Self::encode_codec_action_payload(&action, player_index);
        if std::env::var("FACTORIO_DEBUG").is_ok() {
            eprintln!("[DEBUG] send_codec_action: queuing action player_index={} peer_id={:?}", player_index, self.peer_id);
        }
        self.send_heartbeat_with_actions(&[InputAction::raw(data)]).await
    }

    fn next_reliable(&mut self) -> bool {
        self.reliable_rng.next_bool()
    }

    /// Send a heartbeat with input actions
    pub async fn send_heartbeat_with_actions(&mut self, actions: &[InputAction]) -> Result<()> {
        if !actions.is_empty() {
            if std::env::var("FACTORIO_DEBUG_HB").is_ok() {
                eprintln!("[DEBUG] send_heartbeat_with_actions: queuing {} actions, pending_actions before={}", actions.len(), self.pending_actions.len());
            }
            for action in actions {
                self.pending_actions.push_back(action.clone());
            }
            if std::env::var("FACTORIO_DEBUG_HB").is_ok() {
                eprintln!("[DEBUG] send_heartbeat_with_actions: pending_actions after={}", self.pending_actions.len());
            }
        }
        Ok(())
    }

    async fn flush_gameplay(&mut self) -> Result<()> {
        let debug = std::env::var("FACTORIO_DEBUG_FLUSH").is_ok();
        if self.state != ConnectionState::InGame {
            if debug {
                eprintln!("[DEBUG] flush_gameplay: not InGame, returning");
            }
            return Ok(());
        }

        // Fallback: if we're in InGame and don't have start_sending_tick yet,
        // derive it from confirmed_tick. This handles cases where the server's
        // ClientShouldStartSendingTickClosures message wasn't parsed correctly.
        // Must run BEFORE throttle check so init can proceed.
        // Only use confirmed_tick if it's a reasonable value (> 10000)
        if self.start_sending_tick.is_none() && self.confirmed_tick > 10_000 {
            let start = self.confirmed_tick.wrapping_add(2);
            if debug {
                eprintln!("[DEBUG] flush_gameplay: setting start_sending_tick={} from confirmed_tick={}", start, self.confirmed_tick);
            }
            self.start_sending_tick = Some(start);
            self.client_tick = start;
            self.mark_needs_immediate_heartbeat();
        }

        // Always try to drive the start/init sequence first - BEFORE throttle check.
        // These MUST run to complete the player join handshake, even if we're throttled.
        let _ = self.maybe_send_start_gameplay_heartbeat().await?;
        // If init action was just sent, DON'T send any more ticks this flush
        // (the init action tick is the first tick closure)
        if self.maybe_send_init_action().await? {
            return Ok(());
        }

        let now = std::time::Instant::now();
        let elapsed_ms = self.last_gameplay_send_at.map(|t| now.duration_since(t).as_millis() as u64);
        let should_send = self
            .last_gameplay_send_at
            .map(|t| now.duration_since(t) >= self.heartbeat_interval())
            .unwrap_or(true);
        let has_urgent = !self.pending_skipped_tick_confirms.is_empty()
            || self.pending_latency_confirm.is_some();

        if !should_send && !has_urgent {
            if debug {
                eprintln!("[DEBUG] flush_gameplay: throttled (elapsed_ms={:?}), returning", elapsed_ms);
            }
            return Ok(());
        }
        if debug && should_send {
            eprintln!("[DEBUG] flush_gameplay: NOT throttled (elapsed_ms={:?}), will send", elapsed_ms);
        }

        // Never send gameplay packets before the start-gameplay heartbeat lands.
        if self.pending_start_gameplay {
            if debug {
                eprintln!("[DEBUG] flush_gameplay: pending_start_gameplay=true, returning early");
            }
            return Ok(());
        }

        // If we don't have a start_sending_tick yet, stay in pre-game heartbeat mode.
        if self.start_sending_tick.is_none() {
            let _ = self.send_heartbeat_raw().await?;
            return Ok(());
        }

        // Send sync action confirmations. Previously disabled due to "garbage after message" errors,
        // but not sending them causes server to drop us after 20-30 seconds.
        if let Some(increase) = self.pending_latency_confirm.take() {
            if debug {
                eprintln!("[DEBUG] flush_gameplay: sending latency confirm increase={}", increase);
            }
            let _ = self.send_latency_confirm(increase).await;
        }
        while let Some(tick) = self.pending_skipped_tick_confirms.pop_front() {
            if debug {
                eprintln!("[DEBUG] flush_gameplay: sending skipped tick confirm tick={}", tick);
            }
            let _ = self.send_skipped_tick_confirm(tick).await;
        }

        // While waiting on init action, DON'T send tick closures - the init action
        // must be the first tick closure. Only send raw heartbeats.
        if self.pending_init_action {
            if debug {
                eprintln!("[DEBUG] flush_gameplay: pending_init_action=true, sending raw HB only");
            }
            let _ = self.send_heartbeat_raw().await?;
            return Ok(());
        }

        // After init action but before allow_actions, send empty ticks to stay in sync
        if !self.allow_actions {
            if debug {
                eprintln!("[DEBUG] flush_gameplay: allow_actions=false");
            }
            if self.start_sending_tick.is_some() {
                let _ = self.send_empty_action_tick().await?;
            } else {
                let _ = self.send_heartbeat_raw().await?;
            }
            return Ok(());
        }

        // pending_init_action and !allow_actions are already handled above with early returns
        if self.pending_start_gameplay {
            let _ = self.send_heartbeat_raw().await?;
        } else if self.confirmed_tick == 0 {
            if self.start_sending_tick.is_some() {
                let _ = self.send_empty_action_tick().await?;
            } else {
                let _ = self.send_heartbeat_raw().await?;
            }
        } else if let Some(action) = self.pending_actions.pop_front() {
            if std::env::var("FACTORIO_DEBUG_HB").is_ok() {
                eprintln!("[DEBUG] flush_gameplay: popped action, pending_actions remaining={}", self.pending_actions.len());
            }
            let player_index = match self.player_index {
                Some(idx) => idx,
                None => {
                    self.pending_actions.push_front(action);
                    let _ = self.send_heartbeat_raw().await?;
                    return Ok(());
                }
            };
            let encoded = action.encode(&mut self.chat_seq, player_index)?;
            if std::env::var("FACTORIO_DEBUG_HB").is_ok() {
                eprintln!("[DEBUG] flush_gameplay: encoded action flags=0x{:02x} data_len={}", encoded.flags, encoded.data.len());
            }
            let _ = self.send_action_packet(encoded.flags, &encoded.data).await?;
        } else {
        // No pending actions: send empty tick closures.
        // We need to keep sending to maintain the connection, but cap drift to prevent
        // action ticks being too far in the future.
        if self.start_sending_tick.is_some() {
            let server_echo = if self.confirmed_tick != 0 {
                self.confirmed_tick
            } else {
                self.server_tick
            };
            if server_echo != 0 {
                // Cap empty ticks to exactly 1x lead ahead of confirmed.
                // This ensures that when an action comes in at confirmed+lead, it won't
                // be BEHIND empty ticks we already sent (which causes server to reject).
                let lead = self.desired_tick_lead();
                let target = server_echo.saturating_add(lead);
                let mut sent = 0u32;
                while self.client_tick <= target
                    && self.can_send_tick_closure()
                    && sent < MAX_CATCHUP_TICKS_PER_FLUSH
                {
                    let _ = self.send_empty_action_tick().await?;
                    sent += 1;
                }
                // No keepalive fallback - if we're at the limit, wait for server confirms.
                // This may cause brief pauses but prevents tick ordering issues.
            } else {
                // No server echo yet - send one tick to keep connection alive
                let _ = self.send_empty_action_tick().await?;
            }
        } else {
            // No start_sending_tick - send one tick to keep connection alive
            let _ = self.send_empty_action_tick().await?;
        }
        }
        Ok(())
    }

    /// Check if client is fully in game and can send actions
    pub fn is_in_game(&self) -> bool {
        self.state == ConnectionState::InGame
    }

    /// Receive raw packet data (for analysis)
    pub async fn recv_raw(&mut self) -> Result<Option<Vec<u8>>> {
        self.transport.recv_raw_timeout(Duration::from_millis(10)).await
    }

    /// Receive and process any pending packets (non-blocking)
    pub async fn poll(&mut self) -> Result<Option<ReceivedPacket>> {
        self.debug_poll_counter = self.debug_poll_counter.wrapping_add(1);
        let poll_id = self.debug_poll_counter;
        self.debug_current_poll_id = poll_id;
        let debug_poll = std::env::var("FACTORIO_DEBUG_POLL").is_ok();
        let mut result = None;
        if let Some(data) = self.transport.recv_raw_timeout(Duration::from_millis(1)).await? {
            if !data.is_empty() {
                let msg_type = data[0] & 0x1F;
                if debug_poll && self.state == ConnectionState::InGame {
                    eprintln!("[DEBUG] poll: received msg_type=0x{:02x} len={}", msg_type, data.len());
                }

                if msg_type == MessageType::Empty as u8 {
                    if let Ok((header, payload_start)) = PacketHeader::parse(&data) {
                        if payload_start + 4 <= data.len() {
                            let code = u32::from_le_bytes([
                                data[payload_start],
                                data[payload_start + 1],
                                data[payload_start + 2],
                                data[payload_start + 3],
                            ]);
                            eprintln!("[WARN] Server Empty message: msg_id={} code={}", header.message_id, code);
                        } else {
                            eprintln!("[WARN] Server Empty message: msg_id={} (no code)", header.message_id);
                        }
                    }
                    result = Some(ReceivedPacket::Unknown { msg_type, size: data.len() });
                } else if msg_type == MessageType::ServerToClientHeartbeat as u8 {
                    let _ = self.process_server_heartbeat(&data);
                    // Immediately respond to server heartbeat to stay in sync
                    if self.state == ConnectionState::InGame {
                        if std::env::var("FACTORIO_DEBUG_FLUSH").is_ok() {
                            eprintln!("[DEBUG] poll: received S2C HB in InGame, marking immediate");
                        }
                        self.mark_needs_immediate_heartbeat();
                    }
                    result = Some(ReceivedPacket::Heartbeat { tick: self.server_tick });
                } else if msg_type == MessageType::TransferBlock as u8 {
                    result = Some(ReceivedPacket::MapBlock { size: data.len() });
                } else {
                    result = Some(ReceivedPacket::Unknown { msg_type, size: data.len() });
                }
            }
        }

        if self.state == ConnectionState::InGame {
            let flush_start = std::time::Instant::now();
            let _ = self.flush_gameplay().await;
            let flush_elapsed = flush_start.elapsed().as_millis();
            if std::env::var("FACTORIO_DEBUG_TIMEOUT").is_ok() && flush_elapsed > 100 {
                eprintln!("[DEBUG] poll#{}: flush_gameplay took {}ms", poll_id, flush_elapsed);
            }
        }
        // Debug: check timestamp right before timeout check
        if std::env::var("FACTORIO_DEBUG_TIMEOUT").is_ok() && self.state == ConnectionState::InGame {
            let pre_elapsed = self.last_server_heartbeat_at.map(|t| t.elapsed().as_millis()).unwrap_or(99999);
            if pre_elapsed > 100 {
                eprintln!("[DEBUG] poll#{}: pre-timeout-check elapsed={}ms", poll_id, pre_elapsed);
            }
        }
        self.check_heartbeat_timeout();

        Ok(result)
    }

    fn process_server_heartbeat(&mut self, data: &[u8]) -> Result<()> {
        if data.is_empty() {
            return Ok(());
        }

        let debug = std::env::var("FACTORIO_DEBUG").is_ok();

        let type_byte = data[0];
        if (type_byte & 0x1F) != MessageType::ServerToClientHeartbeat as u8 {
            return Ok(());
        }

        // S2C heartbeats use the generic NetworkMessageHeader (variable length).
        // Parse it properly to get the correct payload offset.
        let (header, payload_start) = match PacketHeader::parse(data) {
            Ok((h, p)) => (h, p),
            Err(e) => {
                if debug {
                    eprintln!("[DEBUG] HB: failed to parse S2C header: {}", e);
                }
                return Ok(());
            }
        };

        // Reassemble fragmented heartbeats before parsing payload.
        if header.fragmented {
            if let Some(reassembled) = self.collect_fragmented_heartbeat(&header, data, payload_start) {
                return self.process_server_heartbeat(&reassembled);
            }
            return Ok(());
        }

        let hb_start = std::time::Instant::now();
        let now = std::time::Instant::now();
        let old_elapsed = self.last_server_heartbeat_at.map(|t| t.elapsed().as_millis());
        self.last_server_heartbeat_at = Some(now);
        // Verify the timestamp was actually set
        let verify_elapsed = self.last_server_heartbeat_at.map(|t| t.elapsed().as_millis()).unwrap_or(99999);
        if std::env::var("FACTORIO_DEBUG_TIMEOUT").is_ok() && self.state == ConnectionState::InGame {
            eprintln!("[DEBUG] poll#{}: process_server_heartbeat START (old_elapsed={:?}ms verify_elapsed={}ms)", self.debug_current_poll_id, old_elapsed, verify_elapsed);
        }
        let _hb_start = hb_start; // keep for later timing

        // Payload layout per binary RE (docs/heartbeat-architecture.md):
        // [0] flags
        // [1..5] heartbeat_sequence (u32 LE)
        // [5..] tick closures / sync actions / heartbeat requests
        if data.len() < payload_start + 5 {
            return Ok(());
        }

        let payload = &data[payload_start..];
        let flags = payload[0];
        let heartbeat_sequence = u32::from_le_bytes([payload[1], payload[2], payload[3], payload[4]]);
        
        // Store the server's heartbeat sequence for synchronization
        // The server's S2C sequence is what we should use as a reference for our C2S sequence
        self.server_seq = heartbeat_sequence;
        
        let has_tick_closures = (flags & 0x06) != 0;
        let all_tick_closures_empty = (flags & 0x08) != 0;
        let has_sync_actions = (flags & 0x10) != 0;

        // Start parsing after flags + heartbeat_sequence
        let mut pos = 5usize;

        let dump_s2c = std::env::var("FACTORIO_DEBUG_S2C_DUMP").is_ok();
        if debug && (self.debug_server_heartbeat_dumped < 6 || (dump_s2c && (flags & 0x06) != 0)) {
            eprintln!(
                "[DEBUG] S2C heartbeat: len={} flags=0x{:02x} seq={} first_bytes={:02x?}",
                data.len(),
                flags,
                heartbeat_sequence,
                &data[..data.len().min(20)]
            );
            if dump_s2c && (flags & 0x06) != 0 && self.debug_server_heartbeat_dumped < 20 {
                if let Ok(mut file) = std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open("/tmp/factorio-s2c-dump.log")
                {
                    use std::fmt::Write as _;
                    use std::io::Write as _;
                    let mut hex_line = String::new();
                    for b in data.iter().take(64) {
                        let _ = write!(hex_line, "{:02x}", b);
                    }
                    let _ = writeln!(
                        file,
                        "len={} flags=0x{:02x} seq={} head={}",
                        data.len(),
                        flags,
                        heartbeat_sequence,
                        hex_line
                    );
                }
            }
            self.debug_server_heartbeat_dumped += 1;
        }

        let mut tick_closure_ok = true;
        if has_tick_closures {
            let single = (flags & 0x04) != 0;
            // Per binary RE: tick closures are parsed via Heartbeat::loadBase logic
            // Single mode (0x04): one TickClosure directly
            // Multi mode (0x02): opt_u32 count, then count TickClosures
            // Each TickClosure starts with u64 tick (not u32 + padding)
            
            let mut reader = BinaryReader::new(&payload[pos..]);
            let parse_start = std::time::Instant::now();
            if let Some(closures) = self.parse_tick_closures_s2c(&mut reader, single, all_tick_closures_empty) {
                if std::env::var("FACTORIO_DEBUG_TIMEOUT").is_ok() && parse_start.elapsed().as_millis() > 100 {
                    eprintln!("[DEBUG] poll#{}: parse_tick_closures_s2c took {}ms", self.debug_current_poll_id, parse_start.elapsed().as_millis());
                }
                pos = pos.saturating_add(reader.position());
                
                // Update server_tick and confirmed_tick from the last closure's tick
                if let Some(last) = closures.last() {
                    let tick = (last.update_tick & 0xFFFFFFFF) as u32;
                    if tick > 0 {
                        self.update_server_tick(tick, debug, "s2c-heartbeat");
                        // Also update confirmed_tick to track server's progress
                        // This allows can_send_tick_closure to permit sending
                        self.update_confirmed_tick(tick, debug, "s2c-closure");
                    }
                }
                
                let exec_start = std::time::Instant::now();
                self.execute_tick_closures(closures);
                if std::env::var("FACTORIO_DEBUG_TIMEOUT").is_ok() && exec_start.elapsed().as_millis() > 100 {
                    eprintln!("[DEBUG] poll#{}: execute_tick_closures took {}ms", self.debug_current_poll_id, exec_start.elapsed().as_millis());
                }
            } else {
                tick_closure_ok = false;
                if debug {
                    eprintln!(
                        "[DEBUG] HB: failed to parse tick closures flags=0x{:02x}",
                        flags
                    );
                    if self.debug_tick_closure_failures < 5 {
                        let head = &payload[pos..payload.len().min(pos + 32)];
                        eprintln!("[DEBUG] HB: tick closure head={:02x?}", head);
                        self.debug_tick_closure_failures += 1;
                    }
                }
                if let Some(offset) = self.find_confirm_record_start(&payload[pos..]) {
                    pos = pos.saturating_add(offset);
                }
            }
        }

        // Parse confirm records deterministically after tick closures
        // Format: [0x02 or 0x03][0x52][flags][crc32 u32][confirmed_tick u32][padding u32]
        let confirm_start = std::time::Instant::now();
        let mut confirm_found = false;
        if has_tick_closures && pos + 15 <= payload.len() {
            // Try deterministic parsing first (exact offset from tick closure parsing)
            let mut reader = BinaryReader::new(&payload[pos..]);
            if let Some((tick, consumed)) = self.parse_confirm_records_deterministic(&mut reader) {
                self.update_confirmed_tick(tick, debug, "confirm-deterministic");
                confirm_found = true;
                pos = pos.saturating_add(consumed);
            } else {
                // Fallback to scanning only if deterministic parsing fails
                let confirm_slice = &payload[pos..];
                if let Some((tick, end)) = self.find_last_confirm_record(confirm_slice) {
                    self.update_confirmed_tick(tick, debug, "confirm-scan");
                    confirm_found = true;
                    let end = pos.saturating_add(end);
                    if end > pos {
                        pos = end;
                    }
                } else if let Some(tick) = self.scan_confirmed_tick(confirm_slice) {
                    self.update_confirmed_tick(tick, debug, "confirm-scan-fallback");
                    confirm_found = true;
                } else if debug && self.debug_confirm_failures < 5 {
                    let tail_start = payload.len().saturating_sub(48);
                    let tail = &payload[tail_start..];
                    eprintln!(
                        "[DEBUG] HB: no confirm record found, tail={:02x?}",
                        tail
                    );
                    self.debug_confirm_failures += 1;
                }
            }
        }
        // Disable the confirm-fallback entirely for now.
        // The confirm-scan should handle normal operation.
        // If we fall behind, we'll rely on the scan to catch up.
        // This preserves our tick lead which is critical for staying online.
        // (Previously this fallback was destroying the tick lead and causing disconnections.)
        if std::env::var("FACTORIO_DEBUG_TIMEOUT").is_ok() && confirm_start.elapsed().as_millis() > 100 {
            eprintln!("[DEBUG] poll#{}: confirm parsing took {}ms", self.debug_current_poll_id, confirm_start.elapsed().as_millis());
        }

        // Debug: timing check point A
        let check_a = std::time::Instant::now();

        while pos < payload.len() && payload[pos] == 0x00 {
            pos += 1;
        }

        // CRITICAL: Also scan for ClientShouldStartSendingTickClosures in remaining data,
        // regardless of flags. The server may send it without setting the 0x10 flag.
        // This is observed in pcap captures where flags=0x06 contains start tick data.
        if self.start_sending_tick.is_none() && pos < payload.len() {
            let remaining = &payload[pos..];
            if let Some(tick) = self.scan_start_tick(remaining) {
                if debug {
                    eprintln!("[DEBUG] HB: found start_tick {} in remaining data (flags=0x{:02x})", tick, flags);
                }
                self.handle_start_sending_tick(tick as u64);
            }
        }

        if pos < payload.len() && (flags & 0x10) != 0 {
            let extra = &payload[pos..];
            if debug {
                eprintln!(
                    "[DEBUG] HB extra: len={} head={:02x?}",
                    extra.len(),
                    &extra[..extra.len().min(24)]
                );
                if self.start_sending_tick.is_none() {
                    if let Some(idx) = extra.iter().position(|&b| b == SynchronizerActionType::ClientShouldStartSendingTickClosures as u8) {
                        let end = (idx + 12).min(extra.len());
                        eprintln!(
                            "[DEBUG] HB: extra contains start-tick marker at {} slice={:02x?}",
                            idx,
                            &extra[idx..end]
                        );
                    }
                }
            }
            let sync_start = std::time::Instant::now();
            let mut parsed = self.apply_sync_actions(extra);
            if std::env::var("FACTORIO_DEBUG_TIMEOUT").is_ok() && sync_start.elapsed().as_millis() > 100 {
                eprintln!("[DEBUG] poll#{}: apply_sync_actions took {}ms", self.debug_current_poll_id, sync_start.elapsed().as_millis());
            }
            if !parsed {
                let find_start = std::time::Instant::now();
                parsed = self.find_and_apply_sync_actions(extra);
                if std::env::var("FACTORIO_DEBUG_TIMEOUT").is_ok() && find_start.elapsed().as_millis() > 100 {
                    eprintln!("[DEBUG] poll#{}: find_and_apply_sync_actions(extra) took {}ms", self.debug_current_poll_id, find_start.elapsed().as_millis());
                }
            }
            if !parsed && !payload.is_empty() {
                let find_start = std::time::Instant::now();
                parsed = self.find_and_apply_sync_actions(payload);
                if std::env::var("FACTORIO_DEBUG_TIMEOUT").is_ok() && find_start.elapsed().as_millis() > 100 {
                    eprintln!("[DEBUG] poll#{}: find_and_apply_sync_actions(payload) took {}ms len={}", self.debug_current_poll_id, find_start.elapsed().as_millis(), payload.len());
                }
            }
            if self.start_sending_tick.is_none() {
                // Check if 0x04 byte exists anywhere in extra
                let has_04 = extra.iter().any(|&b| b == 0x04);
                if debug && has_04 {
                    eprintln!("[DEBUG] HB: extra contains 0x04 byte, len={} head={:02x?}", extra.len(), &extra[..extra.len().min(32)]);
                }
                if let Some(tick) = self.scan_start_tick(extra) {
                    self.handle_start_sending_tick(tick as u64);
                } else if debug && has_04 {
                    eprintln!("[DEBUG] HB: scan_start_tick failed despite 0x04 present");
                }
            }
            if !parsed {
                if debug {
                    eprintln!(
                        "[DEBUG] HB: sync action parse failed flags=0x{:02x} raw_head={:02x?}",
                        flags,
                        &payload[..payload.len().min(64)]
                    );
                }
                self.scan_latency_actions(extra);
            }
        }

        // Debug: timing check point B
        if std::env::var("FACTORIO_DEBUG_TIMEOUT").is_ok() && check_a.elapsed().as_millis() > 100 {
            eprintln!("[DEBUG] poll#{}: sync actions section took {}ms (flags=0x{:02x})", self.debug_current_poll_id, check_a.elapsed().as_millis(), flags);
        }

        // Heartbeat requests list (flag 0x01) is appended after the heartbeat body.
        if (flags & 0x01) != 0 && pos + 1 <= payload.len() {
            let mut reader = BinaryReader::new(&payload[pos..]);
            match reader.read_opt_u32() {
                Ok(count) => {
                    for _ in 0..count {
                        if reader.remaining_slice().len() < 4 {
                            break;
                        }
                        let _ = reader.read_u32_le();
                    }
                    pos = pos.saturating_add(reader.position());
                    if debug {
                        eprintln!("[DEBUG] HB: parsed {} heartbeat requests", count);
                    }
                }
                Err(_) => {
                    pos = pos.saturating_add(1);
                }
            }
        }

        let update_start = std::time::Instant::now();
        self.update_player_index_from_heartbeat(data);
        if std::env::var("FACTORIO_DEBUG_TIMEOUT").is_ok() && update_start.elapsed().as_millis() > 100 {
            eprintln!("[DEBUG] poll#{}: update_player_index_from_heartbeat took {}ms", self.debug_current_poll_id, update_start.elapsed().as_millis());
        }

        // Debug: time how long process_server_heartbeat took
        if std::env::var("FACTORIO_DEBUG_TIMEOUT").is_ok() && self.state == ConnectionState::InGame {
            let hb_elapsed = _hb_start.elapsed().as_millis();
            if hb_elapsed > 100 {
                eprintln!("[DEBUG] poll#{}: process_server_heartbeat END took {}ms", self.debug_current_poll_id, hb_elapsed);
            }
        }

        Ok(())
    }

    fn collect_fragmented_heartbeat(
        &mut self,
        header: &PacketHeader,
        data: &[u8],
        payload_start: usize,
    ) -> Option<Vec<u8>> {
        if header.has_confirmations {
            return None;
        }
        let fragment_id = header.fragment_id?;
        let msg_id = header.message_id;
        if payload_start >= data.len() {
            return None;
        }
        let payload = &data[payload_start..];

        // Drop stale assemblies.
        self.fragmented_heartbeats
            .retain(|_, asm| asm.created_at.elapsed() < Duration::from_secs(2));

        let (type_byte, reassembled) = {
            let entry = self.fragmented_heartbeats.entry(msg_id).or_insert_with(|| {
                FragmentAssembly {
                    type_byte: data[0],
                    fragments: BTreeMap::new(),
                    max_len: 0,
                    last_id: None,
                    created_at: std::time::Instant::now(),
                }
            });

            entry.type_byte = data[0];
            entry.fragments.insert(fragment_id, payload.to_vec());
            let len = payload.len();
            if len > entry.max_len {
                entry.max_len = len;
            } else if entry.max_len > 0 && len < entry.max_len {
                entry.last_id = Some(fragment_id);
            }

            let Some(last_id) = entry.last_id else {
                return None;
            };

            for id in 0..=last_id {
                if !entry.fragments.contains_key(&id) {
                    return None;
                }
            }

            let mut reassembled = Vec::new();
            for id in 0..=last_id {
                if let Some(part) = entry.fragments.get(&id) {
                    reassembled.extend_from_slice(part);
                }
            }

            (entry.type_byte, reassembled)
        };

        self.fragmented_heartbeats.remove(&msg_id);

        // Build a synthetic unfragmented packet: type byte + payload.
        let mut packet = Vec::with_capacity(1 + reassembled.len());
        packet.push(type_byte & !0x40);
        packet.extend_from_slice(&reassembled);
        Some(packet)
    }

    fn update_server_tick(&mut self, tick: u32, debug: bool, source: &str) {
        // Server tick tracks the latest tick closure tick from S2C heartbeats.
        if tick > 0 && tick != self.server_tick {
            // Note: client_seq_base is a heartbeat sequence (~570k), not a game tick.
            // Game ticks can be much higher (37M+ for servers running a while).
            // Only validate first server_tick is in plausible range.
            if self.server_tick == 0 {
                if tick < 1000 {
                    if debug {
                        eprintln!(
                            "[DEBUG] HB: IGNORING suspiciously low first server_tick {} ({})",
                            tick, source
                        );
                    }
                    return;
                }
            }

            // Sanity check: reject obviously wrong tick values
            // Normal server tick is in range ~1 to 100 million (for long-running servers)
            // Values > 1 billion are likely parsing errors (garbage data)
            if tick > 1_000_000_000 {
                if debug {
                    eprintln!(
                        "[DEBUG] HB: IGNORING invalid server_tick {} > 1B ({})",
                        tick, source
                    );
                }
                return;
            }
            
            // Sanity check: reject suspiciously small values that might be garbage
            // Server ticks < 1 million after we've seen higher values are likely parsing errors
            if self.server_tick > 1_000_000 && tick < 1_000_000 {
                if debug {
                    eprintln!(
                        "[DEBUG] HB: IGNORING suspicious server_tick {} < 1M (current={}, {})",
                        tick, self.server_tick, source
                    );
                }
                return;
            }
            
            // Sanity check: reject ticks that are too far from current
            // Normal server tick advance is ~1-4 per heartbeat (~60Hz)
            // If jump is > 1000 ticks (forward or backward), it's likely a parsing error
            if self.server_tick > 0 {
                let (diff, is_forward) = if tick > self.server_tick {
                    (tick - self.server_tick, true)
                } else {
                    (self.server_tick - tick, false)
                };
                // Reject large jumps in either direction
                // For backward jumps, also check if it's a genuine wrap-around (very rare)
                if diff > 1000 {
                    if is_forward || diff < 0x80000000 {
                        // Suspicious jump - likely parsing error
                        if debug {
                            eprintln!(
                                "[DEBUG] HB: IGNORING suspicious server_tick jump {} -> {} (diff={}, forward={}, {})",
                                self.server_tick, tick, diff, is_forward, source
                            );
                        }
                        return;
                    }
                    // If backward jump >= 0x80000000, it might be u32 wrap-around after ~2 years
                    // Allow it but log for debugging
                    if debug {
                        eprintln!(
                            "[DEBUG] HB: Allowing potential wrap-around {} -> {} (diff={}, {})",
                            self.server_tick, tick, diff, source
                        );
                    }
                }
            }
            if debug {
                eprintln!(
                    "[DEBUG] HB: server_tick {} -> {} ({})",
                    self.server_tick, tick, source
                );
            }
            self.server_tick = tick;
            if let Some(start_tick) = self.start_sending_tick {
                if start_tick > tick {
                    self.client_tick_lead = start_tick.saturating_sub(tick).max(1);
                }
            }
        }
    }

    fn update_confirmed_tick(&mut self, tick: u32, debug: bool, source: &str) {
        // Confirmed tick comes from tick confirmation records (can lag server_tick).
        // Note: client_seq_base is a heartbeat sequence (~570k), not a game tick.
        // Game ticks can be much higher (37M+ for servers running a while).
        // Only validate first confirmed_tick is in plausible range.
        if self.confirmed_tick == 0 {
            if tick < 1000 || tick >= 1_000_000_000 {
                if debug {
                    eprintln!(
                        "[DEBUG] HB: IGNORING confirmed_tick {} - out of plausible range ({})",
                        tick, source
                    );
                }
                return;
            }
        }
        // Sanity check: reject obviously wrong tick values
        if self.server_tick > 0 {
            let max_ahead = 60_000u32;
            if tick > self.server_tick.saturating_add(max_ahead) {
                // Sanity check: if tick is > 1M ahead, it's likely garbage data
                if tick > self.server_tick.saturating_add(1_000_000) {
                    if debug {
                        eprintln!(
                            "[DEBUG] HB: IGNORING confirmed_tick {} way ahead of server_tick {} (source={})",
                            tick, self.server_tick, source
                        );
                    }
                    return;
                }
                if debug {
                    eprintln!(
                        "[DEBUG] HB: confirmed_tick {} far ahead of server_tick {} (source={}), accepting",
                        tick, self.server_tick, source
                    );
                }
            }
        }
        if tick > 0 && tick != self.confirmed_tick {
            if debug {
                eprintln!(
                    "[DEBUG] HB: confirmed_tick {} -> {} ({})",
                    self.confirmed_tick, tick, source
                );
            }
            self.confirmed_tick = tick;
            self.tick_sync = (tick & 0xffff) as u16;
            // Only update server_tick from confirmed_tick if tick is reasonable
            // This prevents garbage confirmed_tick values from corrupting server_tick
            if tick <= 1_000_000_000 {
                if self.server_tick == 0 || self.server_tick < tick {
                    self.server_tick = tick;
                }
            } else if debug {
                eprintln!(
                    "[DEBUG] HB: NOT updating server_tick from invalid confirmed_tick {}",
                    tick
                );
            }
            if std::env::var("FACTORIO_DEBUG_HB_FILE").is_ok() {
                if let Ok(mut file) = std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open("/tmp/factorio-client-hb.log")
                {
                    use std::io::Write;
                    let _ = writeln!(
                        file,
                        "S2C_confirm tick={} source={} server_tick={}",
                        tick, source, self.server_tick
                    );
                }
            }
        }
    }

    fn scan_confirmed_tick(&self, data: &[u8]) -> Option<u32> {
        if data.len() < 15 {
            return None;
        }
        let mut last = None;
        for i in 0..=data.len().saturating_sub(15) {
            if (data[i] != 0x02 && data[i] != 0x03) || data[i + 1] != 0x52 {
                continue;
            }
            let flags = data[i + 2];
            if flags > 1 {
                continue;
            }
            let padding = u32::from_le_bytes([
                data[i + 11],
                data[i + 12],
                data[i + 13],
                data[i + 14],
            ]);
            if padding != 0 {
                continue;
            }
            let confirmed_tick = u32::from_le_bytes([
                data[i + 7],
                data[i + 8],
                data[i + 9],
                data[i + 10],
            ]);
            // Sanity check: tick should be reasonable (not too far ahead of server_tick)
            // and not randomly huge (which indicates false positive pattern match).
            // Allow up to 1000 ticks ahead of current server tick.
            const MAX_TICK_AHEAD: u32 = 1000;
            if confirmed_tick > 0 {
                if self.server_tick > 0 {
                    // Normal case: tick should be near server_tick
                    if confirmed_tick <= self.server_tick.saturating_add(MAX_TICK_AHEAD) {
                        last = Some(confirmed_tick);
                    }
                    // Else: false positive, ignore
                } else {
                    // Server tick not known yet, accept any reasonable tick (< 1 billion)
                    if confirmed_tick < 1_000_000_000 {
                        last = Some(confirmed_tick);
                    }
                }
            }
        }
        last
    }

    fn find_last_confirm_record(&self, data: &[u8]) -> Option<(u32, usize)> {
        if data.len() < 15 {
            return None;
        }
        let mut last_tick = None;
        let mut last_end = None;
        for i in 0..=data.len().saturating_sub(15) {
            if (data[i] != 0x02 && data[i] != 0x03) || data[i + 1] != 0x52 {
                continue;
            }
            let flags = data[i + 2];
            if flags > 1 {
                continue;
            }
            let confirmed_tick = u32::from_le_bytes([
                data[i + 7],
                data[i + 8],
                data[i + 9],
                data[i + 10],
            ]);
            if confirmed_tick == 0 {
                continue;
            }
            if self.server_tick > 0 {
                let max_delta = 200_000u32;
                let min_tick = self.server_tick.saturating_sub(max_delta);
                let max_tick = self.server_tick.saturating_add(max_delta);
                if confirmed_tick < min_tick || confirmed_tick > max_tick {
                    continue;
                }
            }
            last_tick = Some(confirmed_tick);
            last_end = Some(i + 15);
        }
        last_tick.zip(last_end)
    }

    fn parse_confirm_records(&self, data: &[u8]) -> Option<(u32, usize)> {
        if data.len() < 15 {
            return None;
        }
        let mut offset = 0usize;
        let mut last_tick = None;
        while offset + 15 <= data.len() {
            if (data[offset] != 0x02 && data[offset] != 0x03) || data[offset + 1] != 0x52 {
                break;
            }
            let flags = data[offset + 2];
            if flags > 1 {
                break;
            }
            let confirmed_tick = u32::from_le_bytes([
                data[offset + 7],
                data[offset + 8],
                data[offset + 9],
                data[offset + 10],
            ]);
            if confirmed_tick == 0 {
                break;
            }
            if self.server_tick > 0 {
                let max_delta = 200_000u32;
                let min_tick = self.server_tick.saturating_sub(max_delta);
                let max_tick = self.server_tick.saturating_add(max_delta);
                if confirmed_tick < min_tick || confirmed_tick > max_tick {
                    break;
                }
            }
            last_tick = Some(confirmed_tick);
            offset += 15;
        }
        last_tick.map(|tick| (tick, offset))
    }

    /// Deterministic confirm record parsing from a BinaryReader
    /// Returns (last_confirmed_tick, bytes_consumed) if successful
    fn parse_confirm_records_deterministic(&self, reader: &mut BinaryReader) -> Option<(u32, usize)> {
        let start_pos = reader.position();
        let mut last_tick = None;
        
        loop {
            // Check for confirm record pattern: [0x02 or 0x03][0x52]
            let remaining = reader.remaining_slice();
            if remaining.len() < 2 || (remaining[0] != 0x02 && remaining[0] != 0x03) || remaining[1] != 0x52 {
                break;
            }
            
            // Read flags
            let flags = match reader.read_u8() {
                Ok(v) => v,
                Err(_) => break,
            };
            // Skip the 0x52 marker (already validated above)
            if reader.read_u8().is_err() {
                break;
            }
            
            if flags > 1 {
                // Not a valid confirm record
                break;
            }
            
            // Read CRC32 (4 bytes)
            if reader.skip(4).is_err() {
                break;
            }
            
            // Read confirmed_tick
            let confirmed_tick = match reader.read_u32_le() {
                Ok(v) => v,
                Err(_) => break,
            };
            
            // Read padding (must be 0)
            let padding = match reader.read_u32_le() {
                Ok(v) => v,
                Err(_) => break,
            };
            
            if padding != 0 {
                break;
            }
            
            if confirmed_tick == 0 {
                break;
            }
            
            // Validate tick is in plausible range
            if self.server_tick > 0 {
                let max_delta = 200_000u32;
                let min_tick = self.server_tick.saturating_sub(max_delta);
                let max_tick = self.server_tick.saturating_add(max_delta);
                if confirmed_tick < min_tick || confirmed_tick > max_tick {
                    break;
                }
            }
            
            last_tick = Some(confirmed_tick);
        }
        
        last_tick.map(|tick| (tick, reader.position() - start_pos))
    }

    fn scan_start_tick(&self, data: &[u8]) -> Option<u32> {
        if data.len() < 6 {
            return None;
        }
        let base = if self.confirmed_tick != 0 {
            self.confirmed_tick
        } else if self.server_tick != 0 {
            self.server_tick
        } else {
            0
        };
        let debug = std::env::var("FACTORIO_DEBUG").is_ok();
        // ClientShouldStartSendingTickClosures tick can be far ahead of current confirmed_tick
        // The server tells us when to start sending, which could be many ticks in the future.
        let max_ahead = 500_000u32;

        // Format observed in Factorio 2.0: [0x04][tick:u64]
        // The tick is u64 directly after the type byte (no player_index prefix).
        for i in 0..=data.len().saturating_sub(9) {
            if data[i] != SynchronizerActionType::ClientShouldStartSendingTickClosures as u8 {
                continue;
            }

            // Try format: [type:u8][tick:u64] - tick directly after type
            if i + 9 <= data.len() {
                let off = i + 1;
                let tick64 = u64::from_le_bytes([
                    data[off], data[off + 1], data[off + 2], data[off + 3],
                    data[off + 4], data[off + 5], data[off + 6], data[off + 7],
                ]);
                // Tick should be reasonable for a game (> 1000, < 100M, fits in u32)
                if tick64 > 1000 && tick64 < 100_000_000 {
                    let tick = tick64 as u32;
                    // If we have a base, validate against it. Otherwise accept if tick looks valid.
                    let valid = if base != 0 {
                        tick >= base.saturating_sub(1000) && tick <= base.saturating_add(max_ahead)
                    } else {
                        true // Accept without base validation if tick looks reasonable
                    };
                    if valid {
                        if debug {
                            eprintln!("[DEBUG] scan_start_tick: found valid tick {} at data[{}] (u64 format, base={})", tick, i, base);
                        }
                        return Some(tick);
                    }
                }
            }

            // Fallback: Try with 1-byte player_index (value < 255), u32 tick
            if i + 6 <= data.len() && data.get(i + 1).map_or(false, |&b| b < 0xff) {
                let off = i + 2;
                if off + 4 <= data.len() {
                    let tick = u32::from_le_bytes([data[off], data[off + 1], data[off + 2], data[off + 3]]);
                    if tick != 0 && base != 0 && tick >= base.saturating_sub(1000) && tick <= base.saturating_add(max_ahead) {
                        if debug {
                            eprintln!("[DEBUG] scan_start_tick: found valid tick {} at data[{}] (1-byte player_index, base={})", tick, i, base);
                        }
                        return Some(tick);
                    }
                }
            }
            // Fallback: Try with 3-byte player_index (0xff + u16), u32 tick
            if i + 8 <= data.len() && data.get(i + 1) == Some(&0xff) {
                let off = i + 4;
                if off + 4 <= data.len() {
                    let tick = u32::from_le_bytes([data[off], data[off + 1], data[off + 2], data[off + 3]]);
                    if tick != 0 && base != 0 && tick >= base.saturating_sub(1000) && tick <= base.saturating_add(max_ahead) {
                        if debug {
                            eprintln!("[DEBUG] scan_start_tick: found valid tick {} at data[{}] (3-byte player_index, base={})", tick, i, base);
                        }
                        return Some(tick);
                    }
                }
            }
        }
        None
    }

    fn find_confirm_record_start(&self, data: &[u8]) -> Option<usize> {
        if data.len() < 15 {
            return None;
        }
        for i in 0..=data.len().saturating_sub(15) {
            if (data[i] != 0x02 && data[i] != 0x03) || data[i + 1] != 0x52 {
                continue;
            }
            let flags = data[i + 2];
            if flags > 1 {
                continue;
            }
            let confirmed_tick = u32::from_le_bytes([
                data[i + 7],
                data[i + 8],
                data[i + 9],
                data[i + 10],
            ]);
            if confirmed_tick == 0 {
                continue;
            }
            if self.server_tick > 0 {
                let max_delta = 200_000u32;
                let min_tick = self.server_tick.saturating_sub(max_delta);
                let max_tick = self.server_tick.saturating_add(max_delta);
                if confirmed_tick < min_tick || confirmed_tick > max_tick {
                    continue;
                }
            }
            return Some(i);
        }
        None
    }

    fn execute_tick_closures(&mut self, closures: Vec<TickClosureData>) {
        // Skip simulation if explicitly disabled
        if std::env::var("FACTORIO_SKIP_SIMULATION").is_ok() {
            return;
        }
        if let Some(sim) = self.simulation.as_mut() {
            for closure in &closures {
                let _ = sim.executor.execute_tick(&mut sim.world, closure);
            }
        }
    }

    fn parse_tick_closures(
        &mut self,
        reader: &mut BinaryReader,
        single: bool,
        all_empty: bool,
        base_tick: u32,
    ) -> Option<Vec<TickClosureData>> {
        let debug = std::env::var("FACTORIO_DEBUG").is_ok();

        let looks_like_confirm_record = |slice: &[u8]| -> bool {
            if slice.len() < 15 {
                return false;
            }
            if (slice[0] != 0x02 && slice[0] != 0x03) || slice[1] != 0x52 {
                return false;
            }
            if slice[2] > 1 {
                return false;
            }
            let padding = u32::from_le_bytes([slice[11], slice[12], slice[13], slice[14]]);
            padding == 0
        };

        let mut parse_once = |reader: &mut BinaryReader, single_mode: bool| -> Option<Vec<TickClosureData>> {
            let remaining = reader.remaining_slice();
            if looks_like_confirm_record(remaining) {
                return Some(Vec::new());
            }
            if all_empty && remaining.is_empty() {
                return Some(Vec::new());
            }
            let count = if single_mode {
                1
            } else {
                match reader.read_opt_u32() {
                    Ok(v) => v as usize,
                    Err(e) => {
                        if debug && self.debug_tick_closure_failures < 5 {
                            eprintln!("[DEBUG] HB: tick closure count read failed: {}", e);
                            self.debug_tick_closure_failures += 1;
                        }
                        return None;
                    }
                }
            };
            if count == 0 {
                return Some(Vec::new());
            }

            let mut closures = Vec::with_capacity(count);
            let mut current_player_index = self.last_action_player_index;
            for idx in 0..count {
                let mut tick_u32 = if single {
                    base_tick
                } else {
                    match reader.read_opt_u32() {
                        Ok(v) if v > 0 => v,
                        Ok(_) | Err(_) => {
                            let offset = (count - 1).saturating_sub(idx) as u32;
                            base_tick.saturating_sub(offset)
                        }
                    }
                };
                if tick_u32 == 0 {
                    let offset = (count - 1).saturating_sub(idx) as u32;
                    tick_u32 = base_tick.saturating_sub(offset);
                }

                let mut action_count = 0usize;
                let mut has_segments = false;
                if !all_empty {
                    // Some server heartbeats omit the empty action list entirely when
                    // there are no actions for the tick. If the next bytes look like
                    // a confirmation record, treat it as an empty closure and consume
                    // the confirmation records inline.
                    let remaining = reader.remaining_slice();
                    if !looks_like_confirm_record(remaining) {
                        let count_and_segments = match reader.read_opt_u32() {
                            Ok(v) => v,
                            Err(e) => {
                                if debug && self.debug_tick_closure_failures < 5 {
                                    eprintln!(
                                        "[DEBUG] HB: failed to read count_and_segments at pos={}: {}",
                                        reader.position(),
                                        e
                                    );
                                    self.debug_tick_closure_failures += 1;
                                }
                                return None;
                            }
                        };
                        action_count = (count_and_segments / 2) as usize;
                        has_segments = (count_and_segments & 1) != 0;
                    } else if let Some((tick, consumed)) = self.parse_confirm_records(remaining) {
                        self.update_confirmed_tick(tick, debug, "confirm-inline");
                        let _ = reader.skip(consumed);
                    }
                }

                if action_count > 8192 {
                    if debug && self.debug_tick_closure_failures < 5 {
                        eprintln!(
                            "[DEBUG] HB: action_count too large: {} (has_segments={})",
                            action_count, has_segments
                        );
                        self.debug_tick_closure_failures += 1;
                    }
                    return None;
                }

                let log_actions = (debug && self.debug_action_packets < 10)
                    || std::env::var("FACTORIO_DEBUG_ACTIONS").is_ok();
                if log_actions && (action_count > 0 || has_segments) {
                    eprintln!(
                        "[DEBUG] HB: tick_closure tick={} actions={} segments={}",
                        tick_u32, action_count, has_segments
                    );
                }
                let mut actions = Vec::with_capacity(action_count);
                for _ in 0..action_count {
                    let player_delta = match reader.read_opt_u16() {
                        Ok(v) => v,
                        Err(e) => {
                            if debug && self.debug_tick_closure_failures < 5 {
                                eprintln!(
                                    "[DEBUG] HB: failed to read player_delta at pos={}: {}",
                                    reader.position(),
                                    e
                                );
                                self.debug_tick_closure_failures += 1;
                            }
                            return None;
                        }
                    };
                    current_player_index = current_player_index.wrapping_add(player_delta);
                    // Action type can be > 255, read as varint (opt_u16)
                    let action_type = match reader.read_opt_u16() {
                        Ok(v) => v,
                        Err(e) => {
                            if debug && self.debug_tick_closure_failures < 5 {
                                eprintln!(
                                    "[DEBUG] HB: failed to read action_type at pos={}: {}",
                                    reader.position(),
                                    e
                                );
                                self.debug_tick_closure_failures += 1;
                            }
                            return None;
                        }
                    };

                    if log_actions && self.debug_action_packets < 30 {
                        eprintln!(
                            "[DEBUG] HB: action_type=0x{:02x} player_index={}",
                            action_type,
                            current_player_index
                        );
                        self.debug_action_packets += 1;
                    }

                    let remaining = reader.remaining_slice();
                    // Encode action_type as varint (u8 if < 255, else 0xFF + u16 LE)
                    let mut temp = Vec::with_capacity(3 + remaining.len());
                    if action_type < 255 {
                        temp.push(action_type as u8);
                    } else {
                        temp.push(0xFF);
                        temp.extend_from_slice(&action_type.to_le_bytes());
                    }
                    temp.extend_from_slice(remaining);
                    let mut temp_reader = BinaryReader::new(&temp);
                    let action = match CodecInputAction::read(&mut temp_reader) {
                        Ok(action) => action,
                        Err(e) => {
                            if debug {
                                eprintln!("[DEBUG] HB: failed to parse input action: {}", e);
                            }
                            return None;
                        }
                    };
                    // Calculate bytes consumed minus the action_type encoding (1 or 3 bytes)
                    let action_type_bytes = if action_type < 255 { 1 } else { 3 };
                    let consumed = temp_reader.position().saturating_sub(action_type_bytes);
                    if reader.skip(consumed).is_err() {
                        if debug {
                            eprintln!("[DEBUG] HB: failed to skip action payload");
                        }
                        return None;
                    }

                    self.apply_player_action(current_player_index, &action, Some(tick_u32));
                    actions.push(TickAction {
                        player_index: current_player_index,
                        action,
                    });
                }

                if has_segments {
                    let segment_count = match reader.read_opt_u32() {
                        Ok(v) => v as usize,
                        Err(e) => {
                            if debug && self.debug_tick_closure_failures < 5 {
                                eprintln!("[DEBUG] HB: failed to read segment_count: {}", e);
                                self.debug_tick_closure_failures += 1;
                            }
                            return None;
                        }
                    };
                    for _ in 0..segment_count {
                        let action_type = match reader.read_opt_u16() {
                            Ok(v) => v,
                            Err(e) => {
                                if debug && self.debug_tick_closure_failures < 5 {
                                    eprintln!("[DEBUG] HB: failed to read segment action_type: {}", e);
                                    self.debug_tick_closure_failures += 1;
                                }
                                return None;
                            }
                        };
                        let _seq = match reader.read_u32_le() {
                            Ok(v) => v,
                            Err(e) => {
                                if debug && self.debug_tick_closure_failures < 5 {
                                    eprintln!("[DEBUG] HB: failed to read segment seq: {}", e);
                                    self.debug_tick_closure_failures += 1;
                                }
                                return None;
                            }
                        };
                        let _player_index = match reader.read_opt_u16() {
                            Ok(v) => v,
                            Err(e) => {
                                if debug && self.debug_tick_closure_failures < 5 {
                                    eprintln!("[DEBUG] HB: failed to read segment player_index: {}", e);
                                    self.debug_tick_closure_failures += 1;
                                }
                                return None;
                            }
                        };
                        let _unknown_a = match reader.read_opt_u32() {
                            Ok(v) => v,
                            Err(e) => {
                                if debug && self.debug_tick_closure_failures < 5 {
                                    eprintln!("[DEBUG] HB: failed to read segment unknown_a: {}", e);
                                    self.debug_tick_closure_failures += 1;
                                }
                                return None;
                            }
                        };
                        let _unknown_b = match reader.read_opt_u32() {
                            Ok(v) => v,
                            Err(e) => {
                                if debug && self.debug_tick_closure_failures < 5 {
                                    eprintln!("[DEBUG] HB: failed to read segment unknown_b: {}", e);
                                    self.debug_tick_closure_failures += 1;
                                }
                                return None;
                            }
                        };
                        if reader.read_string().is_err() {
                            if debug {
                                eprintln!(
                                    "[DEBUG] HB: failed to read segment payload for action_type={}",
                                    action_type
                                );
                            }
                            return None;
                        }
                    }
                }

                if !all_empty {
                    let remaining = reader.remaining_slice();
                    if looks_like_confirm_record(remaining) {
                        if let Some((tick, consumed)) = self.parse_confirm_records(remaining) {
                            self.update_confirmed_tick(tick, debug, "confirm-inline");
                            let _ = reader.skip(consumed);
                        }
                    }
                }

                if self.walk_active {
                    let dx = (self.character_speed * self.walk_dir.0 * 256.0).trunc() / 256.0;
                    let dy = (self.character_speed * self.walk_dir.1 * 256.0).trunc() / 256.0;
                    self.player_x += dx;
                    self.player_y += dy;
                }
                self.walk_last_tick = self.server_tick;

                closures.push(TickClosureData {
                    update_tick: tick_u32,
                    input_actions: actions,
                });
            }

            self.last_action_player_index = current_player_index;
            Some(closures)
        };

        let start_pos = reader.position();
        if let Some(closures) = parse_once(reader, single) {
            return Some(closures);
        }
        reader.set_position(start_pos);
        let slice = reader.remaining_slice();
        let mut temp_reader = BinaryReader::new(slice);
        let retry_single = !single;
        let retry_multi = single;
        if retry_single {
            if let Some(closures) = parse_once(&mut temp_reader, true) {
                reader.set_position(start_pos + temp_reader.position());
                return Some(closures);
            }
        } else if retry_multi {
            if let Some(closures) = parse_once(&mut temp_reader, false) {
                reader.set_position(start_pos + temp_reader.position());
                return Some(closures);
            }
        }
        None
    }

    /// Parse S2C tick closures per binary RE format:
    /// - Single mode (flags & 0x04): one TickClosure directly
    /// - Multi mode (flags & 0x02): opt_u32 count, then count TickClosures
    /// - Each TickClosure starts with u64 tick (not u32 + padding)
    fn parse_tick_closures_s2c(
        &mut self,
        reader: &mut BinaryReader,
        single: bool,
        all_empty: bool,
    ) -> Option<Vec<TickClosureData>> {
        let debug = std::env::var("FACTORIO_DEBUG").is_ok();

        // Read count for multi-mode, or 1 for single-mode
        let count = if single {
            1usize
        } else {
            match reader.read_opt_u32() {
                Ok(v) => v as usize,
                Err(e) => {
                    if debug && self.debug_tick_closure_failures < 5 {
                        eprintln!("[DEBUG] HB: S2C tick closure count read failed: {}", e);
                        self.debug_tick_closure_failures += 1;
                    }
                    return None;
                }
            }
        };

        if count == 0 {
            return Some(Vec::new());
        }

        let mut closures = Vec::with_capacity(count);
        let mut current_player_index = self.last_action_player_index;

        for _ in 0..count {
            // S2C format: each closure starts with u64 tick
            let tick_u64 = match reader.read_u64_le() {
                Ok(v) => v,
                Err(e) => {
                    if debug && self.debug_tick_closure_failures < 5 {
                        eprintln!("[DEBUG] HB: failed to read u64 tick: {}", e);
                        self.debug_tick_closure_failures += 1;
                    }
                    return None;
                }
            };
            let tick_u32 = (tick_u64 & 0xFFFFFFFF) as u32;

            // Validate tick is in reasonable range (> 1000, < 1 billion)
            // Note: client_seq_base is a heartbeat sequence number, NOT a game tick.
            // Game ticks can be much higher (37M+ for servers running a while).
            // Only validate against confirmed_tick if we have one, to catch parsing errors.
            if tick_u32 < 1000 || tick_u32 >= 1_000_000_000 {
                if debug && self.debug_tick_closure_failures < 5 {
                    eprintln!("[DEBUG] HB: tick_closure tick {} out of range (1000..1B)", tick_u32);
                    self.debug_tick_closure_failures += 1;
                }
                return None;
            }
            // If we have confirmed_tick, validate proximity (within 100k ticks)
            if self.confirmed_tick > 10_000 {
                let diff = if tick_u32 > self.confirmed_tick {
                    tick_u32 - self.confirmed_tick
                } else {
                    self.confirmed_tick - tick_u32
                };
                if diff > 100_000 {
                    if debug && self.debug_tick_closure_failures < 5 {
                        eprintln!("[DEBUG] HB: tick_closure tick {} too far from confirmed_tick {} (diff={})",
                            tick_u32, self.confirmed_tick, diff);
                        self.debug_tick_closure_failures += 1;
                    }
                    return None;
                }
            }

            // Read action count and segments flag
            let mut action_count = 0usize;
            let mut has_segments = false;
            
            if !all_empty {
                let count_and_segments = match reader.read_opt_u32() {
                    Ok(v) => v,
                    Err(e) => {
                        if debug && self.debug_tick_closure_failures < 5 {
                            eprintln!("[DEBUG] HB: failed to read count_and_segments: {}", e);
                            self.debug_tick_closure_failures += 1;
                        }
                        return None;
                    }
                };
                action_count = (count_and_segments / 2) as usize;
                has_segments = (count_and_segments & 1) != 0;
            }

            if action_count > 8192 {
                if debug && self.debug_tick_closure_failures < 5 {
                    eprintln!(
                        "[DEBUG] HB: action_count too large: {}",
                        action_count
                    );
                    self.debug_tick_closure_failures += 1;
                }
                return None;
            }

            let log_actions = (debug && self.debug_action_packets < 10)
                || std::env::var("FACTORIO_DEBUG_ACTIONS").is_ok();
            if log_actions && action_count > 0 {
                eprintln!(
                    "[DEBUG] HB: S2C tick_closure tick={} actions={}",
                    tick_u32, action_count
                );
            }

            let mut actions = Vec::with_capacity(action_count);
            for _ in 0..action_count {
                let player_delta = match reader.read_opt_u16() {
                    Ok(v) => v,
                    Err(e) => {
                        if debug && self.debug_tick_closure_failures < 5 {
                            eprintln!(
                                "[DEBUG] HB: failed to read player_delta: {}",
                                e
                            );
                            self.debug_tick_closure_failures += 1;
                        }
                        return None;
                    }
                };
                current_player_index = current_player_index.wrapping_add(player_delta);
                
                // Action type can be > 255, so read as varint (opt_u16)
                let action_type = match reader.read_opt_u16() {
                    Ok(v) => v,
                    Err(e) => {
                        if debug && self.debug_tick_closure_failures < 5 {
                            eprintln!(
                                "[DEBUG] HB: failed to read action_type: {}",
                                e
                            );
                            self.debug_tick_closure_failures += 1;
                        }
                        return None;
                    }
                };

                let remaining = reader.remaining_slice();
                // For action types > 255, create a temp buffer with the type as varint
                let mut temp = Vec::with_capacity(3 + remaining.len());
                if action_type < 255 {
                    temp.push(action_type as u8);
                } else {
                    temp.push(0xFF);
                    temp.extend_from_slice(&action_type.to_le_bytes());
                }
                temp.extend_from_slice(remaining);
                let mut temp_reader = BinaryReader::new(&temp);
                let action = match crate::codec::InputAction::read(&mut temp_reader) {
                    Ok(action) => action,
                    Err(e) => {
                        if debug {
                            eprintln!("[DEBUG] HB: failed to parse input action type={}: {}", action_type, e);
                        }
                        return None;
                    }
                };
                // Calculate bytes consumed minus the action_type encoding (1 or 3 bytes)
                let action_type_bytes = if action_type < 255 { 1 } else { 3 };
                let consumed = temp_reader.position().saturating_sub(action_type_bytes);
                if reader.skip(consumed).is_err() {
                    if debug {
                        eprintln!("[DEBUG] HB: failed to skip action payload");
                    }
                    return None;
                }

                self.apply_player_action(current_player_index, &action, Some(tick_u32));
                actions.push(TickAction {
                    player_index: current_player_index,
                    action,
                });
            }

            if has_segments {
                // Skip segments for now - parse if needed
                let segment_count = match reader.read_opt_u32() {
                    Ok(v) => v as usize,
                    Err(_) => return None,
                };
                for _ in 0..segment_count {
                    let _action_type = reader.read_opt_u16().ok()?;
                    let _seq = reader.read_u32_le().ok()?;
                    let _player_index = reader.read_opt_u16().ok()?;
                    let _unknown_a = reader.read_opt_u32().ok()?;
                    let _unknown_b = reader.read_opt_u32().ok()?;
                    let _ = reader.read_string().ok()?;
                }
            }

            closures.push(TickClosureData {
                update_tick: tick_u32,
                input_actions: actions,
            });
        }

        self.last_action_player_index = current_player_index;
        Some(closures)
    }

    fn apply_sync_actions(&mut self, extra: &[u8]) -> bool {
        // Try parsing with player_index immediately after action_type (preferred),
        // then fall back to trailing player_index for older/edge cases.
        self.apply_sync_actions_with_layout(extra, true, true)
            || self.apply_sync_actions_with_layout(extra, false, true)
    }

    fn apply_sync_actions_with_layout(
        &mut self,
        extra: &[u8],
        player_index_first: bool,
        allow_countless: bool,
    ) -> bool {
        let debug = std::env::var("FACTORIO_DEBUG").is_ok();

        let mut parse_with_count = |count: Option<u32>| -> bool {
            let mut reader = BinaryReader::new(extra);
            if let Some(c) = count {
                // consume the count field
                if reader.read_opt_u32().ok() != Some(c) {
                    return false;
                }
                if c > 128 {
                    return false;
                }
            }
            let mut remaining = count.unwrap_or(u32::MAX);
            let mut actions_seen = 0u32;
            while remaining > 0 && !reader.is_empty() {
                let action_type = match reader.read_u8() {
                    Ok(v) => v,
                    Err(_) => return false,
                };
                let action = match SynchronizerActionType::from_u8(action_type) {
                    Some(v) => v,
                    None => return false,
                };
                if debug {
                    eprintln!("[DEBUG] HB: sync action type=0x{:02x} {:?}", action_type, action);
                }
                if let Err(e) =
                    self.skip_sync_action_data(&mut reader, action, true, player_index_first)
                {
                    if debug {
                        eprintln!(
                            "[DEBUG] HB: failed to parse sync action 0x{:02x}: {}",
                            action_type, e
                        );
                    }
                    return false;
                }
                if std::env::var("FACTORIO_DEBUG_HB_FILE").is_ok() {
                    if let Ok(mut file) = std::fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open("/tmp/factorio-client-sync.log")
                    {
                        use std::io::Write;
                        let _ = writeln!(file, "sync action=0x{:02x}", action_type);
                    }
                }
                actions_seen += 1;
                if let Some(c) = count {
                    if actions_seen >= c {
                        break;
                    }
                    remaining = c.saturating_sub(actions_seen);
                }
            }
            if let Some(c) = count {
                actions_seen == c
            } else {
                true
            }
        };

        let mut reader = BinaryReader::new(extra);
        let counted = reader.read_opt_u32().ok();
        if let Some(c) = counted {
            if parse_with_count(Some(c)) {
                return true;
            }
        }
        if allow_countless {
            return parse_with_count(None);
        }
        false
    }

    fn mark_disconnected(&mut self, reason: impl Into<String>) {
        let reason = reason.into();
        if self.state != ConnectionState::Disconnected {
            eprintln!("[conn] disconnected: {}", reason);
        }
        self.state = ConnectionState::Disconnected;
        self.last_disconnect_reason = Some(reason);
        self.start_sending_tick = None;
        self.allow_actions = false;
        self.pending_init_action = false;
        self.pending_start_gameplay = false;
        self.pending_actions.clear();
        self.pending_confirms.clear();
        self.pending_latency_confirm = None;
        self.client_seq = 0;
        self.client_seq_base = 0;
        self.server_seq_sample = None;
        self.server_seq_rate_hz = 45.0;
        self.last_sent_server_seq = 0;
        self.next_closure_tick = None;
    }

    fn check_heartbeat_timeout(&mut self) {
        if self.state != ConnectionState::InGame {
            return;
        }
        let poll_id = self.debug_current_poll_id;
        let Some(last) = self.last_server_heartbeat_at else {
            if std::env::var("FACTORIO_DEBUG_TIMEOUT").is_ok() {
                eprintln!("[DEBUG] poll#{}: check_heartbeat_timeout: last_server_heartbeat_at is None", poll_id);
            }
            return;
        };
        let elapsed = last.elapsed();
        if std::env::var("FACTORIO_DEBUG_TIMEOUT2").is_ok() && elapsed.as_millis() > 100 {
            eprintln!("[DEBUG] poll#{}: check_heartbeat_timeout: elapsed={}ms", poll_id, elapsed.as_millis());
        }
        if elapsed >= HEARTBEAT_TIMEOUT {
            eprintln!("[DEBUG] poll#{}: check_heartbeat_timeout: elapsed={}ms triggering timeout", poll_id, elapsed.as_millis());
            self.mark_disconnected(format!(
                "server heartbeat timeout ({}ms)",
                elapsed.as_millis()
            ));
        }
    }

    fn is_tick_plausible(&self, tick: u32) -> bool {
        let base = if self.server_tick > 0 {
            self.server_tick
        } else if self.confirmed_tick > 0 {
            self.confirmed_tick
        } else {
            return true;
        };
        let max_delta = 200_000u32;
        tick >= base.saturating_sub(max_delta) && tick <= base.saturating_add(max_delta)
    }

    fn can_parse_sync_actions(&mut self, extra: &[u8]) -> bool {
        self.can_parse_sync_actions_with_layout(extra, true, true)
            || self.can_parse_sync_actions_with_layout(extra, false, true)
    }

    fn can_parse_sync_actions_with_layout(
        &mut self,
        extra: &[u8],
        player_index_first: bool,
        allow_countless: bool,
    ) -> bool {
        let mut parse_with_count = |count: Option<u32>| -> bool {
            let mut reader = BinaryReader::new(extra);
            if let Some(c) = count {
                if reader.read_opt_u32().ok() != Some(c) {
                    return false;
                }
                if c > 128 {
                    return false;
                }
            }
            let mut remaining = count.unwrap_or(u32::MAX);
            let mut actions_seen = 0u32;
            while remaining > 0 && !reader.is_empty() {
                let action_type = match reader.read_u8() {
                    Ok(v) => v,
                    Err(_) => return false,
                };
                let action = match SynchronizerActionType::from_u8(action_type) {
                    Some(v) => v,
                    None => return false,
                };
                if self
                    .skip_sync_action_data(&mut reader, action, false, player_index_first)
                    .is_err()
                {
                    return false;
                }
                actions_seen += 1;
                if let Some(c) = count {
                    if actions_seen >= c {
                        break;
                    }
                    remaining = c.saturating_sub(actions_seen);
                }
            }
            if let Some(c) = count {
                actions_seen == c && reader.position() <= extra.len()
            } else {
                reader.position() == extra.len()
            }
        };

        let mut reader = BinaryReader::new(extra);
        let counted = reader.read_opt_u32().ok();
        if let Some(c) = counted {
            if parse_with_count(Some(c)) {
                return true;
            }
        }
        if allow_countless {
            return parse_with_count(None);
        }
        false
    }

    fn sync_actions_contains_type(
        &mut self,
        extra: &[u8],
        target: SynchronizerActionType,
    ) -> bool {
        self.sync_actions_contains_type_with_layout(extra, target, true, true)
            || self.sync_actions_contains_type_with_layout(extra, target, false, true)
    }

    fn sync_actions_contains_type_with_layout(
        &mut self,
        extra: &[u8],
        target: SynchronizerActionType,
        player_index_first: bool,
        allow_countless: bool,
    ) -> bool {
        let mut parse_with_count = |count: Option<u32>| -> Option<bool> {
            let mut reader = BinaryReader::new(extra);
            if let Some(c) = count {
                if reader.read_opt_u32().ok() != Some(c) {
                    return None;
                }
                if c > 128 {
                    return None;
                }
            }
            let mut remaining = count.unwrap_or(u32::MAX);
            let mut actions_seen = 0u32;
            while remaining > 0 && !reader.is_empty() {
                let action_type = match reader.read_u8() {
                    Ok(v) => v,
                    Err(_) => return None,
                };
                let action = match SynchronizerActionType::from_u8(action_type) {
                    Some(v) => v,
                    None => return None,
                };
                if self
                    .skip_sync_action_data(&mut reader, action, false, player_index_first)
                    .is_err()
                {
                    return None;
                }
                if action == target {
                    return Some(true);
                }
                actions_seen += 1;
                if let Some(c) = count {
                    if actions_seen >= c {
                        break;
                    }
                    remaining = c.saturating_sub(actions_seen);
                }
            }
            Some(false)
        };

        let mut reader = BinaryReader::new(extra);
        let counted = reader.read_opt_u32().ok();
        if let Some(c) = counted {
            if let Some(found) = parse_with_count(Some(c)) {
                if found {
                    return true;
                }
            }
        }
        if allow_countless {
            if let Some(found) = parse_with_count(None) {
                return found;
            }
        }
        false
    }

    fn find_and_apply_sync_actions(&mut self, extra: &[u8]) -> bool {
        if extra.len() < 4 {
            return false;
        }
        let debug = std::env::var("FACTORIO_DEBUG").is_ok();
        let mut best_any: Option<usize> = None;
        let mut best_with_start: Option<usize> = None;
        for start in 0..extra.len() {
            let slice = &extra[start..];
            if self.can_parse_sync_actions(slice) {
                best_any = Some(start);
                if self.start_sending_tick.is_none()
                    && self.sync_actions_contains_type(
                        slice,
                        SynchronizerActionType::ClientShouldStartSendingTickClosures,
                    )
                {
                    best_with_start = Some(start);
                }
            }
        }
        let chosen = best_with_start.or(best_any);
        if let Some(start) = chosen {
            let slice = &extra[start..];
            if self.apply_sync_actions(slice) {
                if debug && start != 0 {
                    eprintln!("[DEBUG] HB: sync action list found at offset {}", start);
                }
                return true;
            }
        }

        // Fallback: scan for ClientShouldStartSendingTickClosures (0x04) directly
        // This handles cases where other sync actions fail to parse but 0x04 is present
        if self.start_sending_tick.is_none() {
            if let Some(tick) = self.scan_for_start_sending_tick(extra) {
                if debug {
                    eprintln!("[DEBUG] HB: found start_sending_tick={} via scan fallback", tick);
                }
                self.handle_start_sending_tick(tick as u64);
                return true;
            }
        }

        false
    }

    /// Scan raw bytes for ClientShouldStartSendingTickClosures (0x04) and extract tick
    fn scan_for_start_sending_tick(&self, data: &[u8]) -> Option<u32> {
        // Format: [0x04][player_index:opt_u16][tick:u32]
        // player_index as opt_u16: if < 0xff, 1 byte; otherwise 3 bytes
        // Note: tick is u32, not u64 (observed in pcap)
        for i in 0..data.len().saturating_sub(5) {
            if data[i] == 0x04 {
                // Try with 1-byte player_index (value < 255), u32 tick
                if i + 1 + 4 <= data.len() && data[i + 1] < 0xff {
                    let tick_bytes = &data[i + 2..i + 6];
                    let tick = u32::from_le_bytes(tick_bytes.try_into().ok()?);
                    if self.is_scanned_tick_valid(tick) {
                        return Some(tick);
                    }
                }
                // Try with 3-byte player_index (0xff marker + u16), u32 tick
                if i + 3 + 4 <= data.len() && data[i + 1] == 0xff {
                    let tick_bytes = &data[i + 4..i + 8];
                    let tick = u32::from_le_bytes(tick_bytes.try_into().ok()?);
                    if self.is_scanned_tick_valid(tick) {
                        return Some(tick);
                    }
                }
            }
        }
        None
    }

    /// Validate a tick value found by scanning raw bytes
    fn is_scanned_tick_valid(&self, tick: u32) -> bool {
        // Absolute bounds: reasonable game tick (> 1000, < 100M)
        if tick <= 1000 || tick >= 100_000_000 {
            return false;
        }
        // If we have server_tick, validate proximity
        if self.server_tick > 0 {
            let diff = if tick > self.server_tick {
                tick - self.server_tick
            } else {
                self.server_tick - tick
            };
            return diff < 200_000;
        }
        // If we have confirmed_tick, validate proximity
        if self.confirmed_tick > 0 {
            let diff = if tick > self.confirmed_tick {
                tick - self.confirmed_tick
            } else {
                self.confirmed_tick - tick
            };
            return diff < 200_000;
        }
        // If we have initial_tick (client_seq_base), validate proximity
        // This is the server tick from the Accept packet
        if self.client_seq_base > 0 {
            let diff = if tick > self.client_seq_base {
                tick - self.client_seq_base
            } else {
                self.client_seq_base - tick
            };
            return diff < 200_000;
        }
        // No reference tick available - reject to avoid false positives
        false
    }

    fn skip_sync_action_data(
        &mut self,
        reader: &mut BinaryReader,
        action: SynchronizerActionType,
        apply: bool,
        player_index_first: bool,
    ) -> Result<()> {
        // Per binary RE: each sync action includes a player_index. Most traces put it
        // immediately after action_type (VarShort). Some legacy paths appear trailing.
        if player_index_first {
            let _player_index = reader.read_opt_u16()?;
        }
        match action {
            SynchronizerActionType::PeerDisconnect => {
                let _ = reader.read_u8()?;
            }
            SynchronizerActionType::NewPeerInfo => {
                let _ = reader.read_string()?;
            }
            SynchronizerActionType::ClientChangedState => {
                let _ = reader.read_u8()?;
            }
            SynchronizerActionType::ClientShouldStartSendingTickClosures => {
                // Format: [action_type:u8][player_index:opt_u16][tick:u32] when player_index_first=true
                // Format: [action_type:u8][tick:u32][player_index:opt_u16] when player_index_first=false
                // Note: tick is u32, not u64 (observed in pcap analysis)
                let tick_u32 = reader.read_u32_le()?;

                // Read player_index only if it wasn't already read at the start
                if !player_index_first {
                    let _player_index = reader.read_opt_u16()?;
                }

                // Validate tick is in plausible range to avoid false positives from CRC data
                if !self.is_tick_plausible(tick_u32) {
                    return Err(Error::InvalidPacket(format!(
                        "start tick {} not plausible (server_tick={}, confirmed_tick={})",
                        tick_u32, self.server_tick, self.confirmed_tick
                    )));
                }

                // Always apply server's start tick - it overrides any fallback we might have set
                if apply {
                    self.handle_start_sending_tick(tick_u32 as u64);
                }
            }
            SynchronizerActionType::MapReadyForDownload => {
                self.skip_map_ready_for_download(reader)?;
            }
            SynchronizerActionType::MapLoadingProgressUpdate
            | SynchronizerActionType::MapSavingProgressUpdate
            | SynchronizerActionType::MapDownloadingProgressUpdate
            | SynchronizerActionType::CatchingUpProgressUpdate
            | SynchronizerActionType::PeerDroppingProgressUpdate => {
                let _ = reader.read_u8()?;
            }
            SynchronizerActionType::SavingForUpdate
            | SynchronizerActionType::PlayerDesynced
            | SynchronizerActionType::BeginPause
            | SynchronizerActionType::EndPause
            | SynchronizerActionType::GameEnd => {}
            SynchronizerActionType::SkippedTickClosure => {
                let tick = reader.read_u64_le()?;
                if apply {
                    let tick_u32 = (tick & 0xffff_ffff) as u32;
                    if !self.is_tick_plausible(tick_u32) {
                        if std::env::var("FACTORIO_DEBUG").is_ok() {
                            eprintln!(
                                "[DEBUG] HB: ignoring SkippedTickClosure tick={} (server_tick={}, confirmed_tick={})",
                                tick_u32, self.server_tick, self.confirmed_tick
                            );
                        }
                        // Consume player_index before returning
                        if !player_index_first {
                            let _ = reader.read_opt_u16()?;
                        }
                        return Ok(());
                    }
                    if std::env::var("FACTORIO_DEBUG").is_ok() {
                        eprintln!(
                            "[DEBUG] HB: SkippedTickClosure tick={} (client_tick={}, confirmed_tick={}, server_tick={})",
                            tick_u32, self.client_tick, self.confirmed_tick, self.server_tick
                        );
                    }
                    if std::env::var("FACTORIO_DEBUG_HB_FILE").is_ok() {
                        if let Ok(mut file) = std::fs::OpenOptions::new()
                            .create(true)
                            .append(true)
                            .open("/tmp/factorio-client-sync.log")
                        {
                            use std::io::Write;
                            let _ = writeln!(
                                file,
                                "sync skipped_tick={} client_tick={} confirmed={} server={}",
                                tick_u32, self.client_tick, self.confirmed_tick, self.server_tick
                            );
                        }
                    }
                    // SkippedTickClosure means the server wants to skip tick X.
                    // If we've already sent tick closures past tick_u32, we should NOT
                    // confirm - the server will reject it as "too late".
                    // We should only update client_tick if we're exactly at tick_u32 (about to send it).
                    let already_sent = self.client_tick > tick_u32;
                    let about_to_send = self.client_tick == tick_u32;
                    
                    if about_to_send {
                        // We're exactly at the skipped tick - skip over it.
                        let old_tick = self.client_tick;
                        self.client_tick = tick_u32.saturating_add(1);
                        if std::env::var("FACTORIO_DEBUG_HB_FILE").is_ok() {
                            if let Ok(mut file) = std::fs::OpenOptions::new()
                                .create(true)
                                .append(true)
                                .open("/tmp/factorio-client-tick.log")
                            {
                                use std::io::Write;
                                let _ = writeln!(
                                    file,
                                    "SKIPPED_TICK: client_tick {} -> {} (skipped={})",
                                    old_tick, self.client_tick, tick_u32
                                );
                            }
                        }
                        // Queue confirmation for the tick we skipped
                        if self
                            .pending_skipped_tick_confirms
                            .back()
                            .copied()
                            != Some(tick)
                        {
                            self.pending_skipped_tick_confirms.push_back(tick);
                        }
                    } else if already_sent {
                        // We've already sent past this tick - don't confirm, it would be too late.
                        if std::env::var("FACTORIO_DEBUG_HB_FILE").is_ok() {
                            if let Ok(mut file) = std::fs::OpenOptions::new()
                                .create(true)
                                .append(true)
                                .open("/tmp/factorio-client-tick.log")
                            {
                                use std::io::Write;
                                let _ = writeln!(
                                    file,
                                    "SKIPPED_TICK_IGNORED: already_sent client_tick={} > skipped={}",
                                    self.client_tick, tick_u32
                                );
                            }
                        }
                    } else {
                        // We're behind the skipped tick (client_tick < tick_u32).
                        // The server will skip this tick, but we still need to send the ticks before it.
                        // Track this tick so we skip it when we reach it in compute_client_tick.
                        if std::env::var("FACTORIO_DEBUG_HB_FILE").is_ok() {
                            if let Ok(mut file) = std::fs::OpenOptions::new()
                                .create(true)
                                .append(true)
                                .open("/tmp/factorio-client-tick.log")
                            {
                                use std::io::Write;
                                let _ = writeln!(
                                    file,
                                    "SKIPPED_TICK_QUEUED: client_tick={} < skipped={} (will skip when we reach it)",
                                    self.client_tick, tick_u32
                                );
                            }
                        }
                        // Track this tick to skip when we reach it
                        self.pending_skipped_ticks.insert(tick_u32);
                        // Queue the confirmation for later (when we actually skip)
                        if self
                            .pending_skipped_tick_confirms
                            .back()
                            .copied()
                            != Some(tick)
                        {
                            self.pending_skipped_tick_confirms.push_back(tick);
                        }
                    }
                }
            }
            SynchronizerActionType::SkippedTickClosureConfirm => {
                let _ = reader.read_u64_le()?;
            }
            SynchronizerActionType::ChangeLatency => {
                let latency = reader.read_u8()?;
                if apply {
                    self.update_latency(latency, action);
                }
            }
            SynchronizerActionType::IncreasedLatencyConfirm => {
                let _tick = reader.read_u64_le()?;
                let latency = reader.read_u8()?;
                if apply {
                    self.update_latency(latency, action);
                }
            }
            SynchronizerActionType::SavingCountdown => {
                let _ = reader.read_u64_le()?;
                let _ = reader.read_u32_le()?;
            }
        }

        // Read trailing player_index for legacy layout.
        if !player_index_first {
            let _player_index = reader.read_opt_u16()?;
        }
        Ok(())
    }

    fn skip_map_ready_for_download(&mut self, reader: &mut BinaryReader) -> Result<()> {
        let transfer_size = reader.read_u64_le()?;
        let debug = std::env::var("FACTORIO_DEBUG").is_ok();
        if transfer_size > 0 && transfer_size < 50_000_000 {
            self.map_transfer_size = Some(transfer_size as u32);
            if debug {
                eprintln!(
                    "[DEBUG] MapReadyForDownload transfer_size={} bytes",
                    transfer_size
                );
            }
        }
        // Best-effort parse: these fields are not fully mapped, so stop early
        // if the payload is shorter than expected.
        if reader.remaining_slice().len() < 8 + 4 + 8 {
            return Ok(());
        }
        let _auxiliary = reader.read_u64_le()?;
        let _crc = reader.read_u32_le()?;
        let map_tick = reader.read_u64_le()?;
        if map_tick > 0 && map_tick <= u32::MAX as u64 {
            let tick_u32 = map_tick as u32;
            self.map_tick = Some(tick_u32);
            if self.client_tick == 0 {
                self.client_tick = tick_u32;
            }
        }

        if reader.remaining_slice().len() < 4 + 4 + 1 + 1 {
            return Ok(());
        }
        let _ = reader.read_u32_le()?;
        let _ = reader.read_u32_le()?;
        let _ = reader.read_bool()?;
        let _ = reader.read_bool()?;

        if reader.remaining_slice().len() < 1 {
            return Ok(());
        }
        let entries = reader.read_opt_u32()? as usize;
        for _ in 0..entries {
            if reader.remaining_slice().len() < 1 {
                return Ok(());
            }
            let _ = reader.read_string()?;
            if reader.remaining_slice().len() < 4 {
                return Ok(());
            }
            let _ = reader.read_u32_le()?;
        }

        if reader.remaining_slice().len() < 1 {
            return Ok(());
        }
        let entries = reader.read_opt_u32()? as usize;
        for _ in 0..entries {
            if reader.remaining_slice().len() < 1 {
                return Ok(());
            }
            let _ = reader.read_string()?;
            self.skip_script_registrations(reader)?;
        }

        if reader.remaining_slice().len() < 1 {
            return Ok(());
        }
        let entries = reader.read_opt_u32()? as usize;
        for _ in 0..entries {
            if reader.remaining_slice().len() < 1 {
                return Ok(());
            }
            let _ = reader.read_string()?;
            if reader.remaining_slice().len() < 1 {
                return Ok(());
            }
            let list_len = reader.read_opt_u32()? as usize;
            for _ in 0..list_len {
                if reader.remaining_slice().len() < 1 {
                    return Ok(());
                }
                let _ = reader.read_string()?;
            }
        }

        Ok(())
    }

    fn skip_script_registrations(&self, reader: &mut BinaryReader) -> Result<()> {
        let list_len = reader.read_opt_u32()? as usize;
        for _ in 0..list_len {
            let _ = reader.read_u32_le()?;
        }

        let list_len = reader.read_opt_u32()? as usize;
        for _ in 0..list_len {
            let _ = reader.read_u64_le()?;
        }

        let list_len = reader.read_opt_u32()? as usize;
        for _ in 0..list_len {
            let _ = reader.read_u32_le()?;
            let _ = reader.read_u32_le()?;
        }

        let _ = reader.read_bool()?;
        let _ = reader.read_bool()?;
        let _ = reader.read_bool()?;

        Ok(())
    }

    fn handle_start_sending_tick(&mut self, tick: u64) {
        let debug = std::env::var("FACTORIO_DEBUG").is_ok();
        let start_tick = tick as u32;

        // Validate game tick is in plausible range (> 1000, < 1 billion)
        // Note: client_seq_base is a heartbeat sequence (~570k), not a game tick.
        // Game ticks can be much higher (37M+ for servers running a while).
        if start_tick < 1000 || start_tick >= 1_000_000_000 {
            if debug {
                eprintln!("[DEBUG] handle_start_sending_tick: REJECTING tick {} - out of plausible range",
                    start_tick);
            }
            return;
        }
        // If we already have confirmed_tick, validate proximity (within 200k ticks)
        if self.confirmed_tick > 10_000 {
            let diff = if start_tick > self.confirmed_tick {
                start_tick - self.confirmed_tick
            } else {
                self.confirmed_tick - start_tick
            };
            if diff > 200_000 {
                if debug {
                    eprintln!("[DEBUG] handle_start_sending_tick: REJECTING tick {} - too far from confirmed_tick {} (diff={})",
                        start_tick, self.confirmed_tick, diff);
                }
                return;
            }
        }

        if debug {
            eprintln!("[DEBUG] handle_start_sending_tick: BEFORE start_sending_tick={:?}, setting to {}", self.start_sending_tick, start_tick);
        }
        let base = if self.confirmed_tick != 0 {
            self.confirmed_tick
        } else {
            self.server_tick
        };
        if base > 0 && start_tick > base {
            let lead = start_tick.saturating_sub(base).max(1);
            self.client_tick_lead = lead.clamp(CLIENT_TICK_LEAD_MIN, CLIENT_TICK_LEAD_MAX);
        }
        self.start_sending_tick = Some(start_tick);
        if debug {
            eprintln!("[DEBUG] handle_start_sending_tick: AFTER start_sending_tick={:?}", self.start_sending_tick);
        }
        self.allow_actions = true;
        let min_tick = if base > 0 {
            base.saturating_add(self.desired_tick_lead())
        } else {
            start_tick
        };
        self.client_tick = start_tick.max(min_tick);
        self.next_closure_tick = Some(self.client_tick.wrapping_add(1));
        self.base_tick = start_tick;
        // Note: We do NOT set pending_start_gameplay here because the state transitions
        // (including ClientChangedState(0x06)) were already sent in send_state_transition().
        // Sending it again confuses the server and causes it to stop responding.
        if self.game_start.is_none() {
            self.game_start = Some(std::time::Instant::now());
        }
        if debug {
            eprintln!(
                "[DEBUG] HB: ClientShouldStartSendingTickClosures tick={}",
                start_tick
            );
        }
    }

    fn update_latency(&mut self, latency: u8, action: SynchronizerActionType) {
        let debug = std::env::var("FACTORIO_DEBUG").is_ok();
        if latency == 0 {
            if debug {
                eprintln!(
                    "[DEBUG] HB: {:?} latency={} ignored (out of range)",
                    action, latency
                );
            }
            return;
        }
        
        // When server sends ChangeLatency, we must jump client_tick forward by the
        // increase amount. The server expects the next tick closure to be at 
        // current_tick + increase. If we don't jump, we get "wrong tick closure".
        let mut increase: u8 = 0;
        if action == SynchronizerActionType::ChangeLatency {
            let old_latency = self.latency_value.unwrap_or(0);
            if latency > old_latency {
                increase = latency - old_latency;
                if debug {
                    eprintln!(
                        "[DEBUG] HB: ChangeLatency old={} new={} increase={} -> jumping client_tick {} -> {}",
                        old_latency, latency, increase, self.client_tick, self.client_tick.wrapping_add(increase as u32)
                    );
                }
                // Jump client_tick forward by the latency increase
                self.client_tick = self.client_tick.wrapping_add(increase as u32);
            }
        }
        
        self.latency_value = Some(latency);
        let new_lead = (((latency as u32) + 15) / 16)
            .clamp(CLIENT_TICK_LEAD_MIN, CLIENT_TICK_LEAD_MAX);
        if debug {
            eprintln!(
                "[DEBUG] HB: {:?} latency={} -> lead={}",
                action, latency, new_lead
            );
        }
        self.client_tick_lead = new_lead;
        if action == SynchronizerActionType::ChangeLatency && increase > 0 {
            // Store the increase amount for the IncreasedLatencyConfirm
            self.pending_latency_confirm = Some(increase);
        }
    }

    fn scan_latency_actions(&mut self, extra: &[u8]) {
        let debug = std::env::var("FACTORIO_DEBUG").is_ok();
        let player_index = self.player_index.or(self.peer_id);
        if extra.len() < 4 {
            return;
        }
        for i in 0..extra.len().saturating_sub(3) {
            let action = extra[i];
            if action != 0x11 && action != 0x12 {
                continue;
            }
            let latency = extra[i + 1];
            if latency == 0 {
                continue;
            }
            let idx = u16::from_le_bytes([extra[i + 2], extra[i + 3]]);
            if let Some(expected) = player_index {
                if idx != expected {
                    continue;
                }
            }
            if debug {
                eprintln!(
                    "[DEBUG] HB: scanned latency type=0x{:02x} latency={} player_index={}",
                    action, latency, idx
                );
            }
            let action_type = if action == 0x11 {
                SynchronizerActionType::ChangeLatency
            } else {
                SynchronizerActionType::IncreasedLatencyConfirm
            };
            self.update_latency(latency, action_type);
            break;
        }
    }

    fn update_player_index_from_heartbeat(&mut self, data: &[u8]) {
        if data.is_empty() || self.username.is_empty() || self.player_index_confirmed {
            return;
        }

        let (header, payload_start) = match PacketHeader::parse(data) {
            Ok(v) => v,
            Err(_) => return,
        };
        if header.message_type != MessageType::ServerToClientHeartbeat {
            return;
        }
        if data.len() <= payload_start {
            return;
        }

        let payload = &data[payload_start..];
        if payload.len() < 5 {
            return;
        }
        let flags = payload[0];
        let has_tick_closures = (flags & 0x06) != 0;
        let mut pos = 5usize;
        if has_tick_closures {
            if let Some(offset) = self.find_confirm_record_start(&payload[pos..]) {
                pos = pos.saturating_add(offset);
            }
        }
        while pos + 15 <= payload.len() {
            if (payload[pos] != 0x02 && payload[pos] != 0x03) || payload[pos + 1] != 0x52 {
                break;
            }
            pos += 15;
            while pos < payload.len() && payload[pos] == 0x00 {
                pos += 1;
            }
        }

        if pos >= payload.len() {
            return;
        }

        let extra = &payload[pos..];
        let debug = std::env::var("FACTORIO_DEBUG").is_ok();
        if debug && self.debug_player_index_dumped < 3 {
            let name = self.username.as_bytes();
            if !name.is_empty() && extra.len() >= name.len() {
                if let Some(name_pos) = extra.windows(name.len()).position(|w| w == name) {
                    let start = name_pos.saturating_sub(12);
                    let end = (name_pos + name.len() + 12).min(extra.len());
                    let mut hex = String::new();
                    for b in &extra[start..end] {
                        let _ = std::fmt::Write::write_fmt(&mut hex, format_args!("{:02x}", b));
                    }
                    eprintln!(
                        "[DEBUG] Player index scan context: offset={} extra_len={} hex={}",
                        name_pos,
                        extra.len(),
                        hex
                    );
                    self.debug_player_index_dumped += 1;
                }
            }
        }
        // Try PlayerJoinGame first - it's the most authoritative source.
        // If found, immediately confirm it to prevent later false positives.
        if let Some(idx) = self.extract_player_index_from_player_join(extra) {
            if debug && self.player_index != Some(idx) {
                eprintln!(
                    "[DEBUG] Player index from PlayerJoinGame: {:?} -> {}",
                    self.player_index,
                    idx
                );
            }
            self.player_index = Some(idx);
            self.player_index_confirmed = true;
            return;
        }

        // Fallback extraction methods (less reliable, don't confirm).
        let player_index = match self
            .extract_player_index_from_extra(extra)
            .or_else(|| self.extract_player_index_from_actions(extra))
            .or_else(|| self.extract_player_index_from_actions(payload))
        {
            Some(idx) => idx,
            None => return,
        };
        // Skip if player_index equals peer_id (likely mis-parsed peer_id as player_index)
        if self.peer_id == Some(player_index) && self.player_index.is_some() {
            return;
        }

        // Only update player_index if we don't have one, or if the current value
        // looks like a peer_id (common early mis-parse), or if it's unconfirmed.
        let needs_update = match self.player_index {
            None => true,
            Some(current) => {
                self.peer_id == Some(current) || !self.player_index_confirmed
            }
        };
        if !needs_update {
            return;
        }
        if self.player_index != Some(player_index) {
            if debug {
                eprintln!(
                    "[DEBUG] Player index update from heartbeat: {:?} -> {}",
                    self.player_index,
                    player_index
                );
            }
            self.player_index = Some(player_index);
        }
        // Treat heartbeat-derived player index as authoritative once we're in game
        // (or waiting on init), so we can send the init action.
        if self.state == ConnectionState::InGame || self.pending_init_action {
            self.player_index_confirmed = true;
        }
    }

    fn extract_player_index_from_extra(&self, extra: &[u8]) -> Option<u16> {
        let name = self.username.as_bytes();
        if name.is_empty() || name.len() > 0xFF {
            return None;
        }

        if extra.len() < name.len() + 3 {
            return None;
        }

        let peer_id = self.peer_id;
        let debug = std::env::var("FACTORIO_DEBUG").is_ok();

        for i in 0..=extra.len() - name.len() {
            if &extra[i..i + name.len()] != name {
                continue;
            }

            let name_len = name.len();

            // Pattern: [..] 0x99 [unknown] [peer_id?] [player_index] 00 01 [len] [username]
            if i >= 7
                && extra[i - 1] as usize == name_len
                && extra[i - 2] == 0x01
                && extra[i - 3] == 0x00
                && extra[i - 7] == 0x99
            {
                let candidate = extra[i - 4] as u16;
                if candidate != 0 {
                    let peer_match = match peer_id {
                        Some(pid) if pid <= u8::MAX as u16 => extra[i - 5] == pid as u8,
                        _ => true,
                    };
                    if peer_match {
                        return Some(candidate);
                    }
                    if debug {
                        eprintln!(
                            "[DEBUG] Player index candidate peer_id mismatch (0x99 pattern): {}",
                            candidate
                        );
                    }
                }
            }

            // Pattern: [..][peer_id?][player_index][00][01][len][username]
            if i >= 4
                && extra[i - 1] as usize == name_len
                && extra[i - 2] == 0x01
                && extra[i - 3] == 0x00
            {
                let candidate = extra[i - 4] as u16;
                if candidate != 0 {
                    let peer_match = match peer_id {
                        Some(pid) if pid <= u8::MAX as u16 && i >= 5 => extra[i - 5] == pid as u8,
                        Some(pid) if pid <= u8::MAX as u16 => false,
                        _ => true,
                    };
                    if peer_match {
                        return Some(candidate);
                    }
                    if debug {
                        eprintln!(
                            "[DEBUG] Player index candidate peer_id mismatch (00 01 pattern): {}",
                            candidate
                        );
                    }
                }
            }

            // Pattern: 01 02 [len] [username] [peer_or_player] 00
            if i >= 3
                && extra[i - 3] == 0x01
                && extra[i - 2] == 0x02
                && extra[i - 1] as usize == name_len
            {
                let idx_pos = i + name_len;
                if idx_pos < extra.len() {
                    let candidate = extra[idx_pos] as u16;
                    if candidate != 0 {
                        if idx_pos + 1 >= extra.len() || extra[idx_pos + 1] == 0x00 {
                            if peer_id.map_or(true, |pid| candidate != pid) {
                                return Some(candidate);
                            }
                            if debug {
                                eprintln!(
                                    "[DEBUG] Player index candidate matches peer_id (01 02 pattern): {}",
                                    candidate
                                );
                            }
                        }
                    }
                }
            }
        }

        None
    }

    fn extract_player_index_from_actions(&self, extra: &[u8]) -> Option<u16> {
        for start in 0..extra.len().saturating_sub(3) {
            let mut reader = BinaryReader::new(&extra[start..]);
            let count_and_segments = match reader.read_opt_u32() {
                Ok(v) => v,
                Err(_) => continue,
            };
            let action_count = count_and_segments / 2;
            if action_count == 0 || action_count > 64 {
                continue;
            }
            let action_type = match reader.read_opt_u16() {
                Ok(v) => v,
                Err(_) => continue,
            };
            if action_type != InputActionType::PlayerJoinGame as u16 {
                continue;
            }
            let player_delta = match reader.read_opt_u16() {
                Ok(v) => v,
                Err(_) => continue,
            };
            if player_delta == 0 {
                continue;
            }
            return Some(player_delta.saturating_sub(1));
        }
        None
    }

    fn extract_player_index_from_player_join(&self, extra: &[u8]) -> Option<u16> {
        let name = self.username.as_bytes();
        if name.is_empty() || name.len() > 0xFFFF {
            return None;
        }
        if extra.len() < name.len() {
            return None;
        }
        // Log username search to HB log
        if let Some(name_pos) = extra.windows(name.len()).position(|w| w == name) {
            if let Ok(mut file) = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open("/tmp/factorio-client-hb.log")
            {
                use std::io::Write;
                let start = name_pos.saturating_sub(20);
                let end = (name_pos + name.len() + 10).min(extra.len());
                let context = &extra[start..end];
                let _ = writeln!(
                    file,
                    "PLAYER_JOIN_SEARCH name_pos={} context={:02x?}",
                    name_pos,
                    context
                );
            }
        }
        let debug = std::env::var("FACTORIO_DEBUG").is_ok();
        for i in 0..=extra.len().saturating_sub(name.len()) {
            if &extra[i..i + name.len()] != name {
                continue;
            }
            if i == 0 {
                continue;
            }
            let len_byte = extra[i - 1] as usize;
            if len_byte != name.len() {
                continue;
            }
            if i < 5 {
                continue;
            }

            let mode = extra[i - 2];
            let player_index = u16::from_le_bytes([extra[i - 4], extra[i - 3]]);
            let peer_id = extra[i - 5] as u16;

            let after = i + name.len();
            if after + 2 > extra.len() {
                continue;
            }
            let flag_a = extra[after];
            let flag_b = extra[after + 1];
            if flag_a > 1 || flag_b > 1 {
                continue;
            }

            if debug {
                eprintln!(
                    "[DEBUG] PlayerJoinGame decoded: peer_id={} player_index={} mode=0x{:02x} flags=({}, {})",
                    peer_id,
                    player_index,
                    mode,
                    flag_a,
                    flag_b
                );
            }
            return Some(player_index);
        }

        None
    }

    /// Run heartbeat loop for a duration, keeping connection alive
    pub async fn run_for(&mut self, duration: Duration) -> Result<()> {
        let start = std::time::Instant::now();
        let mut last_heartbeat = std::time::Instant::now();

        while start.elapsed() < duration {
            // Send heartbeat at regular interval
            if last_heartbeat.elapsed() >= self.heartbeat_interval() {
                self.send_heartbeat().await?;
                last_heartbeat = std::time::Instant::now();
            }

            // Process incoming packets
            self.poll().await?;

            // Small sleep to avoid busy loop
            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        Ok(())
    }

    /// Apply a player action to track player states
    fn apply_player_action(&mut self, player_index: u16, action: &CodecInputAction, tick: Option<u32>) {
        let debug = std::env::var("FACTORIO_DEBUG").is_ok();
        let current_tick = tick.unwrap_or(self.server_tick);

        match action {
            CodecInputAction::PlayerJoinGame { peer_id, player_index_plus_one, username, .. } => {
                // Use the player_index derived from the action list delta as authoritative.
                let join_index = if player_index != 0xFFFF { player_index } else { *player_index_plus_one };
                // Check if this is our own join
                let is_self = username == &self.username
                    || self.peer_id.map_or(false, |pid| pid == *peer_id);
                if is_self && join_index != 0xFFFF {
                    self.player_index = Some(join_index);
                    self.player_index_confirmed = true;
                    if self.peer_id.is_none() {
                        self.peer_id = Some(*peer_id);
                    }
                    if debug {
                        eprintln!("[DEBUG] Self joined: player_index={}", join_index);
                    }
                    self.ensure_sim_player(join_index, Some(username));
                } else if join_index != 0xFFFF {
                    // Track other player - assign initial position from map if available
                    let is_new = !self.other_players.contains_key(&join_index);
                    let initial_pos = if is_new {
                        self.assign_character_position()
                    } else {
                        None
                    };

                    let state = self.other_players.entry(join_index).or_insert_with(|| {
                        let (x, y) = initial_pos.unwrap_or((0.0, 0.0));
                        PlayerState {
                            player_index: join_index,
                            x,
                            y,
                            ..Default::default()
                        }
                    });
                    state.username = Some(username.clone());
                    state.connected = true;
                    state.last_tick = current_tick;
                    if debug {
                        let pos_source = if initial_pos.is_some() { "from map" } else { "default" };
                        eprintln!("[DEBUG] Player joined: {} index={} pos=({:.1},{:.1}) ({})",
                            username, join_index, state.x, state.y, pos_source);
                    }
                    self.ensure_sim_player(join_index, Some(username));
                }
            }

            CodecInputAction::PlayerLeaveGame { peer_id, .. } => {
                // Mark player as disconnected
                if let Some(state) = self.other_players.get_mut(&player_index) {
                    state.connected = false;
                    if debug {
                        eprintln!("[DEBUG] Player left: index={} peer={}", player_index, peer_id);
                    }
                }
            }

            CodecInputAction::StartWalking { direction_x, direction_y } => {
                self.ensure_sim_player(player_index, None);
                if Some(player_index) == self.player_index {
                    self.walk_active = true;
                    self.walk_dir = (*direction_x, *direction_y);
                } else {
                    // Update other player - assign initial position from map if this is a new player
                    let is_new = !self.other_players.contains_key(&player_index);
                    let initial_pos = if is_new {
                        self.assign_character_position()
                    } else {
                        None
                    };

                    let state = self.other_players.entry(player_index).or_insert_with(|| {
                        let (x, y) = initial_pos.unwrap_or((0.0, 0.0));
                        PlayerState {
                            player_index,
                            x,
                            y,
                            ..Default::default()
                        }
                    });

                    // Apply movement for elapsed ticks before changing direction
                    if state.walking {
                        let ticks = current_tick.saturating_sub(state.last_tick);
                        if ticks > 0 {
                            let dx_tick = (self.character_speed * state.walk_direction.0 * 256.0).trunc() / 256.0;
                            let dy_tick = (self.character_speed * state.walk_direction.1 * 256.0).trunc() / 256.0;
                            state.x += dx_tick * ticks as f64;
                            state.y += dy_tick * ticks as f64;
                        }
                    }
                    state.walking = true;
                    state.walk_direction = (*direction_x, *direction_y);
                    state.last_tick = current_tick;
                }
            }

            CodecInputAction::StopWalking => {
                self.ensure_sim_player(player_index, None);
                if Some(player_index) == self.player_index {
                    self.walk_active = false;
                } else if let Some(state) = self.other_players.get_mut(&player_index) {
                    if state.walking {
                        let ticks = current_tick.saturating_sub(state.last_tick);
                        if ticks > 0 {
                            let dx_tick = (self.character_speed * state.walk_direction.0 * 256.0).trunc() / 256.0;
                            let dy_tick = (self.character_speed * state.walk_direction.1 * 256.0).trunc() / 256.0;
                            state.x += dx_tick * ticks as f64;
                            state.y += dy_tick * ticks as f64;
                        }
                    }
                    state.walking = false;
                    state.last_tick = current_tick;
                }
            }

            CodecInputAction::BeginMining { .. } | CodecInputAction::BeginMiningTerrain { .. } => {
                self.ensure_sim_player(player_index, None);
                if Some(player_index) != self.player_index {
                    if let Some(state) = self.other_players.get_mut(&player_index) {
                        state.mining = true;
                    }
                }
            }

            CodecInputAction::StopMining => {
                self.ensure_sim_player(player_index, None);
                if Some(player_index) != self.player_index {
                    if let Some(state) = self.other_players.get_mut(&player_index) {
                        state.mining = false;
                    }
                }
            }

            CodecInputAction::ChangeShootingState { state, .. } => {
                self.ensure_sim_player(player_index, None);
                if Some(player_index) != self.player_index {
                    if let Some(pstate) = self.other_players.get_mut(&player_index) {
                        pstate.shooting = *state != ShootingState::NotShooting;
                    }
                }
            }

            _ => {}
        }
    }


    /// Update all tracked player positions based on current tick
    pub fn update_other_players(&mut self) {
        if self.simulation.is_some() {
            self.sync_simulation_to_server_tick();
            if let Some(sim) = self.simulation.as_ref() {
                for (id, player) in &sim.world.players {
                    if Some(*id) == self.player_index {
                        continue;
                    }
                    let state = self.other_players.entry(*id).or_default();
                    state.player_index = *id;
                    state.x = player.position.to_tiles().0;
                    state.y = player.position.to_tiles().1;
                    state.walking = player.walking;
                    state.walk_direction = player.walking_direction.to_vector();
                    state.connected = player.connected;
                    if state.username.is_none() && !player.name.is_empty() {
                        state.username = Some(player.name.clone());
                    }
                }
            }
            return;
        }
        let current_tick = self.server_tick;
        let players: Vec<u16> = self.other_players.keys().cloned().collect();
        for player_index in players {
            if let Some(state) = self.other_players.get_mut(&player_index) {
                if state.walking {
                    let ticks = current_tick.saturating_sub(state.last_tick);
                    if ticks > 0 {
                        let dx_tick = (self.character_speed * state.walk_direction.0 * 256.0).trunc() / 256.0;
                        let dy_tick = (self.character_speed * state.walk_direction.1 * 256.0).trunc() / 256.0;
                        state.x += dx_tick * ticks as f64;
                        state.y += dy_tick * ticks as f64;
                        state.last_tick = current_tick;
                    }
                }
            }
        }
    }

    fn ensure_sim_player(&mut self, player_index: u16, username: Option<&str>) {
        let sim_exists = match self.simulation.as_ref() {
            Some(s) => s.world.players.contains_key(&player_index),
            None => return,
        };
        if sim_exists {
            if let Some(name) = username {
                if let Some(sim) = self.simulation.as_mut() {
                    if let Some(p) = sim.world.players.get_mut(&player_index) {
                        if p.name.is_empty() {
                            p.name = name.to_string();
                        }
                    }
                }
            }
            return;
        }
        let spawn_pos = self
            .simulation
            .as_ref()
            .map(|s| s.world.spawn_position.to_tiles())
            .unwrap_or((0.0, 0.0));
        let mut player = crate::state::player::Player::new(
            player_index,
            username.map(|s| s.to_string()).unwrap_or_else(|| format!("player-{}", player_index)),
        );
        let mut pos = self.assign_character_position().unwrap_or(spawn_pos);
        if pos == (0.0, 0.0) && (self.player_x != 0.0 || self.player_y != 0.0) {
            pos = (self.player_x, self.player_y);
        }
        player.position = MapPosition::from_tiles(pos.0, pos.1);
        if let Some(sim) = self.simulation.as_mut() {
            sim.world.players.insert(player_index, player);
        }
    }

    fn sync_simulation_to_server_tick(&mut self) {
        let sim = match self.simulation.as_mut() {
            Some(s) => s,
            None => return,
        };
        if self.server_tick <= sim.world.tick {
            return;
        }
        let closure = TickClosureData {
            update_tick: self.server_tick,
            input_actions: Vec::new(),
        };
        let _ = sim.executor.execute_tick(&mut sim.world, &closure);
    }
}

fn read_local_checksums() -> Option<(u32, u32)> {
    let home = std::env::var("HOME").ok();
    let mut paths = Vec::new();
    if let Some(home) = home {
        paths.push(format!("{}/Library/Application Support/factorio-server/factorio-current.log", home));
        paths.push(format!("{}/Library/Application Support/factorio/factorio-current.log", home));
    }
    paths.push("/tmp/factorio-console.log".to_string());

    for path in paths {
        let file = match File::open(&path) {
            Ok(file) => file,
            Err(_) => continue,
        };
        let mut core_checksum = None;
        let mut prototype_checksum = None;
        for line in io::BufReader::new(file).lines().flatten() {
            if core_checksum.is_none() {
                core_checksum = parse_checksum(&line, "Checksum for core:");
            }
            if prototype_checksum.is_none() {
                prototype_checksum = parse_checksum(&line, "Prototype list checksum:");
            }
            if core_checksum.is_some() && prototype_checksum.is_some() {
                return Some((core_checksum.unwrap(), prototype_checksum.unwrap()));
            }
        }
    }

    None
}

fn parse_checksum(line: &str, label: &str) -> Option<u32> {
    let idx = line.find(label)?;
    let rest = &line[idx + label.len()..];
    rest.trim().split_whitespace().next()?.parse().ok()
}

fn default_factorio_data_path() -> Option<PathBuf> {
    if let Ok(path) = std::env::var("FACTORIO_DATA_PATH") {
        let p = PathBuf::from(path);
        if p.exists() {
            return Some(p);
        }
    }
    if let Ok(cwd) = std::env::current_dir() {
        let local = cwd.join("lua");
        if local.exists() {
            return Some(local);
        }
    }
    let mac = PathBuf::from("/Applications/factorio.app/Contents/data");
    if mac.exists() {
        return Some(mac);
    }
    None
}

/// Packet types that can be received
#[derive(Debug)]
pub enum ReceivedPacket {
    Heartbeat { tick: u32 },
    MapBlock { size: usize },
    Unknown { msg_type: u8, size: usize },
}

/// Simple random number generator for connection IDs
fn rand_u64() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let duration = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    let nanos = duration.as_nanos() as u64;
    let secs = duration.as_secs();
    nanos ^ (secs << 32)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connection_state() {
        assert_eq!(ConnectionState::Disconnected, ConnectionState::Disconnected);
    }
}
