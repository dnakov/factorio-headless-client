use std::collections::{HashMap, VecDeque};
use std::fs::File;
use std::io::{self, BufRead};
use std::net::SocketAddr;
use std::time::Duration;

use crate::codec::{
    BinaryReader, BinaryWriter, Direction, InputAction as CodecInputAction, InputActionType,
    MapEntity, MapPosition, ShootingState, SynchronizerActionType, parse_map_data,
};
use crate::error::{Error, Result};
use crate::protocol::message::{
    ConnectionRequest, ConnectionRequestReply, ConnectionRequestReplyConfirm,
    ConnectionAcceptOrDeny, ModInfo, ServerInfo, TransferBlockRequest,
    InputAction,
};
use crate::protocol::packet::{PacketHeader, MessageType};
use crate::protocol::transport::Transport;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

const MAP_DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(60);
const RECV_TIMEOUT: Duration = Duration::from_millis(500);
const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(16); // ~60 Hz
const CLIENT_TICK_LEAD_INITIAL: u32 = 28; // Space Age (2.0) pcap lead: client_tick = confirmed_tick + 1 + 28
const CLIENT_TICK_LEAD_MIN: u32 = 1;
const INITIAL_BLOCK_REQUEST_MAX: u32 = 256; // Avoid over-requesting before transfer size is known.

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

    // Walking state for position tracking
    walking: bool,
    walking_direction: u8,
    last_position_tick: u32,

    // Tick synchronization
    client_tick: u32, // Last sent client tick (increments per C2S gameplay packet).
    server_seq: u32,  // Server heartbeat sequence number (S2C tick_a).
    server_tick: u32,
    confirmed_tick: u32,
    client_tick_lead: u32,
    latency_value: Option<u8>,
    start_sending_tick: Option<u32>,
    allow_actions: bool,
    base_tick: u32,                           // Server tick when we entered InGame
    game_start: Option<std::time::Instant>,   // Time when we entered InGame
    msg_id: u16,          // Heartbeat message id (seeded by ConnectionAcceptOrDeny.initial_msg_id)
    peer_constant: u16,   // Peer constant from server (e.g., 0x41ca), goes in bytes 4-5
    tick_sync: u16,       // confirmed_tick low 16 bits, goes in bytes 6-7
    reliable_rng: ReliableRng, // RNG for reliable bit selection (pcap shows ~50/50)
    map_transfer_size: Option<u32>,

    // Map data
    map_data: Vec<u8>,
    entities: Vec<MapEntity>,

    // Pending confirmations for reliable messages we received
    pending_confirms: Vec<u32>,

    // Chat sequence number (pcap shows incrementing u32 in chat actions)
    chat_seq: u32,

    // Pending input actions to send (one per gameplay tick)
    pending_actions: VecDeque<InputAction>,

    // Send init action once player_index is known
    pending_init_action: bool,
    // Send the first gameplay heartbeat with ClientChangedState(0x07)
    pending_start_gameplay: bool,

    // Track last gameplay send time to avoid multiple sends per tick
    last_gameplay_send_at: Option<std::time::Instant>,

    // Track last mining/cursor position for stop signals
    last_mine_position: Option<MapPosition>,

    // Debug counters for initial gameplay packets
    debug_gameplay_heartbeats: u8,
    debug_action_packets: u8,
    debug_player_index_dumped: u8,
    debug_server_heartbeat_dumped: u8,

    // Other player tracking
    other_players: HashMap<u16, PlayerState>,
    initial_player_positions: Vec<(f64, f64)>,
    /// Tracks which character positions from the map have been assigned
    assigned_position_indices: std::collections::HashSet<usize>,
    /// Character movement speed from prototype data (tiles per tick)
    character_speed: f64,
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
            walking: false,
            walking_direction: 0,
            last_position_tick: 0,
            client_tick: 0,
            server_seq: 0,
            server_tick: 0,
            confirmed_tick: 0,
            client_tick_lead: CLIENT_TICK_LEAD_INITIAL,
            latency_value: None,
            start_sending_tick: None,
            allow_actions: false,
            base_tick: 0,
            game_start: None,
            msg_id: 1,
            peer_constant: 0x41ca, // Default, will be updated from ConnectionAcceptOrDeny
            tick_sync: 0xffff,   // Uses confirmed_tick low 16 bits; 0xffff until known
            reliable_rng,
            map_transfer_size: None,
            map_data: Vec::new(),
            entities: Vec::new(),
            pending_confirms: Vec::new(),
            chat_seq: 1,
            pending_actions: VecDeque::new(),
            pending_init_action: false,
            pending_start_gameplay: false,
            last_gameplay_send_at: None,
            last_mine_position: None,
            debug_gameplay_heartbeats: 0,
            debug_action_packets: 0,
            debug_player_index_dumped: 0,
            debug_server_heartbeat_dumped: 0,
            other_players: HashMap::new(),
            initial_player_positions: Vec::new(),
            assigned_position_indices: std::collections::HashSet::new(),
            character_speed: 0.15, // Default, updated from map data
        })
    }

    pub fn state(&self) -> ConnectionState {
        self.state
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

    pub fn client_tick(&self) -> u32 {
        self.client_tick
    }

    pub fn server_tick(&self) -> u32 {
        self.server_tick
    }

    pub fn map_data(&self) -> &[u8] {
        &self.map_data
    }

    pub fn player_position(&self) -> (f64, f64) {
        (self.player_x, self.player_y)
    }

    pub fn update_position(&mut self) {
        self.update_position_from_ticks();
    }

    pub fn entities(&self) -> &[MapEntity] {
        &self.entities
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
        // Step 1: Query server info to get mod list
        self.state = ConnectionState::QueryingServerInfo;
        self.query_server_info().await?;

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

        // Accept payload carries both peer identifier and potentially player index
        if self.peer_id.is_none() {
            self.peer_id = accept.peer_id.or(accept.player_index);
        }
        // Use player_index from Accept as initial value (will be updated by heartbeats if needed)
        self.player_index = accept.player_index;
        self.player_index_confirmed = false;
        self.server_name = accept.server_name;

        if let Some(msg_id) = accept.initial_msg_id {
            self.msg_id = msg_id;
        }
        if let Some(session_const) = accept.session_constant {
            let debug = std::env::var("FACTORIO_DEBUG").is_ok();
            if debug {
                eprintln!("[DEBUG] Setting peer_constant from Accept: 0x{:04x} -> 0x{:04x}",
                    self.peer_constant, session_const);
            }
            self.peer_constant = session_const;
        }
        // initial_tick is the heartbeat sequence number we must use
        if let Some(tick) = accept.initial_tick {
            let debug = std::env::var("FACTORIO_DEBUG").is_ok();
            if debug {
                eprintln!("[DEBUG] Setting confirmed_tick from Accept initial_tick: {}", tick);
            }
            self.confirmed_tick = tick;
            self.server_tick = tick;
            self.tick_sync = (tick & 0xFFFF) as u16;
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
            let _latency = reader.read_u8()?;
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
                peer_count: None,
                map_tick: None,
                steam_id,
                latency_window: None,
            };

            if debug {
                eprintln!(
                    "[DEBUG] Accept parsed: player_index={} peer_id={:?} initial_tick={:?} initial_msg_id={:?} session_constant={:?}",
                    player_index,
                    peer_id,
                    accept.initial_tick,
                    accept.initial_msg_id,
                    accept.session_constant
                );
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
        self.state = ConnectionState::DownloadingMap;
        self.map_data.clear();

        let debug = std::env::var("FACTORIO_DEBUG").is_ok();

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
        let wait_start = std::time::Instant::now();
        let wait_timeout = Duration::from_millis(500);
        let mut got_server_heartbeat = false;

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
                        if debug {
                            eprintln!("[DEBUG] Received first server heartbeat, now sending client heartbeat");
                        }
                    }
                }
                _ => {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            }
        }

        // Now send our first heartbeat in response to server's heartbeat
        // Use 0xff ticks for initial state 01 03 02 packet (matching PCAP)
        self.send_initial_state_heartbeat_with_ff().await?;

        if debug {
            eprintln!("[DEBUG] Sent initial state heartbeat with 0xff tick fields");
        }

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
            self.send_initial_sync_heartbeat().await?;
            tokio::time::sleep(Duration::from_millis(16)).await;
        }

        // Phase 2: Signal ready for map with trailer 02 03 03 09 00
        self.send_state_heartbeat(&[0x02, 0x03, 0x03, 0x09, 0x00]).await?;

        // Continue looking for transfer size
        if max_block.is_none() {
            let wait_start = std::time::Instant::now();
            while wait_start.elapsed() < Duration::from_millis(200) && max_block.is_none() {
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
            }
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
        let mut all_requested = false;
        let progress_markers: [u8; 12] = [
            0x08, 0x1e, 0x35, 0x44, 0x5c, 0x73, 0x8a, 0xa5, 0xbc, 0xd3, 0xeb, 0xfe,
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
        let batch_size = 50;
        let mut sent = 0u32;
        let mut burst_heartbeat = std::time::Instant::now();
        let mut requested_max = initial_max;
        let mut next_request_block = initial_max.saturating_add(1);
        let mut last_expand = std::time::Instant::now();
        if let Some(max) = max_block {
            if max <= initial_max {
                all_requested = true;
            }
        }
        while sent <= initial_max {
            let batch_end = (sent + batch_size).min(initial_max + 1);
            for i in sent..batch_end {
                let reliable = self.next_reliable();
                let request = TransferBlockRequest::new(i, reliable);
                let _ = self.transport.send_raw(&request.to_bytes()).await;
            }
            sent = batch_end;
            // Send heartbeat every 16ms during burst to stay in latency window
            if burst_heartbeat.elapsed() >= HEARTBEAT_INTERVAL {
                if let Some(max) = max_block {
                    let total = max.saturating_add(1) as f64;
                    if total > 0.0 {
                        let progress = ((received_blocks.len() as f64 / total) * 255.0).round() as u8;
                        last_progress = progress_marker_for(progress, last_progress);
                    }
                }
                let _ = self.send_state_heartbeat(&[0x01, 0x09, last_progress]).await;
                burst_heartbeat = std::time::Instant::now();
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        if debug {
            eprintln!("[DEBUG] Sent {} block requests in batches", initial_max + 1);
        }

        while start.elapsed() < MAP_DOWNLOAD_TIMEOUT {
            if last_heartbeat.elapsed() >= HEARTBEAT_INTERVAL {
                if debug && !self.pending_confirms.is_empty() {
                    eprintln!("[DEBUG] Sending heartbeat with {} pending confirms", self.pending_confirms.len());
                }
                if let Some(max) = max_block {
                    let total = max.saturating_add(1) as f64;
                    if total > 0.0 {
                        let progress = ((received_blocks.len() as f64 / total) * 255.0).round() as u8;
                        last_progress = progress_marker_for(progress, last_progress);
                    }
                }
                let _ = self.send_state_heartbeat(&[0x01, 0x09, last_progress]).await;
                last_heartbeat = std::time::Instant::now();
            }

            if got_all_blocks {
                break;
            }

            // Timeout: if no new blocks for 1000ms, assume complete when size is known.
            if !received_blocks.is_empty() && last_new_block_time.elapsed() > Duration::from_millis(1000) {
                if max_block.is_some() {
                    break;
                }
            }

            let data = match self.transport.recv_raw_timeout(Duration::from_millis(1)).await {
                Ok(Some(d)) if !d.is_empty() => d,
                _ => {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                    continue;
                }
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
                                let reliable = self.next_reliable();
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
                                let reliable = self.next_reliable();
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
                        let reliable = self.next_reliable();
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
                    let reliable = self.next_reliable();
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

            if max_block.is_some() && last_progress != 0xfe {
                let _ = self.send_state_heartbeat(&[0x01, 0x09, 0xfe]).await;
                if let Ok(Some(data)) = self.transport.recv_raw_timeout(Duration::from_millis(8)).await {
                    if !data.is_empty()
                        && (data[0] & 0x1F) == MessageType::ServerToClientHeartbeat as u8
                    {
                        let _ = self.process_server_heartbeat(&data);
                    }
                }
                tokio::time::sleep(Duration::from_millis(16)).await;
                last_progress = 0xfe;
            }

            // Try to parse entities from the map
            if let Ok(parsed) = parse_map_data(&self.map_data) {
                // Store initial player (character) positions from map (before moving entities)
                self.initial_player_positions = parsed.character_positions();
                // Get character speed from prototype data
                self.character_speed = parsed.character_speed();
                self.entities = parsed.entities;
                // Set spawn position if available
                if parsed.player_spawn != (0.0, 0.0) {
                    self.player_x = parsed.player_spawn.0;
                    self.player_y = parsed.player_spawn.1;
                }
            }

            // Send state transition to signal we're ready for gameplay
            // The server expects specific state change signals before we can use gameplay heartbeats
            self.send_state_transition().await?;

            // Sync to the latest confirmed tick before starting gameplay heartbeats.
            self.sync_gameplay_clock().await;

            // Per doc lines 272-285, 541-542: after state trailers, send init action then gameplay heartbeats
            let _ = self.maybe_send_start_gameplay_heartbeat().await?;

            if std::env::var("FACTORIO_SKIP_INIT_ACTION").is_err() {
                // Wait for player index before sending init action
                self.pending_init_action = true;
                self.await_player_index(Duration::from_millis(2000)).await;

                let _ = self.maybe_send_start_gameplay_heartbeat().await?;
                // Send init action (UpdateBlueprintShelf segment) with flags=0x06
                let _ = self.maybe_send_init_action().await?;

                // CRITICAL: Wait for server responses after ready/init before gameplay heartbeats
                // PCAP shows tick2 advances between init and first 0x0e heartbeat
                for _ in 0..10 {
                    if let Ok(Some(data)) = self.transport.recv_raw_timeout(Duration::from_millis(10)).await {
                        if !data.is_empty() && (data[0] & 0x1F) == MessageType::ServerToClientHeartbeat as u8 {
                            let _ = self.process_server_heartbeat(&data);
                        }
                    }
                }

                // Reset client_tick to match current confirmed_tick after receiving server responses
                if self.start_sending_tick.is_none() {
                    let new_target = self.confirmed_tick.wrapping_add(1 + self.client_tick_lead);
                    self.client_tick = new_target;
                }
            }

            let _ = self.maybe_send_start_gameplay_heartbeat().await?;

            // Only NOW start sending gameplay heartbeats with flags=0x0e
            for _ in 0..1 {
                let _ = self.send_heartbeat_raw().await;
                if let Ok(Some(data)) = self.transport.recv_raw_timeout(Duration::from_millis(8)).await {
                    if !data.is_empty() && (data[0] & 0x1F) == MessageType::ServerToClientHeartbeat as u8 {
                        let _ = self.process_server_heartbeat(&data);
                    }
                }
                tokio::time::sleep(Duration::from_millis(16)).await;
            }

            self.state = ConnectionState::InGame;
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

        // State transition trailers per doc lines 533-537
        let trailers: &[&[u8]] = &[
            &[0x03, 0x03, 0x04, 0x06, 0x00, 0x09, 0xff],
            &[0x01, 0x06, 0x40],
            &[0x01, 0x06, 0xa4],
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
                }
            }
            tokio::time::sleep(Duration::from_millis(16)).await;
        }

        Ok(())
    }

    async fn sync_gameplay_clock(&mut self) {
        let debug = std::env::var("FACTORIO_DEBUG").is_ok();
        let start = std::time::Instant::now();
        let mut last_heartbeat = std::time::Instant::now()
            .checked_sub(HEARTBEAT_INTERVAL)
            .unwrap_or_else(std::time::Instant::now);

        // After state transition, send empty heartbeats (flags=0x00) to stay connected
        // Per pcap: after state 4 transition, client sends flags=0x00 heartbeats
        while start.elapsed() < Duration::from_millis(120) {
            match self.transport.recv_raw_timeout(Duration::from_millis(10)).await {
                Ok(Some(data)) if !data.is_empty() && (data[0] & 0x1F) == MessageType::ServerToClientHeartbeat as u8 => {
                    let _ = self.process_server_heartbeat(&data);
                }
                _ => {
                    tokio::task::yield_now().await;
                }
            }
            // Send empty heartbeat (flags=0x00) to stay connected
            if last_heartbeat.elapsed() >= HEARTBEAT_INTERVAL {
                let _ = self.send_heartbeat_raw().await;
                last_heartbeat = std::time::Instant::now();
            }
        }

        let confirmed = if self.confirmed_tick != 0 {
            self.confirmed_tick
        } else {
            self.server_tick
        };
        let mut base_tick = confirmed.wrapping_add(1 + self.client_tick_lead);
        if let Some(start_tick) = self.start_sending_tick {
            base_tick = start_tick;
        }
        self.base_tick = base_tick;
        self.client_tick = base_tick;
        self.game_start = None;

        if debug {
            eprintln!(
                "[DEBUG] State transition complete, base_tick={}, waiting for start tick",
                self.base_tick
            );
        }

        // CRITICAL: After setting game_start, we need to receive a fresh server tick
        // before sending gameplay heartbeats. Otherwise server_tick_echo is stale and
        // server rejects with "heartbeat outside latency window".
        // Send a sync heartbeat to trigger server response, then wait for tick closure.
        let tick_before = self.server_tick;
        let sync_start = std::time::Instant::now();
        while sync_start.elapsed() < Duration::from_millis(500) {
            // Send a sync heartbeat (not gameplay) to stay connected
            let msg_id = self.next_heartbeat_msg_id();
            let type_byte = 0x06u8;
            let (tick_sync, tick, padding): (u16, u32, u16) = if self.confirmed_tick != 0 {
                ((self.confirmed_tick & 0xFFFF) as u16, self.confirmed_tick >> 16, 0x0000)
            } else {
                (0xffff, 0xffffffff, 0xffff)
            };
            let mut packet = Vec::new();
            packet.push(type_byte);
            packet.push(0x00);
            packet.extend_from_slice(&msg_id.to_le_bytes());
            packet.extend_from_slice(&self.peer_constant.to_le_bytes());
            packet.extend_from_slice(&tick_sync.to_le_bytes());
            packet.extend_from_slice(&tick.to_le_bytes());
            packet.extend_from_slice(&padding.to_le_bytes());
            let _ = self.transport.send_raw(&packet).await;

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
        let mut last_send = std::time::Instant::now()
            .checked_sub(HEARTBEAT_INTERVAL)
            .unwrap_or_else(std::time::Instant::now);
        while !self.player_index_confirmed && start.elapsed() < timeout {
            let _ = self.maybe_send_start_gameplay_heartbeat().await;
            if last_send.elapsed() >= HEARTBEAT_INTERVAL {
                let _ = self.send_heartbeat_raw().await;
                last_send = std::time::Instant::now();
            }
            if let Ok(Some(data)) = self.transport.recv_raw_timeout(Duration::from_millis(10)).await {
                if !data.is_empty() && (data[0] & 0x1F) == MessageType::ServerToClientHeartbeat as u8 {
                    let _ = self.process_server_heartbeat(&data);
                }
            }
            tokio::time::sleep(Duration::from_millis(8)).await;
        }
    }

    fn build_init_action(&self) -> Vec<u8> {
        // Init action segment: UpdateBlueprintShelf (0x91)
        // Per docs: seq=0, player_index=1, unknown_a=1, unknown_b=0, payload_len=0x14
        // Payload bytes start with varshort(player_index) then fixed tail.
        let player_index = self.player_index.unwrap_or(1);
        let payload_tail: [u8; 19] = [
            0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];
        let mut payload_writer = BinaryWriter::with_capacity(3 + payload_tail.len());
        payload_writer.write_opt_u16(player_index);
        payload_writer.write_bytes(&payload_tail);
        let payload = payload_writer.into_vec();

        let mut writer = BinaryWriter::with_capacity(32 + payload.len());
        writer.write_opt_u32(0x01); // countAndSegments (hasSegments=1, count=0)
        writer.write_opt_u32(0x01); // segmentCount = 1
        writer.write_opt_u16(0x91); // action_type = UpdateBlueprintShelf
        writer.write_u32_le(0);     // seq = 0
        writer.write_opt_u16(player_index);
        writer.write_opt_u32(1);    // unknown_a = 1
        writer.write_opt_u32(0);    // unknown_b = 0
        writer.write_opt_u32(payload.len() as u32);
        writer.write_bytes(&payload);

        writer.into_vec()
    }

    async fn send_start_gameplay_heartbeat(&mut self) -> Result<()> {
        // ClientChangedState(0x07) sent as a gameplay heartbeat with sync actions (flags=0x1e).
        let debug = std::env::var("FACTORIO_DEBUG").is_ok();
        let msg_id = self.next_heartbeat_msg_id();
        let reliable = self.next_reliable();
        let type_byte = if reliable { 0x26 } else { 0x06 };

        let client_tick = self.compute_client_tick();
        let server_tick_echo = self.server_tick_echo();
        let trailer = [0x01, 0x03, 0x07];

        let mut packet = Vec::with_capacity(22 + trailer.len());
        packet.push(type_byte);
        packet.push(0x1e);
        packet.extend_from_slice(&msg_id.to_le_bytes());
        packet.extend_from_slice(&self.peer_constant.to_le_bytes());
        packet.extend_from_slice(&client_tick.to_le_bytes());
        packet.extend_from_slice(&[0u8; 4]);
        packet.extend_from_slice(&server_tick_echo.to_le_bytes());
        packet.extend_from_slice(&[0u8; 4]);
        packet.extend_from_slice(&trailer);

        if debug {
            eprintln!(
                "[DEBUG] Start gameplay heartbeat (ClientChangedState=0x07) client_tick={} server_tick_echo={}",
                client_tick, server_tick_echo
            );
        }

        self.pending_confirms.clear();
        self.last_gameplay_send_at = Some(std::time::Instant::now());
        self.transport.send_raw(&packet).await
    }

    async fn maybe_send_start_gameplay_heartbeat(&mut self) -> Result<()> {
        if self.pending_start_gameplay
            && self.start_sending_tick.is_some()
            && self.confirmed_tick != 0
        {
            self.send_start_gameplay_heartbeat().await?;
            self.pending_start_gameplay = false;
        }
        Ok(())
    }

    async fn send_init_action(&mut self) -> Result<()> {
        let data = self.build_init_action();
        let debug = std::env::var("FACTORIO_DEBUG").is_ok();
        if debug {
            eprintln!(
                "[DEBUG] Init action: len={} bytes={:02x?}",
                data.len(),
                &data[..data.len().min(24)]
            );
        }
        // PCAP shows flags=0x06 for init action (HasTickClosures | SingleTickClosure)
        self.send_action_packet(0x06, &data).await
    }

    async fn maybe_send_init_action(&mut self) -> Result<()> {
        self.maybe_send_start_gameplay_heartbeat().await?;
        if self.pending_init_action
            && self.player_index_confirmed
            && self.player_index.is_some()
            && self.confirmed_tick != 0
            && self.start_sending_tick.is_some()
        {
            self.send_init_action().await?;
            self.pending_init_action = false;
        }
        Ok(())
    }

    /// Compute the next client tick for a gameplay packet.
    /// Real client increments by 1 per C2S heartbeat/action, not wall-clock time.
    fn compute_client_tick(&mut self) -> u32 {
        if let Some(start_tick) = self.start_sending_tick {
            if self.client_tick < start_tick {
                self.client_tick = start_tick;
            }
            let tick = self.client_tick;
            self.client_tick = self.client_tick.wrapping_add(1);
            return tick;
        }

        let target_tick = if self.confirmed_tick != 0 {
            self.confirmed_tick.wrapping_add(1 + self.client_tick_lead)
        } else if self.server_tick != 0 {
            self.server_tick.wrapping_add(1 + self.client_tick_lead)
        } else {
            1 + self.client_tick_lead
        };

        // Initialize or reset client_tick if it's 0 or too far behind target
        let min_tick = target_tick.wrapping_sub(10);
        if self.client_tick == 0 || self.client_tick < min_tick {
            self.client_tick = target_tick;
        }

        // Clamp to not get too far ahead of confirmed_tick
        if self.confirmed_tick != 0 {
            let max_ahead = self.client_tick_lead.saturating_add(5);
            let max_tick = self.confirmed_tick.wrapping_add(max_ahead);
            if self.client_tick > max_tick {
                self.client_tick = max_tick;
            }
        }

        let tick = self.client_tick;
        self.client_tick = self.client_tick.wrapping_add(1);
        tick
    }

    fn server_tick_echo(&self) -> u32 {
        // Per docs: server_tick_echo = confirmed_tick + 1
        // This echoes back the tick we've confirmed receiving from the server.
        if self.confirmed_tick != 0 {
            self.confirmed_tick.wrapping_add(1)
        } else if self.server_tick != 0 {
            self.server_tick.wrapping_add(1)
        } else {
            0
        }
    }

    fn next_heartbeat_msg_id(&mut self) -> u16 {
        let id = self.msg_id;
        self.msg_id = self.msg_id.wrapping_add(1);
        id
    }

    /// Send a heartbeat with state data (sync actions).
    /// Real client format: type(1) + flags(1) + msg_id(2) + peer_constant(2) + tick_sync(2) + tick(4) + padding(2) + state_data
    async fn send_state_heartbeat(&mut self, state_data: &[u8]) -> Result<()> {
        let debug = std::env::var("FACTORIO_DEBUG").is_ok();
        let reliable = self.next_reliable();
        let msg_id = self.next_heartbeat_msg_id();

        // tick_sync = low 16 bits, tick = high bits shifted, padding = 0 (or 0xffff when unknown)
        let (tick_sync, tick, padding): (u16, u32, u16) = if self.confirmed_tick == 0 {
            (0xffff, 0xffffffff, 0xffff)
        } else {
            ((self.confirmed_tick & 0xFFFF) as u16, self.confirmed_tick >> 16, 0x0000)
        };

        if debug {
            eprintln!(
                "[DEBUG] send_state_heartbeat: state={:02x?} msg_id={} tick_sync=0x{:04x} tick=0x{:08x}",
                state_data, msg_id, tick_sync, tick
            );
        }

        let type_byte = if reliable { 0x26 } else { 0x06 };
        let mut packet = Vec::with_capacity(14 + state_data.len());

        // Real format: type + flags + msg_id + peer_constant + tick_sync(u16) + tick(u32) + padding(u16) + data
        packet.push(type_byte);
        packet.push(0x10); // flags=0x10 (HasSynchronizerActions) for state data
        packet.extend_from_slice(&msg_id.to_le_bytes());
        packet.extend_from_slice(&self.peer_constant.to_le_bytes());
        packet.extend_from_slice(&tick_sync.to_le_bytes());
        packet.extend_from_slice(&tick.to_le_bytes());
        packet.extend_from_slice(&padding.to_le_bytes());
        packet.extend_from_slice(state_data);

        if debug {
            eprintln!("[DEBUG] C2S state_heartbeat packet ({} bytes): {:02x?}", packet.len(), &packet[..packet.len().min(30)]);
        }

        self.pending_confirms.clear();
        self.transport.send_raw(&packet).await
    }

    /// Send the initial state heartbeat with 0xff tick values (state 01 03 02).
    /// Real client format: type(1) + flags(1) + msg_id(2) + peer_constant(2) + tick_sync(2) + tick(4) + padding(2) + state = 17 bytes
    /// Example from PCAP: 2610cba36a23ffffffffffffffff010302
    async fn send_initial_state_heartbeat_with_ff(&mut self) -> Result<()> {
        let reliable = self.next_reliable();
        let msg_id = self.next_heartbeat_msg_id();

        let type_byte = if reliable { 0x26 } else { 0x06 };
        let mut packet = Vec::with_capacity(17);

        packet.push(type_byte);
        packet.push(0x10); // flags=0x10 (HasSynchronizerActions)
        packet.extend_from_slice(&msg_id.to_le_bytes());
        packet.extend_from_slice(&self.peer_constant.to_le_bytes());
        // Use confirmed_tick from Accept if available, otherwise 0xff
        let (tick_sync, tick, padding): (u16, u32, u16) = if self.confirmed_tick != 0 {
            ((self.confirmed_tick & 0xFFFF) as u16, self.confirmed_tick >> 16, 0x0000)
        } else {
            (0xffff, 0xffffffff, 0xffff)
        };
        packet.extend_from_slice(&tick_sync.to_le_bytes());
        packet.extend_from_slice(&tick.to_le_bytes());
        packet.extend_from_slice(&padding.to_le_bytes());
        // State data: 01 03 02
        packet.push(0x01);
        packet.push(0x03);
        packet.push(0x02);

        self.pending_confirms.clear();
        self.transport.send_raw(&packet).await
    }

    /// Send a plain sync heartbeat (no state data) during initial sync phase.
    /// Real client format: type(1) + flags(1) + msg_id(2) + peer_constant(2) + tick_sync(2) + tick(4) + padding(2) = 14 bytes
    async fn send_initial_sync_heartbeat(&mut self) -> Result<()> {
        let reliable = self.next_reliable();
        let msg_id = self.next_heartbeat_msg_id();

        let type_byte = if reliable { 0x26 } else { 0x06 };
        let mut packet = Vec::with_capacity(14);

        packet.push(type_byte);
        packet.push(0x00); // flags=0 for plain heartbeat
        packet.extend_from_slice(&msg_id.to_le_bytes());
        packet.extend_from_slice(&self.peer_constant.to_le_bytes());
        // Use confirmed_tick from Accept if available, otherwise 0xff
        let (tick_sync, tick, padding): (u16, u32, u16) = if self.confirmed_tick != 0 {
            ((self.confirmed_tick & 0xFFFF) as u16, self.confirmed_tick >> 16, 0x0000)
        } else {
            (0xffff, 0xffffffff, 0xffff)
        };
        packet.extend_from_slice(&tick_sync.to_le_bytes());
        packet.extend_from_slice(&tick.to_le_bytes());
        packet.extend_from_slice(&padding.to_le_bytes());

        self.pending_confirms.clear();
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

        self.parse_map_ready_fragment(data, debug)
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
        if data.len() < pos + 4 {
            return None;
        }
        let _server_tick = u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
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
                    self.update_server_tick(map_tick as u32, debug, "map_ready");
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
            if self.skip_sync_action_data(&mut reader, action, false).is_err() {
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

    /// Send empty heartbeat using PCAP-derived Factorio 2.0 format.
    /// Format: type(1) + flags(1) + msg_id(2) + peer_constant(2) + tick(4) + tick2(4)
    async fn send_heartbeat_raw(&mut self) -> Result<()> {
        let msg_id = self.next_heartbeat_msg_id();
        let reliable = self.next_reliable();
        let debug = std::env::var("FACTORIO_DEBUG").is_ok();
        let type_byte = if reliable { 0x26 } else { 0x06 };

        // Only send gameplay heartbeats after server tells us to start sending tick closures
        // and after the initial gameplay start sequence has completed.
        let can_gameplay = self.start_sending_tick.is_some()
            && self.server_tick != 0
            && !self.pending_start_gameplay
            && !self.pending_init_action;

        // Real client format: type(1) + flags(1) + msg_id(2) + peer_constant(2) + tick1(4) + tick2(4) [+ extra data]
        // Note: flags byte comes BEFORE msg_id and peer_constant!

        if can_gameplay {
            // Per docs/binary-reverse-engineering.md lines 199-214:
            // flags 0x0E gameplay heartbeat:
            // [6-9]   client_tick (u32 LE)
            // [10-13] padding1 = 0
            // [14-17] server_tick_echo (u32 LE) = confirmed_tick + 1
            // [18-21] padding2 = 0
            let client_tick = self.compute_client_tick();
            // Use server_tick directly (not +1) to stay within latency window
            let server_tick_echo = self.server_tick_echo();

            let mut packet = Vec::new();
            packet.push(type_byte);
            packet.push(0x0e); // flags = HasTickClosures | SingleTickClosure | LoadTickOnly
            packet.extend_from_slice(&msg_id.to_le_bytes());
            packet.extend_from_slice(&self.peer_constant.to_le_bytes());
            packet.extend_from_slice(&client_tick.to_le_bytes());
            packet.extend_from_slice(&[0u8; 4]); // padding1
            packet.extend_from_slice(&server_tick_echo.to_le_bytes());
            packet.extend_from_slice(&[0u8; 4]); // padding2

            if debug && self.debug_gameplay_heartbeats < 5 {
                eprintln!(
                    "[DEBUG] C2S gameplay heartbeat: reliable={} client_tick={} server_tick_echo={} len={} hex={:02x?}",
                    reliable, client_tick, server_tick_echo, packet.len(), &packet
                );
                self.debug_gameplay_heartbeats += 1;
            }

            self.pending_confirms.clear();
            self.last_gameplay_send_at = Some(std::time::Instant::now());
            self.transport.send_raw(&packet).await
        } else {
            // During initial sync - 14-byte format with tick_sync/tick/padding
            let (tick_sync, tick, padding): (u16, u32, u16) = if self.confirmed_tick != 0 {
                ((self.confirmed_tick & 0xFFFF) as u16, self.confirmed_tick >> 16, 0x0000)
            } else {
                (0xffff, 0xffffffff, 0xffff)
            };

            let mut packet = Vec::new();
            packet.push(type_byte);
            packet.push(0x00); // flags=0x00 for sync
            packet.extend_from_slice(&msg_id.to_le_bytes());
            packet.extend_from_slice(&self.peer_constant.to_le_bytes());
            packet.extend_from_slice(&tick_sync.to_le_bytes());
            packet.extend_from_slice(&tick.to_le_bytes());
            packet.extend_from_slice(&padding.to_le_bytes());

            self.pending_confirms.clear();
            self.last_gameplay_send_at = Some(std::time::Instant::now());
            self.transport.send_raw(&packet).await
        }
    }

    /// Send a heartbeat to keep connection alive (14-byte format)
    pub async fn send_heartbeat(&mut self) -> Result<()> {
        self.send_heartbeat_raw().await
    }

    /// Send heartbeat with action data.
    /// Per docs/binary-reverse-engineering.md lines 215-226:
    /// [6-9]   client_tick
    /// [10-13] padding1 = 0
    /// [14..]  action_data (variable)
    /// [..]    server_tick_echo (u32 LE) = last game tick + 1
    /// [..]    padding2 = 0
    async fn send_action_packet(&mut self, flags: u8, action_data: &[u8]) -> Result<()> {
        if self.start_sending_tick.is_none() {
            return self.send_heartbeat_raw().await;
        }
        let msg_id = self.next_heartbeat_msg_id();
        let reliable = self.next_reliable();

        let client_tick = if self.confirmed_tick == 0 {
            0xffffffffu32
        } else {
            self.compute_client_tick()
        };
        // Use server_tick directly to stay within latency window
        let server_tick_echo = self.server_tick_echo();

        let type_byte = if reliable { 0x26 } else { 0x06 };
        let mut packet = Vec::new();

        // Format: type + flags + msg_id + peer + client_tick + zeros + action_data + server_tick_echo + zeros
        packet.push(type_byte);
        packet.push(flags);
        packet.extend_from_slice(&msg_id.to_le_bytes());
        packet.extend_from_slice(&self.peer_constant.to_le_bytes());
        packet.extend_from_slice(&client_tick.to_le_bytes());  // [6-9] client_tick
        packet.extend_from_slice(&[0u8; 4]);                   // [10-13] padding1
        packet.extend_from_slice(action_data);                 // [14..] action_data
        packet.extend_from_slice(&server_tick_echo.to_le_bytes()); // server_tick_echo
        packet.extend_from_slice(&[0u8; 4]);                   // padding2

        let debug = std::env::var("FACTORIO_DEBUG").is_ok();
        if debug && self.debug_action_packets < 3 {
            eprintln!(
                "[DEBUG] C2S action: msg_id={} flags=0x{:02x} client_tick=0x{:08x} len={}",
                msg_id,
                flags,
                client_tick,
                packet.len()
            );
            self.debug_action_packets += 1;
        }

        self.pending_confirms.clear();
        self.last_gameplay_send_at = Some(std::time::Instant::now());
        self.transport.send_raw(&packet).await
    }

    fn next_reliable(&mut self) -> bool {
        self.reliable_rng.next_bool()
    }

    /// Send a heartbeat with input actions
    pub async fn send_heartbeat_with_actions(&mut self, actions: &[InputAction]) -> Result<()> {
        if !actions.is_empty() {
            for action in actions {
                self.pending_actions.push_back(action.clone());
            }
        }
        Ok(())
    }

    async fn flush_gameplay(&mut self) -> Result<()> {
        if self.state != ConnectionState::InGame {
            return Ok(());
        }
        let now = std::time::Instant::now();
        let should_send = self
            .last_gameplay_send_at
            .map(|t| now.duration_since(t) >= HEARTBEAT_INTERVAL)
            .unwrap_or(true);
        if !should_send {
            return Ok(());
        }

        let _ = self.maybe_send_start_gameplay_heartbeat().await?;

        if !self.allow_actions {
            if let Some(start) = self.game_start {
                if start.elapsed() > Duration::from_millis(500) {
                    self.allow_actions = true;
                }
            }
        }

        if self.pending_init_action && self.player_index.is_none() {
            return self.send_heartbeat_raw().await;
        }

        if self.pending_start_gameplay || self.pending_init_action || !self.allow_actions {
            return self.send_heartbeat_raw().await;
        }

        if let Some(action) = self.pending_actions.pop_front() {
            let player_index = match self.player_index {
                Some(idx) => idx,
                None => {
                    self.pending_actions.push_front(action);
                    return self.send_heartbeat_raw().await;
                }
            };
            let encoded = action.encode(&mut self.chat_seq, player_index)?;
            return self.send_action_packet(encoded.flags, &encoded.data).await;
        }

        self.send_heartbeat_raw().await
    }

    /// Send a chat message
    /// Per docs line 296-300: WriteToConsole is a segmentable action with special format
    pub async fn send_chat(&mut self, message: &str) -> Result<()> {
        if self.state != ConnectionState::InGame {
            return Err(Error::InvalidPacket("must be in game to send chat".into()));
        }

        // Build chat segment action data:
        // payload_bytes = varshort(player_index) + string(message)
        let player_index = self.player_index.unwrap_or(1);
        let mut payload_writer = BinaryWriter::with_capacity(3 + message.len() + 5);
        payload_writer.write_opt_u16(player_index);
        payload_writer.write_string(message);
        let payload = payload_writer.into_vec();

        // Build segment: countAndSegments + segmentCount + action_type + seq + player_index +
        //                unknown_a + unknown_b + string_length + payload
        let mut writer = BinaryWriter::with_capacity(32 + payload.len());
        writer.write_opt_u32(0x01); // countAndSegments (hasSegments=1, count=0)
        writer.write_opt_u32(0x01); // segmentCount = 1
        writer.write_opt_u16(0x68); // action_type = WriteToConsole
        writer.write_u32_le(0);     // seq = 0
        writer.write_opt_u16(player_index);
        writer.write_opt_u32(1);    // unknown_a = 1
        writer.write_opt_u32(0);    // unknown_b = 0
        writer.write_opt_u32(payload.len() as u32);
        writer.write_bytes(&payload);
        let data = writer.into_vec();

        self.send_action_packet(0x06, &data).await
    }

    /// Send start walking action
    pub async fn send_walk(&mut self, direction: u8) -> Result<()> {
        if self.state != ConnectionState::InGame {
            return Err(Error::InvalidPacket("must be in game".into()));
        }
        let dir = Direction::from_u8(direction)
            .ok_or_else(|| Error::InvalidPacket("invalid walking direction".into()))?;
        let action = InputAction::move_direction(dir);

        // Start walking state tracking
        self.walking = true;
        self.walking_direction = direction;
        self.last_position_tick = self.server_tick;

        self.send_heartbeat_with_actions(&[action]).await
    }

    /// Send stop walking action
    pub async fn send_stop_walk(&mut self) -> Result<()> {
        if self.state != ConnectionState::InGame {
            return Err(Error::InvalidPacket("must be in game".into()));
        }

        // Update position for final ticks before stopping
        self.update_position_from_ticks();

        self.walking = false;

        let action = InputAction::stop_walking();
        self.send_heartbeat_with_actions(&[action]).await
    }

    /// Update player position based on elapsed ticks while walking
    fn update_position_from_ticks(&mut self) {
        if !self.walking {
            return;
        }

        let ticks_elapsed = self.server_tick.saturating_sub(self.last_position_tick);
        if ticks_elapsed == 0 {
            return;
        }

        // Character walking speed from prototype data (tiles per tick)
        let speed = self.character_speed;
        let distance = ticks_elapsed as f64 * speed;

        // Update position based on direction
        // Directions: 0=N, 1=NE, 2=E, 3=SE, 4=S, 5=SW, 6=W, 7=NW
        let diag = 0.7071; // 1/sqrt(2)
        match self.walking_direction {
            0 => self.player_y -= distance,                                    // North
            1 => { self.player_x += distance * diag; self.player_y -= distance * diag; } // NE
            2 => self.player_x += distance,                                    // East
            3 => { self.player_x += distance * diag; self.player_y += distance * diag; } // SE
            4 => self.player_y += distance,                                    // South
            5 => { self.player_x -= distance * diag; self.player_y += distance * diag; } // SW
            6 => self.player_x -= distance,                                    // West
            7 => { self.player_x -= distance * diag; self.player_y -= distance * diag; } // NW
            _ => {}
        }

        self.last_position_tick = self.server_tick;
    }

    /// Start mining at a position
    pub async fn send_mine(&mut self, x: f64, y: f64) -> Result<()> {
        if self.state != ConnectionState::InGame {
            return Err(Error::InvalidPacket("must be in game".into()));
        }
        let position = MapPosition::from_tiles(x, y);
        self.last_mine_position = Some(position);
        let action = InputAction::begin_mining_terrain(position);
        self.send_heartbeat_with_actions(&[action]).await
    }

    /// Begin mining (action type 0x02)
    pub async fn send_begin_mine(&mut self) -> Result<()> {
        if self.state != ConnectionState::InGame {
            return Err(Error::InvalidPacket("must be in game".into()));
        }
        let action = InputAction::begin_mining();
        self.send_heartbeat_with_actions(&[action]).await
    }

    /// Stop mining
    pub async fn send_stop_mine(&mut self) -> Result<()> {
        if self.state != ConnectionState::InGame {
            return Err(Error::InvalidPacket("must be in game".into()));
        }
        self.last_mine_position.take();
        let action = InputAction::stop_mining();
        self.send_heartbeat_with_actions(&[action]).await
    }

    /// Build/place an item at a position
    pub async fn send_build(&mut self, x: f64, y: f64, direction: u8) -> Result<()> {
        if self.state != ConnectionState::InGame {
            return Err(Error::InvalidPacket("must be in game".into()));
        }
        let _ = (x, y, direction);
        Err(Error::InvalidPacket("build action encoding not implemented".into()))
    }

    /// Rotate entity at position
    pub async fn send_rotate(&mut self, x: f64, y: f64, reverse: bool) -> Result<()> {
        if self.state != ConnectionState::InGame {
            return Err(Error::InvalidPacket("must be in game".into()));
        }
        let _ = (x, y, reverse);
        Err(Error::InvalidPacket("rotate action encoding not implemented".into()))
    }

    /// Craft items
    pub async fn send_craft(&mut self, recipe_id: u16, count: u32) -> Result<()> {
        if self.state != ConnectionState::InGame {
            return Err(Error::InvalidPacket("must be in game".into()));
        }
        let _ = (recipe_id, count);
        Err(Error::InvalidPacket("craft action encoding not implemented".into()))
    }

    /// Open character inventory GUI
    pub async fn send_open_inventory(&mut self) -> Result<()> {
        if self.state != ConnectionState::InGame {
            return Err(Error::InvalidPacket("must be in game".into()));
        }
        Err(Error::InvalidPacket("open inventory action encoding not implemented".into()))
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
        let mut result = None;
        if let Some(data) = self.transport.recv_raw_timeout(Duration::from_millis(1)).await? {
            if !data.is_empty() {
                let msg_type = data[0] & 0x1F;

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
                    result = Some(ReceivedPacket::Heartbeat { tick: self.server_tick });
                } else if msg_type == MessageType::TransferBlock as u8 {
                    result = Some(ReceivedPacket::MapBlock { size: data.len() });
                } else {
                    result = Some(ReceivedPacket::Unknown { msg_type, size: data.len() });
                }
            }
        }

        if self.state == ConnectionState::InGame {
            let _ = self.flush_gameplay().await;
        }

        Ok(result)
    }

    fn process_server_heartbeat(&mut self, data: &[u8]) -> Result<()> {
        // Skip packets with 0x40 flag (fragmented/different format)
        if data.is_empty() || (data[0] & 0x40) != 0 {
            return Ok(());
        }

        let debug = std::env::var("FACTORIO_DEBUG").is_ok();

        // S2C heartbeat base header:
        // type(1) + flags(1) + heartbeat_sequence(4) = 6 bytes
        let flags = if data.len() >= 2 { data[1] } else { return Ok(()); };
        let has_tick_closures = (flags & 0x02) != 0;
        let single_tick_closure = (flags & 0x04) != 0;

        let header_size = 6;
        if data.len() < header_size {
            return Ok(());
        }

        // Heartbeat sequence number (not a game tick).
        let server_seq = u32::from_le_bytes([data[2], data[3], data[4], data[5]]);
        if server_seq != 0 {
            self.server_seq = server_seq;
        }

        if debug && self.debug_server_heartbeat_dumped < 6 {
            eprintln!(
                "[DEBUG] S2C heartbeat: len={} flags=0x{:02x} seq={} first_bytes={:02x?}",
                data.len(),
                flags,
                server_seq,
                &data[..data.len().min(20)]
            );
            self.debug_server_heartbeat_dumped += 1;
        }

        // Payload starts after header
        let mut pos = header_size;
        // HasTickClosures=0x02, SingleTickClosure=0x04
        if has_tick_closures && pos < data.len() {
            let single = (flags & 0x04) != 0;
            let mut reader = BinaryReader::new(&data[pos..]);
            if self.skip_tick_closures(&mut reader, single) {
                pos = pos.saturating_add(reader.position());
            } else if debug {
                eprintln!(
                    "[DEBUG] HB: failed to skip tick closures flags=0x{:02x}",
                    flags
                );
            }
        }

        // Tick confirmations (marker + flags + crc + confirmed_tick + padding = 15 bytes).
        // Keep the last confirmed_tick for server_tick_echo.
        let mut last_confirmed: Option<u32> = None;
        while pos + 15 <= data.len() && data[pos] == 0x02 && data[pos + 1] == 0x52 {
            let confirmed_tick = u32::from_le_bytes([
                data[pos + 7],
                data[pos + 8],
                data[pos + 9],
                data[pos + 10],
            ]);
            last_confirmed = Some(confirmed_tick);
            pos += 15;
        }
        if let Some(tick) = last_confirmed {
            self.update_confirmed_tick(tick, debug, "confirm");
        }
        while pos < data.len() && data[pos] == 0x00 {
            pos += 1;
        }
        if last_confirmed.is_none() {
            if let Some(tick) = self.scan_confirmed_tick(&data[header_size..]) {
                self.update_confirmed_tick(tick, debug, "confirm-scan");
            }
        }

        if pos < data.len() && (flags & 0x10) != 0 {
            let extra = &data[pos..];
            if debug {
                eprintln!(
                    "[DEBUG] HB extra: len={} head={:02x?}",
                    extra.len(),
                    &extra[..extra.len().min(24)]
                );
            }
            let mut parsed = self.apply_sync_actions(extra);
            if !parsed {
                parsed = self.find_and_apply_sync_actions(extra);
            }
            if !parsed && data.len() > header_size {
                parsed = self.find_and_apply_sync_actions(&data[header_size..]);
            }
            if !parsed {
                if debug {
                    eprintln!(
                        "[DEBUG] HB: sync action parse failed flags=0x{:02x} raw_head={:02x?}",
                        flags,
                        &data[..data.len().min(64)]
                    );
                }
                self.scan_latency_actions(extra);
            }
        }

        self.update_player_index_from_heartbeat(data);

        Ok(())
    }

    fn update_server_tick(&mut self, tick: u32, debug: bool, source: &str) {
        // Server tick tracks the latest tick closure tick from S2C heartbeats.
        if tick > 0 && tick != self.server_tick {
            if debug {
                eprintln!(
                    "[DEBUG] HB: server_tick {} -> {} ({})",
                    self.server_tick, tick, source
                );
            }
            self.server_tick = tick;
        }
        if self.confirmed_tick == 0 && tick > 0 {
            self.confirmed_tick = tick;
            self.tick_sync = (tick & 0xffff) as u16;
        }
    }

    fn update_confirmed_tick(&mut self, tick: u32, debug: bool, source: &str) {
        // Confirmed tick comes from tick confirmation records (can lag server_tick).
        if tick > 0 && tick != self.confirmed_tick {
            if debug {
                eprintln!(
                    "[DEBUG] HB: confirmed_tick {} -> {} ({})",
                    self.confirmed_tick, tick, source
                );
            }
            self.confirmed_tick = tick;
            self.tick_sync = (tick & 0xffff) as u16;
        }
    }

    fn scan_confirmed_tick(&self, data: &[u8]) -> Option<u32> {
        if data.len() < 15 {
            return None;
        }
        let mut last = None;
        for i in 0..=data.len().saturating_sub(15) {
            if data[i] != 0x02 || data[i + 1] != 0x52 {
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
            if confirmed_tick > 0 {
                last = Some(confirmed_tick);
            }
        }
        last
    }

    fn skip_tick_closures(&mut self, reader: &mut BinaryReader, single: bool) -> bool {
        let debug = std::env::var("FACTORIO_DEBUG").is_ok();

        // Standard tick closure parsing for non-empty closures
        let count = if single {
            1
        } else {
            match reader.read_opt_u32() {
                Ok(v) => v as usize,
                Err(_) => return false,
            }
        };
        if count == 0 {
            return true;
        }

        let mut last_tick: Option<u32> = None;
        for _ in 0..count {
            let tick = match reader.read_u64_le() {
                Ok(v) => v,
                Err(_) => {
                    if debug {
                        eprintln!("[DEBUG] HB: failed to read tick in tick closure");
                    }
                    return false;
                }
            };
            if tick > 0 && tick <= u32::MAX as u64 {
                last_tick = Some(tick as u32);
            }
            let count_and_segments = match reader.read_opt_u32() {
                Ok(v) => v,
                Err(_) => {
                    // May be end of closure data
                    return true;
                }
            };
            let action_count = (count_and_segments / 2) as usize;
            let has_segments = (count_and_segments & 1) != 0;

            // Sanity check - server shouldn't send many actions
            if action_count > 32 {
                return true; // Likely not action data
            }

            let mut current_player_index: u16 = 0xFFFF;
            for _ in 0..action_count {
                let action_type = match reader.read_u8() {
                    Ok(v) => v,
                    Err(_) => {
                        if debug {
                            eprintln!("[DEBUG] HB: failed to read action type");
                        }
                        return false;
                    }
                };
                let player_delta = match reader.read_opt_u16() {
                    Ok(v) => v,
                    Err(_) => {
                        if debug {
                            eprintln!("[DEBUG] HB: failed to read action player delta");
                        }
                        return false;
                    }
                };
                current_player_index = current_player_index.wrapping_add(player_delta);

                match action_type {
                    0x22 | 0x34 => {
                        // StopMovementInTheNextTick / ForceFullCRC: no payload.
                    }
                    0xe9 => {
                        // PlayerLeaveGame: u8 reason.
                        if reader.read_u8().is_err() {
                            if debug {
                                eprintln!("[DEBUG] HB: failed to read PlayerLeaveGame payload");
                            }
                            return false;
                        }
                    }
                    0x52 | 0x5e => {
                        // CheckCRCHeuristic / CheckCRC: ActionData::CrcData (u32 + u64).
                        if reader.read_u32_le().is_err() || reader.read_u64_le().is_err() {
                            if debug {
                                eprintln!("[DEBUG] HB: failed to read CRC action payload");
                            }
                            return false;
                        }
                    }
                    _ => {
                        let remaining = reader.remaining_slice();
                        let mut temp = Vec::with_capacity(1 + remaining.len());
                        temp.push(action_type);
                        temp.extend_from_slice(remaining);
                        let mut temp_reader = BinaryReader::new(&temp);
                        let action = match CodecInputAction::read_known(&mut temp_reader) {
                            Ok(action) => action,
                            Err(_) => {
                                if debug {
                                    eprintln!(
                                        "[DEBUG] HB: failed to parse input action type=0x{:02x}",
                                        action_type
                                    );
                                }
                                return false;
                            }
                        };
                        let consumed = temp_reader.position().saturating_sub(1);
                        if reader.skip(consumed).is_err() {
                            if debug {
                                eprintln!("[DEBUG] HB: failed to skip action payload");
                            }
                            return false;
                        }

                        // Track player actions
                        self.apply_player_action(current_player_index, &action, last_tick);
                    }
                }
            }
            if has_segments {
                let segment_count = match reader.read_opt_u32() {
                    Ok(v) => v as usize,
                    Err(_) => return false,
                };
                for _ in 0..segment_count {
                    let len = match reader.read_opt_u32() {
                        Ok(v) => v as usize,
                        Err(_) => return false,
                    };
                    if reader.skip(len).is_err() {
                        if debug {
                            eprintln!("[DEBUG] HB: failed to skip action segment");
                        }
                        return false;
                    }
                }
            }
        }

        if let Some(tick) = last_tick {
            self.update_server_tick(tick, debug, "heartbeat");
        }

        true
    }

    fn apply_sync_actions(&mut self, extra: &[u8]) -> bool {
        let debug = std::env::var("FACTORIO_DEBUG").is_ok();
        let mut reader = BinaryReader::new(extra);
        let count = match reader.read_opt_u32() {
            Ok(v) => v,
            Err(_) => return false,
        };
        if count == 0 {
            return true;
        }
        if count > 64 {
            return false;
        }
        for _ in 0..count {
            let action_type = match reader.read_u8() {
                Ok(v) => v,
                Err(_) => return false,
            };
            let action = match SynchronizerActionType::from_u8(action_type) {
                Some(v) => v,
                None => return false,
            };
            if self.skip_sync_action_data(&mut reader, action, true).is_err() {
                if debug {
                    eprintln!("[DEBUG] HB: failed to parse sync action 0x{:02x}", action_type);
                }
                return false;
            }
            if reader.read_u16_le().is_err() {
                return false;
            }
        }
        true
    }

    fn can_parse_sync_actions(&mut self, extra: &[u8]) -> bool {
        let mut reader = BinaryReader::new(extra);
        let count = match reader.read_opt_u32() {
            Ok(v) => v,
            Err(_) => return false,
        };
        if count > 64 {
            return false;
        }
        for _ in 0..count {
            let action_type = match reader.read_u8() {
                Ok(v) => v,
                Err(_) => return false,
            };
            let action = match SynchronizerActionType::from_u8(action_type) {
                Some(v) => v,
                None => return false,
            };
            if self.skip_sync_action_data(&mut reader, action, false).is_err() {
                return false;
            }
            if reader.read_u16_le().is_err() {
                return false;
            }
        }
        reader.position() == extra.len()
    }

    fn find_and_apply_sync_actions(&mut self, extra: &[u8]) -> bool {
        if extra.len() < 4 {
            return false;
        }
        for start in 0..extra.len() {
            let slice = &extra[start..];
            if self.can_parse_sync_actions(slice) {
                if self.apply_sync_actions(slice) {
                    let debug = std::env::var("FACTORIO_DEBUG").is_ok();
                    if debug && start != 0 {
                        eprintln!("[DEBUG] HB: sync action list found at offset {}", start);
                    }
                    return true;
                }
            }
        }
        false
    }

    fn skip_sync_action_data(&mut self, reader: &mut BinaryReader, action: SynchronizerActionType, apply: bool) -> Result<()> {
        match action {
            SynchronizerActionType::PeerDisconnect => {
                let _ = reader.read_u8()?;
                Ok(())
            }
            SynchronizerActionType::NewPeerInfo => {
                let _ = reader.read_string()?;
                Ok(())
            }
            SynchronizerActionType::ClientChangedState => {
                let _ = reader.read_u8()?;
                Ok(())
            }
            SynchronizerActionType::ClientShouldStartSendingTickClosures => {
                let tick = reader.read_u64_le()?;
                if apply {
                    self.handle_start_sending_tick(tick);
                }
                Ok(())
            }
            SynchronizerActionType::MapReadyForDownload => self.skip_map_ready_for_download(reader),
            SynchronizerActionType::MapLoadingProgressUpdate
            | SynchronizerActionType::MapSavingProgressUpdate
            | SynchronizerActionType::MapDownloadingProgressUpdate
            | SynchronizerActionType::CatchingUpProgressUpdate
            | SynchronizerActionType::PeerDroppingProgressUpdate => {
                let _ = reader.read_u8()?;
                Ok(())
            }
            SynchronizerActionType::SavingForUpdate
            | SynchronizerActionType::PlayerDesynced
            | SynchronizerActionType::BeginPause
            | SynchronizerActionType::EndPause
            | SynchronizerActionType::GameEnd => Ok(()),
            SynchronizerActionType::SkippedTickClosure => {
                let _ = reader.read_u64_le()?;
                Ok(())
            }
            SynchronizerActionType::SkippedTickClosureConfirm => {
                let _ = reader.read_u64_le()?;
                Ok(())
            }
            SynchronizerActionType::ChangeLatency => {
                let latency = reader.read_u8()?;
                if apply {
                    self.update_latency(latency, action);
                }
                Ok(())
            }
            SynchronizerActionType::IncreasedLatencyConfirm => {
                let _tick = reader.read_u64_le()?;
                let latency = reader.read_u8()?;
                if apply {
                    self.update_latency(latency, action);
                }
                Ok(())
            }
            SynchronizerActionType::SavingCountdown => {
                let _ = reader.read_u64_le()?;
                let _ = reader.read_u32_le()?;
                Ok(())
            }
        }
    }

    fn skip_map_ready_for_download(&mut self, reader: &mut BinaryReader) -> Result<()> {
        let transfer_size = reader.read_u64_le()?;
        let _auxiliary = reader.read_u64_le()?;
        let _crc = reader.read_u32_le()?;
        let map_tick = reader.read_u64_le()?;
        if transfer_size > 500_000 && transfer_size < 50_000_000 {
            self.map_transfer_size = Some(transfer_size as u32);
        }
        if map_tick > 0 && map_tick <= u32::MAX as u64 {
            let debug = std::env::var("FACTORIO_DEBUG").is_ok();
            self.update_server_tick(map_tick as u32, debug, "map_ready");
        }
        let _ = reader.read_u32_le()?;
        let _ = reader.read_u32_le()?;
        let _ = reader.read_bool()?;
        let _ = reader.read_bool()?;

        let entries = reader.read_opt_u32()? as usize;
        for _ in 0..entries {
            let _ = reader.read_string()?;
            let _ = reader.read_u32_le()?;
        }

        let entries = reader.read_opt_u32()? as usize;
        for _ in 0..entries {
            let _ = reader.read_string()?;
            self.skip_script_registrations(reader)?;
        }

        let entries = reader.read_opt_u32()? as usize;
        for _ in 0..entries {
            let _ = reader.read_string()?;
            let list_len = reader.read_opt_u32()? as usize;
            for _ in 0..list_len {
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
        self.start_sending_tick = Some(start_tick);
        self.allow_actions = true;
        self.client_tick = start_tick;
        self.base_tick = start_tick;
        self.pending_start_gameplay = true;
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
        self.latency_value = Some(latency);
        let new_lead = latency.saturating_sub(3) as u32;
        let new_lead = new_lead.max(CLIENT_TICK_LEAD_MIN);
        if debug {
            eprintln!(
                "[DEBUG] HB: {:?} latency={} -> lead={}",
                action, latency, new_lead
            );
        }
        self.client_tick_lead = new_lead;
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
            if latency == 0 || latency > 120 {
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

        // Handle reliable vs unreliable header
        let is_reliable = (data[0] & 0x20) != 0;
        let header_size = if is_reliable { 8 } else { 6 };
        let payload_start = header_size + 8;

        if data.len() < payload_start {
            return;
        }

        let mut pos = payload_start;
        while pos + 11 <= data.len() {
            if data[pos] != 0x02 || data[pos + 1] != 0x52 {
                break;
            }
            pos += 11;
            while pos < data.len() && data[pos] == 0x00 {
                pos += 1;
            }
        }

        if pos >= data.len() {
            return;
        }

        let extra = &data[pos..];
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
        let player_index = match self
            .extract_player_index_from_player_join(extra)
            .or_else(|| self.extract_player_index_from_extra(extra))
            .or_else(|| self.extract_player_index_from_actions(extra))
        {
            Some(idx) => idx,
            None => return,
        };

        // Only update player_index if we don't have one or if the current value
        // looks like a peer_id (common early mis-parse), or if in InGame for re-sync.
        let allow_update = self.player_index.is_none()
            || self.player_index == self.peer_id
            || (self.state == ConnectionState::InGame && !self.pending_init_action);
        if !allow_update {
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
            if last_heartbeat.elapsed() >= HEARTBEAT_INTERVAL {
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
                // Check if this is our own join
                if username == &self.username && *player_index_plus_one > 0 {
                    self.player_index = Some(*player_index_plus_one);
                    self.player_index_confirmed = true;
                    if self.peer_id.is_none() {
                        self.peer_id = Some(*peer_id);
                    }
                    if debug {
                        eprintln!("[DEBUG] Self joined: player_index={}", player_index_plus_one);
                    }
                } else if *player_index_plus_one > 0 {
                    // Track other player - assign initial position from map if available
                    let is_new = !self.other_players.contains_key(player_index_plus_one);
                    let initial_pos = if is_new {
                        self.assign_character_position()
                    } else {
                        None
                    };

                    let state = self.other_players.entry(*player_index_plus_one).or_insert_with(|| {
                        let (x, y) = initial_pos.unwrap_or((0.0, 0.0));
                        PlayerState {
                            player_index: *player_index_plus_one,
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
                            username, player_index_plus_one, state.x, state.y, pos_source);
                    }
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
                if Some(player_index) == self.player_index {
                    // Update self position before changing direction
                    self.update_position_from_ticks();
                    self.walking = true;
                    self.last_position_tick = current_tick;
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
                            let speed = self.character_speed * ticks as f64;
                            state.x += state.walk_direction.0 * speed;
                            state.y += state.walk_direction.1 * speed;
                        }
                    }
                    state.walking = true;
                    state.walk_direction = (*direction_x, *direction_y);
                    state.last_tick = current_tick;
                }
            }

            CodecInputAction::StopWalking => {
                if Some(player_index) == self.player_index {
                    self.update_position_from_ticks();
                    self.walking = false;
                } else if let Some(state) = self.other_players.get_mut(&player_index) {
                    // Apply final movement before stopping
                    if state.walking {
                        let ticks = current_tick.saturating_sub(state.last_tick);
                        if ticks > 0 {
                            let speed = self.character_speed * ticks as f64;
                            state.x += state.walk_direction.0 * speed;
                            state.y += state.walk_direction.1 * speed;
                        }
                    }
                    state.walking = false;
                    state.last_tick = current_tick;
                }
            }

            CodecInputAction::BeginMining { .. } | CodecInputAction::BeginMiningTerrain { .. } => {
                if Some(player_index) != self.player_index {
                    if let Some(state) = self.other_players.get_mut(&player_index) {
                        state.mining = true;
                    }
                }
            }

            CodecInputAction::StopMining => {
                if Some(player_index) != self.player_index {
                    if let Some(state) = self.other_players.get_mut(&player_index) {
                        state.mining = false;
                    }
                }
            }

            CodecInputAction::ChangeShootingState { state, .. } => {
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
        let current_tick = self.server_tick;
        let players: Vec<u16> = self.other_players.keys().cloned().collect();
        for player_index in players {
            if let Some(state) = self.other_players.get_mut(&player_index) {
                if state.walking {
                    let ticks = current_tick.saturating_sub(state.last_tick);
                    if ticks > 0 {
                        let speed = self.character_speed * ticks as f64;
                        state.x += state.walk_direction.0 * speed;
                        state.y += state.walk_direction.1 * speed;
                        state.last_tick = current_tick;
                    }
                }
            }
        }
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
