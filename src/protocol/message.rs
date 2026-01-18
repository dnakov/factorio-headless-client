//! Factorio network message types
//! Based on reverse-engineered protocol from pcap analysis

use crate::codec::{BinaryReader, BinaryWriter, Direction, MapPosition};
use crate::codec::input_action::InputAction as CodecInputAction;
use crate::error::{Error, Result};
use super::rand_u32;

use bitflags::bitflags;

bitflags! {
    /// Flags for heartbeat message deserialization (from C# reference)
    /// Used in both ClientToServerHeartbeat and ServerToClientHeartbeat
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct DeserializationMask: u8 {
        /// Message contains RequestsForHeartbeat array
        const HAS_REQUESTS = 0x01;
        /// Message contains TickClosures array
        const HAS_TICK_CLOSURES = 0x02;
        /// TickClosures contains only a single closure (not array)
        const SINGLE_TICK_CLOSURE = 0x04;
        /// Load tick only - don't parse input actions in closures
        const LOAD_TICK_ONLY = 0x08;
        /// Message contains SynchronizerActions array
        const HAS_SYNC_ACTIONS = 0x10;
    }
}

/// A TickClosure contains the input actions for a specific game tick.
/// Format from C# reference (TickClosure.cs):
/// - tCount: VarInt where (count >> 1) = action count, (count & 1) = has segments
/// - For each action: PlayerIndex (VarShort) + InputAction data
/// - If has_segments: Array of InputActionSegments
#[derive(Debug, Clone)]
pub struct TickClosure {
    /// The game tick this closure is for
    pub tick: u32,
    /// Input actions with their player indices
    pub input_actions: Vec<(u16, CodecInputAction)>,
    /// Optional action segments (for large/batched actions)
    pub segments: Vec<Vec<u8>>,
}

impl TickClosure {
    pub fn new(tick: u32) -> Self {
        Self {
            tick,
            input_actions: Vec::new(),
            segments: Vec::new(),
        }
    }

    pub fn with_actions(tick: u32, actions: Vec<(u16, CodecInputAction)>) -> Self {
        Self {
            tick,
            input_actions: actions,
            segments: Vec::new(),
        }
    }

    /// Encode the tick closure for sending to server
    /// Note: This encodes just the closure content, not the tick itself
    /// (tick is written separately in the heartbeat)
    ///
    /// Per docs/binary-reverse-engineering.md lines 236-244:
    /// - countAndHasSegments = count*2 + hasSegments (varlen encoding)
    /// - Each input action: [action_type][player_index_delta][action_data]
    /// - player_index_delta is added to prev (initial prev = 0xFFFF)
    /// - So first action uses delta = player_index + 1
    pub fn encode_content(&self, writer: &mut BinaryWriter) {
        let action_count = self.input_actions.len();
        let has_segments = !self.segments.is_empty();
        // tCount: high bits = action count, low bit = has segments
        let t_count = ((action_count << 1) | (has_segments as usize)) as u32;
        writer.write_opt_u32(t_count);

        // Write each action in protocol order: action_type, player_index_delta, action_data
        // player_index_delta is computed from previous player index (initial prev = 0xFFFF)
        let mut prev_player_index: u16 = 0xFFFF;
        for (player_idx, action) in &self.input_actions {
            // Delta encoding: first action with player_index N has delta = N - 0xFFFF = N + 1 (wrapping)
            let delta = player_idx.wrapping_sub(prev_player_index);
            action.write_protocol_order(writer, delta);
            prev_player_index = *player_idx;
        }

        // Write segments if present
        if has_segments {
            writer.write_opt_u32(self.segments.len() as u32);
            for seg in &self.segments {
                writer.write_opt_u32(seg.len() as u32);
                writer.write_bytes(seg);
            }
        }
    }

    /// Parse a tick closure from a reader
    pub fn parse_content(reader: &mut BinaryReader) -> Result<Self> {
        let t_count = reader.read_opt_u32()?;
        let action_count = (t_count >> 1) as usize;
        let has_segments = (t_count & 1) != 0;

        let mut input_actions = Vec::with_capacity(action_count);
        for _ in 0..action_count {
            let player_idx = reader.read_opt_u16()?;
            let action = CodecInputAction::read(reader)?;
            input_actions.push((player_idx, action));
        }

        let mut segments = Vec::new();
        if has_segments {
            let seg_count = reader.read_opt_u32()? as usize;
            for _ in 0..seg_count {
                let len = reader.read_opt_u32()? as usize;
                let data = reader.read_bytes(len)?.to_vec();
                segments.push(data);
            }
        }

        Ok(Self {
            tick: 0, // Tick is set by caller
            input_actions,
            segments,
        })
    }
}

/// Mod version (3 u8 components) - used for mod info in ConnectionRequestReplyConfirm
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ModVersion {
    pub major: u8,
    pub minor: u8,
    pub patch: u8,
}

impl ModVersion {
    pub fn new(major: u8, minor: u8, patch: u8) -> Self {
        Self { major, minor, patch }
    }

    pub fn read(reader: &mut BinaryReader) -> Result<Self> {
        Ok(Self {
            major: reader.read_u8()?,
            minor: reader.read_u8()?,
            patch: reader.read_u8()?,
        })
    }

    pub fn write(&self, writer: &mut BinaryWriter) {
        writer.write_u8(self.major);
        writer.write_u8(self.minor);
        writer.write_u8(self.patch);
    }
}

impl std::fmt::Display for ModVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

/// Application version using VarShort encoding - used for connection handshake
/// Wire format: [VarShort major][VarShort minor][VarShort patch][u32 buildMode]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ApplicationVersion {
    pub major: u16,
    pub minor: u16,
    pub patch: u16,
    pub build: u32,
}

impl ApplicationVersion {
    pub const FACTORIO_2_0_72: Self = Self {
        major: 2,
        minor: 0,
        patch: 72,
        build: 84292,
    };

    pub fn read(reader: &mut BinaryReader) -> Result<Self> {
        Ok(Self {
            major: reader.read_opt_u16()?,
            minor: reader.read_opt_u16()?,
            patch: reader.read_opt_u16()?,
            build: reader.read_u32_le()?,
        })
    }

    pub fn write(&self, writer: &mut BinaryWriter) {
        writer.write_opt_u16(self.major);
        writer.write_opt_u16(self.minor);
        writer.write_opt_u16(self.patch);
        writer.write_u32_le(self.build);
    }
}

impl std::fmt::Display for ApplicationVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{} (build {})", self.major, self.minor, self.patch, self.build)
    }
}

/// Build version with numeric build number (legacy, use ApplicationVersion for wire format)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BuildVersion {
    pub version: ModVersion,
    pub build: u32,
}

impl BuildVersion {
    pub const FACTORIO_2_0_72: Self = Self {
        version: ModVersion { major: 2, minor: 0, patch: 72 },
        build: 84292,
    };
}

/// Mod information
#[derive(Debug, Clone)]
pub struct ModInfo {
    pub name: String,
    pub version: ModVersion,
    pub crc: u32,
}

impl ModInfo {
    pub fn read(reader: &mut BinaryReader) -> Result<Self> {
        Ok(Self {
            name: reader.read_simple_string()?,
            version: ModVersion::read(reader)?,
            crc: reader.read_u32_le()?,
        })
    }

    pub fn write(&self, writer: &mut BinaryWriter) {
        writer.write_simple_string(&self.name);
        self.version.write(writer);
        writer.write_u32_le(self.crc);
    }
}

/// ConnectionRequest payload (type 2)
/// Sent by client to initiate connection
#[derive(Debug, Clone)]
pub struct ConnectionRequest {
    pub version: ApplicationVersion,
    pub client_request_id: u32,
}

impl ConnectionRequest {
    pub fn new(client_request_id: u32) -> Self {
        Self {
            version: ApplicationVersion::FACTORIO_2_0_72,
            client_request_id,
        }
    }

    pub fn write(&self, writer: &mut BinaryWriter) {
        self.version.write(writer);
        writer.write_u32_le(self.client_request_id);
    }
}

/// ConnectionRequestReply payload (type 3)
/// Server's response to ConnectionRequest
/// Wire format: [ApplicationVersion][client_request_id u32][server_request_id u32][max_packet_size u16]
#[derive(Debug, Clone)]
pub struct ConnectionRequestReply {
    pub version: ApplicationVersion,
    pub client_request_id: u32,
    pub server_request_id: u32,
    pub max_packet_size: u16,
}

impl ConnectionRequestReply {
    pub fn read(reader: &mut BinaryReader) -> Result<Self> {
        Ok(Self {
            version: ApplicationVersion::read(reader)?,
            client_request_id: reader.read_u32_le()?,
            server_request_id: reader.read_u32_le()?,
            max_packet_size: reader.read_u16_le()?,
        })
    }
}

/// ConnectionRequestReplyConfirm payload (type 4)
/// Client's confirmation with full mod list
#[derive(Debug, Clone)]
pub struct ConnectionRequestReplyConfirm {
    pub client_request_id: u32,
    pub server_request_id: u32,
    pub instance_id: u32,
    pub username: String,
    pub password_hash: String,
    pub server_key: String,
    pub timestamp: String,
    pub core_checksum: u32,
    pub prototype_list_checksum: u32,
    pub mods: Vec<ModInfo>,
}

impl ConnectionRequestReplyConfirm {
    pub fn new(
        client_request_id: u32,
        server_request_id: u32,
        username: String,
        mods: Vec<ModInfo>,
    ) -> Self {
        Self {
            client_request_id,
            server_request_id,
            instance_id: rand_u32().max(1), // Must be non-zero
            username,
            password_hash: String::new(),
            server_key: String::new(),
            timestamp: String::new(),
            // Hardcoded for Factorio 2.0.72 - must match server
            core_checksum: 3316885848,
            prototype_list_checksum: 748475845,
            mods,
        }
    }

    pub fn write(&self, writer: &mut BinaryWriter) {
        writer.write_u32_le(self.client_request_id);
        writer.write_u32_le(self.server_request_id);
        writer.write_u32_le(self.instance_id);
        writer.write_simple_string(&self.username);
        writer.write_simple_string(&self.password_hash);
        writer.write_simple_string(&self.server_key);
        writer.write_simple_string(&self.timestamp);
        writer.write_u32_le(self.core_checksum);
        writer.write_u32_le(self.prototype_list_checksum);

        // Mod list
        writer.write_opt_u32(self.mods.len() as u32);
        for m in &self.mods {
            m.write(writer);
        }

        // Settings trailer (observed in real client)
        writer.write_bytes(&[0x05, 0x00, 0x00, 0x00, 0x00, 0x00]);
    }
}

/// ConnectionAcceptOrDeny payload (type 5)
/// From binary RE - full format when accepted:
/// [1 byte]  status
/// [1 byte]  peerCount
/// [4 bytes] mapTick (u32)
/// [4 bytes] unknown (u32)
/// [8 bytes] steamId (u64)
/// [varies]  ClientsPeerInfo
/// [4 bytes] unknown (u32)
/// [4 bytes] unknown (u32)
/// [2 bytes] latencyWindow (u16)
/// [varies]  serverMods
/// [2 bytes] unknown (u16)
#[derive(Debug, Clone)]
pub struct ConnectionAcceptOrDeny {
    pub accepted: bool,
    pub peer_id: Option<u16>,
    pub player_index: Option<u16>,
    pub server_name: Option<String>,
    pub denial_reason: Option<DenialReason>,
    pub initial_tick: Option<u32>,
    pub initial_msg_id: Option<u16>,
    pub session_constant: Option<u16>,
    // New fields from binary RE
    pub peer_count: Option<u8>,
    pub map_tick: Option<u32>,
    pub steam_id: Option<u64>,
    pub latency_window: Option<u16>,
}

impl ConnectionAcceptOrDeny {
    pub fn read(data: &[u8]) -> Result<Self> {
        // The first byte after packet header indicates accept/deny
        // For accept, we get player info; for deny, we get reason
        // Format is complex - simplified parsing for now
        if data.is_empty() {
            return Err(Error::UnexpectedEof);
        }

        // Check if this looks like an accept (has server name string)
        // Real parsing would be more complex
        let mut reader = BinaryReader::new(data);

        // Try to read as accepted message
        // Format: player_index (u16) + server_name (SimpleString) + ...
        if let Ok(player_index) = reader.read_u16_le() {
            if let Ok(server_name) = reader.read_simple_string() {
                return Ok(Self {
                    accepted: true,
                    peer_id: None,
                    player_index: Some(player_index),
                    server_name: Some(server_name),
                    denial_reason: None,
                    initial_tick: None,
                    initial_msg_id: None,
                    session_constant: None,
                    peer_count: None,
                    map_tick: None,
                    steam_id: None,
                    latency_window: None,
                });
            }
        }

        // Must be denial
        Ok(Self {
            accepted: false,
            peer_id: None,
            player_index: None,
            server_name: None,
            denial_reason: Some(DenialReason::Unknown),
            initial_tick: None,
            initial_msg_id: None,
            session_constant: None,
            peer_count: None,
            map_tick: None,
            steam_id: None,
            latency_window: None,
        })
    }
}

/// Reason for connection denial
#[derive(Debug, Clone)]
pub enum DenialReason {
    VersionMismatch,
    ModMismatch,
    CoreModMismatch,
    PasswordRequired,
    WrongPassword,
    UsernameTaken,
    UserBanned,
    ServerFull,
    NotWhitelisted,
    Unknown,
}

/// GameInformationRequest payload (type 16)
/// Just a single byte 0x10 with no additional payload
pub struct GameInformationRequest;

impl GameInformationRequest {
    pub fn write(&self, _writer: &mut BinaryWriter) {
        // No payload - just the type byte
    }
}

/// Parsed server info from GameInformationRequestReply
#[derive(Debug, Clone)]
pub struct ServerInfo {
    pub mods: Vec<ModInfo>,
}

impl ServerInfo {
    /// Parse server info by finding the mod list
    /// The format is complex, so we search for "base" which is always first
    pub fn parse(data: &[u8]) -> Result<Self> {
        // Search for "base" mod name (length 4 + "base")
        for i in 0..data.len().saturating_sub(10) {
            if data[i] == 4 && &data[i + 1..i + 5] == b"base" {
                // Found "base" - the byte before is the mod count
                if i >= 1 {
                    let count = data[i - 1] as usize;
                    if count > 0 && count < 50 {
                        return Self::parse_mods_at(data, i - 1);
                    }
                }
            }
        }
        Err(Error::InvalidPacket("could not find mod list in server info".into()))
    }

    fn parse_mods_at(data: &[u8], start: usize) -> Result<Self> {
        let mut pos = start;
        let mod_count = data[pos] as usize;
        pos += 1;

        let mut mods = Vec::with_capacity(mod_count);
        for _ in 0..mod_count {
            if pos >= data.len() {
                break;
            }

            // Name (SimpleString)
            let name_len = data[pos] as usize;
            pos += 1;
            if pos + name_len > data.len() {
                break;
            }
            let name = String::from_utf8_lossy(&data[pos..pos + name_len]).to_string();
            pos += name_len;

            // Version (3 u8s)
            if pos + 3 > data.len() {
                break;
            }
            let version = ModVersion::new(data[pos], data[pos + 1], data[pos + 2]);
            pos += 3;

            // CRC (u32)
            if pos + 4 > data.len() {
                break;
            }
            let crc = u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
            pos += 4;

            mods.push(ModInfo { name, version, crc });
        }

        Ok(Self { mods })
    }
}

/// TransferBlockRequest (type 12)
/// Client requests a block of map/mod data
/// From pcap: real client mixes reliable (0x2C) and non-reliable (0x0C)
#[derive(Debug, Clone)]
pub struct TransferBlockRequest {
    pub block_number: u32,
    pub reliable: bool,
}

impl TransferBlockRequest {
    pub fn new(block_number: u32, reliable: bool) -> Self {
        Self { block_number, reliable }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let type_byte = if self.reliable { 0x2C } else { 0x0C };
        let mut data = vec![type_byte];
        data.extend_from_slice(&self.block_number.to_le_bytes());
        data
    }
}

/// TransferBlock (type 13)
/// Server sends a block of map/mod data
#[derive(Debug, Clone)]
pub struct TransferBlock {
    pub block_number: u32,
    pub data: Vec<u8>,
}

impl TransferBlock {
    pub fn parse(packet: &[u8]) -> Result<Self> {
        if packet.len() < 5 {
            return Err(Error::UnexpectedEof);
        }
        // First byte is type (0x0D or 0x2D), then u32 block number
        let block_number = u32::from_le_bytes([packet[1], packet[2], packet[3], packet[4]]);
        let data = packet[5..].to_vec();
        Ok(Self { block_number, data })
    }
}

/// ClientToServerHeartbeat (type 6)
/// Space Age gameplay heartbeat header (flags 0x0E/0x06/0x0A).
/// State heartbeats (flags 0x00/0x10) use a different layout.
#[derive(Debug, Clone)]
pub struct ClientToServerHeartbeat {
    pub flags: u8,
    pub msg_id: u16,
    pub peer_constant: u16,
    pub client_tick: u32,
    pub server_tick_echo: u32,
}

impl ClientToServerHeartbeat {
    pub fn new(flags: u8, msg_id: u16, peer_constant: u16, client_tick: u32, server_tick_echo: u32) -> Self {
        Self {
            flags,
            msg_id,
            peer_constant,
            client_tick,
            server_tick_echo,
        }
    }

    pub fn write(&self, writer: &mut BinaryWriter) {
        writer.write_u8(self.flags);
        writer.write_u16_le(self.msg_id);
        writer.write_u16_le(self.peer_constant);
        writer.write_u32_le(self.client_tick);
        writer.write_u32_le(0); // Padding
        writer.write_u32_le(self.server_tick_echo);
        writer.write_u32_le(0); // Padding
    }
}

/// ServerToClientHeartbeat (type 7)
/// Sent by server with game state updates
#[derive(Debug, Clone)]
pub struct ServerToClientHeartbeat {
    pub server_tick: u32,
    pub flags: u8,
}

impl ServerToClientHeartbeat {
    pub fn parse(payload: &[u8]) -> Result<Self> {
        if payload.len() < 10 {
            // Minimal heartbeat with just tick
            return Ok(Self {
                server_tick: 0,
                flags: 0,
            });
        }
        // The server tick is embedded in the payload
        // Format varies based on content, but typically includes tick number
        let mut reader = BinaryReader::new(payload);
        let flags = reader.read_u8()?;
        let _data = reader.read_u16_le()?;
        let server_tick = reader.read_u32_le()?;
        Ok(Self { server_tick, flags })
    }
}

/// Encoded client action payload with its heartbeat flags.
pub struct EncodedAction {
    pub flags: u8,
    pub data: Vec<u8>,
}

/// Client input actions encoded to match the on-wire format observed in pcap.
#[derive(Debug, Clone)]
pub enum InputAction {
    MoveDirection { direction: Direction },
    StopWalking,
    BeginMining,
    StopMining,
    BeginMiningTerrain { position: MapPosition },
    CursorOrMining { position: MapPosition, notify: u8 },
    Chat { message: String },
    EmptyActionTick { tick: u32 },
    Raw { data: Vec<u8> },
}

impl InputAction {
    pub fn move_direction(direction: Direction) -> Self {
        Self::MoveDirection { direction }
    }

    pub fn stop_walking() -> Self {
        Self::StopWalking
    }

    pub fn begin_mining() -> Self {
        Self::BeginMining
    }

    pub fn stop_mining() -> Self {
        Self::StopMining
    }

    pub fn cursor_or_mining(position: MapPosition, notify: u8) -> Self {
        Self::CursorOrMining { position, notify }
    }

    pub fn begin_mining_terrain(position: MapPosition) -> Self {
        Self::BeginMiningTerrain { position }
    }

    pub fn chat(message: impl Into<String>) -> Self {
        Self::Chat {
            message: message.into(),
        }
    }

    pub fn empty_action_tick(tick: u32) -> Self {
        Self::EmptyActionTick { tick }
    }

    pub fn raw(data: Vec<u8>) -> Self {
        Self::Raw { data }
    }

    fn direction_vector(direction: Direction) -> (f64, f64) {
        const SQRT1_2: f64 = 0.7071067811865475;
        match direction {
            Direction::North => (-0.0, 1.0),
            Direction::NorthEast => (SQRT1_2, SQRT1_2),
            Direction::East => (1.0, 0.0),
            Direction::SouthEast => (SQRT1_2, -SQRT1_2),
            Direction::South => (0.0, -1.0),
            Direction::SouthWest => (-SQRT1_2, -SQRT1_2),
            Direction::West => (-1.0, -0.0),
            Direction::NorthWest => (-SQRT1_2, SQRT1_2),
        }
    }

    fn push_var_u16(buf: &mut Vec<u8>, value: u16) {
        if value < 0xff {
            buf.push(value as u8);
        } else {
            buf.push(0xff);
            buf.extend_from_slice(&value.to_le_bytes());
        }
    }

    fn initial_player_delta(player_index: u16) -> u16 {
        // TickClosure starts delta from 0xffff, so first delta = player_index + 1.
        player_index.wrapping_add(1)
    }

    pub fn encode(&self, chat_seq: &mut u32, player_index: u16) -> Result<EncodedAction> {
        let player_delta = Self::initial_player_delta(player_index);
        match self {
            InputAction::MoveDirection { direction } => {
                let (dx, dy) = Self::direction_vector(*direction);
                let mut data = Vec::with_capacity(19);
                data.push(0x02);
                Self::push_var_u16(&mut data, 0x43);
                Self::push_var_u16(&mut data, player_delta);
                data.extend_from_slice(&dx.to_le_bytes());
                data.extend_from_slice(&dy.to_le_bytes());
                Ok(EncodedAction { flags: 0x06, data })
            }
            InputAction::StopWalking => Ok(EncodedAction {
                flags: 0x06,
                data: {
                    let mut data = Vec::with_capacity(3);
                    data.push(0x02);
                    Self::push_var_u16(&mut data, 0x01);
                    Self::push_var_u16(&mut data, player_delta);
                    data
                },
            }),
            InputAction::BeginMining => Ok(EncodedAction {
                flags: 0x06,
                data: {
                    let mut data = Vec::with_capacity(3);
                    data.push(0x02);
                    Self::push_var_u16(&mut data, 0x02);
                    Self::push_var_u16(&mut data, player_delta);
                    data
                },
            }),
            InputAction::StopMining => Ok(EncodedAction {
                flags: 0x06,
                data: {
                    let mut data = Vec::with_capacity(3);
                    data.push(0x02);
                    Self::push_var_u16(&mut data, 0x03);
                    Self::push_var_u16(&mut data, player_delta);
                    data
                },
            }),
            InputAction::BeginMiningTerrain { position } => {
                let mut data = Vec::with_capacity(11);
                data.push(0x02);
                Self::push_var_u16(&mut data, 0x44);
                Self::push_var_u16(&mut data, player_delta);
                data.extend_from_slice(&position.x.raw().to_le_bytes());
                data.extend_from_slice(&position.y.raw().to_le_bytes());
                Ok(EncodedAction { flags: 0x06, data })
            }
            InputAction::CursorOrMining { position, notify } => {
                let mut data = Vec::with_capacity(12);
                data.push(0x02);
                Self::push_var_u16(&mut data, 0x55);
                Self::push_var_u16(&mut data, player_delta);
                data.extend_from_slice(&position.x.raw().to_le_bytes());
                data.extend_from_slice(&position.y.raw().to_le_bytes());
                data.push(*notify);
                Ok(EncodedAction { flags: 0x06, data })
            }
            InputAction::Chat { message } => {
                let seq = if *chat_seq == 0 { 1 } else { *chat_seq };
                *chat_seq = seq.wrapping_add(1);

                let mut payload_writer = BinaryWriter::with_capacity(3 + message.len() + 5);
                payload_writer.write_opt_u16(player_index);
                payload_writer.write_string(message);
                let payload = payload_writer.into_vec();

                let mut writer = BinaryWriter::with_capacity(32 + payload.len());
                writer.write_u8(0x01); // count=0, hasSegments=1
                writer.write_u8(0x01); // one segment
                writer.write_opt_u16(0x68);
                writer.write_u32_le(seq);
                writer.write_opt_u16(player_index);
                writer.write_opt_u32(1);
                writer.write_opt_u32(0);
                writer.write_opt_u32(payload.len() as u32);
                writer.write_bytes(&payload);
                let data = writer.into_vec();

                Ok(EncodedAction { flags: 0x06, data })
            }
            InputAction::EmptyActionTick { tick } => {
                let mut data = Vec::with_capacity(9);
                data.push(0x00);
                data.extend_from_slice(&tick.to_le_bytes());
                data.extend_from_slice(&0u32.to_le_bytes());
                Ok(EncodedAction { flags: 0x0A, data })
            }
            InputAction::Raw { data } => Ok(EncodedAction {
                flags: 0x06,
                data: data.clone(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mod_version() {
        let v = ModVersion::new(2, 0, 72);
        assert_eq!(v.to_string(), "2.0.72");

        let mut writer = BinaryWriter::new();
        v.write(&mut writer);
        assert_eq!(writer.as_slice(), &[2, 0, 72]);
    }

    #[test]
    fn test_connection_request() {
        let req = ConnectionRequest::new(0x12345678);

        let mut writer = BinaryWriter::new();
        req.write(&mut writer);
        let data = writer.into_vec();

        // ApplicationVersion (VarShort*3 + u32) + clientReqId(4) = 7 + 4 = 11 bytes
        assert_eq!(data.len(), 11);
        assert_eq!(&data[0..3], &[2, 0, 72]); // Version VarShort (2.0.72)
        assert_eq!(&data[3..7], &[0x44, 0x49, 0x01, 0x00]); // Build 84292
        assert_eq!(&data[7..11], &[0x78, 0x56, 0x34, 0x12]); // ClientReqId
    }

    #[test]
    fn test_connection_request_reply() {
        // ApplicationVersion (VarShort*3 + u32) + client_request_id + server_request_id + max_packet_size
        let data = [
            2, 0, 72, // Version VarShort (2.0.72)
            0x44, 0x49, 0x01, 0x00, // Build 84292
            0x78, 0x56, 0x34, 0x12, // ClientReqId
            0xAB, 0xCD, 0xEF, 0x12, // ServerReqId
            0xFC, 0x01, // max_packet_size
        ];

        let mut reader = BinaryReader::new(&data);
        let reply = ConnectionRequestReply::read(&mut reader).unwrap();

        assert_eq!(reply.version, ApplicationVersion::FACTORIO_2_0_72);
        assert_eq!(reply.client_request_id, 0x12345678);
        assert_eq!(reply.server_request_id, 0x12EFCDAB);
        assert_eq!(reply.max_packet_size, 0x01FC);
    }
}
