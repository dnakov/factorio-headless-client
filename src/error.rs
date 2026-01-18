#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("connection refused: {reason}")]
    ConnectionRefused { reason: String },

    #[error("connection timeout")]
    ConnectionTimeout,

    #[error("desync detected at tick {tick}: expected CRC {expected:#x}, got {actual:#x}")]
    Desync { tick: u32, expected: u32, actual: u32 },

    #[error("timeout")]
    Timeout,

    #[error("timeout waiting for {operation}")]
    TimeoutWaiting { operation: &'static str },

    #[error("invalid packet: {0}")]
    InvalidPacket(String),

    #[error("invalid message type: {0}")]
    InvalidMessageType(u8),

    #[error("invalid input action type: {0}")]
    InvalidInputAction(u8),

    #[error("protocol version mismatch: server {server}, client {client}")]
    VersionMismatch { server: u32, client: u32 },

    #[error("unexpected end of data")]
    UnexpectedEof,

    #[error("string too long: {len} bytes (max {max})")]
    StringTooLong { len: usize, max: usize },

    #[error("buffer overflow: need {need} bytes, have {have}")]
    BufferOverflow { need: usize, have: usize },

    #[error("disconnected: {reason}")]
    Disconnected { reason: String },

    #[error("not connected")]
    NotConnected,

    #[error("io error: {0}")]
    Io(String),
}

pub type Result<T> = std::result::Result<T, Error>;
