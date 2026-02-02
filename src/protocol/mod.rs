pub mod packet;
pub mod message;
pub mod transport;
pub mod connection;

pub(crate) fn rand_u32() -> u32 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let duration = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    (duration.as_nanos() as u32) ^ (duration.as_secs() as u32)
}

pub use packet::{
    PacketHeader, PacketBuilder, MessageType,
    encode_type_byte, parse_type_byte, MAX_PACKET_SIZE,
};
pub use message::{
    BuildVersion, ModInfo, ModVersion,
    ConnectionRequest, ConnectionRequestReply, ConnectionRequestReplyConfirm,
    ConnectionAcceptOrDeny, DenialReason, ServerInfo,
    TransferBlockRequest, TransferBlock,
    ClientToServerHeartbeat,
    InputAction,
};
pub use transport::Transport;
pub use connection::{Connection, ConnectionState, PlayerState, ReceivedPacket};
pub use connection::ConnectionActions;
