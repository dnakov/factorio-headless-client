use crate::error::{Error, Result};

/// Network message types (bits 0-4 of type byte)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum MessageType {
    Ping = 0,
    PingReply = 1,
    ConnectionRequest = 2,
    ConnectionRequestReply = 3,
    ConnectionRequestReplyConfirm = 4,
    ConnectionAcceptOrDeny = 5,
    ClientToServerHeartbeat = 6,
    ServerToClientHeartbeat = 7,
    GetOwnAddress = 8,
    GetOwnAddressReply = 9,
    NatPunchRequest = 10,
    NatPunch = 11,
    TransferBlockRequest = 12,
    TransferBlock = 13,
    RequestForHeartbeatWhenDisconnecting = 14,
    LANBroadcast = 15,
    GameInformationRequest = 16,
    GameInformationRequestReply = 17,
    Empty = 18,
}

impl MessageType {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v & 0x1F {
            0 => Some(Self::Ping),
            1 => Some(Self::PingReply),
            2 => Some(Self::ConnectionRequest),
            3 => Some(Self::ConnectionRequestReply),
            4 => Some(Self::ConnectionRequestReplyConfirm),
            5 => Some(Self::ConnectionAcceptOrDeny),
            6 => Some(Self::ClientToServerHeartbeat),
            7 => Some(Self::ServerToClientHeartbeat),
            8 => Some(Self::GetOwnAddress),
            9 => Some(Self::GetOwnAddressReply),
            10 => Some(Self::NatPunchRequest),
            11 => Some(Self::NatPunch),
            12 => Some(Self::TransferBlockRequest),
            13 => Some(Self::TransferBlock),
            14 => Some(Self::RequestForHeartbeatWhenDisconnecting),
            15 => Some(Self::LANBroadcast),
            16 => Some(Self::GameInformationRequest),
            17 => Some(Self::GameInformationRequestReply),
            18 => Some(Self::Empty),
            _ => None,
        }
    }
}

/// Type byte flags (combined with message type)
/// Bit 5 = reliable/sequenced flag (real client sets this)
/// Bit 6 = is fragmented
/// Bit 7 = unused
const TYPE_RELIABLE_BIT: u8 = 0x20;
const TYPE_FRAGMENTED_BIT: u8 = 0x40;

/// Message ID flags
/// Bit 15 = has confirmations to ACK
const MSG_ID_CONFIRM_BIT: u16 = 0x8000;

/// Encode a type byte for sending
pub fn encode_type_byte(msg_type: MessageType, reliable: bool, fragmented: bool) -> u8 {
    let mut byte = msg_type as u8;
    if reliable {
        byte |= TYPE_RELIABLE_BIT;
    }
    if fragmented {
        byte |= TYPE_FRAGMENTED_BIT;
    }
    byte
}

/// Parse a type byte received from network
pub fn parse_type_byte(byte: u8) -> (MessageType, bool, bool) {
    let msg_type = MessageType::from_u8(byte & 0x1F).unwrap_or(MessageType::Empty);
    let reliable = (byte & TYPE_RELIABLE_BIT) != 0;
    let fragmented = (byte & TYPE_FRAGMENTED_BIT) != 0;
    (msg_type, reliable, fragmented)
}

/// Parsed packet header from received data
#[derive(Debug, Clone)]
pub struct PacketHeader {
    pub message_type: MessageType,
    pub reliable: bool,
    pub fragmented: bool,
    pub server_flag: bool,
    pub message_id: u16,
    pub has_confirmations: bool,
    pub fragment_id: Option<u16>,
    pub confirmations: Vec<u32>,
}

impl PacketHeader {
    /// Parse header from raw packet data, returns header and payload start position
    pub fn parse(data: &[u8]) -> Result<(Self, usize)> {
        if data.is_empty() {
            return Err(Error::UnexpectedEof);
        }

        let mut pos = 0;

        // Type byte
        let type_byte = data[pos];
        pos += 1;
        let (message_type, reliable, fragmented_flag) = parse_type_byte(type_byte);
        let server_flag = (type_byte & 0x80) != 0;
        // Fragment ID exists for ANY message type when bit6 is set
        let fragmented = fragmented_flag;

        // Determine if msg_id is present based on doc:
        // - Types 2, 4 (ConnectionRequest, ConnectionRequestReplyConfirm): ALWAYS have msg_id
        // - Types 0, 1, 3 (Ping, PingReply, ConnectionRequestReply): msg_id only if bit6 set
        // - Types 5-18: msg_id only if bit6 set
        let has_msg_id = match message_type {
            MessageType::ConnectionRequest | MessageType::ConnectionRequestReplyConfirm => true,
            _ => fragmented_flag, // bit6 must be set for msg_id
        };

        if !has_msg_id {
            return Ok((
                Self {
                    message_type,
                    reliable,
                    fragmented,
                    server_flag,
                    message_id: 0,
                    has_confirmations: false,
                    fragment_id: None,
                    confirmations: Vec::new(),
                },
                1,
            ));
        }

        // Message ID
        if pos + 2 > data.len() {
            return Err(Error::UnexpectedEof);
        }
        let raw_msg_id = u16::from_le_bytes([data[pos], data[pos + 1]]);
        pos += 2;
        let has_confirmations = (raw_msg_id & MSG_ID_CONFIRM_BIT) != 0;
        let message_id = raw_msg_id & 0x7FFF;

        // Fragment ID (VarShort) if bit6 is set (for ANY message type)
        let fragment_id = if fragmented_flag {
            let (frag_id, consumed) = read_var_short(&data[pos..])?;
            pos += consumed;
            Some(frag_id)
        } else {
            None
        };

        // Confirmations if present
        let confirmations = if has_confirmations {
            let (count, consumed) = read_var_int(&data[pos..])?;
            pos += consumed;
            let mut confs = Vec::with_capacity(count as usize);
            for _ in 0..count {
                if pos + 4 > data.len() {
                    return Err(Error::UnexpectedEof);
                }
                let conf_id = u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
                pos += 4;
                confs.push(conf_id);
            }
            confs
        } else {
            Vec::new()
        };

        Ok((
            Self {
                message_type,
                reliable,
                fragmented,
                server_flag,
                message_id,
                has_confirmations,
                fragment_id,
                confirmations,
            },
            pos,
        ))
    }
}

/// Build a packet for sending
pub struct PacketBuilder {
    data: Vec<u8>,
}

impl PacketBuilder {
    pub fn new(msg_type: MessageType, msg_id: u16, reliable: bool) -> Self {
        let mut data = Vec::with_capacity(64);
        data.push(encode_type_byte(msg_type, reliable, false));
        data.extend_from_slice(&msg_id.to_le_bytes()); // No confirm bit
        Self { data }
    }

    pub fn payload(mut self, payload: &[u8]) -> Self {
        self.data.extend_from_slice(payload);
        self
    }

    pub fn build(self) -> Vec<u8> {
        self.data
    }
}

/// Read VarShort from slice, returns (value, bytes_consumed)
fn read_var_short(data: &[u8]) -> Result<(u16, usize)> {
    if data.is_empty() {
        return Err(Error::UnexpectedEof);
    }
    let first = data[0];
    if first == 0xFF {
        if data.len() < 3 {
            return Err(Error::UnexpectedEof);
        }
        let v = u16::from_le_bytes([data[1], data[2]]);
        Ok((v, 3))
    } else {
        Ok((first as u16, 1))
    }
}

/// Read VarInt from slice, returns (value, bytes_consumed)
fn read_var_int(data: &[u8]) -> Result<(u32, usize)> {
    if data.is_empty() {
        return Err(Error::UnexpectedEof);
    }
    let first = data[0];
    if first == 0xFF {
        if data.len() < 5 {
            return Err(Error::UnexpectedEof);
        }
        let v = u32::from_le_bytes([data[1], data[2], data[3], data[4]]);
        Ok((v, 5))
    } else {
        Ok((first as u32, 1))
    }
}

/// Maximum packet size (MTU-safe)
pub const MAX_PACKET_SIZE: usize = 1400;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_type_byte_encoding() {
        // ConnectionRequest with reliable flag
        let byte = encode_type_byte(MessageType::ConnectionRequest, true, false);
        assert_eq!(byte, 0x22);

        // ConnectionRequestReplyConfirm with reliable flag
        let byte = encode_type_byte(MessageType::ConnectionRequestReplyConfirm, true, false);
        assert_eq!(byte, 0x24);

        // GameInformationRequest (no reliable flag)
        let byte = encode_type_byte(MessageType::GameInformationRequest, false, false);
        assert_eq!(byte, 0x10);
    }

    #[test]
    fn test_type_byte_parsing() {
        // 0x22 = 0b00100010 = type 2 + reliable bit (bit 5)
        let (msg_type, reliable, fragmented) = parse_type_byte(0x22);
        assert_eq!(msg_type, MessageType::ConnectionRequest);
        assert!(reliable);
        assert!(!fragmented);

        // 0x63 = 0b01100011 = type 3 + reliable (bit 5) + fragmented (bit 6)
        let (msg_type, reliable, fragmented) = parse_type_byte(0x63);
        assert_eq!(msg_type, MessageType::ConnectionRequestReply);
        assert!(reliable);
        assert!(fragmented);

        // 0xC3 = 0b11000011 = type 3 + fragmented (bit 6) + bit 7 (unused)
        let (msg_type, _reliable, fragmented) = parse_type_byte(0xC3);
        assert_eq!(msg_type, MessageType::ConnectionRequestReply);
        assert!(fragmented);
    }

    #[test]
    fn test_packet_header_parse() {
        // Simple packet: type=3 + reliable + fragmented = 0x63
        // msgId=15 with confirm bit = 0x800F
        let data = [
            0x63, // Type 3 + reliable (bit 5) + fragmented (bit 6)
            0x0F, 0x80, // MsgId 15 with confirm bit
            0x00, // FragId = 0 (VarShort)
            0x01, // 1 confirmation (VarInt)
            0x01, 0x00, 0x00, 0x00, // Confirmation ID 1
            // Payload would follow here
        ];

        let (header, payload_start) = PacketHeader::parse(&data).unwrap();
        assert_eq!(header.message_type, MessageType::ConnectionRequestReply);
        assert!(header.reliable);
        assert!(header.fragmented);
        assert_eq!(header.message_id, 15);
        assert!(header.has_confirmations);
        assert_eq!(header.fragment_id, Some(0));
        assert_eq!(header.confirmations, vec![1]);
        // type(1) + msgId(2) + fragId(1) + confCount(1) + confId(4) = 9
        assert_eq!(payload_start, 9);
    }

    #[test]
    fn test_packet_builder() {
        let packet = PacketBuilder::new(MessageType::ConnectionRequest, 1, true)
            .payload(&[0x02, 0x00, 0x48])
            .build();

        assert_eq!(packet[0], 0x22); // Type with reliable bit
        assert_eq!(&packet[1..3], &[0x01, 0x00]); // MsgId = 1
        assert_eq!(&packet[3..], &[0x02, 0x00, 0x48]); // Payload
    }
}
