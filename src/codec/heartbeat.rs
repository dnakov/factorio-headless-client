//! Heartbeat packet parsing
//!
//! # Factorio Multiplayer Protocol - Deterministic Lockstep
//!
//! Factorio uses deterministic lockstep simulation:
//! - Server sends tick confirmations (not game state)
//! - All clients run identical simulation from inputs
//! - Entity destruction, inventory, etc. computed locally
//! - No explicit entity/inventory updates in packets
//!
//! # Server Heartbeat Format (Type 0x07/0x27)
//!
//! ## Header (2 bytes):
//!   Byte 0:     Type (0x07 or 0x27 with reliable flag)
//!   Byte 1:     Flags
//!
//! ## Body (pcap):
//!   Bytes 2-5:   Server tick (u32 LE)
//!   Bytes 6-9:   Confirmed tick (u32 LE)
//!   Bytes 10-13: Padding zeros
//!   Bytes 14+:   Tick confirmations + optional player update
//!
//! ## Tick Confirmation Format:
//!   Bytes 0-2:   0x02 0x52 0x01 (marker)
//!   Bytes 3-6:   CRC/checksum (4 bytes)
//!   Bytes 7-10:  Confirmed tick (u32 LE)
//!
//! ## Player State Update (if flags & 0x10):
//!   Byte 0: 0x01 (segment marker)
//!   Byte 1: 0x0b (update type)
//!   Byte 2: Frame counter (incrementing)
//!   Byte 3: Player index
//!   Byte 4: 0x00 (padding)

use crate::error::Result;

/// Parsed heartbeat packet
#[derive(Debug, Clone)]
pub struct ServerHeartbeat {
    pub flags: u8,
    pub server_tick: u32,
    pub confirmed_tick: Option<u32>,
    pub tick_confirmations: Vec<TickConfirmation>,
    pub player_update: Option<PlayerStateUpdate>,
}

/// Server confirmation of a processed tick
/// Used for synchronization - client can verify CRC matches
#[derive(Debug, Clone)]
pub struct TickConfirmation {
    pub checksum: [u8; 4],
    pub tick: u32,
}

/// Player state update
#[derive(Debug, Clone)]
pub struct PlayerStateUpdate {
    pub update_type: u8,
    pub frame: u8,
    pub player_index: u8,
}

impl ServerHeartbeat {
    /// Parse a server heartbeat packet
    pub fn parse(data: &[u8]) -> Result<Self> {
        if data.len() < 6 {
            return Err(crate::error::Error::InvalidPacket(
                format!("Heartbeat too short: {} bytes", data.len())
            ));
        }

        let packet_type = data[0] & 0x1F;
        if packet_type != 7 {
            return Err(crate::error::Error::InvalidPacket(
                format!("Not a heartbeat packet: type {}", packet_type)
            ));
        }

        let flags = data[1];
        let server_tick = u32::from_le_bytes([data[2], data[3], data[4], data[5]]);
        let mut confirmed_tick = None;
        let has_player_update = (flags & 0x10) != 0;

        if data.len() >= 10 {
            confirmed_tick = Some(u32::from_le_bytes([data[6], data[7], data[8], data[9]]));
        }

        let payload_start = 14usize;

        // Parse tick confirmations
        // Marker format: 0x02 0x52 0x01 (3 bytes) + checksum (4 bytes) + tick (4 bytes) = 11 bytes
        let mut tick_confirmations = Vec::new();
        let mut pos = payload_start;
        while pos + 11 <= data.len() {
            if data[pos] != 0x02 || data[pos + 1] != 0x52 || data[pos + 2] != 0x01 {
                break;
            }

            let checksum = [data[pos + 3], data[pos + 4], data[pos + 5], data[pos + 6]];
            let conf_tick = u32::from_le_bytes([data[pos + 7], data[pos + 8], data[pos + 9], data[pos + 10]]);

            tick_confirmations.push(TickConfirmation {
                checksum,
                tick: conf_tick,
            });

            pos += 11;
            while pos < data.len() && data[pos] == 0x00 {
                pos += 1;
            }
        }

        // Parse player state update if present
        let player_update = if has_player_update && pos + 5 <= data.len() {
            // Look for 0x01 0x0b marker
            let mut found = None;
            while pos + 5 <= data.len() {
                if data[pos] == 0x01 && data[pos + 1] == 0x0b {
                    found = Some(PlayerStateUpdate {
                        update_type: data[pos + 1],
                        frame: data[pos + 2],
                        player_index: data[pos + 3],
                    });
                    break;
                } else {
                    pos += 1;
                }
            }
            found
        } else {
            None
        };

        Ok(ServerHeartbeat {
            flags,
            server_tick,
            confirmed_tick,
            tick_confirmations,
            player_update,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex_to_bytes(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .filter_map(|i| u8::from_str_radix(&s[i..i + 2], 16).ok())
            .collect()
    }

    #[test]
    fn test_parse_29byte_packet() {
        // Flags 0x06, standard heartbeat
        let data = hex_to_bytes(
            "0706902701004027010000000000025201ffa21efa3f27010000000000"
        );

        let hb = ServerHeartbeat::parse(&data).unwrap();
        assert_eq!(hb.flags, 0x06);
        assert_eq!(hb.server_tick, 75664);
        assert_eq!(hb.tick_confirmations.len(), 1);
    }

    #[test]
    fn test_parse_34byte_packet() {
        // Flags 0x16, heartbeat with extra tail data
        let data = hex_to_bytes(
            "0716892701003f27010000000000025201024400273e27010000000000010204746573740f00"
        );

        let hb = ServerHeartbeat::parse(&data).unwrap();
        assert_eq!(hb.flags, 0x16);
        assert_eq!(hb.server_tick, 75657);
    }
}
