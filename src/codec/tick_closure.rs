//! TickClosure serialization for Factorio heartbeat protocol
//!
//! Wire format (from binary RE):
//! ```text
//! [8 bytes]  tick (u64 LE)
//! [1-5 bytes] inputActionCountAndHasSegments (count*2 + hasSegments, varlen)
//! [varies]   inputActions (for each action):
//!     [1-3 bytes] playerIndex (varlen u16)
//!     [varies]    actionData
//! [1-5 bytes] segmentCount (only if hasSegments)
//! [varies]   inputActionSegments
//! ```

use crate::codec::{BinaryWriter, InputAction};

/// A TickClosure contains all input actions for a single game tick
#[derive(Debug, Clone, Default)]
pub struct TickClosure {
    pub tick: u64,
    pub input_actions: Vec<TickInputAction>,
    pub segments: Vec<InputActionSegment>,
}

/// An input action within a tick closure, includes player index
#[derive(Debug, Clone)]
pub struct TickInputAction {
    pub player_index: u16,
    pub action: InputAction,
}

/// Segment for large or multi-part actions (rarely used)
#[derive(Debug, Clone)]
pub struct InputActionSegment {
    pub data: Vec<u8>,
}

impl TickClosure {
    pub fn new(tick: u64) -> Self {
        Self { tick, ..Self::default() }
    }

    pub fn with_action(tick: u64, player_index: u16, action: InputAction) -> Self {
        Self {
            tick,
            input_actions: vec![TickInputAction { player_index, action }],
            segments: Vec::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.input_actions.is_empty() && self.segments.is_empty()
    }

    pub fn add_action(&mut self, player_index: u16, action: InputAction) {
        self.input_actions.push(TickInputAction { player_index, action });
    }

    /// Serialize the tick closure to bytes
    pub fn write(&self, writer: &mut BinaryWriter) {
        // Tick (u64 LE)
        writer.write_u64_le(self.tick);

        // inputActionCountAndHasSegments = count * 2 + hasSegments
        let has_segments = !self.segments.is_empty();
        let count_and_segments = (self.input_actions.len() as u32) * 2 + if has_segments { 1 } else { 0 };
        write_varlen(writer, count_and_segments);

        // Input actions
        for tick_action in &self.input_actions {
            // Player index (varlen u16)
            write_varlen_u16(writer, tick_action.player_index);

            // Action data
            tick_action.action.write(writer);
        }

        // Segments (if any)
        if has_segments {
            write_varlen(writer, self.segments.len() as u32);
            for segment in &self.segments {
                writer.write_opt_u32(segment.data.len() as u32);
                writer.write_bytes(&segment.data);
            }
        }
    }
}

/// Write variable-length count encoding
/// If count < 255: 1 byte
/// If count >= 255: 1 byte (0xFF) + 4 bytes (u32 LE)
fn write_varlen(writer: &mut BinaryWriter, count: u32) {
    if count < 255 {
        writer.write_u8(count as u8);
    } else {
        writer.write_u8(0xFF);
        writer.write_u32_le(count);
    }
}

/// Write variable-length u16 encoding
/// If value < 255: 1 byte
/// If value >= 255: 1 byte (0xFF) + 2 bytes (u16 LE)
fn write_varlen_u16(writer: &mut BinaryWriter, value: u16) {
    if value < 255 {
        writer.write_u8(value as u8);
    } else {
        writer.write_u8(0xFF);
        writer.write_u16_le(value);
    }
}

/// Write tick closure count to a writer
pub fn write_tick_closure_count(writer: &mut BinaryWriter, count: usize) {
    write_varlen(writer, count as u32);
}

/// Calculate flags byte based on tick closures
pub fn calculate_flags(tick_closures: &[TickClosure], has_sync_actions: bool, has_heartbeat_requests: bool) -> u8 {
    let mut flags = 0u8;

    if !tick_closures.is_empty() {
        if tick_closures.len() == 1 {
            flags |= 0x06;  // Single tick closure
        } else {
            flags |= 0x02;  // Multiple tick closures
        }

        // Check if all tick closures are empty
        let all_empty = tick_closures.iter().all(|tc| tc.is_empty());
        if all_empty {
            flags |= 0x08;  // All closures empty
        }
    }

    if has_sync_actions {
        flags |= 0x10;  // Has synchronizer actions
    }

    if has_heartbeat_requests {
        flags |= 0x01;  // Has heartbeat requests
    }

    flags
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::Direction;

    #[test]
    fn test_empty_tick_closure() {
        let tc = TickClosure::new(1000);
        assert!(tc.is_empty());

        let mut writer = BinaryWriter::new();
        tc.write(&mut writer);

        let data = writer.as_slice();
        // 8 bytes tick + 1 byte count (0)
        assert_eq!(data.len(), 9);
        assert_eq!(u64::from_le_bytes([data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7]]), 1000);
        assert_eq!(data[8], 0); // count * 2 + hasSegments = 0
    }

    #[test]
    fn test_tick_closure_with_action() {
        let tc = TickClosure::with_action(
            5000,
            1,
            InputAction::StartWalking { direction_x: 0.0, direction_y: -1.0 },
        );
        assert!(!tc.is_empty());

        let mut writer = BinaryWriter::new();
        tc.write(&mut writer);

        let data = writer.as_slice();
        // Should have: 8 bytes tick + 1 byte count + player_index + action
        assert!(data.len() > 9);
    }

    #[test]
    fn test_calculate_flags() {
        // No closures
        assert_eq!(calculate_flags(&[], false, false), 0x00);

        // Single empty closure
        let single_empty = vec![TickClosure::new(1000)];
        assert_eq!(calculate_flags(&single_empty, false, false), 0x06 | 0x08);

        // Single non-empty closure
        let single_action = vec![TickClosure::with_action(
            1000,
            1,
            InputAction::StopWalking,
        )];
        assert_eq!(calculate_flags(&single_action, false, false), 0x06);

        // With sync actions
        assert_eq!(calculate_flags(&single_action, true, false), 0x06 | 0x10);

        // With heartbeat requests
        assert_eq!(calculate_flags(&single_action, false, true), 0x06 | 0x01);
    }
}
