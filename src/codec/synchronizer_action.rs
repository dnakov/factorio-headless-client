//! SynchronizerAction types for Factorio heartbeat protocol
//!
//! From binary reverse engineering - these are control actions embedded in heartbeats.

use crate::codec::BinaryWriter;

/// SynchronizerActionType enum values (Space Age 2.0, from binary RE)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SynchronizerActionType {
    GameEnd = 0x00,
    PeerDisconnect = 0x01,
    NewPeerInfo = 0x02,
    ClientChangedState = 0x03,
    ClientShouldStartSendingTickClosures = 0x04,
    MapReadyForDownload = 0x05,
    MapLoadingProgressUpdate = 0x06,
    MapSavingProgressUpdate = 0x07,
    SavingForUpdate = 0x08,
    MapDownloadingProgressUpdate = 0x09,
    CatchingUpProgressUpdate = 0x0A,
    PeerDroppingProgressUpdate = 0x0B,
    PlayerDesynced = 0x0C,
    BeginPause = 0x0D,
    EndPause = 0x0E,
    SkippedTickClosure = 0x0F,
    SkippedTickClosureConfirm = 0x10,
    ChangeLatency = 0x11,
    IncreasedLatencyConfirm = 0x12,
    SavingCountdown = 0x13,
}

impl SynchronizerActionType {
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0x00 => Some(Self::GameEnd),
            0x01 => Some(Self::PeerDisconnect),
            0x02 => Some(Self::NewPeerInfo),
            0x03 => Some(Self::ClientChangedState),
            0x04 => Some(Self::ClientShouldStartSendingTickClosures),
            0x05 => Some(Self::MapReadyForDownload),
            0x06 => Some(Self::MapLoadingProgressUpdate),
            0x07 => Some(Self::MapSavingProgressUpdate),
            0x08 => Some(Self::SavingForUpdate),
            0x09 => Some(Self::MapDownloadingProgressUpdate),
            0x0A => Some(Self::CatchingUpProgressUpdate),
            0x0B => Some(Self::PeerDroppingProgressUpdate),
            0x0C => Some(Self::PlayerDesynced),
            0x0D => Some(Self::BeginPause),
            0x0E => Some(Self::EndPause),
            0x0F => Some(Self::SkippedTickClosure),
            0x10 => Some(Self::SkippedTickClosureConfirm),
            0x11 => Some(Self::ChangeLatency),
            0x12 => Some(Self::IncreasedLatencyConfirm),
            0x13 => Some(Self::SavingCountdown),
            _ => None,
        }
    }
}

/// Synchronizer action with its data
#[derive(Debug, Clone)]
pub enum SynchronizerAction {
    GameEnd,
    PeerDisconnect { disconnect_type: u8 },
    NewPeerInfo { peer_name: String },
    ClientChangedState { state: u8 },
    ClientShouldStartSendingTickClosures { tick: u64 },
    MapReadyForDownload { data: Vec<u8> },
    MapLoadingProgressUpdate { progress: u8 },
    MapSavingProgressUpdate { progress: u8 },
    SavingForUpdate,
    MapDownloadingProgressUpdate { progress: u8 },
    CatchingUpProgressUpdate { progress: u8 },
    PeerDroppingProgressUpdate { progress: u8 },
    PlayerDesynced,
    BeginPause,
    EndPause,
    SkippedTickClosure { tick: u64 },
    SkippedTickClosureConfirm { tick: u64 },
    ChangeLatency { latency: u8 },
    IncreasedLatencyConfirm { tick: u64, latency: u8 },
    SavingCountdown { tick: u64, remaining: u32 },
}

impl SynchronizerAction {
    pub fn action_type(&self) -> SynchronizerActionType {
        match self {
            Self::GameEnd => SynchronizerActionType::GameEnd,
            Self::PeerDisconnect { .. } => SynchronizerActionType::PeerDisconnect,
            Self::NewPeerInfo { .. } => SynchronizerActionType::NewPeerInfo,
            Self::ClientChangedState { .. } => SynchronizerActionType::ClientChangedState,
            Self::ClientShouldStartSendingTickClosures { .. } => SynchronizerActionType::ClientShouldStartSendingTickClosures,
            Self::MapReadyForDownload { .. } => SynchronizerActionType::MapReadyForDownload,
            Self::MapLoadingProgressUpdate { .. } => SynchronizerActionType::MapLoadingProgressUpdate,
            Self::MapSavingProgressUpdate { .. } => SynchronizerActionType::MapSavingProgressUpdate,
            Self::SavingForUpdate => SynchronizerActionType::SavingForUpdate,
            Self::MapDownloadingProgressUpdate { .. } => SynchronizerActionType::MapDownloadingProgressUpdate,
            Self::CatchingUpProgressUpdate { .. } => SynchronizerActionType::CatchingUpProgressUpdate,
            Self::PeerDroppingProgressUpdate { .. } => SynchronizerActionType::PeerDroppingProgressUpdate,
            Self::PlayerDesynced => SynchronizerActionType::PlayerDesynced,
            Self::BeginPause => SynchronizerActionType::BeginPause,
            Self::EndPause => SynchronizerActionType::EndPause,
            Self::SkippedTickClosure { .. } => SynchronizerActionType::SkippedTickClosure,
            Self::SkippedTickClosureConfirm { .. } => SynchronizerActionType::SkippedTickClosureConfirm,
            Self::ChangeLatency { .. } => SynchronizerActionType::ChangeLatency,
            Self::IncreasedLatencyConfirm { .. } => SynchronizerActionType::IncreasedLatencyConfirm,
            Self::SavingCountdown { .. } => SynchronizerActionType::SavingCountdown,
        }
    }

    /// Serialize the synchronizer action to bytes
    pub fn write(&self, writer: &mut BinaryWriter) {
        writer.write_u8(self.action_type() as u8);

        match self {
            Self::GameEnd => {}
            Self::PeerDisconnect { disconnect_type } => {
                writer.write_u8(*disconnect_type);
            }
            Self::NewPeerInfo { peer_name } => {
                writer.write_string(peer_name);
            }
            Self::ClientChangedState { state } => {
                writer.write_u8(*state);
            }
            Self::ClientShouldStartSendingTickClosures { tick } => {
                writer.write_u64_le(*tick);
            }
            Self::MapReadyForDownload { data } => {
                writer.write_bytes(data);
            }
            Self::MapLoadingProgressUpdate { progress } => {
                writer.write_u8(*progress);
            }
            Self::MapSavingProgressUpdate { progress } => {
                writer.write_u8(*progress);
            }
            Self::SavingForUpdate | Self::PlayerDesynced | Self::BeginPause | Self::EndPause => {}
            Self::MapDownloadingProgressUpdate { progress } => {
                writer.write_u8(*progress);
            }
            Self::CatchingUpProgressUpdate { progress } => {
                writer.write_u8(*progress);
            }
            Self::PeerDroppingProgressUpdate { progress } => {
                writer.write_u8(*progress);
            }
            Self::SkippedTickClosure { tick } => {
                writer.write_u64_le(*tick);
            }
            Self::SkippedTickClosureConfirm { tick } => {
                writer.write_u64_le(*tick);
            }
            Self::ChangeLatency { latency } => {
                writer.write_u8(*latency);
            }
            Self::IncreasedLatencyConfirm { tick, latency } => {
                writer.write_u64_le(*tick);
                writer.write_u8(*latency);
            }
            Self::SavingCountdown { tick, remaining } => {
                writer.write_u64_le(*tick);
                writer.write_u32_le(*remaining);
            }
        }
    }
}

/// Write synchronizer action count to a writer
pub fn write_sync_action_count(writer: &mut BinaryWriter, count: usize) {
    if count < 255 {
        writer.write_u8(count as u8);
    } else {
        writer.write_u8(0xFF);
        writer.write_u32_le(count as u32);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_action_type_from_u8() {
        assert_eq!(SynchronizerActionType::from_u8(0x00), Some(SynchronizerActionType::GameEnd));
        assert_eq!(SynchronizerActionType::from_u8(0x04), Some(SynchronizerActionType::ClientShouldStartSendingTickClosures));
        assert_eq!(SynchronizerActionType::from_u8(0x0F), Some(SynchronizerActionType::SkippedTickClosure));
        assert_eq!(SynchronizerActionType::from_u8(0x99), None);
    }

    #[test]
    fn test_write_client_changed_state() {
        let action = SynchronizerAction::ClientChangedState { state: 0x7f };
        let mut writer = BinaryWriter::new();
        action.write(&mut writer);

        let data = writer.as_slice();
        assert_eq!(data[0], 0x03); // ClientChangedState type
        assert_eq!(data.len(), 2); // 1 byte type + 1 byte payload
    }
}
