use crate::codec::{InputAction, MapPosition, Direction};
use crate::state::{PlayerId, EntityId};

/// Game events that can be received
#[derive(Debug, Clone)]
pub enum GameEvent {
    /// Tick advanced
    TickAdvanced {
        tick: u32,
    },

    /// Player joined the game
    PlayerJoined {
        player_id: PlayerId,
        name: String,
    },

    /// Player left the game
    PlayerLeft {
        player_id: PlayerId,
        reason: DisconnectReason,
    },

    /// Player changed position
    PlayerMoved {
        player_id: PlayerId,
        position: MapPosition,
        direction: Direction,
    },

    /// Entity was created
    EntityCreated {
        entity_id: EntityId,
        name: String,
        position: MapPosition,
    },

    /// Entity was removed
    EntityRemoved {
        entity_id: EntityId,
    },

    /// Chat message received
    ChatMessage {
        player_id: Option<PlayerId>,
        message: String,
    },

    /// Research completed
    ResearchCompleted {
        technology: String,
    },

    /// Research started
    ResearchStarted {
        technology: String,
    },

    /// Input action was executed (for debugging)
    ActionExecuted {
        player_id: PlayerId,
        action: InputAction,
    },

    /// Map download progress
    MapDownloadProgress {
        received: usize,
        total: usize,
    },

    /// Map download completed
    MapDownloadComplete,

    /// Connection established
    Connected {
        player_index: PlayerId,
    },

    /// Disconnected from server
    Disconnected {
        reason: DisconnectReason,
    },

    /// Desync detected
    Desync {
        tick: u32,
        local_crc: u32,
        server_crc: u32,
    },
}

/// Reason for disconnection
#[derive(Debug, Clone)]
pub enum DisconnectReason {
    UserRequested,
    Kicked,
    Banned,
    Timeout,
    ServerShutdown,
    VersionMismatch,
    ModMismatch,
    Desync,
    Other(String),
}

impl DisconnectReason {
    pub fn from_code(code: u8) -> Self {
        match code {
            0 => Self::UserRequested,
            1 => Self::Kicked,
            2 => Self::Banned,
            3 => Self::Timeout,
            4 => Self::ServerShutdown,
            5 => Self::VersionMismatch,
            6 => Self::ModMismatch,
            7 => Self::Desync,
            _ => Self::Other(format!("unknown code: {}", code)),
        }
    }
}

/// Event handler trait
pub trait EventHandler {
    fn on_event(&mut self, event: GameEvent);
}

/// Simple event collector
#[derive(Debug, Default)]
pub struct EventCollector {
    events: Vec<GameEvent>,
}

impl EventCollector {
    pub fn new() -> Self {
        Self { events: Vec::new() }
    }

    pub fn drain(&mut self) -> Vec<GameEvent> {
        std::mem::take(&mut self.events)
    }

    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }
}

impl EventHandler for EventCollector {
    fn on_event(&mut self, event: GameEvent) {
        self.events.push(event);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_collector() {
        let mut collector = EventCollector::new();
        assert!(collector.is_empty());

        collector.on_event(GameEvent::TickAdvanced { tick: 1 });
        collector.on_event(GameEvent::TickAdvanced { tick: 2 });

        assert!(!collector.is_empty());

        let events = collector.drain();
        assert_eq!(events.len(), 2);
        assert!(collector.is_empty());
    }
}
