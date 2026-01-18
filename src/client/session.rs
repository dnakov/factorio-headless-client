use std::net::SocketAddr;
use std::time::Duration;

use crate::error::Result;
use crate::protocol::{Connection, ConnectionState};
use crate::state::{GameWorld, PlayerId};
use crate::client::events::{GameEvent, EventCollector};

/// Client configuration
#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub server_addr: SocketAddr,
    pub username: String,
    pub receive_timeout: Duration,
}

impl ClientConfig {
    pub fn new(server_addr: SocketAddr, username: impl Into<String>) -> Self {
        Self {
            server_addr,
            username: username.into(),
            receive_timeout: Duration::from_millis(100),
        }
    }
}

/// Builder for creating client sessions
pub struct ClientBuilder {
    config: ClientConfig,
}

impl ClientBuilder {
    pub fn new(server_addr: SocketAddr, username: impl Into<String>) -> Self {
        Self {
            config: ClientConfig::new(server_addr, username),
        }
    }

    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.config.receive_timeout = timeout;
        self
    }

    pub async fn connect(self) -> Result<Session> {
        Session::connect(self.config).await
    }
}

/// Active game session
pub struct Session {
    connection: Connection,
    world: GameWorld,
    local_player_id: Option<PlayerId>,
    events: EventCollector,
}

impl Session {
    async fn connect(config: ClientConfig) -> Result<Self> {
        let mut connection = Connection::new(
            config.server_addr,
            config.username,
        ).await?;

        connection.connect().await?;

        let local_player_id = connection.player_index();

        Ok(Self {
            connection,
            world: GameWorld::new(),
            local_player_id,
            events: EventCollector::new(),
        })
    }

    /// Get the current game world state
    pub fn world(&self) -> &GameWorld {
        &self.world
    }

    /// Get mutable access to the game world
    pub fn world_mut(&mut self) -> &mut GameWorld {
        &mut self.world
    }

    /// Get the local player ID
    pub fn local_player_id(&self) -> Option<PlayerId> {
        self.local_player_id
    }

    /// Get the current game tick
    pub fn current_tick(&self) -> u32 {
        self.world.tick
    }

    /// Get the server name
    pub fn server_name(&self) -> Option<&str> {
        self.connection.server_name()
    }

    /// Drain pending events
    pub fn drain_events(&mut self) -> Vec<GameEvent> {
        self.events.drain()
    }

    /// Check if connected
    pub fn is_connected(&self) -> bool {
        self.connection.state() == ConnectionState::Connected
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_config() {
        let addr: SocketAddr = "127.0.0.1:34197".parse().unwrap();
        let config = ClientConfig::new(addr, "TestBot");

        assert_eq!(config.username, "TestBot");
        assert_eq!(config.server_addr, addr);
    }
}
