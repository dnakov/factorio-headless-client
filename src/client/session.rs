use std::net::SocketAddr;
use std::time::Duration;

use crate::error::Result;
use crate::protocol::{Connection, ConnectionState};
use crate::state::{GameWorld, PlayerId};
use crate::state::entity::entity_type_from_name;
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
        let mut world = GameWorld::new();

        if let Some(map) = connection.parsed_map.take() {
            Self::populate_world_from_map(&mut world, &map);
        }

        Ok(Self {
            connection,
            world,
            local_player_id,
            events: EventCollector::new(),
        })
    }

    fn populate_world_from_map(world: &mut GameWorld, map: &crate::codec::map_transfer::MapData) {
        world.seed = map.seed;
        world.tick = map.ticks_played;

        if let Some(nauvis) = world.nauvis_mut() {
            for tile in &map.tiles {
                let chunk_pos = crate::codec::ChunkPosition {
                    x: tile.x.div_euclid(32),
                    y: tile.y.div_euclid(32),
                };
                let chunk = nauvis.get_or_create_chunk(chunk_pos);
                chunk.generated = true;
                let lx = tile.x.rem_euclid(32) as u8;
                let ly = tile.y.rem_euclid(32) as u8;
                chunk.set_tile(lx, ly, crate::state::surface::Tile::new(&tile.name));
            }

            for map_ent in &map.entities {
                let id = nauvis.entities.len() as u32 + 1;
                let pos = crate::codec::MapPosition {
                    x: crate::codec::Fixed32((map_ent.x * 256.0) as i32),
                    y: crate::codec::Fixed32((map_ent.y * 256.0) as i32),
                };
                let mut entity = crate::state::entity::Entity::new(id, map_ent.name.clone(), pos);
                entity.entity_type = entity_type_from_name(&map_ent.name);
                nauvis.entities.insert(id, entity);
            }
        }
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
