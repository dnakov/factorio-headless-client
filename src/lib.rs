//! Factorio Client API
//!
//! A Rust library for connecting to Factorio multiplayer servers
//! and interacting with the game programmatically.

pub mod codec;
pub mod error;
pub mod protocol;
pub mod state;
pub mod simulation;
pub mod client;
pub mod bot;
pub mod daemon;
pub mod lua;
pub use factorio_mapgen as noise;

pub use error::{Error, Result};
pub use protocol::{
    Connection, ConnectionState, BuildVersion, MessageType,
    ModInfo, ModVersion, ServerInfo,
};
pub use codec::{
    Fixed32, MapPosition, TilePosition, ChunkPosition,
    Direction, Color, BoundingBox,
    InputAction, InputActionType,
    ShootingState, RidingAcceleration, RidingDirection,
    MapTransfer, MapData,
};
pub use state::{
    GameWorld, Surface, Entity, EntityType,
    Player, PlayerId, Inventory, ItemStack,
};
pub use simulation::{TickExecutor, TickResult, ActionExecutor};
pub use client::{Session, ClientBuilder, ActionBuilder, GameEvent};
pub use bot::{PlayerController, CraftingManager};
pub use lua::{FactorioLua, Prototypes};
