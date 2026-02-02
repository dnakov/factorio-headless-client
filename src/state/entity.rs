use crate::codec::{MapPosition, Direction, Color};
use crate::lua::prototype::EntityPrototype;
use crate::state::inventory::{Inventory, ItemStack};
use std::collections::HashMap;

/// Unique entity identifier
pub type EntityId = u32;

/// Entity unit number (stable across saves)
pub type UnitNumber = u32;

/// Entity in the game world
#[derive(Debug, Clone)]
pub struct Entity {
    pub id: EntityId,
    pub unit_number: Option<UnitNumber>,
    pub name: String,
    pub entity_type: EntityType,
    pub position: MapPosition,
    pub direction: Direction,
    pub health: Option<f32>,
    pub max_health: Option<f32>,
    pub active: bool,
    pub data: EntityData,
    pub inventories: HashMap<String, Inventory>,
    pub item_stack: Option<ItemStack>,
}

impl Entity {
    pub fn new(id: EntityId, name: String, position: MapPosition) -> Self {
        Self {
            id,
            unit_number: None,
            name,
            entity_type: EntityType::Unknown,
            position,
            direction: Direction::North,
            health: None,
            max_health: None,
            active: true,
            data: EntityData::None,
            inventories: HashMap::new(),
            item_stack: None,
        }
    }

    pub fn with_type(mut self, entity_type: EntityType) -> Self {
        self.entity_type = entity_type;
        self
    }

    pub fn with_direction(mut self, direction: Direction) -> Self {
        self.direction = direction;
        self
    }
}

pub fn default_entity_data_for_type(entity_type: EntityType) -> EntityData {
    match entity_type {
        EntityType::AssemblingMachine => EntityData::AssemblingMachine(Default::default()),
        EntityType::Furnace => EntityData::Furnace(Default::default()),
        EntityType::Container | EntityType::LogisticContainer => EntityData::Container(Default::default()),
        EntityType::TransportBelt | EntityType::UndergroundBelt | EntityType::Splitter => {
            EntityData::TransportBelt(Default::default())
        }
        EntityType::Inserter => EntityData::Inserter(Default::default()),
        EntityType::MiningDrill => EntityData::MiningDrill(Default::default()),
        EntityType::Lab => EntityData::Lab(Default::default()),
        EntityType::Accumulator => EntityData::Accumulator(Default::default()),
        EntityType::ArithmeticCombinator
        | EntityType::DeciderCombinator
        | EntityType::ConstantCombinator => EntityData::Combinator(Default::default()),
        EntityType::TrainStop => EntityData::TrainStop(Default::default()),
        EntityType::RocketSilo => EntityData::RocketSilo(Default::default()),
        EntityType::Roboport => EntityData::Roboport(Default::default()),
        _ => EntityData::None,
    }
}

pub fn init_entity_inventories(entity: &mut Entity) {
    match entity.entity_type {
        EntityType::Container | EntityType::LogisticContainer => {
            entity.inventories.insert("main".into(), Inventory::new(48));
        }
        EntityType::Furnace => {
            entity.inventories.insert("source".into(), Inventory::new(2));
            entity.inventories.insert("result".into(), Inventory::new(2));
            entity.inventories.insert("fuel".into(), Inventory::new(1));
        }
        EntityType::AssemblingMachine => {
            entity.inventories.insert("input".into(), Inventory::new(6));
            entity.inventories.insert("output".into(), Inventory::new(6));
            entity.inventories.insert("fuel".into(), Inventory::new(1));
        }
        EntityType::MiningDrill => {
            entity.inventories.insert("output".into(), Inventory::new(6));
            entity.inventories.insert("fuel".into(), Inventory::new(1));
        }
        _ => {}
    }
}

pub fn init_belt_metadata(entity: &mut Entity) {
    let crate::state::entity::EntityData::TransportBelt(ref mut data) = entity.data else {
        return;
    };
    data.is_underground = entity.entity_type == EntityType::UndergroundBelt;
    data.is_splitter = entity.entity_type == EntityType::Splitter;
    if !data.is_underground {
        data.underground_type = None;
    }
}

pub fn apply_entity_prototype(entity: &mut Entity, proto: &EntityPrototype) {
    match &mut entity.data {
        EntityData::Resource(data) => {
            if data.mining_time <= 0.0 {
                if let Some(mining_time) = proto.resource_mining_time {
                    data.mining_time = mining_time as f32;
                }
            }
        }
        EntityData::AssemblingMachine(data) => {
            if data.crafting_speed <= 0.0 {
                if let Some(speed) = proto.crafting_speed {
                    data.crafting_speed = speed as f32;
                }
            }
        }
        EntityData::Furnace(data) => {
            if data.crafting_speed <= 0.0 {
                if let Some(speed) = proto.crafting_speed {
                    data.crafting_speed = speed as f32;
                }
            }
        }
        EntityData::MiningDrill(data) => {
            if data.mining_speed <= 0.0 {
                if let Some(speed) = proto.mining_speed {
                    data.mining_speed = speed as f32;
                }
            }
        }
        EntityData::TransportBelt(data) => {
            if data.underground_max_distance.is_none() {
                data.underground_max_distance = proto.underground_max_distance;
            }
        }
        _ => {}
    }
}

/// Entity type categories
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EntityType {
    // Resources
    Resource,

    // Logistics
    TransportBelt,
    UndergroundBelt,
    Splitter,
    Loader,
    Inserter,
    Container,
    LogisticContainer,
    Pipe,
    PipeToGround,
    Pump,

    // Production
    AssemblingMachine,
    Furnace,
    RocketSilo,
    Lab,
    MiningDrill,
    OffshorePump,
    Boiler,
    Generator,
    SolarPanel,
    Accumulator,
    Reactor,
    HeatPipe,

    // Circuit network
    ArithmeticCombinator,
    DeciderCombinator,
    ConstantCombinator,
    PowerSwitch,
    ProgrammableSpeaker,
    Lamp,

    // Defense
    Wall,
    Gate,
    Turret,
    AmmoTurret,
    ElectricTurret,
    FluidTurret,
    ArtilleryTurret,
    Radar,

    // Vehicles
    Car,
    Tank,
    SpiderVehicle,
    Locomotive,
    CargoWagon,
    FluidWagon,
    ArtilleryWagon,

    // Rails
    StraightRail,
    CurvedRail,
    RailSignal,
    RailChainSignal,
    TrainStop,

    // Robots
    RoboportEquipment,
    Roboport,
    ConstructionRobot,
    LogisticRobot,

    // Player
    Character,
    Corpse,

    // Misc
    ElectricPole,
    Tree,
    SimpleEntity,
    Fish,
    Cliff,

    Unknown,
}


pub fn entity_type_from_name(name: &str) -> EntityType {
    match name {
        n if n.ends_with("-ore") || n == "crude-oil" || n == "uranium-ore" => EntityType::Resource,
        n if n.starts_with("tree-") || n.starts_with("dead-") => EntityType::Tree,
        n if n.starts_with("simple-entity") => EntityType::SimpleEntity,
        "fish" => EntityType::Fish,
        "character" => EntityType::Character,
        n if n.contains("transport-belt") => EntityType::TransportBelt,
        n if n.contains("underground-belt") => EntityType::UndergroundBelt,
        n if n.contains("splitter") => EntityType::Splitter,
        n if n.contains("inserter") => EntityType::Inserter,
        n if n.contains("assembling-machine") => EntityType::AssemblingMachine,
        n if n.contains("furnace") => EntityType::Furnace,
        n if n.contains("mining-drill") => EntityType::MiningDrill,
        n if n.contains("electric-pole") || n.contains("substation") => EntityType::ElectricPole,
        n if n.contains("pipe") && !n.contains("heat") => EntityType::Pipe,
        n if n.contains("chest") || n.contains("container") => EntityType::Container,
        n if n.contains("turret") => EntityType::Turret,
        n if n.contains("wall") => EntityType::Wall,
        n if n.contains("radar") => EntityType::Radar,
        n if n.contains("roboport") => EntityType::Roboport,
        n if n.contains("solar-panel") => EntityType::SolarPanel,
        n if n.contains("accumulator") => EntityType::Accumulator,
        n if n.contains("lab") => EntityType::Lab,
        n if n.contains("cliff") => EntityType::Cliff,
        n if n.contains("lamp") => EntityType::Lamp,
        _ => EntityType::Unknown,
    }
}

/// Entity-specific data
#[derive(Debug, Clone)]
pub enum EntityData {
    None,
    Resource(ResourceData),
    Inserter(InserterData),
    AssemblingMachine(AssemblingMachineData),
    Furnace(FurnaceData),
    Container(ContainerData),
    TransportBelt(TransportBeltData),
    MiningDrill(MiningDrillData),
    Lab(LabData),
    Accumulator(AccumulatorData),
    Combinator(CombinatorData),
    TrainStop(TrainStopData),
    RocketSilo(RocketSiloData),
    Roboport(RoboportData),
}

#[derive(Debug, Clone, Default)]
pub struct ResourceData {
    pub amount: u32,
    pub infinite: bool,
    pub mining_time: f32,
}

#[derive(Debug, Clone, Default)]
pub struct InserterData {
    pub pickup_position: Option<MapPosition>,
    pub drop_position: Option<MapPosition>,
    pub filter_mode: Option<String>,
    pub filters: Vec<String>,
    pub stack_size_override: Option<u8>,
    pub cooldown: u32,
}

#[derive(Debug, Clone, Default)]
pub struct AssemblingMachineData {
    pub recipe: Option<String>,
    pub crafting_progress: f32,
    pub crafting_speed: f32,
    pub productivity_bonus: f32,
}

#[derive(Debug, Clone, Default)]
pub struct FurnaceData {
    pub smelting_recipe: Option<String>,
    pub crafting_progress: f32,
    pub crafting_speed: f32,
}

#[derive(Debug, Clone, Default)]
pub struct ContainerData {
    pub bar: Option<u16>,
}

#[derive(Debug, Clone, Default)]
pub struct TransportBeltData {
    pub line_contents: Vec<String>,
    pub lane_items: [Vec<String>; 2],
    pub lane_progress: [f64; 2],
    pub underground_max_distance: Option<u8>,
    pub is_underground: bool,
    pub is_splitter: bool,
    pub underground_type: Option<u8>,
}

#[derive(Debug, Clone, Default)]
pub struct MiningDrillData {
    pub mining_target: Option<String>,
    pub mining_progress: f32,
    pub mining_speed: f32,
}

#[derive(Debug, Clone, Default)]
pub struct LabData {
    pub researching: bool,
    pub research_progress: f32,
}

#[derive(Debug, Clone, Default)]
pub struct AccumulatorData {
    pub energy: f64,
    pub max_energy: f64,
}

#[derive(Debug, Clone, Default)]
pub struct CombinatorData {
    pub parameters: HashMap<String, i32>,
    pub is_on: bool,
}

#[derive(Debug, Clone, Default)]
pub struct TrainStopData {
    pub station_name: String,
    pub train_limit: Option<u32>,
    pub color: Option<Color>,
}

#[derive(Debug, Clone, Default)]
pub struct RocketSiloData {
    pub rocket_parts: u32,
    pub rocket_ready: bool,
    pub auto_launch: bool,
}

#[derive(Debug, Clone, Default)]
pub struct RoboportData {
    pub available_construction_robots: u32,
    pub available_logistic_robots: u32,
    pub total_construction_robots: u32,
    pub total_logistic_robots: u32,
}
