use crate::codec::{MapPosition, Direction, Color};
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
pub struct InserterData {
    pub pickup_position: Option<MapPosition>,
    pub drop_position: Option<MapPosition>,
    pub filter_mode: Option<String>,
    pub filters: Vec<String>,
    pub stack_size_override: Option<u8>,
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
}

#[derive(Debug, Clone, Default)]
pub struct ContainerData {
    pub bar: Option<u16>,
}

#[derive(Debug, Clone, Default)]
pub struct TransportBeltData {
    pub line_contents: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub struct MiningDrillData {
    pub mining_target: Option<String>,
    pub mining_progress: f32,
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

