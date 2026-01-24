use std::collections::HashMap;
use std::path::Path;
use std::sync::OnceLock;
use mlua::{Result as LuaResult, Table};
use crate::lua::FactorioLua;

static PROTOTYPES: OnceLock<Prototypes> = OnceLock::new();

pub struct Prototypes {
    items: HashMap<String, ItemPrototype>,
    recipes: HashMap<String, RecipePrototype>,
    tiles: HashMap<String, TilePrototype>,
    entities: HashMap<String, EntityPrototype>,
}

#[derive(Debug, Clone)]
pub struct EntityPrototype {
    pub name: String,
    pub collision_box: [f64; 4], // [x1, y1, x2, y2] relative to entity center
    pub collides_player: bool,
}

#[derive(Debug, Clone)]
pub struct ItemPrototype {
    pub name: String,
    pub stack_size: u32,
    pub weight: Option<f64>,
    pub fuel_value: Option<String>,
    pub fuel_category: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Ingredient {
    pub name: String,
    pub amount: u32,
    pub ingredient_type: String,
}

#[derive(Debug, Clone)]
pub struct RecipePrototype {
    pub name: String,
    pub category: String,
    pub energy_required: f64,
    pub ingredients: Vec<Ingredient>,
    pub results: Vec<Ingredient>,
}

#[derive(Debug, Clone)]
pub struct TilePrototype {
    pub name: String,
    pub collision_mask: Option<Vec<String>>,
    pub walking_speed_modifier: f64,
    pub map_color: Option<(u8, u8, u8)>,
}

impl Prototypes {
    pub fn load(factorio_path: &Path) -> LuaResult<Self> {
        let lua = FactorioLua::new(factorio_path)?;
        lua.load_dataloader()?;
        lua.load_util()?;
        lua.load_base_items()?;
        lua.load_base_entities()?;

        let data_raw = lua.data_raw()?;

        let items = extract_items(&data_raw)?;
        let entities = extract_entities(&data_raw)?;
        let recipes = HashMap::new();
        let tiles = HashMap::new();

        Ok(Self { items, recipes, tiles, entities })
    }

    pub fn global() -> Option<&'static Prototypes> {
        PROTOTYPES.get()
    }

    pub fn init_global(factorio_path: &Path) -> LuaResult<&'static Prototypes> {
        if let Some(p) = PROTOTYPES.get() {
            return Ok(p);
        }

        let protos = Self::load(factorio_path)?;
        Ok(PROTOTYPES.get_or_init(|| protos))
    }

    pub fn item(&self, name: &str) -> Option<&ItemPrototype> {
        self.items.get(name)
    }

    pub fn stack_size(&self, name: &str) -> u32 {
        self.items.get(name).map(|i| i.stack_size).unwrap_or(50)
    }

    pub fn recipe(&self, name: &str) -> Option<&RecipePrototype> {
        self.recipes.get(name)
    }

    pub fn tile(&self, name: &str) -> Option<&TilePrototype> {
        self.tiles.get(name)
    }

    pub fn items(&self) -> &HashMap<String, ItemPrototype> {
        &self.items
    }

    pub fn recipes(&self) -> &HashMap<String, RecipePrototype> {
        &self.recipes
    }

    pub fn tiles(&self) -> &HashMap<String, TilePrototype> {
        &self.tiles
    }

    pub fn entity(&self, name: &str) -> Option<&EntityPrototype> {
        self.entities.get(name)
    }

    pub fn entities(&self) -> &HashMap<String, EntityPrototype> {
        &self.entities
    }
}

fn extract_items(data_raw: &Table) -> LuaResult<HashMap<String, ItemPrototype>> {
    let mut items = HashMap::new();

    // Item types to extract from
    let item_types = ["item", "tool", "ammo", "armor", "gun", "capsule", "module", "rail-planner", "item-with-entity-data"];

    for item_type in item_types {
        if let Ok(type_table) = data_raw.get::<Table>(item_type) {
            for pair in type_table.pairs::<String, Table>() {
                if let Ok((name, proto)) = pair {
                    let stack_size: u32 = proto.get("stack_size").unwrap_or(50);
                    let weight: Option<f64> = proto.get("weight").ok();
                    let fuel_value: Option<String> = proto.get("fuel_value").ok();
                    let fuel_category: Option<String> = proto.get("fuel_category").ok();

                    items.insert(name.clone(), ItemPrototype {
                        name,
                        stack_size,
                        weight,
                        fuel_value,
                        fuel_category,
                    });
                }
            }
        }
    }

    Ok(items)
}

fn extract_entities(data_raw: &Table) -> LuaResult<HashMap<String, EntityPrototype>> {
    let mut entities = HashMap::new();

    let entity_types = [
        "furnace", "assembling-machine", "transport-belt", "underground-belt",
        "splitter", "inserter", "container", "logistic-container", "pipe",
        "pipe-to-ground", "electric-pole", "lamp", "mining-drill",
        "offshore-pump", "boiler", "generator", "solar-panel", "accumulator",
        "reactor", "rocket-silo", "beacon", "lab", "roboport", "radar",
        "wall", "gate", "storage-tank", "pump", "arithmetic-combinator",
        "decider-combinator", "constant-combinator", "train-stop",
        "rail-signal", "rail-chain-signal", "locomotive", "cargo-wagon",
        "fluid-wagon", "artillery-wagon", "car", "spider-vehicle",
        "turret", "ammo-turret", "electric-turret", "fluid-turret",
        "artillery-turret", "unit-spawner", "tree", "simple-entity",
        "resource", "cliff", "straight-rail", "curved-rail",
        "heat-pipe", "centrifuge", "land-mine",
    ];

    for entity_type in entity_types {
        let type_table: Table = match data_raw.get(entity_type) {
            Ok(t) => t,
            Err(_) => continue,
        };
        for pair in type_table.pairs::<String, Table>() {
            let (name, proto) = match pair {
                Ok(p) => p,
                Err(_) => continue,
            };

            let (collision_box, collides_player) = extract_collision(&proto);
            entities.insert(name.clone(), EntityPrototype {
                name,
                collision_box,
                collides_player,
            });
        }
    }

    Ok(entities)
}

fn extract_collision(proto: &Table) -> ([f64; 4], bool) {
    let collision_box = extract_collision_box(proto).unwrap_or([-0.4, -0.4, 0.4, 0.4]);

    // Check collision_mask.layers.player (defaults to true for most entities)
    let collides_player = match proto.get::<Table>("collision_mask") {
        Ok(mask) => match mask.get::<Table>("layers") {
            Ok(layers) => layers.get::<bool>("player").unwrap_or(true),
            Err(_) => true,
        },
        Err(_) => true,
    };

    (collision_box, collides_player)
}

fn extract_collision_box(proto: &Table) -> Option<[f64; 4]> {
    let cbox: Table = proto.get("collision_box").ok()?;
    let p1: Table = cbox.get(1).ok()?;
    let p2: Table = cbox.get(2).ok()?;
    let x1: f64 = p1.get(1).ok()?;
    let y1: f64 = p1.get(2).ok()?;
    let x2: f64 = p2.get(1).ok()?;
    let y2: f64 = p2.get(2).ok()?;
    Some([x1, y1, x2, y2])
}

pub fn stack_size(item_name: &str) -> u32 {
    Prototypes::global().map(|p| p.stack_size(item_name)).unwrap_or(50)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_prototypes() {
        let factorio_path = Path::new("/Applications/factorio.app/Contents/data");
        if !factorio_path.exists() {
            eprintln!("Factorio not installed, skipping test");
            return;
        }

        let protos = Prototypes::load(factorio_path).expect("Failed to load prototypes");

        // Check that we loaded some items
        assert!(!protos.items.is_empty(), "No items loaded");

        // Check specific items
        let iron_plate = protos.item("iron-plate");
        assert!(iron_plate.is_some(), "iron-plate not found");
        assert_eq!(iron_plate.unwrap().stack_size, 100);

        let coal = protos.item("coal");
        assert!(coal.is_some(), "coal not found");
        assert_eq!(coal.unwrap().stack_size, 50);
    }
}
