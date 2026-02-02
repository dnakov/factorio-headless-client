use std::collections::HashMap;
use std::path::Path;
use std::sync::OnceLock;
use mlua::{Result as LuaResult, Table, Value};
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
    pub resource_infinite: bool,
    pub resource_mining_time: Option<f64>,
    pub pickup_position: Option<(f64, f64)>,
    pub drop_position: Option<(f64, f64)>,
    pub inserter_stack_size: Option<u8>,
    pub inserter_rotation_speed: Option<f64>,
    pub inserter_extension_speed: Option<f64>,
    pub belt_speed: Option<f64>,
    pub underground_max_distance: Option<u8>,
    pub crafting_speed: Option<f64>,
    pub mining_speed: Option<f64>,
    pub running_speed: Option<f64>,
    pub distance_per_frame: Option<f64>,
    pub maximum_corner_sliding_distance: Option<f64>,
}

#[derive(Debug, Clone)]
pub struct ItemPrototype {
    pub name: String,
    pub item_type: String,
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
    if let Err(e) = lua.load_base_tiles() {
        eprintln!("[lua] skipping base tiles: {}", e);
    }
    if factorio_path.join("space-age").exists() {
        let space_age_tiles = [
            "space-age/prototypes/tile/tiles.lua",
            "space-age/prototypes/tile/tiles-aquilo.lua",
            "space-age/prototypes/tile/tiles-fulgora.lua",
            "space-age/prototypes/tile/tiles-gleba.lua",
            "space-age/prototypes/tile/tiles-vulcanus.lua",
        ];
        for file in space_age_tiles {
            if let Err(e) = lua.load_prototype_file(file) {
                eprintln!("[lua] skipping {}: {}", file, e);
            }
        }
    }

        let data_raw = lua.data_raw()?;

    let items = extract_items(&data_raw)?;
    let entities = extract_entities(&data_raw)?;
    let tiles = extract_tiles(&data_raw)?;
    let recipes = extract_recipes(&data_raw)?;

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
                        item_type: item_type.to_string(),
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
            let resource_infinite = if entity_type == "resource" {
                proto.get::<bool>("infinite").unwrap_or(false)
            } else {
                false
            };
            let resource_mining_time = if entity_type == "resource" {
                proto.get::<Table>("minable").ok().and_then(|t| t.get::<f64>("mining_time").ok())
            } else {
                None
            };
            let (pickup_position, drop_position) = if entity_type == "inserter" {
                (
                    proto.get::<Value>("pickup_position").ok().and_then(parse_position),
                    proto.get::<Value>("insert_position").ok().and_then(parse_position),
                )
            } else {
                (None, None)
            };
            let inserter_stack_size = if entity_type == "inserter" {
                proto.get::<u8>("stack_size_bonus").ok()
            } else {
                None
            };
            let inserter_rotation_speed = if entity_type == "inserter" {
                proto.get::<f64>("rotation_speed").ok()
            } else {
                None
            };
            let inserter_extension_speed = if entity_type == "inserter" {
                proto.get::<f64>("extension_speed").ok()
            } else {
                None
            };
            let belt_speed = proto.get::<f64>("speed").ok();
            let underground_max_distance = proto.get::<u8>("max_distance").ok();
            let crafting_speed = proto.get::<f64>("crafting_speed").ok();
            let mining_speed = proto.get::<f64>("mining_speed").ok();
            let running_speed = proto.get::<f64>("running_speed").ok();
            let distance_per_frame = proto.get::<f64>("distance_per_frame").ok();
            let maximum_corner_sliding_distance = proto.get::<f64>("maximum_corner_sliding_distance").ok();
            entities.insert(name.clone(), EntityPrototype {
                name,
                collision_box,
                collides_player,
                resource_infinite,
                resource_mining_time,
                pickup_position,
                drop_position,
                inserter_stack_size,
                inserter_rotation_speed,
                inserter_extension_speed,
                belt_speed,
                underground_max_distance,
                crafting_speed,
                mining_speed,
                running_speed,
                distance_per_frame,
                maximum_corner_sliding_distance,
            });
        }
    }

    Ok(entities)
}

fn extract_recipes(data_raw: &Table) -> LuaResult<HashMap<String, RecipePrototype>> {
    let mut recipes = HashMap::new();
    let recipe_table: Table = match data_raw.get("recipe") {
        Ok(t) => t,
        Err(_) => return Ok(recipes),
    };

    for pair in recipe_table.pairs::<String, Table>() {
        let (name, proto) = match pair {
            Ok(p) => p,
            Err(_) => continue,
        };

        let data = if let Ok(normal) = proto.get::<Table>("normal") {
            normal
        } else {
            proto.clone()
        };

        let category: String = data.get("category").unwrap_or_else(|_| "crafting".to_string());
        let energy_required: f64 = data.get("energy_required").unwrap_or(0.5);
        let ingredients = data
            .get::<Table>("ingredients")
            .ok()
            .map(parse_ingredients)
            .unwrap_or_default();

        let results = if let Ok(results_table) = data.get::<Table>("results") {
            parse_ingredients(results_table)
        } else if let Ok(result_name) = data.get::<String>("result") {
            let count: u32 = data.get("result_count").unwrap_or(1);
            vec![Ingredient { name: result_name, amount: count, ingredient_type: "item".to_string() }]
        } else {
            Vec::new()
        };

        recipes.insert(name.clone(), RecipePrototype {
            name,
            category,
            energy_required,
            ingredients,
            results,
        });
    }

    Ok(recipes)
}

fn parse_position(value: Value) -> Option<(f64, f64)> {
    match value {
        Value::Table(t) => {
            if let Ok(x) = t.get::<f64>("x") {
                if let Ok(y) = t.get::<f64>("y") {
                    return Some((x, y));
                }
            }
            let x = t.get::<f64>(1).ok()?;
            let y = t.get::<f64>(2).ok()?;
            Some((x, y))
        }
        _ => None,
    }
}

fn parse_ingredients(table: Table) -> Vec<Ingredient> {
    let mut list = Vec::new();
    for value in table.sequence_values::<Value>() {
        let value = match value {
            Ok(v) => v,
            Err(_) => continue,
        };
        if let Some(ing) = parse_ingredient(value) {
            list.push(ing);
        }
    }
    list
}

fn parse_ingredient(value: Value) -> Option<Ingredient> {
    match value {
        Value::Table(t) => {
            if let Ok(name) = t.get::<String>("name") {
                let amount: u32 = t.get("amount").unwrap_or(1);
                let ingredient_type: String = t.get("type").unwrap_or_else(|_| "item".to_string());
                Some(Ingredient { name, amount, ingredient_type })
            } else {
                let name: String = t.get(1).ok()?;
                let amount: u32 = t.get(2).unwrap_or(1);
                let ingredient_type: String = t.get("type").unwrap_or_else(|_| "item".to_string());
                Some(Ingredient { name, amount, ingredient_type })
            }
        }
        _ => None,
    }
}

fn extract_tiles(data_raw: &Table) -> LuaResult<HashMap<String, TilePrototype>> {
    let mut tiles = HashMap::new();
    let tile_table: Table = match data_raw.get("tile") {
        Ok(t) => t,
        Err(_) => return Ok(tiles),
    };

    for pair in tile_table.pairs::<String, Table>() {
        let (name, proto) = match pair {
            Ok(p) => p,
            Err(_) => continue,
        };

        let collision_mask = extract_collision_mask(&proto);
        let walking_speed_modifier: f64 = proto.get("walking_speed_modifier").unwrap_or(1.0);
        let map_color = extract_map_color(&proto);

        tiles.insert(name.clone(), TilePrototype {
            name,
            collision_mask,
            walking_speed_modifier,
            map_color,
        });
    }

    Ok(tiles)
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

fn extract_collision_mask(proto: &Table) -> Option<Vec<String>> {
    let mask: Table = proto.get("collision_mask").ok()?;

    if let Ok(layers) = mask.get::<Table>("layers") {
        let mut out = Vec::new();
        for pair in layers.pairs::<String, mlua::Value>() {
            let (layer, val) = pair.ok()?;
            let enabled = match val {
                mlua::Value::Boolean(b) => b,
                _ => false,
            };
            if enabled {
                out.push(layer);
            }
        }
        if !out.is_empty() {
            return Some(out);
        }
    }

    let mut out = Vec::new();
    for layer in mask.sequence_values::<String>() {
        if let Ok(v) = layer {
            out.push(v);
        }
    }
    if out.is_empty() {
        None
    } else {
        Some(out)
    }
}

fn extract_map_color(proto: &Table) -> Option<(u8, u8, u8)> {
    let map_color: Table = proto.get("map_color").ok()?;
    if let (Ok(r), Ok(g), Ok(b)) = (
        map_color.get::<f64>("r"),
        map_color.get::<f64>("g"),
        map_color.get::<f64>("b"),
    ) {
        return Some((clamp_color(r), clamp_color(g), clamp_color(b)));
    }
    if let (Ok(r), Ok(g), Ok(b)) = (
        map_color.get::<f64>(1),
        map_color.get::<f64>(2),
        map_color.get::<f64>(3),
    ) {
        return Some((clamp_color(r), clamp_color(g), clamp_color(b)));
    }
    None
}

fn clamp_color(v: f64) -> u8 {
    if v <= 1.0 {
        (v * 255.0).round().clamp(0.0, 255.0) as u8
    } else {
        v.round().clamp(0.0, 255.0) as u8
    }
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
