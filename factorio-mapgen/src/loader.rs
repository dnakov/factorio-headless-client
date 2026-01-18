//! Factorio data loader
//!
//! Loads noise expressions from Factorio's actual Lua files.

use std::path::Path;
use std::collections::HashMap;
use mlua::{Lua, Table, Value, Result as LuaResult};
use crate::expression::{parse_expression, Expr};
use crate::compiler::FunctionDef;

/// Compute starting lake position from seed using Factorio's getStartingLakePositions algorithm
/// Reverse-engineered from MapGenSettings::getStartingLakePositions() at 0x1014b4424
///
/// The algorithm:
/// 1. For each starting position, generate a random angle using XORshift
/// 2. Place lake at radius 72 tiles from starting position at that angle
fn compute_starting_lake_position(seed: u32) -> (i32, i32) {
    let state = seed.max(0x155);

    // SIMD XORshift state (v20.2s initialized from max(seed, 0x155))
    let v20_0 = state;
    let v20_1 = state;

    // Scalar XORshift (from 0x1014b45a4-0x1014b45ac)
    // eor w8, w21, w21, lsl 13
    // lsl w21, w21, 0xc
    // bfxil w21, w8, 0x13, 0xd  (bits 0-12 of w21 = bits 19-31 of w8)
    let w8 = state ^ (state << 13);
    let w21 = ((state << 12) & 0xFFFFE000) | ((w8 >> 19) & 0x1FFF);

    // SIMD XORshift from binary
    // Shift constants at 0x10296c0d8: d8={3,2}, d9={17,4}, d10={-11,-25}, d11={0xffe00000,0xffffff80}
    // ushl v2.2s, v20.2s, v8.2s  -> v2[i] = v20[i] << d8[i]
    // ushl v3.2s, v20.2s, v9.2s  -> v3[i] = v20[i] << d9[i]
    // eor v2.8b, v2.8b, v20.8b   -> v2 = v2 ^ v20
    // ushl v2.2s, v2.2s, v10.2s  -> v2[i] = v2[i] >> -d10[i] (right shift)
    // and v3.8b, v3.8b, v11.8b   -> v3 = v3 & mask
    // orr v20.8b, v2.8b, v3.8b   -> v20 = v2 | v3
    let v2_0 = ((v20_0 << 3) ^ v20_0) >> 11;
    let v2_1 = ((v20_1 << 2) ^ v20_1) >> 25;
    let v3_0 = (v20_0 << 17) & 0xFFE00000;
    let v3_1 = (v20_1 << 4) & 0xFFFFFF80;
    let v20_new_0 = v2_0 | v3_0;
    let v20_new_1 = v2_1 | v3_1;

    // Combine to get random value (from 0x1014b45c8-0x1014b45d4)
    // mov w8, v20.s[1]; fmov w9, s20; eor w9, w9, w21; eor w8, w8, w9
    let rand = v20_new_1 ^ v20_new_0 ^ w21;

    // Convert to angle: rand * 2^-32 * 2π
    let scale = 1.0 / 4294967296.0;
    let two_pi = std::f64::consts::PI * 2.0;
    let angle = (rand as f64) * scale * two_pi;

    // Starting position is (0, 0) - radius is 75 tiles (from 0x4052c00000000000)
    let radius = 75.0;
    let start_x = 0.0;
    let start_y = 0.0;

    // Compute lake position
    // From binary at 0x1014b46bc-0x1014b46d0: both use fadd (addition)
    // lake_x = starting_x + radius * cos(angle)
    // lake_y = starting_y + radius * sin(angle)
    let lake_x = start_x + radius * angle.cos();
    let lake_y = start_y + radius * angle.sin();

    // Binary uses fcvtzs (truncate to integer) then lsl 8 (multiply by 256)
    let x = (lake_x as i32) * 256;
    let y = (lake_y as i32) * 256;

    (x, y)
}

/// Noise expression definition from Lua
#[derive(Debug, Clone)]
pub struct NoiseExpressionDef {
    pub name: String,
    pub expression: Expr,
    pub local_expressions: HashMap<String, Expr>,
}

/// Noise function definition from Lua
#[derive(Debug, Clone)]
pub struct NoiseFunctionDef {
    pub name: String,
    pub parameters: Vec<String>,
    pub expression: Expr,
    pub local_expressions: HashMap<String, Expr>,
}

/// Tile definition from Lua
#[derive(Debug, Clone)]
pub struct TileDef {
    pub name: String,
    pub probability_expression: Option<String>,
}

/// Loaded Factorio data
pub struct FactorioData {
    pub noise_expressions: HashMap<String, NoiseExpressionDef>,
    pub noise_functions: HashMap<String, NoiseFunctionDef>,
    pub tiles: Vec<TileDef>,
    /// Starting lake positions (computed from seed)
    pub starting_lake_positions: Vec<(f32, f32)>,
    /// Starting positions (spawn points)
    pub starting_positions: Vec<(f32, f32)>,
}

impl FactorioData {
    /// Load from Factorio's data directory with map seed
    pub fn load_with_seed(factorio_path: &Path, seed: u32) -> Result<Self, String> {
        let lua = Lua::new();

        // Compute starting lake position from seed
        let lake_pos = compute_starting_lake_position(seed);
        let lake_x = lake_pos.0 as f32 / 256.0;
        let lake_y = lake_pos.1 as f32 / 256.0;

        // Set up basic Lua environment
        setup_lua_env(&lua, seed, lake_pos).map_err(|e| e.to_string())?;

        Self::load_from_lua(&lua, factorio_path, vec![(lake_x, lake_y)], vec![(0.0, 0.0)])
    }

    /// Load from Factorio's data directory (default seed)
    pub fn load(factorio_path: &Path) -> Result<Self, String> {
        Self::load_with_seed(factorio_path, 0)
    }

    fn load_from_lua(lua: &Lua, factorio_path: &Path, starting_lake_positions: Vec<(f32, f32)>, starting_positions: Vec<(f32, f32)>) -> Result<Self, String> {

        // Load noise-functions.lua (core)
        let noise_functions_path = factorio_path.join("core/prototypes/noise-functions.lua");
        if noise_functions_path.exists() {
            let content = std::fs::read_to_string(&noise_functions_path)
                .map_err(|e| format!("Failed to read noise-functions.lua: {}", e))?;
            lua.load(&content)
                .set_name("noise-functions.lua")
                .exec()
                .map_err(|e| format!("Failed to execute noise-functions.lua: {}", e))?;
        }

        // Load noise-programs.lua (core)
        let noise_programs_path = factorio_path.join("core/prototypes/noise-programs.lua");
        if noise_programs_path.exists() {
            let content = std::fs::read_to_string(&noise_programs_path)
                .map_err(|e| format!("Failed to read noise-programs.lua: {}", e))?;
            lua.load(&content)
                .set_name("noise-programs.lua")
                .exec()
                .map_err(|e| format!("Failed to execute noise-programs.lua: {}", e))?;
        }

        // Load base noise-expressions.lua (contains expression_in_range_base and other functions)
        let base_noise_expr_path = factorio_path.join("base/prototypes/noise-expressions.lua");
        if base_noise_expr_path.exists() {
            let content = std::fs::read_to_string(&base_noise_expr_path)
                .map_err(|e| format!("Failed to read base noise-expressions.lua: {}", e))?;
            lua.load(&content)
                .set_name("base-noise-expressions.lua")
                .exec()
                .map_err(|e| format!("Failed to execute base noise-expressions.lua: {}", e))?;
        }

        // Load tiles.lua (contains tile definitions with autoplace expressions)
        let tiles_path = factorio_path.join("base/prototypes/tile/tiles.lua");
        if tiles_path.exists() {
            // We need to set up stubs for the tile-related modules
            setup_tile_stubs(&lua).map_err(|e| e.to_string())?;

            // Load util.lua from core/lualib (needed by tiles.lua)
            let util_path = factorio_path.join("core/lualib/util.lua");
            if util_path.exists() {
                let util_content = std::fs::read_to_string(&util_path)
                    .map_err(|e| format!("Failed to read util.lua: {}", e))?;
                lua.load(&util_content)
                    .set_name("util.lua")
                    .exec()
                    .map_err(|e| format!("Failed to execute util.lua: {}", e))?;
            }

            let content = std::fs::read_to_string(&tiles_path)
                .map_err(|e| format!("Failed to read tiles.lua: {}", e))?;
            lua.load(&content)
                .set_name("tiles.lua")
                .exec()
                .map_err(|e| format!("Failed to execute tiles.lua: {}", e))?;
        }

        // Extract definitions from data.raw
        let globals = lua.globals();
        let data: Table = globals.get("data").map_err(|e| e.to_string())?;
        let raw: Table = data.get("raw").map_err(|e| e.to_string())?;

        let noise_expressions = extract_noise_expressions(&raw)?;
        let noise_functions = extract_noise_functions(&raw)?;
        let tiles = extract_tiles(&raw)?;

        Ok(Self {
            noise_expressions,
            noise_functions,
            tiles,
            starting_lake_positions,
            starting_positions,
        })
    }

    /// Load with default path for macOS
    pub fn load_default() -> Result<Self, String> {
        let path = Path::new("/Applications/factorio.app/Contents/data");
        if path.exists() {
            Self::load(path)
        } else {
            Err("Factorio not found at default path".to_string())
        }
    }

    /// Get a noise expression by name
    pub fn get_expression(&self, name: &str) -> Option<&NoiseExpressionDef> {
        self.noise_expressions.get(name)
    }

    /// Get a noise function by name
    pub fn get_function(&self, name: &str) -> Option<&NoiseFunctionDef> {
        self.noise_functions.get(name)
    }

    /// Convert to compiler function definitions
    pub fn to_function_defs(&self) -> HashMap<String, FunctionDef> {
        let mut defs = HashMap::new();

        for (name, func) in &self.noise_functions {
            defs.insert(name.clone(), FunctionDef {
                parameters: func.parameters.clone(),
                expression: func.expression.clone(),
                local_expressions: func.local_expressions.clone(),
            });
        }

        defs
    }
}

fn setup_lua_env(lua: &Lua, seed: u32, lake_pos: (i32, i32)) -> LuaResult<()> {
    // Convert lake position from fixed-point to tile coordinates
    let lake_x = lake_pos.0 as f64 / 256.0;
    let lake_y = lake_pos.1 as f64 / 256.0;

    let lua_code = format!(r#"
        -- data:extend stub
        data = {{ raw = {{}} }}
        function data:extend(t)
            for _, proto in ipairs(t) do
                if proto.type and proto.name then
                    data.raw[proto.type] = data.raw[proto.type] or {{}}
                    data.raw[proto.type][proto.name] = proto
                end
            end
        end

        -- Basic stubs
        map_seed = {}
        -- var() returns default control values:
        -- :bias controls default to 0 (neutral)
        -- :frequency controls default to 1.0 (normal)
        -- :size controls default to 1.0 (normal)
        function var(name)
            if string.find(name, ":bias") then
                return 0  -- bias defaults to neutral
            else
                return 1.0  -- frequency/size default to 1.0 (normal)
            end
        end
        function clamp(v, min, max) return math.max(min, math.min(max, v)) end
        function lerp(a, b, t) return a + (b - a) * t end

        -- Noise function stubs (actual implementation in Rust)
        function basis_noise(args) return 0 end
        function multioctave_noise(args) return 0 end
        function quick_multioctave_noise(args) return 0 end
        function variable_persistence_multioctave_noise(args) return 0 end
        function quick_multioctave_noise_persistence(args) return 0 end
        function amplitude_corrected_multioctave_noise(args) return 0 end
        function distance_from_nearest_point(args) return 0 end
        function random_penalty(args) return 0 end
        function spot_noise(args) return 0 end

        -- Control table
        control = setmetatable({{}}, {{
            __index = function(_, key) return 1.0 end
        }})

        -- Global variables
        water_level = 0
        cliff_richness = 1
        cliff_elevation_interval = 40
        starting_lake_positions = {{{{{}, {}}}}}
        starting_positions = {{{{0, 0}}}}
        starting_area_radius = 150
        segmentation_multiplier = 1.0"#, seed, lake_x, lake_y);

    lua.load(&lua_code).exec()?;

    lua.load(r#"

        -- defines table (used by util.lua and other files)
        defines = {
            direction = {
                north = 0,
                northnortheast = 1,
                northeast = 2,
                eastnortheast = 3,
                east = 4,
                eastsoutheast = 5,
                southeast = 6,
                southsoutheast = 7,
                south = 8,
                southsouthwest = 9,
                southwest = 10,
                westsouthwest = 11,
                west = 12,
                westnorthwest = 13,
                northwest = 14,
                northnorthwest = 15,
            },
            events = setmetatable({}, { __index = function() return 0 end }),
            flow_precision_index = { one_second = 0, one_minute = 1, ten_minutes = 2 },
        }

        -- serpent stub (serialization library)
        serpent = { block = function() return "" end, line = function() return "" end }
    "#).exec()
}

fn extract_noise_expressions(raw: &Table) -> Result<HashMap<String, NoiseExpressionDef>, String> {
    let mut expressions = HashMap::new();

    let noise_expr_table: Table = match raw.get("noise-expression") {
        Ok(t) => t,
        Err(_) => return Ok(expressions),
    };

    for pair in noise_expr_table.pairs::<String, Table>() {
        let (name, proto) = pair.map_err(|e| e.to_string())?;

        let expr_str: String = match proto.get("expression") {
            Ok(s) => s,
            Err(_) => continue,
        };

        let expression = match parse_expression(&expr_str) {
            Ok(e) => e,
            Err(e) => {
                eprintln!("Warning: Failed to parse expression '{}': {}", name, e);
                continue;
            }
        };

        let mut local_expressions = HashMap::new();
        if let Ok(locals) = proto.get::<Table>("local_expressions") {
            for local_pair in locals.pairs::<String, Value>() {
                if let Ok((local_name, local_value)) = local_pair {
                    let local_expr_str = match local_value {
                        Value::String(s) => s.to_str().map(|s| s.to_string()).unwrap_or_else(|_| "0".to_string()),
                        Value::Number(n) => n.to_string(),
                        Value::Integer(n) => n.to_string(),
                        _ => continue,
                    };
                    if let Ok(local_expr) = parse_expression(&local_expr_str) {
                        local_expressions.insert(local_name, local_expr);
                    }
                }
            }
        }

        expressions.insert(name.clone(), NoiseExpressionDef {
            name,
            expression,
            local_expressions,
        });
    }

    Ok(expressions)
}

fn setup_tile_stubs(lua: &Lua) -> LuaResult<()> {
    lua.load(r#"
        -- Stubs for tile-related requires (returns actual tables, not metatabled functions)
        local stub_modules = {}
        stub_modules["prototypes.entity.sounds"] = setmetatable({}, {
            __index = function() return function() return {} end end
        })
        stub_modules["__base__/prototypes/tile/tile-sounds"] = {
            walking = setmetatable({}, { __index = function() return {} end }),
            driving = setmetatable({}, { __index = function() return {} end }),
            ambient = setmetatable({}, { __index = function() return function() return {} end end }),
            building = setmetatable({}, { __index = function() return {} end })
        }
        stub_modules["prototypes.tile.tile-trigger-effects"] = setmetatable({}, {
            __index = function() return function() return {} end end
        })
        stub_modules["__base__/prototypes/tile/tile-pollution-values"] = { water = {}, grass = {} }
        stub_modules["__base__/prototypes/tile/tile-collision-masks"] = setmetatable({}, {
            __index = function() return function() return {} end end
        })
        stub_modules["__base__/prototypes/tile/tile-graphics"] = {
            tile_spritesheet_layout = {},
            patch_for_inner_corner_of_transition_between_transition = function() return {} end
        }

        local old_require = require
        require = function(name)
            if stub_modules[name] then
                return stub_modules[name]
            end
            return {}
        end

        -- Tile graphics stubs
        tile_graphics = stub_modules["__base__/prototypes/tile/tile-graphics"]
        function tile_variations_template() return {} end
        function patch_for_inner_corner_of_transition_between_transition() return {} end

        -- Tile tables
        base_tiles_util = {}
        water_tile_type_names = {}
        out_of_map_tile_type_names = {}
        default_transition_group_id = 0
        water_transition_group_id = 1
        out_of_map_transition_group_id = 2

        -- colors table used by tiles.lua for colored concrete
        colors = {}

        -- table.deepcopy that returns an actual shallow copy for iteration
        function table.deepcopy(t)
            if type(t) ~= "table" then return t end
            local copy = {}
            for k, v in pairs(t) do
                copy[k] = v
            end
            return copy
        end

        -- Stubs for transitions
        grass_transitions = {}
        grass_transitions_between_transitions = {}
        dirt_transitions = {}
        dirt_transitions_between_transitions = {}
        sand_transitions = {}
        sand_transitions_between_transitions = {}
        landfill_transitions = {}
        landfill_transitions_between_transitions = {}
        concrete_transitions = {}
        concrete_transitions_between_transitions = {}
        stone_transitions = {}
        stone_transitions_between_transitions = {}
        dark_dirt_transitions = {}
        dark_dirt_transitions_between_transitions = {}
    "#).exec()
}

fn extract_tiles(raw: &Table) -> Result<Vec<TileDef>, String> {
    let mut tiles = Vec::new();

    let tile_table: Table = match raw.get("tile") {
        Ok(t) => t,
        Err(_) => return Ok(tiles),
    };

    for pair in tile_table.pairs::<String, Table>() {
        let (name, proto) = pair.map_err(|e| e.to_string())?;

        // Get autoplace.probability_expression if it exists
        let prob_expr: Option<String> = proto.get::<Table>("autoplace")
            .ok()
            .and_then(|autoplace| autoplace.get::<String>("probability_expression").ok());

        tiles.push(TileDef {
            name,
            probability_expression: prob_expr,
        });
    }

    // Sort tiles by name for consistent ordering
    tiles.sort_by(|a, b| a.name.cmp(&b.name));

    Ok(tiles)
}

fn extract_noise_functions(raw: &Table) -> Result<HashMap<String, NoiseFunctionDef>, String> {
    let mut functions = HashMap::new();

    let noise_func_table: Table = match raw.get("noise-function") {
        Ok(t) => t,
        Err(_) => return Ok(functions),
    };

    for pair in noise_func_table.pairs::<String, Table>() {
        let (name, proto) = pair.map_err(|e| e.to_string())?;

        let expr_str: String = match proto.get("expression") {
            Ok(s) => s,
            Err(_) => continue,
        };

        let expression = match parse_expression(&expr_str) {
            Ok(e) => e,
            Err(e) => {
                eprintln!("Warning: Failed to parse function '{}': {}", name, e);
                continue;
            }
        };

        let mut parameters = Vec::new();
        if let Ok(params) = proto.get::<Table>("parameters") {
            for i in 1..=params.len().unwrap_or(0) {
                if let Ok(param) = params.get::<String>(i) {
                    parameters.push(param);
                }
            }
        }

        let mut local_expressions = HashMap::new();
        if let Ok(locals) = proto.get::<Table>("local_expressions") {
            for local_pair in locals.pairs::<String, Value>() {
                if let Ok((local_name, local_value)) = local_pair {
                    let local_expr_str = match local_value {
                        Value::String(s) => s.to_str().map(|s| s.to_string()).unwrap_or_else(|_| "0".to_string()),
                        Value::Number(n) => n.to_string(),
                        Value::Integer(n) => n.to_string(),
                        _ => continue,
                    };
                    if let Ok(local_expr) = parse_expression(&local_expr_str) {
                        local_expressions.insert(local_name, local_expr);
                    }
                }
            }
        }

        functions.insert(name.clone(), NoiseFunctionDef {
            name,
            parameters,
            expression,
            local_expressions,
        });
    }

    Ok(functions)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_factorio_data() {
        let path = Path::new("/Applications/factorio.app/Contents/data");
        if !path.exists() {
            eprintln!("Factorio not installed, skipping test");
            return;
        }

        let data = FactorioData::load(path).expect("Failed to load Factorio data");

        // Check that we loaded some expressions
        assert!(!data.noise_expressions.is_empty(), "No noise expressions loaded");

        // Check specific expressions
        assert!(data.noise_expressions.contains_key("elevation"), "elevation not found");
        assert!(data.noise_expressions.contains_key("moisture"), "moisture not found");
        assert!(data.noise_expressions.contains_key("aux"), "aux not found");

        println!("Loaded {} noise expressions", data.noise_expressions.len());
        println!("Loaded {} noise functions", data.noise_functions.len());
    }
}
