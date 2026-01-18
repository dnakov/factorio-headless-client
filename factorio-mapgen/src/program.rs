//! NoiseProgram - builds and executes noise operations from Lua definitions
//!
//! Loads noise expression parameters from Factorio's Lua files at runtime,
//! then executes them efficiently in Rust.

use std::path::Path;
use std::collections::HashMap;
use mlua::{Lua, Table, Result as LuaResult};

use crate::cache::{NoiseCache, TILES_PER_CHUNK};
use crate::operations::{PerlinNoise, MultioctaveNoiseOp, QuickMultioctaveNoiseOp, BasisNoiseOp, AbsOp, NoiseOp};

/// Tile definition with autoplace parameters
#[derive(Debug, Clone)]
pub struct TileDef {
    pub name: &'static str,
    pub aux_from: f32,
    pub moisture_from: f32,
    pub aux_to: f32,
    pub moisture_to: f32,
    pub noise_layer_seed: i64,
    /// Secondary range for tiles with max() expressions
    pub secondary_range: Option<(f32, f32, f32, f32)>,
}

/// Noise expression parameters extracted from Lua
#[derive(Debug, Clone)]
pub struct NoiseExprParams {
    pub name: String,
    pub seed1: i64,
    pub octaves: u32,
    pub persistence: f32,
    pub input_scale: f32,
    pub output_scale: f32,
    pub offset_x: f32,
    pub octave_output_scale_multiplier: Option<f32>,
    pub octave_input_scale_multiplier: Option<f32>,
}

/// NoiseProgram that generates terrain for entire chunks
pub struct NoiseProgram {
    seed: u32,
    moisture_params: NoiseExprParams,
    aux_params: NoiseExprParams,
    hills_params: NoiseExprParams,
    tiles: Vec<TileDef>,
    /// Control settings
    moisture_frequency: f32,
    moisture_bias: f32,
    aux_frequency: f32,
    aux_bias: f32,
    water_frequency: f32,
}

impl NoiseProgram {
    /// Load noise program from Factorio's Lua data files
    pub fn from_lua(seed: u32, lua_data_path: &Path) -> LuaResult<Self> {
        let lua = Lua::new();

        // Set up basic environment
        setup_lua_env(&lua)?;

        // Load noise-programs.lua
        let noise_programs_path = lua_data_path.join("core/prototypes/noise-programs.lua");
        if noise_programs_path.exists() {
            let content = std::fs::read_to_string(&noise_programs_path)
                .map_err(|e| mlua::Error::runtime(format!("Failed to read noise-programs.lua: {}", e)))?;
            lua.load(&content).set_name("noise-programs.lua").exec()?;
        }

        // Extract parameters from data.raw
        let globals = lua.globals();
        let data: Table = globals.get("data")?;
        let raw: Table = data.get("raw")?;
        let noise_exprs: Table = raw.get("noise-expression").unwrap_or_else(|_| lua.create_table().unwrap());

        // Extract moisture_noise parameters
        let moisture_params = extract_noise_params(&noise_exprs, "moisture_noise", 6)?;

        // Extract aux_noise parameters
        let aux_params = extract_noise_params(&noise_exprs, "aux_noise", 7)?;

        // Extract nauvis_hills parameters
        let hills_params = extract_noise_params(&noise_exprs, "nauvis_hills", 900)?;

        // Get control settings (default values)
        let moisture_frequency = 1.0;
        let moisture_bias = 0.0;
        let aux_frequency = 1.0;
        let aux_bias = 0.0;
        let water_frequency = 1.0;

        // Load tile definitions
        let tiles = load_tile_definitions();

        Ok(Self {
            seed,
            moisture_params,
            aux_params,
            hills_params,
            tiles,
            moisture_frequency,
            moisture_bias,
            aux_frequency,
            aux_bias,
            water_frequency,
        })
    }

    /// Create with default parameters (when Lua files not available)
    pub fn with_defaults(seed: u32) -> Self {
        Self {
            seed,
            moisture_params: NoiseExprParams {
                name: "moisture_noise".into(),
                seed1: 6,
                octaves: 4,
                persistence: 0.5,
                input_scale: 1.0 / 256.0,
                output_scale: 0.125,
                offset_x: 30000.0,
                octave_output_scale_multiplier: Some(1.5),
                octave_input_scale_multiplier: Some(1.0 / 3.0),
            },
            aux_params: NoiseExprParams {
                name: "aux_noise".into(),
                seed1: 7,
                octaves: 4,
                persistence: 0.5,
                input_scale: 1.0 / 2048.0,
                output_scale: 0.25,
                offset_x: 20000.0,
                octave_output_scale_multiplier: Some(0.5),
                octave_input_scale_multiplier: Some(3.0),
            },
            hills_params: NoiseExprParams {
                name: "nauvis_hills".into(),
                seed1: 900,
                octaves: 4,
                persistence: 0.5,
                input_scale: 1.5 / 90.0, // segmentation_multiplier / 90
                output_scale: 1.0,
                offset_x: 0.0,
                octave_output_scale_multiplier: None,
                octave_input_scale_multiplier: None,
            },
            tiles: load_tile_definitions(),
            moisture_frequency: 1.0,
            moisture_bias: 0.0,
            aux_frequency: 1.0,
            aux_bias: 0.0,
            water_frequency: 1.0,
        }
    }

    /// Compute tiles for an entire chunk
    pub fn compute_chunk(&self, chunk_x: i32, chunk_y: i32) -> [&'static str; TILES_PER_CHUNK] {
        let mut cache = NoiseCache::new(self.seed);
        cache.init_chunk(chunk_x, chunk_y);

        // Compute nauvis_hills
        self.compute_hills(&mut cache);

        // Compute nauvis_plateaus from hills
        self.compute_plateaus(&mut cache);

        // Compute moisture
        self.compute_moisture(&mut cache);

        // Compute aux
        self.compute_aux(&mut cache);

        // Select tiles based on moisture/aux
        self.select_tiles(&cache)
    }

    fn compute_hills(&self, cache: &mut NoiseCache) {
        let segmentation = 1.5 * self.water_frequency;

        // nauvis_hills = abs(multioctave_noise{...})
        let op = MultioctaveNoiseOp {
            output: "hills_raw",
            seed1: self.hills_params.seed1,
            octaves: self.hills_params.octaves,
            persistence: self.hills_params.persistence,
            input_scale: segmentation / 90.0,
            output_scale: 1.0,
            offset_x: 0.0,
        };
        op.execute(cache);

        // abs()
        let abs_op = AbsOp {
            output: "nauvis_hills",
            input: "hills_raw",
        };
        abs_op.execute(cache);

        // nauvis_hills_cliff_level
        let cliff_op = BasisNoiseOp {
            output: "cliff_level_raw",
            seed1: 99584,
            input_scale: segmentation / 500.0,
            output_scale: 0.6,
            offset_x: 0.0,
        };
        cliff_op.execute(cache);

        // cliff_level = clamp(0.65 + cliff_level_raw, 0.15, 1.15)
        let raw: Vec<f32> = cache.get("cliff_level_raw").unwrap().to_vec();
        let cliff_level = cache.get_mut("nauvis_hills_cliff_level");
        for i in 0..TILES_PER_CHUNK {
            cliff_level[i] = (0.65 + raw[i]).clamp(0.15, 1.15);
        }
    }

    fn compute_plateaus(&self, cache: &mut NoiseCache) {
        // nauvis_plateaus = 0.5 + clamp((nauvis_hills - cliff_level) * 10, -0.5, 0.5)
        let hills: Vec<f32> = cache.get("nauvis_hills").unwrap().to_vec();
        let cliff_level: Vec<f32> = cache.get("nauvis_hills_cliff_level").unwrap().to_vec();
        let plateaus = cache.get_mut("nauvis_plateaus");

        for i in 0..TILES_PER_CHUNK {
            let diff = (hills[i] - cliff_level[i]) * 10.0;
            plateaus[i] = 0.5 + diff.clamp(-0.5, 0.5);
        }
    }

    fn compute_moisture(&self, cache: &mut NoiseCache) {
        let freq = self.moisture_frequency;

        // moisture_noise
        let op = QuickMultioctaveNoiseOp {
            output: "moisture_noise",
            seed1: self.moisture_params.seed1,
            octaves: self.moisture_params.octaves,
            input_scale: freq * self.moisture_params.input_scale,
            output_scale: self.moisture_params.output_scale,
            offset_x: self.moisture_params.offset_x / freq,
            octave_output_scale_multiplier: self.moisture_params.octave_output_scale_multiplier.unwrap_or(0.5),
            octave_input_scale_multiplier: self.moisture_params.octave_input_scale_multiplier.unwrap_or(2.0),
        };
        op.execute(cache);

        // moisture = clamp(0.4 + bias + noise - 0.08 * (plateaus - 0.6), 0, 1)
        let noise: Vec<f32> = cache.get("moisture_noise").unwrap().to_vec();
        let plateaus: Vec<f32> = cache.get("nauvis_plateaus").unwrap().to_vec();
        let moisture = cache.get_mut("moisture");

        for i in 0..TILES_PER_CHUNK {
            let val = 0.4 + self.moisture_bias + noise[i] - 0.08 * (plateaus[i] - 0.6);
            moisture[i] = val.clamp(0.0, 1.0);
        }
    }

    fn compute_aux(&self, cache: &mut NoiseCache) {
        let freq = self.aux_frequency;

        // aux_noise
        let op = QuickMultioctaveNoiseOp {
            output: "aux_noise",
            seed1: self.aux_params.seed1,
            octaves: self.aux_params.octaves,
            input_scale: freq * self.aux_params.input_scale,
            output_scale: self.aux_params.output_scale,
            offset_x: self.aux_params.offset_x / freq,
            octave_output_scale_multiplier: self.aux_params.octave_output_scale_multiplier.unwrap_or(0.5),
            octave_input_scale_multiplier: self.aux_params.octave_input_scale_multiplier.unwrap_or(2.0),
        };
        op.execute(cache);

        // aux = clamp(0.5 + bias + 0.06 * (plateaus - 0.4) + noise, 0, 1)
        let noise: Vec<f32> = cache.get("aux_noise").unwrap().to_vec();
        let plateaus: Vec<f32> = cache.get("nauvis_plateaus").unwrap().to_vec();
        let aux = cache.get_mut("aux");

        for i in 0..TILES_PER_CHUNK {
            let val = 0.5 + self.aux_bias + 0.06 * (plateaus[i] - 0.4) + noise[i];
            aux[i] = val.clamp(0.0, 1.0);
        }
    }

    fn select_tiles(&self, cache: &NoiseCache) -> [&'static str; TILES_PER_CHUNK] {
        let mut result = ["dirt-1"; TILES_PER_CHUNK];

        let aux = cache.get("aux").unwrap();
        let moisture = cache.get("moisture").unwrap();
        let x_vals = cache.get("x").unwrap();
        let y_vals = cache.get("y").unwrap();

        // Pre-compute noise layer values for each tile
        let mut tile_noise: HashMap<i64, Vec<f32>> = HashMap::new();
        for tile in &self.tiles {
            if !tile_noise.contains_key(&tile.noise_layer_seed) {
                let noise = compute_noise_layer(cache.seed, tile.noise_layer_seed, x_vals, y_vals);
                tile_noise.insert(tile.noise_layer_seed, noise);
            }
        }

        for i in 0..TILES_PER_CHUNK {
            let a = aux[i];
            let m = moisture[i];

            let mut best_prob = f32::NEG_INFINITY;
            let mut best_tile = "dirt-1";

            for tile in &self.tiles {
                let layer_noise = &tile_noise[&tile.noise_layer_seed];

                // Primary range
                let mut prob = expression_in_range(a, m, tile.aux_from, tile.moisture_from, tile.aux_to, tile.moisture_to);

                // Secondary range (max with primary)
                if let Some((af, mf, at, mt)) = tile.secondary_range {
                    let prob2 = expression_in_range(a, m, af, mf, at, mt);
                    prob = prob.max(prob2);
                }

                // Add noise layer
                prob += layer_noise[i];

                if prob > best_prob {
                    best_prob = prob;
                    best_tile = tile.name;
                }
            }

            result[i] = best_tile;
        }

        result
    }
}

fn setup_lua_env(lua: &Lua) -> LuaResult<()> {
    lua.load(r#"
        data = { raw = {} }
        function data:extend(t)
            for _, proto in ipairs(t) do
                if proto.type and proto.name then
                    data.raw[proto.type] = data.raw[proto.type] or {}
                    data.raw[proto.type][proto.name] = proto
                end
            end
        end

        map_seed = 0
        function var(name) return 1.0 end
        function clamp(v, min, max) return math.max(min, math.min(max, v)) end
        function lerp(a, b, t) return a + (b - a) * t end
    "#).exec()
}

fn extract_noise_params(noise_exprs: &Table, name: &str, default_seed: i64) -> LuaResult<NoiseExprParams> {
    if let Ok(expr) = noise_exprs.get::<Table>(name) {
        let expression: String = expr.get("expression").unwrap_or_default();

        // Parse the expression string to extract parameters
        // This is a simplified parser - real implementation would be more robust
        let seed1 = parse_seed1(&expression).unwrap_or(default_seed);
        let octaves = parse_param(&expression, "octaves").unwrap_or(4.0) as u32;
        let persistence = parse_param(&expression, "persistence").unwrap_or(0.5) as f32;
        let input_scale = parse_param(&expression, "input_scale").unwrap_or(1.0 / 256.0) as f32;
        let output_scale = parse_param(&expression, "output_scale").unwrap_or(0.125) as f32;
        let offset_x = parse_param(&expression, "offset_x").unwrap_or(0.0) as f32;
        let octave_output_scale_multiplier = parse_param(&expression, "octave_output_scale_multiplier").map(|v| v as f32);
        let octave_input_scale_multiplier = parse_param(&expression, "octave_input_scale_multiplier").map(|v| v as f32);

        Ok(NoiseExprParams {
            name: name.to_string(),
            seed1,
            octaves,
            persistence,
            input_scale,
            output_scale,
            offset_x,
            octave_output_scale_multiplier,
            octave_input_scale_multiplier,
        })
    } else {
        // Return defaults
        Ok(NoiseExprParams {
            name: name.to_string(),
            seed1: default_seed,
            octaves: 4,
            persistence: 0.5,
            input_scale: 1.0 / 256.0,
            output_scale: 0.125,
            offset_x: 0.0,
            octave_output_scale_multiplier: None,
            octave_input_scale_multiplier: None,
        })
    }
}

fn parse_seed1(expr: &str) -> Option<i64> {
    // Look for seed1 = N pattern
    if let Some(idx) = expr.find("seed1") {
        let rest = &expr[idx..];
        if let Some(eq_idx) = rest.find('=') {
            let after_eq = rest[eq_idx + 1..].trim_start();
            // Parse number
            let num_str: String = after_eq.chars()
                .take_while(|c| c.is_ascii_digit() || *c == '-')
                .collect();
            return num_str.parse().ok();
        }
    }
    None
}

fn parse_param(expr: &str, param: &str) -> Option<f64> {
    if let Some(idx) = expr.find(param) {
        let rest = &expr[idx..];
        if let Some(eq_idx) = rest.find('=') {
            let after_eq = rest[eq_idx + 1..].trim_start();
            // Parse number or fraction
            let num_str: String = after_eq.chars()
                .take_while(|c| c.is_ascii_digit() || *c == '.' || *c == '-' || *c == '/')
                .collect();

            // Handle fractions like "1/256"
            if num_str.contains('/') {
                let parts: Vec<&str> = num_str.split('/').collect();
                if parts.len() == 2 {
                    let a: f64 = parts[0].parse().ok()?;
                    let b: f64 = parts[1].parse().ok()?;
                    return Some(a / b);
                }
            }
            return num_str.parse().ok();
        }
    }
    None
}

fn load_tile_definitions() -> Vec<TileDef> {
    vec![
        // Grass (high moisture)
        TileDef { name: "grass-1", aux_from: -10.0, moisture_from: 0.7, aux_to: 11.0, moisture_to: 11.0, noise_layer_seed: 19, secondary_range: None },
        TileDef { name: "grass-2", aux_from: 0.45, moisture_from: 0.45, aux_to: 11.0, moisture_to: 0.8, noise_layer_seed: 20, secondary_range: None },
        TileDef { name: "grass-3", aux_from: -10.0, moisture_from: 0.6, aux_to: 0.65, moisture_to: 0.9, noise_layer_seed: 21, secondary_range: None },
        TileDef { name: "grass-4", aux_from: -10.0, moisture_from: 0.5, aux_to: 0.55, moisture_to: 0.7, noise_layer_seed: 22, secondary_range: None },

        // Dirt
        TileDef { name: "dry-dirt", aux_from: 0.45, moisture_from: -10.0, aux_to: 0.55, moisture_to: 0.35, noise_layer_seed: 13, secondary_range: None },
        TileDef { name: "dirt-1", aux_from: -10.0, moisture_from: 0.25, aux_to: 0.45, moisture_to: 0.3, noise_layer_seed: 6, secondary_range: Some((0.4, -10.0, 0.45, 0.25)) },
        TileDef { name: "dirt-2", aux_from: -10.0, moisture_from: 0.3, aux_to: 0.45, moisture_to: 0.35, noise_layer_seed: 7, secondary_range: None },
        TileDef { name: "dirt-3", aux_from: -10.0, moisture_from: 0.35, aux_to: 0.55, moisture_to: 0.4, noise_layer_seed: 8, secondary_range: None },
        TileDef { name: "dirt-4", aux_from: 0.55, moisture_from: -10.0, aux_to: 0.6, moisture_to: 0.35, noise_layer_seed: 9, secondary_range: Some((0.6, 0.3, 11.0, 0.35)) },
        TileDef { name: "dirt-5", aux_from: -10.0, moisture_from: 0.4, aux_to: 0.55, moisture_to: 0.45, noise_layer_seed: 10, secondary_range: None },
        TileDef { name: "dirt-6", aux_from: -10.0, moisture_from: 0.45, aux_to: 0.55, moisture_to: 0.5, noise_layer_seed: 11, secondary_range: None },
        TileDef { name: "dirt-7", aux_from: -10.0, moisture_from: 0.5, aux_to: 0.55, moisture_to: 0.55, noise_layer_seed: 12, secondary_range: None },

        // Sand (low moisture)
        TileDef { name: "sand-1", aux_from: -10.0, moisture_from: -10.0, aux_to: 0.25, moisture_to: 0.15, noise_layer_seed: 36, secondary_range: None },
        TileDef { name: "sand-2", aux_from: -10.0, moisture_from: 0.15, aux_to: 0.3, moisture_to: 0.2, noise_layer_seed: 37, secondary_range: Some((0.25, -10.0, 0.3, 0.15)) },
        TileDef { name: "sand-3", aux_from: -10.0, moisture_from: 0.2, aux_to: 0.4, moisture_to: 0.25, noise_layer_seed: 38, secondary_range: Some((0.3, -10.0, 0.4, 0.2)) },

        // Red desert (high aux)
        TileDef { name: "red-desert-0", aux_from: 0.55, moisture_from: 0.35, aux_to: 11.0, moisture_to: 0.5, noise_layer_seed: 30, secondary_range: None },
        TileDef { name: "red-desert-1", aux_from: 0.6, moisture_from: -10.0, aux_to: 0.7, moisture_to: 0.3, noise_layer_seed: 31, secondary_range: Some((0.7, 0.25, 11.0, 0.3)) },
        TileDef { name: "red-desert-2", aux_from: 0.7, moisture_from: -10.0, aux_to: 0.8, moisture_to: 0.25, noise_layer_seed: 32, secondary_range: Some((0.8, 0.2, 11.0, 0.25)) },
        TileDef { name: "red-desert-3", aux_from: 0.8, moisture_from: -10.0, aux_to: 11.0, moisture_to: 0.2, noise_layer_seed: 33, secondary_range: None },
    ]
}

fn expression_in_range(aux: f32, moisture: f32, aux_from: f32, moisture_from: f32, aux_to: f32, moisture_to: f32) -> f32 {
    let slope = 20.0;
    let output_scale = 1.0;

    let mid_aux = (aux_from + aux_to) * 0.5;
    let half_aux = (aux_to - aux_from) * 0.5;
    let mid_moisture = (moisture_from + moisture_to) * 0.5;
    let half_moisture = (moisture_to - moisture_from) * 0.5;

    let peak_aux = ((half_aux - (aux - mid_aux).abs()) * slope).min(output_scale);
    let peak_moisture = ((half_moisture - (moisture - mid_moisture).abs()) * slope).min(output_scale);

    peak_aux.min(peak_moisture).min(output_scale)
}

fn compute_noise_layer(seed: u32, layer_seed: i64, x_vals: &[f32], y_vals: &[f32]) -> Vec<f32> {
    let mut result = vec![0.0f32; TILES_PER_CHUNK];
    let noise = PerlinNoise::new(seed, layer_seed);

    let input_scale = 1.0 / 6.0;
    let output_scale = 2.0 / 3.0;
    let persistence = 0.7f32;

    for i in 0..TILES_PER_CHUNK {
        let base_x = x_vals[i];
        let base_y = y_vals[i];

        let mut total = 0.0f32;
        let mut amplitude = 1.0f32;
        let mut frequency = input_scale;
        let mut max_amplitude = 0.0f32;

        for _ in 0..4 {
            total += noise.noise(base_x * frequency, base_y * frequency) * amplitude;
            max_amplitude += amplitude;
            amplitude *= persistence;
            frequency *= 2.0;
        }

        result[i] = (total / max_amplitude) * output_scale;
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_program_defaults() {
        let program = NoiseProgram::with_defaults(12345);
        let tiles = program.compute_chunk(0, 0);

        // Should have a variety of tiles
        let unique: std::collections::HashSet<_> = tiles.iter().collect();
        assert!(unique.len() > 1, "Expected variety of tiles");
    }
}
