//! Noise functions for Factorio terrain generation
//!
//! Implements Factorio's exact noise algorithm based on reverse engineering
//! of the game binary. Uses xorshift PRNG for permutation table shuffling
//! and XOR-based hashing for gradient lookups.

use mlua::{Lua, Result as LuaResult, Table, Value, MultiValue};
use std::path::Path;

/// Factorio's xorshift PRNG (from reverse engineering Noise::setSeed)
fn xorshift(mut x: u32) -> u32 {
    x ^= x << 13;
    x ^= x >> 19;
    x ^= x << 12;
    x
}

/// Standard 2D gradient vectors (8 directions)
const GRADIENTS: [(f64, f64); 8] = [
    (1.0, 0.0), (0.707, 0.707), (0.0, 1.0), (-0.707, 0.707),
    (-1.0, 0.0), (-0.707, -0.707), (0.0, -1.0), (0.707, -0.707),
];

/// Factorio noise generator with proper permutation tables
struct FactorioNoise {
    perm: [u8; 256],
    seed_byte: u8,
}

impl FactorioNoise {
    fn new(seed: u32, seed1: i64) -> Self {
        let combined_seed = seed.wrapping_add((seed1 as u32).wrapping_mul(12345));
        let seed_byte = (combined_seed & 0xFF) as u8;

        // Initialize permutation table with Fisher-Yates shuffle using xorshift
        let mut perm = [0u8; 256];
        for i in 0..256 {
            perm[i] = i as u8;
        }

        let mut rng_state = combined_seed.max(341); // Avoid 0 seed issues
        for i in (1..256).rev() {
            rng_state = xorshift(rng_state);
            let j = (rng_state as usize) % (i + 1);
            perm.swap(i, j);
        }

        Self { perm, seed_byte }
    }

    /// Quintic interpolation (smootherstep)
    fn fade(t: f64) -> f64 {
        t * t * t * (t * (t * 6.0 - 15.0) + 10.0)
    }

    /// Linear interpolation
    fn lerp(a: f64, b: f64, t: f64) -> f64 {
        a + t * (b - a)
    }

    /// Gradient dot product
    fn grad(&self, xi: i32, yi: i32, x: f64, y: f64) -> f64 {
        let h = ((self.perm[(xi & 0xFF) as usize] ^ self.seed_byte ^ self.perm[(yi & 0xFF) as usize]) & 7) as usize;
        let (gx, gy) = GRADIENTS[h];
        gx * x + gy * y
    }

    /// 2D Perlin noise at position (x, y)
    fn noise(&self, x: f64, y: f64) -> f64 {
        let xi = x.floor() as i32;
        let yi = y.floor() as i32;
        let xf = x - xi as f64;
        let yf = y - yi as f64;

        let u = Self::fade(xf);
        let v = Self::fade(yf);

        let n00 = self.grad(xi, yi, xf, yf);
        let n10 = self.grad(xi + 1, yi, xf - 1.0, yf);
        let n01 = self.grad(xi, yi + 1, xf, yf - 1.0);
        let n11 = self.grad(xi + 1, yi + 1, xf - 1.0, yf - 1.0);

        let nx0 = Self::lerp(n00, n10, u);
        let nx1 = Self::lerp(n01, n11, u);
        Self::lerp(nx0, nx1, v)
    }
}

/// Factorio's basis_noise implementation
fn basis_noise_impl(x: f64, y: f64, seed0: u32, seed1: i64, input_scale: f64, output_scale: f64) -> f64 {
    let noise = FactorioNoise::new(seed0, seed1);
    let scaled_x = x * input_scale;
    let scaled_y = y * input_scale;
    noise.noise(scaled_x, scaled_y) * output_scale
}

/// Factorio's multioctave_noise (fBm)
fn multioctave_noise_impl(
    x: f64, y: f64, seeds: (u32, i64),
    octaves: u32, persistence: f64, scales: (f64, f64, f64),
) -> f64 {
    let (input_scale, output_scale, offset_x) = scales;
    let noise = FactorioNoise::new(seeds.0, seeds.1);
    let adjusted_x = x + offset_x;

    let mut total = 0.0;
    let mut amplitude = 1.0;
    let mut frequency = input_scale;
    let mut max_amplitude = 0.0;

    for _ in 0..octaves {
        total += noise.noise(adjusted_x * frequency, y * frequency) * amplitude;
        max_amplitude += amplitude;
        amplitude *= persistence;
        frequency *= 2.0;
    }

    (total / max_amplitude) * output_scale
}

/// Factorio's quick_multioctave_noise (variable octave scales)
fn quick_multioctave_noise_impl(
    x: f64, y: f64, seed0: u32, seed1: i64,
    octaves: u32, scales: (f64, f64, f64, f64, f64),
) -> f64 {
    let (input_scale, output_scale, offset_x, octave_output_scale_multiplier, octave_input_scale_multiplier) = scales;
    let noise = FactorioNoise::new(seed0, seed1);
    let adjusted_x = x + offset_x;

    let mut total = 0.0;
    let mut amplitude = 1.0;
    let mut frequency = input_scale;
    let mut max_amplitude = 0.0;

    for _ in 0..octaves {
        total += noise.noise(adjusted_x * frequency, y * frequency) * amplitude;
        max_amplitude += amplitude;
        amplitude *= octave_output_scale_multiplier;
        frequency *= octave_input_scale_multiplier;
    }

    (total / max_amplitude) * output_scale
}

/// Factorio's expression_in_range native implementation
/// Computes how well a point (aux, moisture) fits within a rectangular range
/// Returns: min(aux_peak, moisture_peak) where:
///   peak = clamp((halfwidth - |value - midpoint|) * slope, -inf, output_scale)
fn expression_in_range_impl(
    slope: f64, output_scale: f64,
    point: (f64, f64), from: (f64, f64), to: (f64, f64),
) -> f64 {
    let aux_midpoint = (from.0 + to.0) * 0.5;
    let aux_halfwidth = (to.0 - from.0) * 0.5;
    let aux_peak = ((aux_halfwidth - (point.0 - aux_midpoint).abs()) * slope).min(output_scale);

    let moisture_midpoint = (from.1 + to.1) * 0.5;
    let moisture_halfwidth = (to.1 - from.1) * 0.5;
    let moisture_peak = ((moisture_halfwidth - (point.1 - moisture_midpoint).abs()) * slope).min(output_scale);

    aux_peak.min(moisture_peak).min(output_scale)
}

/// Register all noise primitives in Lua
pub fn register_noise_functions(lua: &Lua, map_seed: u32) -> LuaResult<()> {
    let globals = lua.globals();

    // Set map_seed as a global
    globals.set("map_seed", map_seed)?;

    // x and y will be set per-tile evaluation
    globals.set("x", 0.0)?;
    globals.set("y", 0.0)?;

    // Register clamp function
    let clamp_fn = lua.create_function(|_, (value, min, max): (f64, f64, f64)| {
        Ok(value.clamp(min, max))
    })?;
    globals.set("clamp", clamp_fn)?;

    // Register lerp function
    let lerp_fn = lua.create_function(|_, (a, b, t): (f64, f64, f64)| {
        Ok(a + (b - a) * t)
    })?;
    globals.set("lerp", lerp_fn)?;

    // Register abs function
    let abs_fn = lua.create_function(|_, value: f64| Ok(value.abs()))?;
    globals.set("abs", abs_fn)?;

    // Register min/max
    let min_fn = lua.create_function(|_, args: MultiValue| {
        let mut result = f64::MAX;
        for arg in args {
            if let Value::Number(n) = arg {
                result = result.min(n);
            } else if let Value::Integer(n) = arg {
                result = result.min(n as f64);
            }
        }
        Ok(result)
    })?;
    globals.set("min", min_fn)?;

    let max_fn = lua.create_function(|_, args: MultiValue| {
        let mut result = f64::MIN;
        for arg in args {
            if let Value::Number(n) = arg {
                result = result.max(n);
            } else if let Value::Integer(n) = arg {
                result = result.max(n as f64);
            }
        }
        Ok(result)
    })?;
    globals.set("max", max_fn)?;

    // Register var function (returns control settings)
    let var_fn = lua.create_function(|_, name: String| {
        // Default control values: bias -> 0, size/frequency -> 1
        let value = if name.contains(":bias") { 0.0 } else { 1.0 };
        Ok(value)
    })?;
    globals.set("var", var_fn)?;

    // Register slider_to_linear
    let slider_fn = lua.create_function(|_, (slider, min, max): (f64, f64, f64)| {
        let scale = slider.log2() / 6.0f64.log2();
        Ok(min + 0.5 * (max - min) * (1.0 + scale))
    })?;
    globals.set("slider_to_linear", slider_fn)?;

    // Register distance (from origin)
    lua.load("distance = math.sqrt(x*x + y*y)").exec()?;

    // Register basis_noise
    let map_seed_copy = map_seed;
    let basis_noise_fn = lua.create_function(move |_, args: Table| {
        let x: f64 = args.get("x")?;
        let y: f64 = args.get("y")?;
        let seed0: u32 = args.get::<Value>("seed0")?.as_integer().unwrap_or(map_seed_copy as i64) as u32;
        let seed1: i64 = args.get::<Value>("seed1")?.as_integer().unwrap_or(0);
        let input_scale: f64 = args.get("input_scale").unwrap_or(1.0);
        let output_scale: f64 = args.get("output_scale").unwrap_or(1.0);

        Ok(basis_noise_impl(x, y, seed0, seed1, input_scale, output_scale))
    })?;
    globals.set("basis_noise", basis_noise_fn)?;

    // Register multioctave_noise
    let map_seed_copy = map_seed;
    let multioctave_fn = lua.create_function(move |_, args: Table| {
        let x: f64 = args.get("x")?;
        let y: f64 = args.get("y")?;
        let seed0: u32 = args.get::<Value>("seed0")?.as_integer().unwrap_or(map_seed_copy as i64) as u32;
        let seed1: i64 = args.get::<Value>("seed1")?.as_integer().unwrap_or(0);
        let octaves: u32 = args.get("octaves").unwrap_or(4);
        let persistence: f64 = args.get("persistence").unwrap_or(0.5);
        let input_scale: f64 = args.get("input_scale").unwrap_or(1.0);
        let output_scale: f64 = args.get("output_scale").unwrap_or(1.0);
        let offset_x: f64 = args.get("offset_x").unwrap_or(0.0);

        Ok(multioctave_noise_impl(x, y, (seed0, seed1), octaves, persistence, (input_scale, output_scale, offset_x)))
    })?;
    globals.set("multioctave_noise", multioctave_fn)?;

    // Register quick_multioctave_noise
    let map_seed_copy = map_seed;
    let quick_multioctave_fn = lua.create_function(move |_, args: Table| {
        let x: f64 = args.get("x")?;
        let y: f64 = args.get("y")?;
        let seed0: u32 = args.get::<Value>("seed0")?.as_integer().unwrap_or(map_seed_copy as i64) as u32;
        let seed1: i64 = args.get::<Value>("seed1")?.as_integer().unwrap_or(0);
        let octaves: u32 = args.get("octaves").unwrap_or(4);
        let input_scale: f64 = args.get("input_scale").unwrap_or(1.0);
        let output_scale: f64 = args.get("output_scale").unwrap_or(1.0);
        let offset_x: f64 = args.get("offset_x").unwrap_or(0.0);
        let octave_output_scale_multiplier: f64 = args.get("octave_output_scale_multiplier").unwrap_or(0.5);
        let octave_input_scale_multiplier: f64 = args.get("octave_input_scale_multiplier").unwrap_or(2.0);

        Ok(quick_multioctave_noise_impl(
            x, y, seed0, seed1, octaves,
            (input_scale, output_scale, offset_x, octave_output_scale_multiplier, octave_input_scale_multiplier)
        ))
    })?;
    globals.set("quick_multioctave_noise", quick_multioctave_fn)?;

    // Register noise_layer_noise (returns noise at a position for a given layer seed)
    let map_seed_copy = map_seed;
    let noise_layer_fn = lua.create_function(move |lua, layer_seed: i64| {
        let globals = lua.globals();
        let x: f64 = globals.get("x").unwrap_or(0.0);
        let y: f64 = globals.get("y").unwrap_or(0.0);

        Ok(multioctave_noise_impl(x, y, (map_seed_copy, layer_seed), 4, 0.7, (1.0/6.0, 2.0/3.0, 0.0)))
    })?;
    globals.set("noise_layer_noise", noise_layer_fn)?;

    // Register expression_in_range - the core native function
    // expression_in_range(slope, output_scale, aux, moisture, aux_from, moisture_from, aux_to, moisture_to)
    let expression_in_range_fn = lua.create_function(
        |_, (slope, output_scale, aux, moisture, aux_from, moisture_from, aux_to, moisture_to): (f64, f64, f64, f64, f64, f64, f64, f64)| {
            Ok(expression_in_range_impl(slope, output_scale, (aux, moisture), (aux_from, moisture_from), (aux_to, moisture_to)))
        }
    )?;
    globals.set("expression_in_range", expression_in_range_fn)?;

    // Register expression_in_range_base - wrapper that uses current aux/moisture from globals
    // expression_in_range_base(aux_from, moisture_from, aux_to, moisture_to)
    let expression_in_range_base_fn = lua.create_function(
        |lua, (aux_from, moisture_from, aux_to, moisture_to): (f64, f64, f64, f64)| {
            let globals = lua.globals();
            let aux: f64 = globals.get("aux").unwrap_or(0.5);
            let moisture: f64 = globals.get("moisture").unwrap_or(0.4);
            // Use slope=20, output_scale=1 as per Factorio's definition
            Ok(expression_in_range_impl(20.0, 1.0, (aux, moisture), (aux_from, moisture_from), (aux_to, moisture_to)))
        }
    )?;
    globals.set("expression_in_range_base", expression_in_range_base_fn)?;

    // Register distance_from_nearest_point (simplified - assumes starting at origin)
    let distance_fn = lua.create_function(|_, args: Table| {
        let x: f64 = args.get("x")?;
        let y: f64 = args.get("y")?;
        // For now, assume single starting position at origin
        // In full implementation, would check args.get("points") and find nearest
        Ok((x * x + y * y).sqrt())
    })?;
    globals.set("distance_from_nearest_point", distance_fn)?;

    // Register distance_from_nearest_point_x
    let distance_x_fn = lua.create_function(|_, (x, _y, _points): (f64, f64, Value)| {
        Ok(x.abs())
    })?;
    globals.set("distance_from_nearest_point_x", distance_x_fn)?;

    // Register distance_from_nearest_point_y
    let distance_y_fn = lua.create_function(|_, (_x, y, _points): (f64, f64, Value)| {
        Ok(y.abs())
    })?;
    globals.set("distance_from_nearest_point_y", distance_y_fn)?;

    // Register variable_persistence_multioctave_noise
    let map_seed_copy = map_seed;
    let vp_multioctave_fn = lua.create_function(move |lua, args: Table| {
        let x: f64 = args.get("x")?;
        let y: f64 = args.get("y")?;
        let seed0: u32 = args.get::<Value>("seed0")?.as_integer().unwrap_or(map_seed_copy as i64) as u32;
        let seed1: i64 = args.get::<Value>("seed1")?.as_integer().unwrap_or(0);
        let octaves: u32 = args.get("octaves").unwrap_or(4);
        let input_scale: f64 = args.get("input_scale").unwrap_or(1.0);
        let output_scale: f64 = args.get("output_scale").unwrap_or(1.0);
        let offset_x: f64 = args.get("offset_x").unwrap_or(0.0);
        // For variable persistence, try to get persistence as expression result
        let persistence: f64 = args.get("persistence").unwrap_or(0.5);

        Ok(multioctave_noise_impl(x, y, (seed0, seed1), octaves, persistence, (input_scale, output_scale, offset_x)))
    })?;
    globals.set("variable_persistence_multioctave_noise", vp_multioctave_fn)?;

    // Register amplitude_corrected_multioctave_noise
    let map_seed_copy = map_seed;
    let ac_multioctave_fn = lua.create_function(move |_, args: Table| {
        let x: f64 = args.get("x")?;
        let y: f64 = args.get("y")?;
        let seed0: u32 = args.get::<Value>("seed0")?.as_integer().unwrap_or(map_seed_copy as i64) as u32;
        let seed1: i64 = args.get::<Value>("seed1")?.as_integer().unwrap_or(0);
        let octaves: u32 = args.get("octaves").unwrap_or(4);
        let persistence: f64 = args.get("persistence").unwrap_or(0.5);
        let input_scale: f64 = args.get("input_scale").unwrap_or(1.0);
        let offset_x: f64 = args.get("offset_x").unwrap_or(0.0);
        let amplitude: f64 = args.get("amplitude").unwrap_or(1.0);

        // Amplitude-corrected just multiplies by amplitude at the end
        Ok(multioctave_noise_impl(x, y, (seed0, seed1), octaves, persistence, (input_scale, amplitude, offset_x)))
    })?;
    globals.set("amplitude_corrected_multioctave_noise", ac_multioctave_fn)?;

    // Register quick_multioctave_noise_persistence (same as quick_multioctave but with persistence param)
    let map_seed_copy = map_seed;
    let qm_persistence_fn = lua.create_function(move |_, args: Table| {
        let x: f64 = args.get("x")?;
        let y: f64 = args.get("y")?;
        let seed0: u32 = args.get::<Value>("seed0")?.as_integer().unwrap_or(map_seed_copy as i64) as u32;
        let seed1: i64 = args.get::<Value>("seed1")?.as_integer().unwrap_or(0);
        let octaves: u32 = args.get("octaves").unwrap_or(4);
        let persistence: f64 = args.get("persistence").unwrap_or(0.5);
        let input_scale: f64 = args.get("input_scale").unwrap_or(1.0);
        let output_scale: f64 = args.get("output_scale").unwrap_or(1.0);
        let octave_input_scale_multiplier: f64 = args.get("octave_input_scale_multiplier").unwrap_or(2.0);

        Ok(quick_multioctave_noise_impl(
            x, y, seed0, seed1, octaves,
            (input_scale, output_scale, 0.0, persistence, octave_input_scale_multiplier)
        ))
    })?;
    globals.set("quick_multioctave_noise_persistence", qm_persistence_fn)?;

    // Register log2
    let log2_fn = lua.create_function(|_, x: f64| Ok(x.log2()))?;
    globals.set("log2", log2_fn)?;

    // Register sqrt
    let sqrt_fn = lua.create_function(|_, x: f64| Ok(x.sqrt()))?;
    globals.set("sqrt", sqrt_fn)?;

    // Register if function (ternary)
    let if_fn = lua.create_function(|_, (cond, then_val, else_val): (bool, f64, f64)| {
        Ok(if cond { then_val } else { else_val })
    })?;
    globals.set("if", if_fn)?;

    // Register slider_rescale
    let slider_rescale_fn = lua.create_function(|_, (slider, exp): (f64, f64)| {
        Ok(slider.powf(exp))
    })?;
    globals.set("slider_rescale", slider_rescale_fn)?;

    // Register control settings as global table
    lua.load(r#"
        control = setmetatable({}, {
            __index = function(_, key)
                -- Default control values
                local defaults = {
                    ["water:frequency"] = 1.0,
                    ["water:size"] = 1.0,
                }
                return defaults[key] or 1.0
            end
        })
    "#).exec()?;

    // Placeholders for expressions that need starting positions
    globals.set("starting_positions", lua.create_table()?)?;
    globals.set("starting_lake_positions", lua.create_table()?)?;
    globals.set("starting_area_radius", 150.0)?;

    // Cliff settings
    globals.set("cliff_richness", 1.0)?;
    globals.set("cliff_elevation_interval", 40.0)?;

    // Define -inf and inf
    globals.set("inf", f64::INFINITY)?;

    Ok(())
}

/// Terrain generator using Lua expressions
pub struct TerrainGenerator {
    lua: Lua,
    moisture_expr: Option<String>,
    aux_expr: Option<String>,
    // Cache for batch tile generation
    cached_chunk: Option<(i32, i32)>,  // (chunk_x, chunk_y)
    cached_tiles: Vec<&'static str>,   // 32x32 = 1024 tiles
}

impl TerrainGenerator {
    pub fn new(map_seed: u32, factorio_path: Option<&Path>) -> LuaResult<Self> {
        let lua = Lua::new();
        register_noise_functions(&lua, map_seed)?;

        let mut generator = Self {
            lua,
            moisture_expr: None,
            aux_expr: None,
            cached_chunk: None,
            cached_tiles: Vec::with_capacity(1024),
        };

        // Try to load Factorio noise expressions
        if let Some(path) = factorio_path {
            generator.load_noise_programs(path)?;
        } else {
            // Use simplified expressions if Factorio not available
            generator.use_simplified_expressions(map_seed)?;
        }

        // Pre-compile tile selection function for batch processing
        generator.lua.load(r#"
            -- Compute tile for a single position (called from Rust batch loop)
            function compute_tile_at(px, py)
                -- Set position globals
                x = px
                y = py
                distance = math.sqrt(px*px + py*py)

                -- Compute moisture and aux
                local m = moisture_full_at(px, py)
                local a = aux_full_at(px, py)
                moisture = m
                aux = a

                -- Find best tile using expression_in_range_base
                local best_tile = "grass-1"
                local best_prob = -1e308

                local function check(name, prob)
                    if prob > best_prob then
                        best_prob = prob
                        best_tile = name
                    end
                end

                -- Grass tiles
                check("grass-1", expression_in_range_base(-10, 0.7, 11, 11) + noise_layer_noise(19))
                check("grass-2", expression_in_range_base(0.45, 0.45, 11, 0.8) + noise_layer_noise(20))
                check("grass-3", expression_in_range_base(-10, 0.6, 0.65, 0.9) + noise_layer_noise(21))
                check("grass-4", expression_in_range_base(-10, 0.5, 0.55, 0.7) + noise_layer_noise(22))

                -- Dirt tiles
                check("dry-dirt", expression_in_range_base(0.45, -10, 0.55, 0.35) + noise_layer_noise(13))
                check("dirt-1", max(expression_in_range_base(-10, 0.25, 0.45, 0.3), expression_in_range_base(0.4, -10, 0.45, 0.25)) + noise_layer_noise(6))
                check("dirt-2", expression_in_range_base(-10, 0.3, 0.45, 0.35) + noise_layer_noise(7))
                check("dirt-3", expression_in_range_base(-10, 0.35, 0.55, 0.4) + noise_layer_noise(8))
                check("dirt-4", max(expression_in_range_base(0.55, -10, 0.6, 0.35), expression_in_range_base(0.6, 0.3, 11, 0.35)) + noise_layer_noise(9))
                check("dirt-5", expression_in_range_base(-10, 0.4, 0.55, 0.45) + noise_layer_noise(10))
                check("dirt-6", expression_in_range_base(-10, 0.45, 0.55, 0.5) + noise_layer_noise(11))
                check("dirt-7", expression_in_range_base(0.55, 0.35, 11, 0.45) + noise_layer_noise(12))

                -- Sand tiles
                check("sand-1", expression_in_range_base(-10, -10, 0.25, 0.15) + noise_layer_noise(14))
                check("sand-2", max(expression_in_range_base(-10, 0.15, 0.3, 0.2), expression_in_range_base(0.25, -10, 0.3, 0.15)) + noise_layer_noise(15))
                check("sand-3", max(expression_in_range_base(-10, 0.2, 0.4, 0.25), expression_in_range_base(0.3, -10, 0.4, 0.2)) + noise_layer_noise(16))

                -- Red desert tiles
                check("red-desert-0", expression_in_range_base(0.55, 0.35, 11, 0.5) + noise_layer_noise(17))
                check("red-desert-1", max(expression_in_range_base(0.6, -10, 0.7, 0.3), expression_in_range_base(0.7, 0.25, 11, 0.3)) + noise_layer_noise(18))
                check("red-desert-2", max(expression_in_range_base(0.55, 0.45, 0.6, 11), expression_in_range_base(0.6, 0.5, 11, 11)) + noise_layer_noise(23))
                check("red-desert-3", expression_in_range_base(0.65, 0.4, 11, 0.5) + noise_layer_noise(24))

                return best_tile
            end

            -- Batch compute tiles for a chunk
            function compute_chunk_tiles(chunk_x, chunk_y)
                local tiles = {}
                local base_x = chunk_x * 32
                local base_y = chunk_y * 32
                for dy = 0, 31 do
                    for dx = 0, 31 do
                        tiles[dy * 32 + dx + 1] = compute_tile_at(base_x + dx, base_y + dy)
                    end
                end
                return tiles
            end
        "#).exec()?;

        Ok(generator)
    }

    fn load_noise_programs(&mut self, factorio_path: &Path) -> LuaResult<()> {
        // Load core noise functions
        let noise_functions_path = factorio_path.join("core/prototypes/noise-functions.lua");
        if noise_functions_path.exists() {
            let content = std::fs::read_to_string(&noise_functions_path)
                .map_err(|e| mlua::Error::runtime(format!("Failed to read noise-functions.lua: {}", e)))?;
            // We can't load this directly as it uses data:extend, just skip for now
        }

        // For now, use simplified expressions
        self.moisture_expr = Some("0.4 + quick_multioctave_noise{x=x, y=y, seed0=map_seed, seed1=6, octaves=4, input_scale=1/256, output_scale=0.125, offset_x=30000, octave_output_scale_multiplier=1.5, octave_input_scale_multiplier=1/3}".to_string());
        self.aux_expr = Some("0.5 + quick_multioctave_noise{x=x, y=y, seed0=map_seed, seed1=7, octaves=4, input_scale=1/2048, output_scale=0.25, offset_x=20000, octave_output_scale_multiplier=0.5, octave_input_scale_multiplier=3}".to_string());

        Ok(())
    }

    fn use_simplified_expressions(&mut self, _map_seed: u32) -> LuaResult<()> {
        // Load all the Factorio noise expressions as Lua functions
        // Based on core/prototypes/noise-programs.lua

        self.lua.load(r#"
            -- Segmentation multiplier
            nauvis_segmentation_multiplier = 1.5 * (control["water:frequency"] or 1.0)

            -- Hills noise - medium-scale plateau/mesa features
            function nauvis_hills_at(px, py)
                return math.abs(multioctave_noise{
                    x = px, y = py,
                    persistence = 0.5,
                    seed0 = map_seed,
                    seed1 = 900,
                    octaves = 4,
                    input_scale = nauvis_segmentation_multiplier / 90
                })
            end

            -- Hills cliff level determines mesa height threshold
            function nauvis_hills_cliff_level_at(px, py)
                return clamp(0.65 + basis_noise{
                    x = px, y = py,
                    seed0 = map_seed,
                    seed1 = 99584,
                    input_scale = nauvis_segmentation_multiplier / 500,
                    output_scale = 0.6
                }, 0.15, 1.15)
            end

            -- Plateaus - flattened hills
            function nauvis_plateaus_at(px, py)
                local hills = nauvis_hills_at(px, py)
                local cliff_level = nauvis_hills_cliff_level_at(px, py)
                return 0.5 + clamp((hills - cliff_level) * 10, -0.5, 0.5)
            end

            -- Moisture noise component
            function moisture_noise_at(px, py)
                return quick_multioctave_noise{
                    x = px, y = py,
                    seed0 = map_seed,
                    seed1 = 6,
                    octaves = 4,
                    input_scale = 1/256,
                    output_scale = 0.125,
                    offset_x = 30000,
                    octave_output_scale_multiplier = 1.5,
                    octave_input_scale_multiplier = 1/3
                }
            end

            -- Aux noise component
            function aux_noise_at(px, py)
                return quick_multioctave_noise{
                    x = px, y = py,
                    seed0 = map_seed,
                    seed1 = 7,
                    octaves = 4,
                    input_scale = 1/2048,
                    output_scale = 0.25,
                    offset_x = 20000,
                    octave_output_scale_multiplier = 0.5,
                    octave_input_scale_multiplier = 3
                }
            end

            -- Full moisture calculation (nauvis style with plateaus)
            function moisture_full_at(px, py)
                local plateaus = nauvis_plateaus_at(px, py)
                local noise = moisture_noise_at(px, py)
                -- moisture_main = clamp(0.4 + bias + noise - 0.08 * (plateaus - 0.6), 0, 1)
                local main = clamp(0.4 + noise - 0.08 * (plateaus - 0.6), 0, 1)
                return main
            end

            -- Full aux calculation (nauvis style with plateaus)
            function aux_full_at(px, py)
                local plateaus = nauvis_plateaus_at(px, py)
                local noise = aux_noise_at(px, py)
                -- aux = clamp(0.5 + bias + 0.06 * (plateaus - 0.4) + noise, 0, 1)
                return clamp(0.5 + 0.06 * (plateaus - 0.4) + noise, 0, 1)
            end
        "#).exec()?;

        // Use the full expressions that include plateau influence
        self.moisture_expr = Some("moisture_full_at(x, y)".to_string());
        self.aux_expr = Some("aux_full_at(x, y)".to_string());
        Ok(())
    }

    /// Generate moisture value at position
    pub fn moisture_at(&self, x: i32, y: i32) -> f64 {
        let globals = self.lua.globals();
        globals.set("x", x as f64).ok();
        globals.set("y", y as f64).ok();
        globals.set("distance", ((x*x + y*y) as f64).sqrt()).ok();

        if let Some(expr) = &self.moisture_expr {
            self.lua.load(expr).eval().unwrap_or(0.4)
        } else {
            0.4
        }
    }

    /// Generate aux value at position
    pub fn aux_at(&self, x: i32, y: i32) -> f64 {
        let globals = self.lua.globals();
        globals.set("x", x as f64).ok();
        globals.set("y", y as f64).ok();
        globals.set("distance", ((x*x + y*y) as f64).sqrt()).ok();

        if let Some(expr) = &self.aux_expr {
            self.lua.load(expr).eval().unwrap_or(0.5)
        } else {
            0.5
        }
    }

    /// Generate tile name at position using cached chunk computation
    pub fn tile_at(&mut self, x: i32, y: i32) -> &'static str {
        let chunk_x = x.div_euclid(32);
        let chunk_y = y.div_euclid(32);

        // Check if we have this chunk cached
        if self.cached_chunk != Some((chunk_x, chunk_y)) {
            // Compute the entire chunk at once using a single Lua call
            self.compute_chunk(chunk_x, chunk_y);
        }

        // Get tile from cache
        let local_x = x.rem_euclid(32) as usize;
        let local_y = y.rem_euclid(32) as usize;
        let idx = local_y * 32 + local_x;

        self.cached_tiles.get(idx).copied().unwrap_or("grass-1")
    }

    /// Compute all tiles for a chunk using a pre-compiled Lua function call
    fn compute_chunk(&mut self, chunk_x: i32, chunk_y: i32) {
        self.cached_tiles.clear();

        // Get the pre-compiled compute_chunk_tiles function and call it
        let globals = self.lua.globals();
        let result: Result<Vec<String>, _> = globals
            .get::<mlua::Function>("compute_chunk_tiles")
            .and_then(|func| func.call((chunk_x, chunk_y)));

        match result {
            Ok(tiles) => {
                // Convert to static strings
                for tile_name in tiles {
                    let static_name: &'static str = match tile_name.as_str() {
                        "grass-1" => "grass-1",
                        "grass-2" => "grass-2",
                        "grass-3" => "grass-3",
                        "grass-4" => "grass-4",
                        "dirt-1" => "dirt-1",
                        "dirt-2" => "dirt-2",
                        "dirt-3" => "dirt-3",
                        "dirt-4" => "dirt-4",
                        "dirt-5" => "dirt-5",
                        "dirt-6" => "dirt-6",
                        "dirt-7" => "dirt-7",
                        "dry-dirt" => "dry-dirt",
                        "sand-1" => "sand-1",
                        "sand-2" => "sand-2",
                        "sand-3" => "sand-3",
                        "red-desert-0" => "red-desert-0",
                        "red-desert-1" => "red-desert-1",
                        "red-desert-2" => "red-desert-2",
                        "red-desert-3" => "red-desert-3",
                        _ => "grass-1",
                    };
                    self.cached_tiles.push(static_name);
                }
                self.cached_chunk = Some((chunk_x, chunk_y));
            }
            Err(e) => {
                eprintln!("Lua error computing chunk: {}", e);
                // Fill with default tiles on error
                self.cached_tiles.resize(1024, "grass-1");
                self.cached_chunk = Some((chunk_x, chunk_y));
            }
        }
    }
}

// Thread-local generator for use in map_transfer
thread_local! {
    static GENERATOR: std::cell::RefCell<Option<TerrainGenerator>> = std::cell::RefCell::new(None);
}

/// Initialize the terrain generator with a seed
pub fn init_generator(seed: u32) {
    GENERATOR.with(|g| {
        *g.borrow_mut() = TerrainGenerator::new(seed, None).ok();
    });
}

/// Generate a tile at position using the Lua-based generator
pub fn generate_tile(x: i32, y: i32, seed: u32) -> &'static str {
    GENERATOR.with(|g| {
        let mut gen = g.borrow_mut();
        if gen.is_none() {
            *gen = TerrainGenerator::new(seed, None).ok();
        }
        if let Some(generator) = gen.as_mut() {
            generator.tile_at(x, y)
        } else {
            "grass-1" // Fallback
        }
    })
}

// Keep old exports for compatibility
pub fn generate_moisture(x: f64, y: f64, seed: u32) -> f64 {
    GENERATOR.with(|g| {
        let mut gen = g.borrow_mut();
        if gen.is_none() {
            *gen = TerrainGenerator::new(seed, None).ok();
        }
        if let Some(generator) = gen.as_ref() {
            generator.moisture_at(x as i32, y as i32)
        } else {
            0.4
        }
    })
}

pub fn generate_aux(x: f64, y: f64, seed: u32) -> f64 {
    GENERATOR.with(|g| {
        let mut gen = g.borrow_mut();
        if gen.is_none() {
            *gen = TerrainGenerator::new(seed, None).ok();
        }
        if let Some(generator) = gen.as_ref() {
            generator.aux_at(x as i32, y as i32)
        } else {
            0.5
        }
    })
}

pub fn generate_elevation(_x: f64, _y: f64, _seed: u32) -> f64 {
    10.0 // Simplified - always above water
}
