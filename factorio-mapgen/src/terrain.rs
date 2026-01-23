//! High-level terrain generator
//!
//! Uses compiled noise programs loaded from Factorio's actual Lua files.

use std::path::Path;

use crate::loader::FactorioData;
use crate::expression::{parse_expression, Expr};
use crate::compiler::{Compiler, CompiledProgram, FunctionDef, REG_AUX, REG_MOISTURE, REG_ELEVATION};
use crate::executor::ExecContext;
use crate::cache::TILES_PER_CHUNK;

/// Compiled tile definition
struct CompiledTile {
    name: String,
    probability_program: CompiledProgram,
}

/// High-level terrain generator
pub struct TerrainGenerator {
    seed: u32,
    elevation_program: CompiledProgram,
    moisture_program: CompiledProgram,
    aux_program: CompiledProgram,
    tiles: Vec<CompiledTile>,
    tile_names: Vec<String>,
    moisture_bias: f32,
    aux_bias: f32,
    controls: std::collections::HashMap<String, f32>,
}

impl TerrainGenerator {
    /// Create a new terrain generator, loading from Factorio's data files
    pub fn new(seed: u32) -> Result<Self, String> {
        Self::new_with_controls(seed, &std::collections::HashMap::new())
    }

    /// Create a new terrain generator with map gen control overrides
    pub fn new_with_controls(seed: u32, controls: &std::collections::HashMap<String, f32>) -> Result<Self, String> {
        let factorio_path = Path::new("/Applications/factorio.app/Contents/data");

        if factorio_path.exists() {
            Self::from_factorio_with_controls(seed, factorio_path, controls)
        } else {
            Err("Factorio not found at /Applications/factorio.app/Contents/data".to_string())
        }
    }

    /// Create from Factorio's actual Lua files
    pub fn from_factorio(seed: u32, factorio_path: &Path) -> Result<Self, String> {
        Self::from_factorio_with_controls(seed, factorio_path, &std::collections::HashMap::new())
    }

    /// Create from Factorio's actual Lua files with map gen control overrides
    pub fn from_factorio_with_controls(seed: u32, factorio_path: &Path, controls: &std::collections::HashMap<String, f32>) -> Result<Self, String> {
        let data = FactorioData::load_with_seed(factorio_path, seed)?;

        let controls_owned = controls.clone();

        // Build a compiler helper with all functions loaded
        let build_compiler = |data: &FactorioData| -> Compiler {
            let mut c = Compiler::new();
            for (name, &value) in &controls_owned {
                c.set_control(name, value);
            }

            // Set point lists for distance_from_nearest_point
            c.set_point_list("starting_lake_positions", data.starting_lake_positions.clone());
            c.set_point_list("starting_positions", data.starting_positions.clone());

            // Add functions in sorted order for deterministic compilation
            let mut func_names: Vec<_> = data.noise_functions.keys().collect();
            func_names.sort();
            for name in func_names {
                let func = &data.noise_functions[name];
                c.add_function(name, FunctionDef {
                    parameters: func.parameters.clone(),
                    expression: func.expression.clone(),
                    local_expressions: func.local_expressions.clone(),
                });
            }
            // Add expressions in sorted order for deterministic compilation
            let mut expr_names: Vec<_> = data.noise_expressions.keys().collect();
            expr_names.sort();
            for name in expr_names {
                let expr = &data.noise_expressions[name];
                c.add_expression_with_locals(name, expr.expression.clone(), expr.local_expressions.clone());
            }
            c
        };

        // Compile elevation expression (via ExprRef to handle local expressions)
        let elevation_program = {
            let mut c = build_compiler(&data);
            let out = c.compile(&Expr::ExprRef("elevation".to_string()));
            c.build(out)
        };

        // Compile moisture expression
        let moisture_program = {
            let mut c = build_compiler(&data);
            let out = c.compile(&Expr::ExprRef("moisture".to_string()));
            c.build(out)
        };

        // Compile aux expression
        let aux_program = {
            let mut c = build_compiler(&data);
            let out = c.compile(&Expr::ExprRef("aux".to_string()));
            c.build(out)
        };

        // Compile tile probability programs
        let mut tiles = Vec::new();
        let mut tile_names = Vec::new();

        // Add water tiles first (they don't have autoplace but we check elevation)
        tile_names.push("water".to_string());
        tile_names.push("deepwater".to_string());

        for tile_def in &data.tiles {
            // Skip water tiles - they're handled by elevation check, not probability
            if tile_def.name.contains("water") {
                continue;
            }
            if let Some(prob_expr_str) = &tile_def.probability_expression {
                match parse_expression(prob_expr_str) {
                    Ok(prob_expr) => {
                        let mut c = build_compiler(&data);
                        c.register_tile_inputs(); // aux, moisture, elevation as pre-computed inputs
                        let out = c.compile(&prob_expr);
                        let program = c.build(out);
                        tile_names.push(tile_def.name.clone());
                        tiles.push(CompiledTile {
                            name: tile_def.name.clone(),
                            probability_program: program,
                        });
                    }
                    Err(e) => {
                        eprintln!("Warning: Failed to parse tile '{}' expression: {}", tile_def.name, e);
                    }
                }
            }
        }

        // Add out-of-map as last resort
        tile_names.push("out-of-map".to_string());

        eprintln!("DEBUG: Loaded {} tile probability programs: {:?}", tiles.len(), tile_names);

        Ok(Self {
            seed,
            elevation_program,
            moisture_program,
            aux_program,
            tiles,
            tile_names,
            moisture_bias: 0.0,
            aux_bias: 0.0,
            controls: controls_owned,
        })
    }

    /// Set moisture bias (added to computed moisture values)
    pub fn with_moisture_bias(mut self, bias: f32) -> Self {
        self.moisture_bias = bias;
        self
    }

    /// Set aux bias (added to computed aux values)
    pub fn with_aux_bias(mut self, bias: f32) -> Self {
        self.aux_bias = bias;
        self
    }

    /// Set both moisture and aux biases
    pub fn with_biases(mut self, moisture_bias: f32, aux_bias: f32) -> Self {
        self.moisture_bias = moisture_bias;
        self.aux_bias = aux_bias;
        self
    }

    /// Compute all tiles for a chunk
    pub fn compute_chunk(&self, chunk_x: i32, chunk_y: i32) -> [u8; TILES_PER_CHUNK] {
        // Execute elevation
        let mut elev_ctx = ExecContext::new(self.seed, self.elevation_program.num_registers);
        elev_ctx.init_chunk(chunk_x, chunk_y);
        elev_ctx.execute(&self.elevation_program);
        let elevation: [f32; TILES_PER_CHUNK] = *elev_ctx.get_reg(self.elevation_program.output_reg);

        // Execute moisture (needed for aux and tile programs that reference it)
        let mut moist_ctx = ExecContext::new(self.seed, self.moisture_program.num_registers);
        moist_ctx.init_chunk(chunk_x, chunk_y);
        moist_ctx.execute(&self.moisture_program);
        let mut moisture: [f32; TILES_PER_CHUNK] = *moist_ctx.get_reg(self.moisture_program.output_reg);

        // Apply moisture bias
        if self.moisture_bias != 0.0 {
            for m in &mut moisture {
                *m = (*m + self.moisture_bias).clamp(0.0, 1.0);
            }
        }

        // Execute aux
        let mut aux_ctx = ExecContext::new(self.seed, self.aux_program.num_registers);
        aux_ctx.init_chunk(chunk_x, chunk_y);
        aux_ctx.execute(&self.aux_program);
        let mut aux: [f32; TILES_PER_CHUNK] = *aux_ctx.get_reg(self.aux_program.output_reg);

        // Apply aux bias
        if self.aux_bias != 0.0 {
            for a in &mut aux {
                *a = (*a + self.aux_bias).clamp(0.0, 1.0);
            }
        }


        // Compute tile probabilities
        let mut tile_probs: Vec<[f32; TILES_PER_CHUNK]> = Vec::with_capacity(self.tiles.len());
        for tile in &self.tiles {
            let mut tile_ctx = ExecContext::new(self.seed, tile.probability_program.num_registers);
            tile_ctx.init_chunk(chunk_x, chunk_y);
            // Set aux, moisture, and elevation in context
            self.set_aux_moisture_elevation(&mut tile_ctx, &aux, &moisture, &elevation);
            tile_ctx.execute(&tile.probability_program);
            tile_probs.push(*tile_ctx.get_reg(tile.probability_program.output_reg));
        }

        // Select tiles
        let mut result = [0u8; TILES_PER_CHUNK];

        for i in 0..TILES_PER_CHUNK {
            // Check water first
            if elevation[i] <= -2.0 {
                result[i] = 1; // deepwater (index 1)
                continue;
            }
            if elevation[i] <= 0.0 {
                result[i] = 0; // water (index 0)
                continue;
            }

            // Find best land tile
            let mut best_prob = f32::NEG_INFINITY;
            let mut best_tile = (self.tile_names.len() - 1) as u8; // out-of-map

            for (idx, probs) in tile_probs.iter().enumerate() {
                if probs[i] > best_prob {
                    best_prob = probs[i];
                    best_tile = (idx + 2) as u8; // +2 because water and deepwater are 0,1
                }
            }

            result[i] = best_tile;
        }

        result
    }

    fn set_aux_moisture_elevation(&self, ctx: &mut ExecContext, aux: &[f32; TILES_PER_CHUNK], moisture: &[f32; TILES_PER_CHUNK], elevation: &[f32; TILES_PER_CHUNK]) {
        ctx.set_reg(REG_AUX, aux);
        ctx.set_reg(REG_MOISTURE, moisture);
        ctx.set_reg(REG_ELEVATION, elevation);
    }

    /// Get tile name from index
    pub fn tile_name(&self, idx: u8) -> &str {
        self.tile_names.get(idx as usize).map(|s| s.as_str()).unwrap_or("out-of-map")
    }
}

// ============================================================================
// Thread-local generator
// ============================================================================

thread_local! {
    static GENERATOR: std::cell::RefCell<Option<TerrainGenerator>> = const { std::cell::RefCell::new(None) };
}

pub fn init_generator(seed: u32) {
    GENERATOR.with(|g| {
        *g.borrow_mut() = TerrainGenerator::new(seed).ok();
    });
}

pub fn generate_tile(x: i32, y: i32, seed: u32) -> &'static str {
    GENERATOR.with(|g| {
        let mut gen = g.borrow_mut();
        if gen.is_none() {
            *gen = TerrainGenerator::new(seed).ok();
        }
        if let Some(generator) = gen.as_ref() {
            let chunk_x = x.div_euclid(32);
            let chunk_y = y.div_euclid(32);
            let local_x = x.rem_euclid(32) as usize;
            let local_y = y.rem_euclid(32) as usize;

            let tiles = generator.compute_chunk(chunk_x, chunk_y);
            let idx = tiles[local_y * 32 + local_x];
            // Can't return generator.tile_name() because of borrow, return static fallback
            match idx {
                0 => "water",
                1 => "deepwater",
                _ => "grass-1", // fallback
            }
        } else {
            "out-of-map"
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_terrain_generator() {
        let path = Path::new("/Applications/factorio.app/Contents/data");
        if !path.exists() {
            eprintln!("Factorio not installed, skipping test");
            return;
        }

        let gen = TerrainGenerator::from_factorio(794420221, path).expect("Failed to create generator");



        // Verify tile variety
        let tiles_0_0 = gen.compute_chunk(0, 0);
        let unique: std::collections::HashSet<u8> = tiles_0_0.iter().cloned().collect();
        assert!(unique.len() >= 2, "Should have multiple tile types at origin");

        // Verify water at lake (chunk containing starting_lake_positions)
        let lake_tiles = gen.compute_chunk(-2, -2);
        let water_count = lake_tiles.iter().filter(|&&t| t <= 1).count();
        assert!(water_count > 500, "Should have water at lake chunk (-2, -2)");

        // Verify loaded data
        assert!(gen.tile_names.len() > 10, "Should load many tile names");
        assert!(gen.tiles.len() > 10, "Should load many tile programs");
    }
}
