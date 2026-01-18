//! Noise program executor
//!
//! Executes compiled noise programs on register-based caches.

use crate::compiler::{CompiledProgram, Op, REG_MAP_SEED};
use crate::expression::BinOp;
use crate::cache::TILES_PER_CHUNK;
use std::collections::HashMap;

/// Execution context with register storage
pub struct ExecContext {
    pub seed: u32,
    regs: Vec<[f32; TILES_PER_CHUNK]>,
    seed_overrides: HashMap<usize, i64>,
}

impl ExecContext {
    pub fn new(seed: u32, num_registers: usize) -> Self {
        Self {
            seed,
            regs: vec![[0.0; TILES_PER_CHUNK]; num_registers.max(4)],
            seed_overrides: HashMap::new(),
        }
    }

    pub fn init_chunk(&mut self, chunk_x: i32, chunk_y: i32) {
        let base_x = (chunk_x * 32) as f32;
        let base_y = (chunk_y * 32) as f32;

        for dy in 0..32 {
            for dx in 0..32 {
                let i = dy * 32 + dx;
                // Tile centers: add 0.5 to get center of tile
                self.regs[0][i] = base_x + dx as f32 + 0.5;
                self.regs[1][i] = base_y + dy as f32 + 0.5;
                let (x, y) = (self.regs[0][i], self.regs[1][i]);
                self.regs[2][i] = (x * x + y * y).sqrt();
            }
        }
        for v in &mut self.regs[3] { *v = self.seed as f32; }
    }

    pub fn get_reg(&self, idx: usize) -> &[f32; TILES_PER_CHUNK] {
        &self.regs[idx]
    }

    pub fn set_reg(&mut self, idx: usize, values: &[f32; TILES_PER_CHUNK]) {
        if idx < self.regs.len() {
            self.regs[idx] = *values;
        }
    }

    /// Get seed0 value, using exact u32 if it's the map_seed register (to avoid f32 precision loss)
    fn get_seed0(&self, reg: usize) -> u32 {
        if let Some(&seed) = self.seed_overrides.get(&reg) {
            seed as u32
        } else if reg == REG_MAP_SEED {
            self.seed
        } else {
            self.regs[reg][0] as u32
        }
    }

    fn get_seed1(&self, reg: usize) -> i64 {
        if let Some(&seed) = self.seed_overrides.get(&reg) {
            seed
        } else {
            self.regs[reg][0] as i64
        }
    }

    pub fn execute(&mut self, program: &CompiledProgram) {
        self.seed_overrides = program.seed_overrides.clone();
        for op in &program.ops {
            self.execute_op(op);
        }
    }

    pub fn execute_single(&mut self, op: &Op) {
        self.execute_op(op);
    }

    fn execute_op(&mut self, op: &Op) {
        match op {
            Op::LoadConst { dst, value } => {
                for v in &mut self.regs[*dst] { *v = *value; }
            }
            Op::Copy { dst, src } => {
                self.regs[*dst] = self.regs[*src];
            }
            Op::BinOp { dst, a, b, op } => self.exec_binop(*dst, *a, *b, *op),
            Op::UnaryOp { dst, src, op } => self.exec_unary(*dst, *src, *op),
            Op::BasisNoise { dst, x, y, seed0, seed1, input_scale, output_scale, offset_x, offset_y } => {
                self.exec_basis_noise(*dst, [*x, *y, *seed0, *seed1, *input_scale, *output_scale, *offset_x, *offset_y]);
            }
            Op::MultioctaveNoise { dst, x, y, seed0, seed1, octaves, persistence, input_scale, output_scale, offset_x, offset_y } => {
                self.exec_multioctave(*dst, [*x, *y, *seed0, *seed1, *octaves, *persistence, *input_scale, *output_scale, *offset_x, *offset_y]);
            }
            Op::QuickMultioctaveNoise { dst, x, y, seed0, seed1, octaves, input_scale, output_scale, offset_x, offset_y, octave_output_scale_multiplier, octave_input_scale_multiplier, octave_seed0_shift } => {
                self.exec_quick_multioctave(*dst, [*x, *y, *seed0, *seed1, *octaves, *input_scale, *output_scale, *offset_x, *offset_y, *octave_output_scale_multiplier, *octave_input_scale_multiplier, *octave_seed0_shift]);
            }
            Op::VariablePersistenceMultioctaveNoise { dst, x, y, seed0, seed1, octaves, persistence, input_scale, output_scale, offset_x, offset_y } => {
                self.exec_var_pers_multioctave(*dst, [*x, *y, *seed0, *seed1, *octaves, *persistence, *input_scale, *output_scale, *offset_x, *offset_y]);
            }
            Op::DistanceFromNearestPoint { dst, x, y, points, max_distance } => {
                let (xv, yv) = (self.regs[*x], self.regs[*y]);
                let max_dist = self.regs[*max_distance][0];
                for i in 0..TILES_PER_CHUNK {
                    let mut min_dist = max_dist;
                    for &(px, py) in points {
                        let dx = xv[i] - px;
                        let dy = yv[i] - py;
                        let dist = (dx * dx + dy * dy).sqrt();
                        if dist < min_dist {
                            min_dist = dist;
                        }
                    }
                    self.regs[*dst][i] = min_dist;
                }
            }
            Op::RandomPenalty { dst, x, y, seed, source, amplitude } => {
                self.exec_random_penalty(*dst, *x, *y, *seed, *source, *amplitude);
            }
            Op::SpotNoise {
                dst, x, y, seed0, seed1, region_size,
                density_expression, spot_quantity_expression, spot_radius_expression,
                spot_favorability_expression, basement_value, candidate_spot_count,
                suggested_minimum_candidate_point_spacing, skip_span, skip_offset,
                hard_region_target_quantity, maximum_spot_basement_radius,
            } => {
                self.exec_spot_noise(*dst, [
                    *x, *y, *seed0, *seed1, *region_size,
                    *density_expression, *spot_quantity_expression, *spot_radius_expression,
                    *spot_favorability_expression, *basement_value, *candidate_spot_count,
                    *suggested_minimum_candidate_point_spacing, *skip_span, *skip_offset,
                    *hard_region_target_quantity, *maximum_spot_basement_radius,
                ]);
            }
            Op::Abs { dst, src } => self.exec_unary_fn(*dst, *src, |x| x.abs()),
            Op::Sqrt { dst, src } => self.exec_unary_fn(*dst, *src, |x| x.sqrt()),
            Op::Log2 { dst, src } => self.exec_unary_fn(*dst, *src, |x| x.log2()),
            Op::Sin { dst, src } => self.exec_unary_fn(*dst, *src, |x| x.sin()),
            Op::Cos { dst, src } => self.exec_unary_fn(*dst, *src, |x| x.cos()),
            Op::Floor { dst, src } => self.exec_unary_fn(*dst, *src, |x| x.floor()),
            Op::Ceil { dst, src } => self.exec_unary_fn(*dst, *src, |x| x.ceil()),
            Op::Clamp { dst, value, min, max } => {
                let (val, mn, mx) = (self.regs[*value], self.regs[*min], self.regs[*max]);
                for i in 0..TILES_PER_CHUNK {
                    self.regs[*dst][i] = val[i].clamp(mn[i], mx[i]);
                }
            }
            Op::Lerp { dst, a, b, t } => {
                let (av, bv, tv) = (self.regs[*a], self.regs[*b], self.regs[*t]);
                for i in 0..TILES_PER_CHUNK {
                    self.regs[*dst][i] = av[i] + (bv[i] - av[i]) * tv[i];
                }
            }
            Op::Min { dst, a, b } => {
                let (av, bv) = (self.regs[*a], self.regs[*b]);
                for i in 0..TILES_PER_CHUNK {
                    self.regs[*dst][i] = av[i].min(bv[i]);
                }
            }
            Op::Max { dst, a, b } => {
                let (av, bv) = (self.regs[*a], self.regs[*b]);
                for i in 0..TILES_PER_CHUNK {
                    self.regs[*dst][i] = av[i].max(bv[i]);
                }
            }
            Op::If { dst, cond, then_val, else_val } => {
                let (cv, tv, ev) = (self.regs[*cond], self.regs[*then_val], self.regs[*else_val]);
                for i in 0..TILES_PER_CHUNK {
                    self.regs[*dst][i] = if cv[i] != 0.0 { tv[i] } else { ev[i] };
                }
            }
            Op::ExpressionInRange { dst, slope, influence, var1, var2, from1, from2, to1, to2 } => {
                self.exec_expression_in_range(*dst, [*slope, *influence, *var1, *var2, *from1, *from2, *to1, *to2]);
            }
        }
    }

    fn exec_expression_in_range(&mut self, dst: usize, r: [usize; 8]) {
        let [slope, influence, var1, var2, from1, from2, to1, to2] = r;
        let (v1, v2) = (self.regs[var1], self.regs[var2]);
        let slope_val = self.regs[slope][0];
        let influence_val = self.regs[influence][0];
        let (f1, f2, t1, t2) = (self.regs[from1][0], self.regs[from2][0], self.regs[to1][0], self.regs[to2][0]);
        for i in 0..TILES_PER_CHUNK {
            // expression_in_range computes whether (var1, var2) falls within rect from (from1,from2) to (to1,to2)
            // with a soft falloff based on slope
            let mid1 = (f1 + t1) * 0.5;
            let half1 = (t1 - f1) * 0.5;
            let mid2 = (f2 + t2) * 0.5;
            let half2 = (t2 - f2) * 0.5;
            let p1 = ((half1 - (v1[i] - mid1).abs()) * slope_val).min(influence_val);
            let p2 = ((half2 - (v2[i] - mid2).abs()) * slope_val).min(influence_val);
            self.regs[dst][i] = p1.min(p2);
        }
    }

    fn exec_binop(&mut self, dst: usize, a: usize, b: usize, op: BinOp) {
        let (av, bv) = (self.regs[a], self.regs[b]);
        for i in 0..TILES_PER_CHUNK {
            self.regs[dst][i] = match op {
                BinOp::Add => av[i] + bv[i],
                BinOp::Sub => av[i] - bv[i],
                BinOp::Mul => av[i] * bv[i],
                BinOp::Div => av[i] / bv[i],
                BinOp::Pow => av[i].powf(bv[i]),
                BinOp::Mod => av[i] % bv[i],
                BinOp::Lt => if av[i] < bv[i] { 1.0 } else { 0.0 },
                BinOp::Le => if av[i] <= bv[i] { 1.0 } else { 0.0 },
                BinOp::Gt => if av[i] > bv[i] { 1.0 } else { 0.0 },
                BinOp::Ge => if av[i] >= bv[i] { 1.0 } else { 0.0 },
                BinOp::Eq => if (av[i] - bv[i]).abs() < 1e-9 { 1.0 } else { 0.0 },
                BinOp::Ne => if (av[i] - bv[i]).abs() >= 1e-9 { 1.0 } else { 0.0 },
                BinOp::And => if av[i] != 0.0 && bv[i] != 0.0 { 1.0 } else { 0.0 },
                BinOp::Or => if av[i] != 0.0 || bv[i] != 0.0 { 1.0 } else { 0.0 },
            };
        }
    }

    fn exec_unary(&mut self, dst: usize, src: usize, op: crate::expression::UnaryOp) {
        let sv = self.regs[src];
        for i in 0..TILES_PER_CHUNK {
            self.regs[dst][i] = match op {
                crate::expression::UnaryOp::Neg => -sv[i],
                crate::expression::UnaryOp::Not => if sv[i] == 0.0 { 1.0 } else { 0.0 },
            };
        }
    }

    fn exec_unary_fn(&mut self, dst: usize, src: usize, f: fn(f32) -> f32) {
        let sv = self.regs[src];
        for i in 0..TILES_PER_CHUNK { self.regs[dst][i] = f(sv[i]); }
    }

    fn exec_basis_noise(&mut self, dst: usize, r: [usize; 8]) {
        let [x, y, seed0, seed1, in_scale, out_scale, offset_x, offset_y] = r;
        let (xv, yv) = (self.regs[x], self.regs[y]);
        let noise = PerlinNoise::new(self.get_seed0(seed0), self.get_seed1(seed1));
        let (is, os) = (self.regs[in_scale][0] as f32, self.regs[out_scale][0] as f32);
        let (ox, oy) = (self.regs[offset_x][0] as f32, self.regs[offset_y][0] as f32);
        for i in 0..TILES_PER_CHUNK {
            let nx = (xv[i] + ox) * is;
            let ny = (yv[i] + oy) * is;
            let val = noise.noise(nx, ny) * os;
            self.regs[dst][i] = val;
        }
    }

    fn exec_multioctave(&mut self, dst: usize, r: [usize; 10]) {
        let [x, y, seed0, seed1, octaves, persistence, in_scale, out_scale, offset_x, offset_y] = r;
        let (xv, yv) = (self.regs[x], self.regs[y]);
        let seed0_val = self.get_seed0(seed0);
        let seed1_val = self.get_seed1(seed1);
        let octaves_val = self.regs[octaves][0] as u32;
        let persistence_val: f32 = self.regs[persistence][0];
        let input_scale: f32 = self.regs[in_scale][0];
        let output_scale: f32 = self.regs[out_scale][0];
        let offset_x_val: f32 = self.regs[offset_x][0];
        let offset_y_val: f32 = self.regs[offset_y][0];

        for v in &mut self.regs[dst] { *v = 0.0; }

        // Same Noise object for ALL octaves (confirmed from binary: no per-octave seed change)
        let noise = PerlinNoise::new(seed0_val, seed1_val);

        // Output normalization (from 0x1015d759c): ensures RMS ≈ output_scale
        let inv_pers = 1.0f32 / persistence_val;
        let normalized_out = if (inv_pers - 1.0).abs() < f32::EPSILON {
            // persistence == 1.0: normalize by 1/sqrt(octaves) (from 0x1015d74bc)
            ((output_scale as f64) / (octaves_val as f64).sqrt()) as f32
        } else if inv_pers.abs() < f32::EPSILON {
            output_scale
        } else {
            // General case: matches binary's f32 log2/exp2f then f64 sqrt
            let r = inv_pers * inv_pers;
            let num = r - 1.0f32;
            let log_r = r.log2();
            let exp_val = (log_r * octaves_val as f32).exp2();
            let den = exp_val - 1.0f32;
            let ratio = num / den;
            ((ratio as f64).sqrt() * output_scale as f64) as f32
        };

        // Vector path (fastVectorMultioctaveNoise at 0x1015d7b10):
        // combined_scale = input_scale, halved each octave (0x1015d7cdc: fmul s11, s11, s14)
        // noise_x = (f32)((f64)(combined_scale * x[i]) + oct * 17.17) + offset_x
        // noise_y = combined_scale * y[i] + offset_y
        // Batch noise called with input_scale=1.0 (0x1015d7cb0), offset passed separately
        let mut combined_scale = input_scale;
        let mut cur_out_scale = normalized_out;

        for oct in 0..octaves_val {
            for i in 0..TILES_PER_CHUNK {
                let scaled_x = combined_scale * xv[i];
                let noise_x = ((scaled_x as f64) + oct as f64 * 17.17) as f32 + offset_x_val;
                let noise_y = combined_scale * yv[i] + offset_y_val;
                self.regs[dst][i] += noise.noise(noise_x, noise_y) * cur_out_scale;
            }
            combined_scale *= 0.5;
            cur_out_scale *= inv_pers;
        }
    }

    fn exec_quick_multioctave(&mut self, dst: usize, r: [usize; 12]) {
        let [x, y, seed0, seed1, octaves, in_scale, out_scale, offset_x, offset_y, out_mult, in_mult, octave_seed0_shift] = r;
        // Per r2 analysis of QuickMultioctaveNoise::run at 0x1015e9364:
        // - Each octave creates a fresh BasisNoise with seed0 incremented by octave_seed0_shift
        // - input_scale and output_scale are f32, multiplied by their respective multipliers each octave
        // - At 0x1015e9590: fmul s8, s8, s12 (input_scale *= in_mult)
        // - At 0x1015e9594: fmul s9, s9, s13 (output_scale *= out_mult)
        // - At 0x1015e9598: add w21, w21, w28 (seed0 += octave_seed0_shift)
        let (xv, yv) = (self.regs[x], self.regs[y]);
        let seed0_base = self.get_seed0(seed0);
        let seed1_val = self.get_seed1(seed1);
        let octaves_val = self.regs[octaves][0] as u32;
        let mut cur_in_scale: f32 = self.regs[in_scale][0];
        let mut cur_out_scale: f32 = self.regs[out_scale][0];
        let offset_x_val: f32 = self.regs[offset_x][0];
        let offset_y_val: f32 = self.regs[offset_y][0];
        let out_mult_val: f32 = self.regs[out_mult][0];
        let in_mult_val: f32 = self.regs[in_mult][0];
        let seed0_inc = self.regs[octave_seed0_shift][0] as i32 as u32;

        for v in &mut self.regs[dst] { *v = 0.0; }

        let mut cur_seed0 = seed0_base;
        for _ in 0..octaves_val {
            let noise = PerlinNoise::new(cur_seed0, seed1_val);

            for i in 0..TILES_PER_CHUNK {
                let nx = (xv[i] + offset_x_val) * cur_in_scale;
                let ny = (yv[i] + offset_y_val) * cur_in_scale;
                self.regs[dst][i] += noise.noise(nx, ny) * cur_out_scale;
            }

            cur_in_scale *= in_mult_val;
            cur_out_scale *= out_mult_val;
            cur_seed0 = cur_seed0.wrapping_add(seed0_inc);
        }
    }

    fn exec_var_pers_multioctave(&mut self, dst: usize, r: [usize; 10]) {
        let [x, y, seed0, seed1, octaves, persistence, in_scale, out_scale, offset_x, offset_y] = r;
        // From disassembly of VariablePersistenceMultioctaveNoise::run at 0x1015ed264
        // and constructor at 0x10160d260:
        // Constructor transforms (before storing in struct):
        //   input_scale *= 0.5  (at 0x10160d38c: fmul s0, s0, s1 where s1=0.5)
        //   output_scale *= 2^octaves  (at 0x10160d3b4: fmul d0, d0, d1 where d0=pow(2,N))
        // Run function:
        //   Same Noise object for all octaves, no per-octave spatial offset
        //   Accumulate noise, multiply by per-tile persistence between octaves
        //   input_scale halves each octave, final multiply by output_scale
        let (xv, yv, pv) = (self.regs[x], self.regs[y], self.regs[persistence]);
        let seed0_val = self.get_seed0(seed0);
        let seed1_val = self.get_seed1(seed1);
        let octaves_val = self.regs[octaves][0] as u32;
        let input_scale: f32 = self.regs[in_scale][0];
        let output_scale: f32 = self.regs[out_scale][0];
        let offset_x_val: f32 = self.regs[offset_x][0];
        let offset_y_val: f32 = self.regs[offset_y][0];

        // Apply constructor transforms matching binary at 0x10160d260
        let output_scale = (2.0f64.powi(octaves_val as i32) * output_scale as f64) as f32;
        let input_scale = input_scale * 0.5;

        for v in &mut self.regs[dst] { *v = 0.0; }

        let noise = PerlinNoise::new(seed0_val, seed1_val);
        let mut cur_in_scale: f32 = input_scale;

        for oct in 0..octaves_val {
            for i in 0..TILES_PER_CHUNK {
                let nx = (xv[i] + offset_x_val) * cur_in_scale;
                let ny = (yv[i] + offset_y_val) * cur_in_scale;
                self.regs[dst][i] += noise.noise(nx, ny);
            }
            if oct < octaves_val - 1 {
                for i in 0..TILES_PER_CHUNK {
                    self.regs[dst][i] *= pv[i];
                }
            }
            cur_in_scale *= 0.5;
        }
        for i in 0..TILES_PER_CHUNK {
            self.regs[dst][i] *= output_scale;
        }
    }

    fn exec_random_penalty(&mut self, dst: usize, x: usize, y: usize, seed: usize, source: usize, amplitude: usize) {
        let (xv, yv, sv, av) = (self.regs[x], self.regs[y], self.regs[source], self.regs[amplitude]);
        let seed_val = self.get_seed0(seed);
        for i in 0..TILES_PER_CHUNK {
            let h = hash_coords(xv[i] as i32, yv[i] as i32, seed_val);
            self.regs[dst][i] = sv[i] - (h as f32 / u32::MAX as f32) * av[i];
        }
    }

    fn exec_spot_noise(&mut self, dst: usize, r: [usize; 16]) {
        let [x, y, seed0, seed1, region_size,
            density_expression, spot_quantity_expression, spot_radius_expression,
            spot_favorability_expression, basement_value, candidate_spot_count,
            suggested_minimum_candidate_point_spacing, skip_span, skip_offset,
            hard_region_target_quantity, maximum_spot_basement_radius] = r;
        let (xv, yv) = (self.regs[x], self.regs[y]);
        let seed0_val = self.get_seed0(seed0);
        let seed1_val = self.get_seed1(seed1) as i32;
        let region_size_val = self.regs[region_size][0];
        let density_vals = self.regs[density_expression];
        let spot_quantity_vals = self.regs[spot_quantity_expression];
        let spot_radius_vals = self.regs[spot_radius_expression];
        let spot_favorability_vals = self.regs[spot_favorability_expression];
        let basement_val = self.regs[basement_value][0];
        let candidate_count = self.regs[candidate_spot_count][0] as u32;
        let min_spacing = self.regs[suggested_minimum_candidate_point_spacing][0];
        let skip_span_val = self.regs[skip_span][0] as u32;
        let skip_offset_val = self.regs[skip_offset][0] as u32;
        let hard_target = self.regs[hard_region_target_quantity][0];
        let max_basement_radius = self.regs[maximum_spot_basement_radius][0];

        for i in 0..TILES_PER_CHUNK {
            let px = xv[i];
            let py = yv[i];
            let spot_radius = spot_radius_vals[i];
            let spot_favorability = spot_favorability_vals[i];
            let density = density_vals[i];
            let spot_quantity = spot_quantity_vals[i];

            // Compute which region this point is in
            let region_x = (px / region_size_val).floor() as i32;
            let region_y = (py / region_size_val).floor() as i32;

            // Search neighboring regions for spots
            let mut best_value = basement_val;

            for ry in (region_y - 1)..=(region_y + 1) {
                for rx in (region_x - 1)..=(region_x + 1) {
                    // Generate candidate spots for this region
                    let region_seed = hash_region(rx, ry, seed0_val, seed1_val as u32);

                    // Determine how many spots to place in this region
                    let spots_in_region = compute_region_spot_count(
                        region_seed, (density, region_size_val), candidate_count,
                        hard_target, skip_span_val, skip_offset_val,
                    );

                    for spot_idx in 0..spots_in_region {
                        // Generate spot position within region
                        let spot_seed = xorshift(region_seed.wrapping_add(spot_idx));
                        let spot_x = (rx as f32) * region_size_val
                            + (spot_seed as f32 / u32::MAX as f32) * region_size_val;
                        let spot_y = (ry as f32) * region_size_val
                            + (xorshift(spot_seed) as f32 / u32::MAX as f32) * region_size_val;

                        // Compute distance to spot
                        let dx = px - spot_x;
                        let dy = py - spot_y;
                        let dist = (dx * dx + dy * dy).sqrt();

                        // Compute spot value based on distance
                        let effective_radius = spot_radius.max(1.0);
                        if dist < effective_radius {
                            // Inside spot - compute value based on distance from center
                            let normalized_dist = dist / effective_radius;
                            // Smooth falloff from center
                            let falloff = 1.0 - normalized_dist * normalized_dist;
                            let spot_value = falloff * spot_quantity * spot_favorability;
                            best_value = best_value.max(spot_value);
                        } else if dist < max_basement_radius {
                            // In basement zone - gradual falloff to basement value
                            let basement_falloff = (dist - effective_radius) / (max_basement_radius - effective_radius);
                            let basement_contrib = basement_val * basement_falloff;
                            best_value = best_value.max(basement_contrib);
                        }
                    }
                }
            }

            self.regs[dst][i] = best_value;
        }
    }
}

/// Simple xorshift for hashing (not used for permutation shuffle)
fn xorshift(x: u32) -> u32 {
    let x = x ^ (x << 13);
    let x = x ^ (x >> 17);
    x ^ (x << 5)
}

/// Hash function for region coordinates
fn hash_region(rx: i32, ry: i32, seed0: u32, seed1: u32) -> u32 {
    let mut h = seed0;
    h = xorshift(h.wrapping_add(rx as u32));
    h = xorshift(h.wrapping_add(ry as u32));
    h = xorshift(h.wrapping_add(seed1));
    h
}

/// Compute how many spots should be in a region
fn compute_region_spot_count(
    region_seed: u32,
    region: (f32, f32),
    candidate_count: u32,
    hard_target: f32,
    skip_span: u32,
    skip_offset: u32,
) -> u32 {
    let (density, region_size) = region;
    let region_area = region_size * region_size;
    let expected_spots = density * region_area;

    // Use Poisson-like distribution
    let base_count = if hard_target > 0.0 {
        hard_target as u32
    } else {
        // Random count based on expected value
        let random_val = (region_seed as f32) / (u32::MAX as f32);
        let count = (expected_spots + random_val * expected_spots.sqrt()).round() as u32;
        count.min(candidate_count)
    };

    // Apply skip_span/skip_offset filtering
    if skip_span > 1 {
        // Only include spots where (spot_index % skip_span) == skip_offset
        (base_count + skip_span - 1 - skip_offset) / skip_span
    } else {
        base_count
    }
}


/// Factorio's xorshift PRNG with separate state for scalar and two SIMD lanes
/// From r2 disassembly of Noise::setSeed at 0x1015d63fc
fn factorio_xorshift_step(scalar: &mut u32, simd: &mut [u32; 2]) -> u32 {
    // Scalar part: bfxil instruction behavior
    let x = *scalar;
    let tmp = x ^ (x << 13);
    let shifted = x << 12;
    // Per NOISE_REVERSE_ENGINEERING.md: (shifted & 0xFFFFE000) | ((tmp >> 19) & 0x1FFF)
    *scalar = (shifted & 0xFFFFE000) | ((tmp >> 19) & 0x1FFF);

    // SIMD lane operations from r2 disassembly:
    // ushl v5.2s, v4.2s, v1.2s   -> v5 = s << [3, 2]
    // ushl v6.2s, v4.2s, v0.2s   -> v6 = s << [17, 4]
    // eor v4 = v5 ^ s            -> (s << [3,2]) ^ s
    // ushl v4 >> [11, 25]        -> right shift via negative USHL
    // and v5 = v6 & masks        -> (s << [17, 4]) & [0xffe00000, 0xffffff80]
    // orr v4 = v4 | v5           -> combine
    let s0 = simd[0];
    simd[0] = (((s0 << 3) ^ s0) >> 11) | ((s0 << 17) & 0xffe00000);

    let s1 = simd[1];
    simd[1] = (((s1 << 2) ^ s1) >> 25) | ((s1 << 4) & 0xffffff80);

    // XOR all three updated states
    *scalar ^ simd[0] ^ simd[1]
}

/// Factorio's noise implementation (from r2 reverse engineering)
/// Uses 256 gradients computed with Factorio's polynomial sin/cos approximation.
struct FactorioNoise {
    perm1: [u8; 256],
    perm2: [u8; 256],
    gradients: [(f32, f32); 256],
    seed_byte: u8,
    seed0: u32,
    seed1: i64,
    combined_seed: u32,
}

impl FactorioNoise {
    fn new(seed: u32, seed1: i64) -> Self {
        // Factorio combines seeds: combined = seed0 + ((seed1 >> 8) * 7)
        let s1 = seed1 as u32;
        let combined = seed.wrapping_add((s1 >> 8).wrapping_mul(7));
        let seed_byte_idx = (s1 & 0xFF) as usize;

        let mut perm1 = [0u8; 256];
        let mut perm2 = [0u8; 256];
        for i in 0..256 {
            perm1[i] = i as u8;
            perm2[i] = i as u8;
        }

        // 4-shuffle: copy → perm1 → perm2 → gradients
        // Seed byte comes from a separately-shuffled copy
        let init = combined.max(0x155);
        let mut scalar = init;
        let mut simd = [init, init];

        let mut copy = perm1;
        for i in (1..=255).rev() {
            let r = factorio_xorshift_step(&mut scalar, &mut simd);
            copy.swap(i, (r as usize) % (i + 1));
        }
        let seed_byte = copy[seed_byte_idx];

        for i in (1..=255).rev() {
            let r = factorio_xorshift_step(&mut scalar, &mut simd);
            perm1.swap(i, (r as usize) % (i + 1));
        }

        for i in (1..=255).rev() {
            let r = factorio_xorshift_step(&mut scalar, &mut simd);
            perm2.swap(i, (r as usize) % (i + 1));
        }

        let mut gradients = Self::generate_gradients();
        for i in (1..=255).rev() {
            let r = factorio_xorshift_step(&mut scalar, &mut simd);
            gradients.swap(i, (r as usize) % (i + 1));
        }

        Self { perm1, perm2, gradients, seed_byte, seed0: seed, seed1, combined_seed: combined }
    }

    fn factorio_sincos(angle_rad: f64) -> (f64, f64) {
        // Use standard unit circle - NOISE_REVERSE_ENGINEERING.md says gradients are unit vectors
        let cos = angle_rad.cos();
        let sin = angle_rad.sin();
        (cos, sin)
    }

    fn _factorio_sincos_polynomial(angle_rad: f64) -> (f64, f64) {
        // Polynomial approximation from Factorio binary (kept for reference)
        const INV_2PI: f64 = 0.15915494309189535;
        const X1: f64 = -41.341678992182025;
        const X2: f64 = 6.283185269630412;
        const X3: f64 = -76.56887678023256;
        const X4: f64 = 81.60201529595571;
        const X5: f64 = 39.65735524898863;
        const SCALE: f64 = 4.200003814697266;

        #[inline]
        fn tri_wave(v: f64) -> f64 {
            let add = if v > 0.0 { 0.5 } else { -0.5 };
            let round = (v + add) as i64 as f64;
            let frac = (v - round).abs();
            0.25 - frac
        }

        #[inline]
        fn poly(t: f64) -> f64 {
            let d4 = t * t;
            let d5 = d4 * d4;
            let d6 = d5 * d5;
            let d23 = X2 + (X1 * d4);
            let d20 = X4 + (X3 * d4);
            let d20 = d5 * d20;
            let d20 = d20 + d23;
            let d21 = X5 * d6;
            d20 + d21
        }

        let turns = angle_rad * INV_2PI;
        let cos_t = tri_wave(turns);
        let sin_t = tri_wave(turns - 0.25);

        let cos = cos_t * poly(cos_t) * SCALE;
        let sin = sin_t * poly(sin_t) * SCALE;
        (cos, sin)
    }

    fn generate_gradients() -> [(f32, f32); 256] {
        let mut gradients = [(0.0f32, 0.0f32); 256];
        for i in 0..256 {
            let angle = (i as f64) * std::f64::consts::TAU / 256.0;
            let (cos, sin) = Self::factorio_sincos_poly(angle);
            gradients[i] = (cos as f32, sin as f32);
        }
        gradients
    }

    fn factorio_sincos_poly(angle_rad: f64) -> (f64, f64) {
        const INV_2PI: f64 = 0.15915494309189535;
        const X1: f64 = -41.341678992182025;
        const X2: f64 = 6.283185269630412;
        const X3: f64 = -76.56887678023256;
        const X4: f64 = 81.60201529595571;
        const X5: f64 = 39.65735524898863;
        const SCALE: f64 = 4.200003814697266;

        #[inline]
        fn tri_wave(v: f64) -> f64 {
            let add = if v > 0.0 { 0.5 } else { -0.5 };
            let round = (v + add) as i64 as f64;
            let frac = (v - round).abs();
            0.25 - frac
        }

        #[inline]
        fn poly(t: f64) -> f64 {
            let d4 = t * t;
            let d5 = d4 * d4;
            let d6 = d5 * d5;
            let d23 = X2 + (X1 * d4);
            let d20 = X4 + (X3 * d4);
            let d20 = d5 * d20;
            let d20 = d20 + d23;
            let d21 = X5 * d6;
            d20 + d21
        }

        let turns = angle_rad * INV_2PI;
        let cos_t = tri_wave(turns);
        let sin_t = tri_wave(turns - 0.25);
        let cos = cos_t * poly(cos_t) * SCALE;
        let sin = sin_t * poly(sin_t) * SCALE;
        (cos, sin)
    }

    #[inline(always)]
    fn hash(&self, xi: i32, yi: i32) -> usize {
        // From r2 RE of gradientsLine at 0x1015d7390:
        // hash = perm1[y] ^ seed_byte ^ perm2[x]
        // (perm1 at offset 6, perm2 at offset 0x106)
        let h = self.perm1[(yi & 0xFF) as usize] ^ self.seed_byte ^ self.perm2[(xi & 0xFF) as usize];
        h as usize
    }

    #[inline(always)]
    fn noise(&self, x: f32, y: f32) -> f32 {
        let xi = x.floor() as i32;
        let yi = y.floor() as i32;
        let xf = x - xi as f32;
        let yf = y - yi as f32;

        // Radial weight function: w = (1 - min(dist², 1))³
        // This gives non-zero values even at integer coordinates because
        // it considers ALL corners with their radial weights
        let mut sum = 0.0f32;
        for dy in 0..2i32 {
            for dx in 0..2i32 {
                let (gx, gy) = self.gradients[self.hash(xi + dx, yi + dy)];
                let fx = xf - dx as f32;
                let fy = yf - dy as f32;
                let dist2 = fx * fx + fy * fy;
                let w_base = (1.0 - dist2.min(1.0)).max(0.0);
                let w = w_base * w_base * w_base;
                sum += (gx * fx + gy * fy) * w;
            }
        }
        sum
    }

}

type PerlinNoise = FactorioNoise;

fn hash_coords(x: i32, y: i32, seed: u32) -> u32 {
    xorshift(xorshift(seed.wrapping_add(x as u32)).wrapping_add(y as u32))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exec_context() {
        let mut ctx = ExecContext::new(12345, 10);
        ctx.init_chunk(0, 0);
        assert_eq!(ctx.get_reg(0)[0], 0.0);
        assert_eq!(ctx.get_reg(1)[0], 0.0);
        assert_eq!(ctx.get_reg(0)[31], 31.0);
    }
}
