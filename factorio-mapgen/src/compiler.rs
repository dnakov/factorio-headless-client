//! Noise program compiler
//!
//! Compiles parsed noise expressions into executable bytecode operations.

use std::collections::HashMap;
use crate::expression::{Expr, BinOp, UnaryOp, string_seed};

/// Compiled noise program
pub struct CompiledProgram {
    pub ops: Vec<Op>,
    pub output_reg: usize,
    pub num_registers: usize,
    pub seed_overrides: HashMap<usize, i64>,
}

/// Low-level operation
#[derive(Debug, Clone)]
pub enum Op {
    /// Load constant into register
    LoadConst { dst: usize, value: f32 },
    /// Copy register
    Copy { dst: usize, src: usize },
    /// Binary operation: dst = a op b
    BinOp { dst: usize, a: usize, b: usize, op: BinOp },
    /// Unary operation: dst = op(src)
    UnaryOp { dst: usize, src: usize, op: UnaryOp },

    // Native noise functions
    BasisNoise {
        dst: usize,
        x: usize,
        y: usize,
        seed0: usize,
        seed1: usize,
        input_scale: usize,
        output_scale: usize,
        offset_x: usize,
        offset_y: usize,
    },
    MultioctaveNoise {
        dst: usize,
        x: usize,
        y: usize,
        seed0: usize,
        seed1: usize,
        octaves: usize,
        persistence: usize,
        input_scale: usize,
        output_scale: usize,
        offset_x: usize,
        offset_y: usize,
    },
    QuickMultioctaveNoise {
        dst: usize,
        x: usize,
        y: usize,
        seed0: usize,
        seed1: usize,
        octaves: usize,
        input_scale: usize,
        output_scale: usize,
        offset_x: usize,
        offset_y: usize,
        octave_output_scale_multiplier: usize,
        octave_input_scale_multiplier: usize,
        octave_seed0_shift: usize,
    },
    VariablePersistenceMultioctaveNoise {
        dst: usize,
        x: usize,
        y: usize,
        seed0: usize,
        seed1: usize,
        octaves: usize,
        persistence: usize,
        input_scale: usize,
        output_scale: usize,
        offset_x: usize,
        offset_y: usize,
    },
    DistanceFromNearestPoint {
        dst: usize,
        x: usize,
        y: usize,
        max_distance: usize,
        points: Vec<(f32, f32)>, // List of points to measure distance from
    },
    RandomPenalty {
        dst: usize,
        x: usize,
        y: usize,
        seed: usize,
        source: usize,
        amplitude: usize,
    },
    SpotNoise {
        dst: usize,
        x: usize,
        y: usize,
        seed0: usize,
        seed1: usize,
        region_size: usize,
        density_expression: usize,
        spot_quantity_expression: usize,
        spot_radius_expression: usize,
        spot_favorability_expression: usize,
        basement_value: usize,
        candidate_spot_count: usize,
        suggested_minimum_candidate_point_spacing: usize,
        skip_span: usize,
        skip_offset: usize,
        hard_region_target_quantity: usize,
        maximum_spot_basement_radius: usize,
    },
    /// expression_in_range - native tile autoplace function
    /// Computes probability based on whether (var1, var2) falls within the given rectangular range
    ExpressionInRange {
        dst: usize,
        slope: usize,
        influence: usize,
        var1: usize,  // Usually aux
        var2: usize,  // Usually moisture
        from1: usize,
        from2: usize,
        to1: usize,
        to2: usize,
    },

    // Math functions
    Abs { dst: usize, src: usize },
    Sqrt { dst: usize, src: usize },
    Log2 { dst: usize, src: usize },
    Sin { dst: usize, src: usize },
    Cos { dst: usize, src: usize },
    Floor { dst: usize, src: usize },
    Ceil { dst: usize, src: usize },

    // Control flow
    Clamp { dst: usize, value: usize, min: usize, max: usize },
    Lerp { dst: usize, a: usize, b: usize, t: usize },
    Min { dst: usize, a: usize, b: usize },
    Max { dst: usize, a: usize, b: usize },
    If { dst: usize, cond: usize, then_val: usize, else_val: usize },
}

/// Expression definition with optional local bindings
#[derive(Debug, Clone)]
pub struct ExpressionDef {
    pub expression: Expr,
    pub local_expressions: HashMap<String, Expr>,
}

/// Compiler state
pub struct Compiler {
    /// Next register to allocate
    next_reg: usize,
    /// Named register mappings
    named_regs: HashMap<String, usize>,
    /// Operations
    ops: Vec<Op>,
    /// Expression definitions from Lua (with their local bindings)
    expr_defs: HashMap<String, ExpressionDef>,
    /// Function definitions from Lua
    func_defs: HashMap<String, FunctionDef>,
    /// Default values for control variables
    control_defaults: HashMap<String, f32>,
    /// Exact seed overrides for string literals (avoid f32 precision loss)
    seed_overrides: HashMap<usize, i64>,
    /// Point lists (like starting_lake_positions)
    point_lists: HashMap<String, Vec<(f32, f32)>>,
}

/// Function definition from Lua
#[derive(Debug, Clone)]
pub struct FunctionDef {
    pub parameters: Vec<String>,
    pub expression: Expr,
    pub local_expressions: HashMap<String, Expr>,
}

/// Well-known register indices
pub const REG_X: usize = 0;
pub const REG_Y: usize = 1;
pub const REG_DISTANCE: usize = 2;
pub const REG_MAP_SEED: usize = 3;
pub const REG_AUX: usize = 4;
pub const REG_MOISTURE: usize = 5;
pub const REG_ELEVATION: usize = 6;

impl Compiler {
    pub fn new() -> Self {
        let mut named_regs = HashMap::new();
        // Pre-allocate standard input registers
        named_regs.insert("x".to_string(), REG_X);
        named_regs.insert("y".to_string(), REG_Y);
        named_regs.insert("distance".to_string(), REG_DISTANCE);
        named_regs.insert("map_seed".to_string(), REG_MAP_SEED);
        // Note: aux, moisture, elevation are NOT pre-registered here.
        // They must be compiled when referenced, then set_reg() is used
        // to provide pre-computed values for tile probability expressions.

        let mut control_defaults = HashMap::new();
        // Map generation control settings - default to 1.0 for frequency/size, 0.0 for bias
        control_defaults.insert("control:water:frequency".to_string(), 1.0);
        control_defaults.insert("control:water:size".to_string(), 1.0);
        control_defaults.insert("control:moisture:frequency".to_string(), 1.0);
        control_defaults.insert("control:moisture:bias".to_string(), 0.0);
        control_defaults.insert("control:aux:frequency".to_string(), 1.0);
        control_defaults.insert("control:aux:bias".to_string(), 0.0);
        control_defaults.insert("control:temperature:frequency".to_string(), 1.0);
        control_defaults.insert("control:temperature:bias".to_string(), 0.0);
        // Starting area bias (typically 0 for default generation)
        control_defaults.insert("starting_bias_change".to_string(), 0.0);
        control_defaults.insert("starting_bias_magnitude".to_string(), 0.0);
        // Starting area radius (tiles) for default map gen
        control_defaults.insert("starting_area_radius".to_string(), 150.0);
        // Starting area moisture/aux frequency and size (default map gen uses 1.0)
        control_defaults.insert("control:starting_area_moisture:frequency".to_string(), 1.0);
        control_defaults.insert("control:starting_area_moisture:size".to_string(), 1.0);
        control_defaults.insert("control:starting_area_aux:frequency".to_string(), 1.0);
        control_defaults.insert("control:starting_area_aux:size".to_string(), 1.0);

        Self {
            next_reg: 7, // Start after x, y, distance, map_seed, aux, moisture, elevation
            named_regs,
            ops: Vec::new(),
            expr_defs: HashMap::new(),
            func_defs: HashMap::new(),
            control_defaults,
            seed_overrides: HashMap::new(),
            point_lists: HashMap::new(),
        }
    }

    /// Set a point list (like starting_lake_positions)
    pub fn set_point_list(&mut self, name: &str, points: Vec<(f32, f32)>) {
        self.point_lists.insert(name.to_string(), points);
    }

    /// Register aux, moisture, elevation as pre-computed input registers.
    /// Call this before compiling tile probability expressions that reference these values.
    pub fn register_tile_inputs(&mut self) {
        self.named_regs.insert("aux".to_string(), REG_AUX);
        self.named_regs.insert("moisture".to_string(), REG_MOISTURE);
        self.named_regs.insert("elevation".to_string(), REG_ELEVATION);
    }

    /// Add expression definition (simple, no locals)
    pub fn add_expression(&mut self, name: &str, expr: Expr) {
        self.expr_defs.insert(name.to_string(), ExpressionDef {
            expression: expr,
            local_expressions: HashMap::new(),
        });
    }

    /// Add expression definition with local bindings
    pub fn add_expression_with_locals(&mut self, name: &str, expr: Expr, locals: HashMap<String, Expr>) {
        self.expr_defs.insert(name.to_string(), ExpressionDef {
            expression: expr,
            local_expressions: locals,
        });
    }

    /// Add function definition
    pub fn add_function(&mut self, name: &str, def: FunctionDef) {
        self.func_defs.insert(name.to_string(), def);
    }

    /// Allocate a new register
    fn alloc_reg(&mut self) -> usize {
        let reg = self.next_reg;
        self.next_reg += 1;
        reg
    }

    /// Get or create a named register
    fn get_or_alloc_reg(&mut self, name: &str) -> usize {
        if let Some(&reg) = self.named_regs.get(name) {
            reg
        } else {
            let reg = self.alloc_reg();
            self.named_regs.insert(name.to_string(), reg);
            reg
        }
    }

    /// Compile an expression, returning the output register
    pub fn compile(&mut self, expr: &Expr) -> usize {
        match expr {
            Expr::Const(n) => {
                let dst = self.alloc_reg();
                self.ops.push(Op::LoadConst { dst, value: *n as f32 });
                dst
            }

            Expr::StringLiteral(s) => {
                // String literal - check if it's a control variable
                if let Some(&val) = self.control_defaults.get(s) {
                    let dst = self.alloc_reg();
                    self.ops.push(Op::LoadConst { dst, value: val });
                    dst
                } else {
                    // Convert to seed hash for other uses (like noise function seeds)
                    let seed = string_seed(s);
                    let dst = self.alloc_reg();
                    self.ops.push(Op::LoadConst { dst, value: seed as f32 });
                    self.seed_overrides.insert(dst, seed);
                    dst
                }
            }

            Expr::Var(name) => {
                // Built-in variables
                match name.as_str() {
                    "x" => 0,
                    "y" => 1,
                    "distance" => 2,
                    "map_seed" => 3,
                    "pi" => {
                        let dst = self.alloc_reg();
                        self.ops.push(Op::LoadConst { dst, value: std::f32::consts::PI });
                        dst
                    }
                    "inf" => {
                        let dst = self.alloc_reg();
                        self.ops.push(Op::LoadConst { dst, value: f32::INFINITY });
                        dst
                    }
                    "cliff_richness" => {
                        let dst = self.alloc_reg();
                        self.ops.push(Op::LoadConst { dst, value: 1.0 });
                        dst
                    }
                    "cliff_elevation_interval" => {
                        let dst = self.alloc_reg();
                        self.ops.push(Op::LoadConst { dst, value: 40.0 });
                        dst
                    }
                    _ => {
                        // Check if it's a control variable
                        if let Some(&val) = self.control_defaults.get(name) {
                            let dst = self.alloc_reg();
                            self.ops.push(Op::LoadConst { dst, value: val });
                            dst
                        } else {
                            self.get_or_alloc_reg(name)
                        }
                    }
                }
            }

            Expr::ExprRef(name) => {
                // Check if already compiled
                if let Some(&reg) = self.named_regs.get(name) {
                    return reg;
                }

                // Look up expression definition
                if let Some(expr_def) = self.expr_defs.get(name).cloned() {
                    // Save current state for scoping
                    let saved_regs = self.named_regs.clone();
                    let saved_expr_defs = self.expr_defs.clone();

                    // Add all local_expressions to expr_defs FIRST so they can be resolved
                    // when compiling each other (locals may reference other locals)
                    for (local_name, local_expr) in &expr_def.local_expressions {
                        self.expr_defs.insert(local_name.clone(), ExpressionDef {
                            expression: local_expr.clone(),
                            local_expressions: HashMap::new(),
                        });
                    }

                    // Compile local_expressions in sorted order for determinism
                    let mut local_names: Vec<_> = expr_def.local_expressions.keys().cloned().collect();
                    local_names.sort();
                    for local_name in &local_names {
                        if !self.named_regs.contains_key(local_name) {
                            let local_expr = &expr_def.local_expressions[local_name];
                            let reg = self.compile(local_expr);
                            self.named_regs.insert(local_name.clone(), reg);
                        }
                    }

                    // Compile the main expression
                    let reg = self.compile(&expr_def.expression);

                    // Restore state
                    self.named_regs = saved_regs;
                    self.expr_defs = saved_expr_defs;
                    self.named_regs.insert(name.clone(), reg);
                    reg
                } else if let Some(&val) = self.control_defaults.get(name) {
                    // Control variable referenced as expression
                    let dst = self.alloc_reg();
                    self.ops.push(Op::LoadConst { dst, value: val });
                    self.named_regs.insert(name.clone(), dst);
                    dst
                } else {
                    // Unknown reference - return a default
                    let dst = self.alloc_reg();
                    self.ops.push(Op::LoadConst { dst, value: 0.0 });
                    dst
                }
            }

            Expr::BinOp(a, op, b) => {
                let a_reg = self.compile(a);
                let b_reg = self.compile(b);
                let dst = self.alloc_reg();
                self.ops.push(Op::BinOp { dst, a: a_reg, b: b_reg, op: *op });
                dst
            }

            Expr::UnaryOp(op, expr) => {
                let src = self.compile(expr);
                let dst = self.alloc_reg();
                self.ops.push(Op::UnaryOp { dst, src, op: *op });
                dst
            }

            Expr::FunctionCall { name, args } => {
                self.compile_function_call(name, args)
            }

            Expr::Call { name, args } => {
                self.compile_positional_call(name, args)
            }
        }
    }

    fn compile_function_call(&mut self, name: &str, args: &HashMap<String, Expr>) -> usize {
        match name {
            "basis_noise" => {
                let x = args.get("x").map(|e| self.compile(e)).unwrap_or(0);
                let y = args.get("y").map(|e| self.compile(e)).unwrap_or(1);
                let seed0 = args.get("seed0").map(|e| self.compile(e)).unwrap_or(3);
                let seed1 = args.get("seed1").map(|e| self.compile(e)).unwrap_or_else(|| {
                    let r = self.alloc_reg();
                    self.ops.push(Op::LoadConst { dst: r, value: 0.0 });
                    r
                });
                let input_scale = args.get("input_scale").map(|e| self.compile(e)).unwrap_or_else(|| {
                    let r = self.alloc_reg();
                    self.ops.push(Op::LoadConst { dst: r, value: 1.0 });
                    r
                });
                let output_scale = args.get("output_scale").map(|e| self.compile(e)).unwrap_or_else(|| {
                    let r = self.alloc_reg();
                    self.ops.push(Op::LoadConst { dst: r, value: 1.0 });
                    r
                });
                let offset_x = self.compile_arg_or_default(args.get("offset_x"), 0.0);
                let offset_y = self.compile_arg_or_default(args.get("offset_y"), 0.0);

                let dst = self.alloc_reg();
                self.ops.push(Op::BasisNoise {
                    dst, x, y, seed0, seed1, input_scale, output_scale, offset_x, offset_y
                });
                dst
            }

            "multioctave_noise" => {
                let x = args.get("x").map(|e| self.compile(e)).unwrap_or(0);
                let y = args.get("y").map(|e| self.compile(e)).unwrap_or(1);
                let seed0 = args.get("seed0").map(|e| self.compile(e)).unwrap_or(3);
                let seed1 = self.compile_arg_or_default(args.get("seed1"), 0.0);
                let octaves = self.compile_arg_or_default(args.get("octaves"), 4.0);
                let persistence = self.compile_arg_or_default(args.get("persistence"), 0.5);
                let input_scale = self.compile_arg_or_default(args.get("input_scale"), 1.0);
                let output_scale = self.compile_arg_or_default(args.get("output_scale"), 1.0);
                let offset_x = self.compile_arg_or_default(args.get("offset_x"), 0.0);
                let offset_y = self.compile_arg_or_default(args.get("offset_y"), 0.0);

                let dst = self.alloc_reg();
                self.ops.push(Op::MultioctaveNoise {
                    dst, x, y, seed0, seed1, octaves, persistence, input_scale, output_scale, offset_x, offset_y
                });
                dst
            }

            "quick_multioctave_noise" => {
                let x = args.get("x").map(|e| self.compile(e)).unwrap_or(0);
                let y = args.get("y").map(|e| self.compile(e)).unwrap_or(1);
                let seed0 = args.get("seed0").map(|e| self.compile(e)).unwrap_or(3);
                let seed1 = self.compile_arg_or_default(args.get("seed1"), 0.0);
                let octaves = self.compile_arg_or_default(args.get("octaves"), 4.0);
                let input_scale = self.compile_arg_or_default(args.get("input_scale"), 1.0);
                let output_scale = self.compile_arg_or_default(args.get("output_scale"), 1.0);
                let offset_x = self.compile_arg_or_default(args.get("offset_x"), 0.0);
                let offset_y = self.compile_arg_or_default(args.get("offset_y"), 0.0);
                let octave_output_scale_multiplier = self.compile_arg_or_default(
                    args.get("octave_output_scale_multiplier"), 0.5
                );
                let octave_input_scale_multiplier = self.compile_arg_or_default(
                    args.get("octave_input_scale_multiplier"), 0.5
                );
                let octave_seed0_shift = self.compile_arg_or_default(
                    args.get("octave_seed0_shift"), 1.0
                );

                let dst = self.alloc_reg();
                self.ops.push(Op::QuickMultioctaveNoise {
                    dst, x, y, seed0, seed1, octaves, input_scale, output_scale, offset_x, offset_y,
                    octave_output_scale_multiplier, octave_input_scale_multiplier, octave_seed0_shift
                });
                dst
            }

            "variable_persistence_multioctave_noise" => {
                let x = args.get("x").map(|e| self.compile(e)).unwrap_or(0);
                let y = args.get("y").map(|e| self.compile(e)).unwrap_or(1);
                let seed0 = args.get("seed0").map(|e| self.compile(e)).unwrap_or(3);
                let seed1 = self.compile_arg_or_default(args.get("seed1"), 0.0);
                let octaves = self.compile_arg_or_default(args.get("octaves"), 4.0);
                let persistence = self.compile_arg_or_default(args.get("persistence"), 0.5);
                let input_scale = self.compile_arg_or_default(args.get("input_scale"), 1.0);
                let output_scale = self.compile_arg_or_default(args.get("output_scale"), 1.0);
                let offset_x = self.compile_arg_or_default(args.get("offset_x"), 0.0);
                let offset_y = self.compile_arg_or_default(args.get("offset_y"), 0.0);

                let dst = self.alloc_reg();
                self.ops.push(Op::VariablePersistenceMultioctaveNoise {
                    dst, x, y, seed0, seed1, octaves, persistence, input_scale, output_scale, offset_x, offset_y
                });
                dst
            }

            "distance_from_nearest_point" => {
                let x = args.get("x").map(|e| self.compile(e)).unwrap_or(0);
                let y = args.get("y").map(|e| self.compile(e)).unwrap_or(1);
                let max_distance = self.compile_arg_or_default(args.get("maximum_distance"), 1024.0);

                // Get the points from the points argument (typically an ExprRef to a point list)
                let points = match args.get("points") {
                    Some(Expr::ExprRef(name)) => {
                        self.point_lists.get(name).cloned().unwrap_or_else(|| vec![(0.0, 0.0)])
                    }
                    _ => vec![(0.0, 0.0)], // Default to origin
                };

                let dst = self.alloc_reg();
                self.ops.push(Op::DistanceFromNearestPoint { dst, x, y, max_distance, points });
                dst
            }

            "random_penalty" => {
                let x = args.get("x").map(|e| self.compile(e)).unwrap_or(0);
                let y = args.get("y").map(|e| self.compile(e)).unwrap_or(1);
                let seed = self.compile_arg_or_default(args.get("seed"), 0.0);
                let source = self.compile_arg_or_default(args.get("source"), 1.0);
                let amplitude = self.compile_arg_or_default(args.get("amplitude"), 1.0);

                let dst = self.alloc_reg();
                self.ops.push(Op::RandomPenalty { dst, x, y, seed, source, amplitude });
                dst
            }

            "spot_noise" => self.compile_spot_noise(args),

            // Check if it's a user-defined function
            _ => {
                if let Some(func) = self.func_defs.get(name).cloned() {
                    self.compile_user_function(&func, args)
                } else {
                    // Unknown function - return 0
                    let dst = self.alloc_reg();
                    self.ops.push(Op::LoadConst { dst, value: 0.0 });
                    dst
                }
            }
        }
    }

    fn compile_spot_noise(&mut self, args: &HashMap<String, Expr>) -> usize {
        let x = args.get("x").map(|e| self.compile(e)).unwrap_or(0);
        let y = args.get("y").map(|e| self.compile(e)).unwrap_or(1);
        let seed0 = args.get("seed0").map(|e| self.compile(e)).unwrap_or(3);
        let seed1 = self.compile_arg_or_default(args.get("seed1"), 0.0);
        let region_size = self.compile_arg_or_default(args.get("region_size"), 512.0);
        let density_expression = self.compile_arg_or_default(args.get("density_expression"), 0.001);
        let spot_quantity_expression = self.compile_arg_or_default(args.get("spot_quantity_expression"), 1.0);
        let spot_radius_expression = self.compile_arg_or_default(args.get("spot_radius_expression"), 32.0);
        let spot_favorability_expression = self.compile_arg_or_default(args.get("spot_favorability_expression"), 1.0);
        let basement_value = self.compile_arg_or_default(args.get("basement_value"), -1000.0);
        let candidate_spot_count = self.compile_arg_or_default(
            args.get("candidate_spot_count").or(args.get("candidate_point_count")),
            21.0
        );
        let suggested_minimum_candidate_point_spacing = self.compile_arg_or_default(
            args.get("suggested_minimum_candidate_point_spacing"),
            32.0
        );
        let skip_span = self.compile_arg_or_default(args.get("skip_span"), 1.0);
        let skip_offset = self.compile_arg_or_default(args.get("skip_offset"), 0.0);
        let hard_region_target_quantity = self.compile_arg_or_default(args.get("hard_region_target_quantity"), 0.0);
        let maximum_spot_basement_radius = self.compile_arg_or_default(args.get("maximum_spot_basement_radius"), 128.0);

        let dst = self.alloc_reg();
        self.ops.push(Op::SpotNoise {
            dst, x, y, seed0, seed1, region_size,
            density_expression, spot_quantity_expression, spot_radius_expression,
            spot_favorability_expression, basement_value, candidate_spot_count,
            suggested_minimum_candidate_point_spacing, skip_span, skip_offset,
            hard_region_target_quantity, maximum_spot_basement_radius,
        });
        dst
    }

    fn compile_positional_call(&mut self, name: &str, args: &[Expr]) -> usize {
        match name {
            "abs" => {
                let src = args.first().map(|e| self.compile(e)).unwrap_or(0);
                let dst = self.alloc_reg();
                self.ops.push(Op::Abs { dst, src });
                dst
            }
            "sqrt" => {
                let src = args.first().map(|e| self.compile(e)).unwrap_or(0);
                let dst = self.alloc_reg();
                self.ops.push(Op::Sqrt { dst, src });
                dst
            }
            "log2" => {
                let src = args.first().map(|e| self.compile(e)).unwrap_or(0);
                let dst = self.alloc_reg();
                self.ops.push(Op::Log2 { dst, src });
                dst
            }
            "sin" => {
                let src = args.first().map(|e| self.compile(e)).unwrap_or(0);
                let dst = self.alloc_reg();
                self.ops.push(Op::Sin { dst, src });
                dst
            }
            "cos" => {
                let src = args.first().map(|e| self.compile(e)).unwrap_or(0);
                let dst = self.alloc_reg();
                self.ops.push(Op::Cos { dst, src });
                dst
            }
            "floor" => {
                let src = args.first().map(|e| self.compile(e)).unwrap_or(0);
                let dst = self.alloc_reg();
                self.ops.push(Op::Floor { dst, src });
                dst
            }
            "ceil" => {
                let src = args.first().map(|e| self.compile(e)).unwrap_or(0);
                let dst = self.alloc_reg();
                self.ops.push(Op::Ceil { dst, src });
                dst
            }
            "clamp" => {
                let value = args.first().map(|e| self.compile(e)).unwrap_or(0);
                let min = args.get(1).map(|e| self.compile(e)).unwrap_or_else(|| {
                    let r = self.alloc_reg();
                    self.ops.push(Op::LoadConst { dst: r, value: 0.0 });
                    r
                });
                let max = args.get(2).map(|e| self.compile(e)).unwrap_or_else(|| {
                    let r = self.alloc_reg();
                    self.ops.push(Op::LoadConst { dst: r, value: 1.0 });
                    r
                });
                let dst = self.alloc_reg();
                self.ops.push(Op::Clamp { dst, value, min, max });
                dst
            }
            "lerp" => {
                let a = args.first().map(|e| self.compile(e)).unwrap_or(0);
                let b = args.get(1).map(|e| self.compile(e)).unwrap_or(0);
                let t = args.get(2).map(|e| self.compile(e)).unwrap_or(0);
                let dst = self.alloc_reg();
                self.ops.push(Op::Lerp { dst, a, b, t });
                dst
            }
            "min" => {
                if args.len() < 2 {
                    return args.first().map(|e| self.compile(e)).unwrap_or(0);
                }
                let mut result = self.compile(&args[0]);
                for arg in &args[1..] {
                    let b = self.compile(arg);
                    let dst = self.alloc_reg();
                    self.ops.push(Op::Min { dst, a: result, b });
                    result = dst;
                }
                result
            }
            "max" => {
                if args.len() < 2 {
                    return args.first().map(|e| self.compile(e)).unwrap_or(0);
                }
                let mut result = self.compile(&args[0]);
                for arg in &args[1..] {
                    let b = self.compile(arg);
                    let dst = self.alloc_reg();
                    self.ops.push(Op::Max { dst, a: result, b });
                    result = dst;
                }
                result
            }
            "var" => {
                // var('control:name') - return control variable value
                if let Some(Expr::StringLiteral(name)) = args.first() {
                    if let Some(&val) = self.control_defaults.get(name) {
                        let dst = self.alloc_reg();
                        self.ops.push(Op::LoadConst { dst, value: val });
                        dst
                    } else {
                        eprintln!("WARNING: Unknown control variable '{}', defaulting to 1.0", name);
                        let dst = self.alloc_reg();
                        self.ops.push(Op::LoadConst { dst, value: 1.0 });
                        dst
                    }
                } else {
                    // Fallback for non-string arguments
                    args.first().map(|e| self.compile(e)).unwrap_or_else(|| {
                        let dst = self.alloc_reg();
                        self.ops.push(Op::LoadConst { dst, value: 1.0 });
                        dst
                    })
                }
            }
            "if" => {
                let cond = args.first().map(|e| self.compile(e)).unwrap_or(0);
                let then_val = args.get(1).map(|e| self.compile(e)).unwrap_or(0);
                let else_val = args.get(2).map(|e| self.compile(e)).unwrap_or(0);
                let dst = self.alloc_reg();
                self.ops.push(Op::If { dst, cond, then_val, else_val });
                dst
            }
            "expression_in_range" => {
                // expression_in_range(slope, influence, var1, var2, from1, from2, to1, to2)
                let slope = self.compile_arg_or_default(args.first(), 20.0);
                let influence = self.compile_arg_or_default(args.get(1), 1.0);
                let var1 = args.get(2).map(|e| self.compile(e)).unwrap_or(0);
                let var2 = args.get(3).map(|e| self.compile(e)).unwrap_or(0);
                let from1 = self.compile_arg_or_default(args.get(4), -10.0);
                let from2 = self.compile_arg_or_default(args.get(5), -10.0);
                let to1 = self.compile_arg_or_default(args.get(6), 10.0);
                let to2 = self.compile_arg_or_default(args.get(7), 10.0);
                let dst = self.alloc_reg();
                self.ops.push(Op::ExpressionInRange {
                    dst, slope, influence, var1, var2, from1, from2, to1, to2
                });
                dst
            }
            // User-defined functions called with positional args
            _ => {
                if let Some(func) = self.func_defs.get(name).cloned() {
                    // Convert positional to named args
                    let mut named = HashMap::new();
                    for (i, param) in func.parameters.iter().enumerate() {
                        if let Some(arg) = args.get(i) {
                            named.insert(param.clone(), arg.clone());
                        }
                    }
                    self.compile_user_function(&func, &named)
                } else {
                    // Unknown function
                    let dst = self.alloc_reg();
                    self.ops.push(Op::LoadConst { dst, value: 0.0 });
                    dst
                }
            }
        }
    }

    fn compile_user_function(&mut self, func: &FunctionDef, args: &HashMap<String, Expr>) -> usize {
        // Save current state
        let saved_regs = self.named_regs.clone();
        let saved_expr_defs = self.expr_defs.clone();

        // Bind parameters (in defined order, which is deterministic)
        for param in &func.parameters {
            if let Some(arg) = args.get(param) {
                let reg = self.compile(arg);
                self.named_regs.insert(param.clone(), reg);
            }
        }

        // Add local_expressions to expr_defs so they can be resolved by ExprRef
        for (name, expr) in &func.local_expressions {
            self.expr_defs.insert(name.clone(), ExpressionDef {
                expression: expr.clone(),
                local_expressions: HashMap::new(),
            });
        }

        // Compile main expression (local_expressions will be compiled on-demand via ExprRef)
        let result = self.compile(&func.expression);

        // Restore state
        self.named_regs = saved_regs;
        self.expr_defs = saved_expr_defs;

        result
    }

    fn compile_arg_or_default(&mut self, arg: Option<&Expr>, default: f32) -> usize {
        if let Some(expr) = arg {
            self.compile(expr)
        } else {
            let r = self.alloc_reg();
            self.ops.push(Op::LoadConst { dst: r, value: default });
            r
        }
    }

    /// Build the final compiled program
    pub fn build(self, output_reg: usize) -> CompiledProgram {
        CompiledProgram {
            ops: self.ops,
            output_reg,
            num_registers: self.next_reg,
            seed_overrides: self.seed_overrides,
        }
    }
}

impl Default for Compiler {
    fn default() -> Self {
        Self::new()
    }
}
