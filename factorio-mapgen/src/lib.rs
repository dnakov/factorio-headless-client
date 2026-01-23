//! Terrain generation using Factorio's Lua noise expressions
//!
//! Architecture:
//! 1. `loader` - Loads Factorio's actual Lua noise expression files
//! 2. `expression` - Parses expression strings into AST
//! 3. `compiler` - Compiles AST to executable operations
//! 4. `executor` - Runs compiled programs on register-based caches
//! 5. `terrain` - High-level API using compiled programs

mod expression;
mod compiler;
mod executor;
mod loader;
pub mod terrain;
mod cache;
mod operations;
mod program;

pub use terrain::{TerrainGenerator, init_generator, generate_tile};
pub use loader::FactorioData;
pub use expression::{Expr, BinOp, UnaryOp, parse_expression};
pub use compiler::{Compiler, CompiledProgram, FunctionDef, Op, REG_AUX, REG_MOISTURE, REG_ELEVATION};
pub use executor::ExecContext;

pub use cache::{NoiseCache, CHUNK_SIZE, TILES_PER_CHUNK};

pub use operations::{NoiseOp, PerlinNoise};
pub use program::{NoiseProgram, TileDef, NoiseExprParams};
