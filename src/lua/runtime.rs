use std::path::{Path, PathBuf};
use mlua::{Lua, Result as LuaResult, Value, Table};

pub struct FactorioLua {
    lua: Lua,
    factorio_path: PathBuf,
}

impl FactorioLua {
    pub fn new(factorio_path: &Path) -> LuaResult<Self> {
        let lua = Lua::new();
        let factorio_path = factorio_path.to_path_buf();

        {
            let globals = lua.globals();

            // Set up path for require
            let core_path = factorio_path.join("core/lualib");
            let base_path = factorio_path.join("base/prototypes");

            let package: Table = globals.get("package")?;
            let path: String = package.get("path")?;
            let new_path = format!(
                "{};{}/?.lua;{}/?.lua",
                path,
                core_path.display(),
                base_path.display()
            );
            package.set("path", new_path)?;

            // Create serpent stub (used by dataloader for error messages)
            let serpent = lua.create_table()?;
            serpent.set("block", lua.create_function(|_, v: Value| {
                Ok(format!("{:?}", v))
            })?)?;
            globals.set("serpent", serpent)?;

            // Create mods table (include installed mod folders if present)
            let mods = lua.create_table()?;
            mods.set("base", "2.0.0")?;
            for mod_name in ["space-age", "elevated-rails"] {
                if factorio_path.join(mod_name).exists() {
                    mods.set(mod_name, "2.0.0")?;
                }
            }
            globals.set("mods", mods)?;

            // Create defines table with all 16 direction constants
            let defines = lua.create_table()?;
            let direction = lua.create_table()?;
            direction.set("north", 0)?;
            direction.set("northnortheast", 1)?;
            direction.set("northeast", 2)?;
            direction.set("eastnortheast", 3)?;
            direction.set("east", 4)?;
            direction.set("eastsoutheast", 5)?;
            direction.set("southeast", 6)?;
            direction.set("southsoutheast", 7)?;
            direction.set("south", 8)?;
            direction.set("southsouthwest", 9)?;
            direction.set("southwest", 10)?;
            direction.set("westsouthwest", 11)?;
            direction.set("west", 12)?;
            direction.set("westnorthwest", 13)?;
            direction.set("northwest", 14)?;
            direction.set("northnorthwest", 15)?;
            defines.set("direction", direction)?;
            globals.set("defines", defines)?;

            // Set up unit constants (used by item.lua for weight)
            globals.set("kg", 1000.0)?;
            globals.set("gram", 1.0)?;
            globals.set("meter", 1.0)?;
            globals.set("second", 1.0)?;
            globals.set("minute", 60.0)?;
            globals.set("hour", 3600.0)?;

            // Stub for sound volume_multiplier function
            globals.set("volume_multiplier", lua.create_function(|_, _: (String, f64)| {
                Ok(mlua::Value::Nil)
            })?)?;

            // Custom require that handles __base__ and __core__ prefixes
            let factorio_path_clone = factorio_path.clone();
            let custom_require = lua.create_function(move |lua, modname: String| {
                let globals = lua.globals();
                let loaded: Table = {
                    let package: Table = globals.get("package")?;
                    package.get("loaded")?
                };

                // Check if already loaded
                if let Ok(module) = loaded.get::<Value>(modname.as_str()) {
                    if module != Value::Nil {
                        return Ok(module);
                    }
                }

                // Resolve path
                let resolved = resolve_module_path(&factorio_path_clone, &modname);

                if let Some(path) = resolved {
                    if path.exists() {
                        let content = std::fs::read_to_string(&path)
                            .map_err(|e| mlua::Error::runtime(format!("Failed to read {}: {}", path.display(), e)))?;

                        let chunk = lua.load(&content)
                            .set_name(modname.as_str());

                        let result: Value = chunk.eval()?;

                        // Cache result
                        let cache_value = if result == Value::Nil {
                            Value::Boolean(true)
                        } else {
                            result.clone()
                        };
                        loaded.set(modname.as_str(), cache_value)?;

                        return Ok(result);
                    }
                }

                // Fall back to standard require behavior for not found
                Err(mlua::Error::runtime(format!("module '{}' not found", modname)))
            })?;
            globals.set("require", custom_require)?;
        }

        Ok(Self { lua, factorio_path })
    }

    pub fn load_dataloader(&self) -> LuaResult<()> {
        // Create a stub for the duplicate checker (dataloader requires it)
        self.lua.load(r#"
            local checker = {}
            function checker.check_for_duplicates(t, e) end
            function checker.check_for_overwrites(t, e) end
            package.loaded["data-duplicate-checker"] = checker
        "#).exec()?;

        // Now load dataloader
        let dataloader_path = self.factorio_path.join("core/lualib/dataloader.lua");
        let dataloader_content = std::fs::read_to_string(&dataloader_path)
            .map_err(|e| mlua::Error::runtime(format!("Failed to read dataloader.lua: {}", e)))?;
        self.lua.load(&dataloader_content).set_name("dataloader.lua").exec()?;

        Ok(())
    }

    pub fn load_util(&self) -> LuaResult<()> {
        let util_path = self.factorio_path.join("core/lualib/util.lua");
        let util_content = std::fs::read_to_string(&util_path)
            .map_err(|e| mlua::Error::runtime(format!("Failed to read util.lua: {}", e)))?;
        self.lua.load(&util_content).set_name("util.lua").exec()?;

        // Register util in package.loaded
        let globals = self.lua.globals();
        let util: Value = globals.get("util")?;
        let package: Table = globals.get("package")?;
        let loaded: Table = package.get("loaded")?;
        loaded.set("util", util)?;

        Ok(())
    }

    pub fn load_prototype_file(&self, relative_path: &str) -> LuaResult<()> {
        let full_path = self.factorio_path.join(relative_path);
        let content = std::fs::read_to_string(&full_path)
            .map_err(|e| mlua::Error::runtime(format!("Failed to read {}: {}", full_path.display(), e)))?;
        self.lua.load(&content).set_name(relative_path).exec()?;
        Ok(())
    }

    pub fn load_base_items(&self) -> LuaResult<()> {
        // First load dependencies that item.lua requires
        self.load_item_sounds()?;
        self.load_item_tints()?;
        self.load_factoriopedia_simulations()?;
        self.load_entity_sounds()?;

        self.load_prototype_file("base/prototypes/item.lua")
    }

    fn load_item_sounds(&self) -> LuaResult<()> {
        // Create a stub for item_sounds that returns empty tables
        self.lua.load(r#"
            local item_sounds = {
                brick_inventory_move = {},
                brick_inventory_pickup = {},
                wood_inventory_move = {},
                wood_inventory_pickup = {},
                resource_inventory_move = {},
                resource_inventory_pickup = {},
                metal_small_inventory_move = {},
                metal_small_inventory_pickup = {},
                metal_large_inventory_move = {},
                metal_large_inventory_pickup = {},
                wire_inventory_move = {},
                wire_inventory_pickup = {},
                fluid_inventory_move = {},
                fluid_inventory_pickup = {},
                inserter_inventory_move = {},
                inserter_inventory_pickup = {},
                mechanical_inventory_move = {},
                mechanical_inventory_pickup = {},
                rail_inventory_move = {},
                rail_inventory_pickup = {},
                gun_inventory_move = {},
                gun_inventory_pickup = {},
                ammo_inventory_move = {},
                ammo_inventory_pickup = {},
                armor_inventory_move = {},
                armor_inventory_pickup = {},
                science_inventory_move = {},
                science_inventory_pickup = {},
                robot_inventory_move = {},
                robot_inventory_pickup = {},
                electric_small_inventory_move = {},
                electric_small_inventory_pickup = {},
                electric_large_inventory_move = {},
                electric_large_inventory_pickup = {},
                logistics_inventory_move = {},
                logistics_inventory_pickup = {},
                nuclear_inventory_move = {},
                nuclear_inventory_pickup = {},
                module_inventory_move = {},
                module_inventory_pickup = {},
                coin_inventory_move = {},
                coin_inventory_pickup = {},
                capsule_inventory_move = {},
                capsule_inventory_pickup = {},
                default_inventory_move = {},
                default_inventory_pickup = {},
            }
            package.loaded["__base__.prototypes.item_sounds"] = item_sounds
        "#).exec()
    }

    fn load_item_tints(&self) -> LuaResult<()> {
        self.lua.load(r#"
            local item_tints = {
                yellowing_coal = {r=1, g=1, b=1},
            }
            package.loaded["__base__.prototypes.item-tints"] = item_tints
        "#).exec()
    }

    fn load_factoriopedia_simulations(&self) -> LuaResult<()> {
        self.lua.load(r#"
            local simulations = {}
            package.loaded["__base__.prototypes.factoriopedia-simulations"] = simulations
        "#).exec()
    }

    fn load_entity_sounds(&self) -> LuaResult<()> {
        self.lua.load(r#"
            local sounds = {}
            package.loaded["prototypes.entity.sounds"] = sounds
        "#).exec()
    }

    pub fn load_base_entities(&self) -> LuaResult<()> {
        self.lua.load(r#"
            package.loaded["circuit-connector-sprites"] = {}
            package.loaded["prototypes.entity.hit-effects"] = {}
            package.loaded["prototypes.entity.pipecovers"] = {}
            package.loaded["prototypes.entity.assemblerpipes"] = {}
            package.loaded["prototypes.entity.laser-sounds"] = {}
            package.loaded["prototypes.entity.character-animations"] = {}
            package.loaded["prototypes.entity.spidertron-animations"] = {}
            package.loaded["prototypes.entity.spawner-animation"] = {}
            package.loaded["prototypes.entity.biter-animations"] = {}
            package.loaded["prototypes.entity.spitter-animations"] = {}
            package.loaded["prototypes.entity.worm-animations"] = {}
            package.loaded["__base__/prototypes/planet/procession-graphic-catalogue-types"] = {}
            package.loaded["__base__/prototypes/planet/procession-audio-catalogue-types"] = {}
            package.loaded["__base__.prototypes.entity.cargo-pod-catalogue"] = {}
        "#).exec()?;

        let entity_files = [
            "base/prototypes/entity/entities.lua",
            "base/prototypes/entity/trees.lua",
            "base/prototypes/entity/resources.lua",
            "base/prototypes/entity/transport-belts.lua",
            "base/prototypes/entity/mining-drill.lua",
            "base/prototypes/entity/turrets.lua",
            "base/prototypes/entity/crash-site.lua",
            "base/prototypes/entity/enemies.lua",
            "base/prototypes/entity/trains.lua",
        ];
        for file in entity_files {
            if let Err(e) = self.load_prototype_file(file) {
                eprintln!("[lua] skipping {}: {}", file, e);
            }
        }
        Ok(())
    }

    pub fn load_base_recipes(&self) -> LuaResult<()> {
        self.load_prototype_file("base/prototypes/recipe.lua")
    }

    pub fn load_base_tiles(&self) -> LuaResult<()> {
        // Tiles have dependencies on sounds/graphics; stub the minimal helpers.
        self.lua.load(r#"
            if not sound_variations then
                function sound_variations(...) return {} end
            end
            if not default_tile_sounds_advanced_volume_control then
                function default_tile_sounds_advanced_volume_control() return {} end
            end
        "#).exec()?;

        self.load_prototype_file("base/prototypes/tile/tiles.lua")
    }

    pub fn data_raw(&self) -> LuaResult<Table> {
        let globals = self.lua.globals();
        let data: Table = globals.get("data")?;
        data.get("raw")
    }

    pub fn lua(&self) -> &Lua {
        &self.lua
    }
}

fn resolve_module_path(factorio_path: &Path, modname: &str) -> Option<PathBuf> {
    if let Some(stripped) = modname.strip_prefix("__") {
        if let Some(idx) = stripped.find("__/") {
            let mod_name = &stripped[..idx];
            let rel = &stripped[(idx + 3)..];
            return Some(factorio_path.join(mod_name).join(format!("{}.lua", rel)));
        }
        if let Some(idx) = stripped.find("__.") {
            let mod_name = &stripped[..idx];
            let rel = &stripped[(idx + 3)..].replace('.', "/");
            return Some(factorio_path.join(mod_name).join(format!("{}.lua", rel)));
        }
    }
    // Handle __base__ prefix
    if modname.starts_with("__base__.") {
        let relative = modname.strip_prefix("__base__.").unwrap().replace('.', "/");
        return Some(factorio_path.join("base").join(format!("{}.lua", relative)));
    }

    // Handle __core__ prefix
    if modname.starts_with("__core__.") {
        let relative = modname.strip_prefix("__core__.").unwrap().replace('.', "/");
        return Some(factorio_path.join("core").join(format!("{}.lua", relative)));
    }

    // Handle prototypes prefix (common in requires)
    if modname.starts_with("prototypes.") {
        let relative = modname.replace('.', "/");
        return Some(factorio_path.join("base").join(format!("{}.lua", relative)));
    }

    // Regular module - try core/lualib first
    let core_path = factorio_path.join("core/lualib").join(format!("{}.lua", modname));
    if core_path.exists() {
        return Some(core_path);
    }

    // Then try base/prototypes
    let base_path = factorio_path.join("base/prototypes").join(format!("{}.lua", modname));
    if base_path.exists() {
        return Some(base_path);
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_factorio_lua_new() {
        let factorio_path = Path::new("/Applications/factorio.app/Contents/data");
        if !factorio_path.exists() {
            eprintln!("Factorio not installed, skipping test");
            return;
        }

        let lua = FactorioLua::new(factorio_path).expect("Failed to create FactorioLua");
        lua.load_dataloader().expect("Failed to load dataloader");
        lua.load_util().expect("Failed to load util");

        // Verify data.raw exists - data_raw() returns Result<Table>,
        // if we got here it worked
        let _data_raw = lua.data_raw().expect("Failed to get data.raw");
    }
}
