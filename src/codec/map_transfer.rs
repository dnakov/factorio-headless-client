use std::io::{Read, Cursor};
use std::collections::HashMap;
use flate2::read::ZlibDecoder;

use crate::codec::BinaryReader;
use crate::error::{Error, Result};
use super::map_types::{SurfaceData, ChunkData};
use super::map_settings::skip_map_settings;
use factorio_mapgen::TerrainGenerator;

/// Map data decompressor and parser
pub struct MapTransfer {
    blocks: Vec<(u32, Vec<u8>)>,
    expected_size: u32,
}

impl MapTransfer {
    pub fn new(expected_size: u32) -> Self {
        Self {
            blocks: Vec::new(),
            expected_size,
        }
    }

    pub fn add_block(&mut self, block_number: u32, data: Vec<u8>) {
        self.blocks.push((block_number, data));
    }

    pub fn is_complete(&self) -> bool {
        let total_size: usize = self.blocks.iter().map(|(_, d)| d.len()).sum();
        total_size >= self.expected_size as usize
    }

    pub fn received_size(&self) -> usize {
        self.blocks.iter().map(|(_, d)| d.len()).sum()
    }

    pub fn finish(mut self) -> Vec<u8> {
        self.blocks.sort_by_key(|(n, _)| *n);
        self.blocks.into_iter().flat_map(|(_, data)| data).collect()
    }
}

/// Prototype ID mappings from level.dat0
#[derive(Debug, Clone, Default)]
pub struct PrototypeMappings {
    pub tables: HashMap<String, HashMap<u16, String>>,
    pub entity_groups: HashMap<u16, String>,
}

impl PrototypeMappings {
    pub fn entity_name(&self, id: u16) -> Option<&String> {
        self.tables.get("Entity")?.get(&id)
    }

    pub fn entity_group(&self, id: u16) -> Option<&String> {
        self.entity_groups.get(&id)
    }

    pub fn item_name(&self, id: u16) -> Option<&String> {
        self.tables.get("ItemPrototype")?.get(&id)
    }

    pub fn recipe_name(&self, id: u16) -> Option<&String> {
        self.tables.get("Recipe")?.get(&id)
    }

    pub fn tile_name(&self, id: u16) -> Option<&String> {
        self.tables.get("TilePrototype")?.get(&id)
    }

    pub fn tile_id_by_name(&self, name: &str) -> Option<u16> {
        self.tables
            .get("TilePrototype")?
            .iter()
            .find(|(_, tile_name)| tile_name.as_str() == name)
            .map(|(id, _)| *id)
    }

    pub fn character_speed(&self) -> f64 {
        0.15
    }
}

/// Parse map data from raw bytes
pub fn parse_map_data(data: &[u8]) -> Result<MapData> {
    if data.len() >= 4 && &data[0..4] == b"PK\x03\x04" {
        return parse_zip_map(data);
    }
    let decompressed = decompress_if_needed(data)?;
    MapData::parse(&decompressed)
}

/// Map deserializer state for delta-encoded positions
pub struct MapDeserializer<'a> {
    pub reader: BinaryReader<'a>,
    last_x: i32,
    last_y: i32,
}

impl<'a> MapDeserializer<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self {
            reader: BinaryReader::new(data),
            last_x: 0,
            last_y: 0,
        }
    }

    /// Read MapPosition using delta encoding (doc lines 1446-1503)
    pub fn read_map_position(&mut self) -> Result<(i32, i32)> {
        let dx = self.reader.read_i16_le()?;
        if dx == 0x7FFF {
            let x = self.reader.read_i32_le()?;
            let y = self.reader.read_i32_le()?;
            self.last_x = x;
            self.last_y = y;
            Ok((x, y))
        } else {
            let dy = self.reader.read_i16_le()?;
            let x = self.last_x.wrapping_add(dx as i32);
            let y = self.last_y.wrapping_add(dy as i32);
            self.last_x = x;
            self.last_y = y;
            Ok((x, y))
        }
    }

    /// Convert raw MapPosition to tiles (1/128 tile units)
    pub fn to_tiles(raw: (i32, i32)) -> (f64, f64) {
        (raw.0 as f64 / 128.0, raw.1 as f64 / 128.0)
    }

    pub fn reset_position(&mut self) {
        self.last_x = 0;
        self.last_y = 0;
    }
}

fn read_map_position_delta(reader: &mut BinaryReader, last: &mut (i32, i32)) -> Result<(i32, i32)> {
    let dx = reader.read_i16_le()?;
    if dx == 0x7FFF {
        let x = reader.read_i32_le()?;
        let y = reader.read_i32_le()?;
        *last = (x, y);
        Ok((x, y))
    } else {
        let dy = reader.read_i16_le()?;
        let x = last.0.wrapping_add(dx as i32);
        let y = last.1.wrapping_add(dy as i32);
        *last = (x, y);
        Ok((x, y))
    }
}

// ============================================================================
// FrequencySizeRichness (doc lines 969-975)
// ============================================================================
#[derive(Debug, Clone, Default)]
struct FrequencySizeRichness {
    frequency: f32,
    size: f32,
    richness: f32,
}

impl FrequencySizeRichness {
    fn read(reader: &mut BinaryReader) -> Result<Self> {
        Ok(Self {
            frequency: reader.read_f32_le()?,
            size: reader.read_f32_le()?,
            richness: reader.read_f32_le()?,
        })
    }
}

// ============================================================================
// AutoplaceSettings (doc lines 976-980)
// ============================================================================
#[derive(Debug, Clone, Default)]
struct AutoplaceSettings {
    treat_missing_as_default: bool,
    settings: HashMap<String, FrequencySizeRichness>,
}

impl AutoplaceSettings {
    fn read(reader: &mut BinaryReader) -> Result<Self> {
        let treat_missing_as_default = reader.read_bool()?;
        let count = reader.read_opt_u32()? as usize;
        let mut settings = HashMap::with_capacity(count);
        for _ in 0..count {
            let key = reader.read_string()?;
            let value = FrequencySizeRichness::read(reader)?;
            settings.insert(key, value);
        }
        Ok(Self { treat_missing_as_default, settings })
    }
}

// ============================================================================
// CliffPlacementSettings (doc lines 982-993)
// ============================================================================
#[derive(Debug, Clone, Default)]
struct CliffPlacementSettings {
    name: String,
    control: String,
    cliff_elevation_0: f32,
    cliff_elevation_interval: f32,
    cliff_smoothing: f32,
    richness: f32,
}

impl CliffPlacementSettings {
    fn read(reader: &mut BinaryReader) -> Result<Self> {
        Ok(Self {
            name: reader.read_string()?,
            control: reader.read_string()?,
            cliff_elevation_0: reader.read_f32_le()?,
            cliff_elevation_interval: reader.read_f32_le()?,
            cliff_smoothing: reader.read_f32_le()?,
            richness: reader.read_f32_le()?,
        })
    }
}

// ============================================================================
// TerritorySettings (doc lines 995-1002)
// ============================================================================
#[derive(Debug, Clone, Default)]
struct TerritorySettings {
    units: Vec<String>,
    territory_index_expression: String,
    territory_variation_expression: String,
    minimum_territory_size: u32,
}

impl TerritorySettings {
    fn read(reader: &mut BinaryReader) -> Result<Self> {
        let count = reader.read_opt_u32()? as usize;
        let mut units = Vec::with_capacity(count);
        if count == 0 {
            return Ok(Self {
                units,
                territory_index_expression: String::new(),
                territory_variation_expression: String::new(),
                minimum_territory_size: 0,
            });
        }

        for _ in 0..count {
            units.push(reader.read_string()?);
        }
        let territory_index_expression = reader.read_string()?;
        let territory_variation_expression = reader.read_string()?;
        let minimum_territory_size = reader.read_u32_le()?;

        Ok(Self {
            units,
            territory_index_expression,
            territory_variation_expression,
            minimum_territory_size,
        })
    }
}

// ============================================================================
// MapGenSettings (doc lines 907-929)
// ============================================================================
#[derive(Debug, Clone, Default)]
struct MapGenSettings {
    autoplace_controls: HashMap<String, FrequencySizeRichness>,
    autoplace_settings: HashMap<String, AutoplaceSettings>,
    default_enable_all_autoplace_controls: bool,
    seed: u32,
    width: u32,
    height: u32,
    unknown_0x78: u32,
    unknown_0x7c: u32,
    unknown_0x80: u32,
    unknown_0x84: u32,
    unknown_0x88: u16,
    unknown_0x8a: u16,
    starting_area: f32,
    peaceful_mode: bool,
    no_enemies_mode: bool,
    starting_points: Vec<(i32, i32)>,
    property_expression_names: HashMap<String, String>,
    cliff_settings: CliffPlacementSettings,
    territory_settings: TerritorySettings,
}

impl MapGenSettings {
    fn read(reader: &mut BinaryReader) -> Result<Self> {
        // 1) autoplace_controls (map<string, FrequencySizeRichness>)
        let count = reader.read_opt_u32()? as usize;
        #[cfg(test)]
        eprintln!("  MapGenSettings: autoplace_controls count={}, pos={}", count, reader.position());
        let mut autoplace_controls = HashMap::with_capacity(count);
        for _ in 0..count {
            let key = reader.read_string()?;
            let value = FrequencySizeRichness::read(reader)?;
            autoplace_controls.insert(key, value);
        }
        #[cfg(test)]
        eprintln!("  MapGenSettings: autoplace_controls done, pos={}", reader.position());

        // 2) autoplace_settings (map<string, AutoplaceSettings>)
        let count = reader.read_opt_u32()? as usize;
        #[cfg(test)]
        eprintln!("  MapGenSettings: autoplace_settings count={}, pos={}", count, reader.position());
        let mut autoplace_settings = HashMap::with_capacity(count);
        for _ in 0..count {
            let key = reader.read_string()?;
            let value = AutoplaceSettings::read(reader)?;
            autoplace_settings.insert(key, value);
        }
        #[cfg(test)]
        eprintln!("  MapGenSettings: autoplace_settings done, pos={}", reader.position());

        // 3) default_enable_all_autoplace_controls
        let default_enable_all_autoplace_controls = reader.read_bool()?;
        #[cfg(test)]
        eprintln!("  MapGenSettings: default_enable_all={}, pos={}", default_enable_all_autoplace_controls, reader.position());

        // 4-6) seed, width, height
        let seed = reader.read_u32_le()?;
        let width = reader.read_u32_le()?;
        let height = reader.read_u32_le()?;
        #[cfg(test)]
        eprintln!("  MapGenSettings: seed={}, w={}, h={}, pos={}", seed, width, height, reader.position());

        // 7-12) unknown fields (doc lines 917-922)
        let unknown_0x78 = reader.read_u32_le()?;
        let unknown_0x7c = reader.read_u32_le()?;
        let unknown_0x80 = reader.read_u32_le()?;
        let unknown_0x84 = reader.read_u32_le()?;
        let unknown_0x88 = reader.read_u16_le()?;
        let unknown_0x8a = reader.read_u16_le()?;
        // UNDOCUMENTED: Extra u32 between 0x8a and starting_area (Space Age 2.0)
        let _unknown_0x8c = reader.read_u32_le()?;
        #[cfg(test)]
        eprintln!("  MapGenSettings: unknowns done (incl 0x8c={}), pos={}, next bytes: {:02x?}",
            _unknown_0x8c, reader.position(), reader.remaining_slice().get(..16));

        // 13) starting_area (f32)
        let starting_area = reader.read_f32_le()?;
        #[cfg(test)]
        eprintln!("  MapGenSettings: starting_area={}, pos={}", starting_area, reader.position());

        // 14-15) peaceful_mode, no_enemies_mode
        let peaceful_mode = reader.read_bool()?;
        let no_enemies_mode = reader.read_bool()?;
        #[cfg(test)]
        eprintln!("  MapGenSettings: peaceful={}, no_enemies={}, pos={}", peaceful_mode, no_enemies_mode, reader.position());

        // 16) starting_points (vector<MapPosition>)
        let count = reader.read_opt_u32()? as usize;
        #[cfg(test)]
        eprintln!("  MapGenSettings: starting_points count={}, pos={}", count, reader.position());
        let mut starting_points = Vec::with_capacity(count);
        let mut last = (0, 0);
        for _ in 0..count {
            let (x, y) = read_map_position_delta(reader, &mut last)?;
            starting_points.push((x, y));
        }
        #[cfg(test)]
        eprintln!("  MapGenSettings: starting_points done, pos={}", reader.position());

        // 17) property_expression_names (map<string, string>)
        let count = reader.read_opt_u32()? as usize;
        #[cfg(test)]
        eprintln!("  MapGenSettings: property_expression_names count={}, pos={}", count, reader.position());
        let mut property_expression_names = HashMap::with_capacity(count);
        for i in 0..count {
            let key = reader.read_string().map_err(|e| {
                #[cfg(test)]
                eprintln!("  MapGenSettings: property_expression_names[{}] KEY failed at pos={}, bytes: {:02x?}",
                    i, reader.position(), reader.remaining_slice().get(..20));
                e
            })?;
            let value = reader.read_string().map_err(|e| {
                #[cfg(test)]
                eprintln!("  MapGenSettings: property_expression_names[{}] VALUE failed at pos={}, key={}",
                    i, reader.position(), key);
                e
            })?;
            property_expression_names.insert(key, value);
        }
        #[cfg(test)]
        eprintln!("  MapGenSettings: property_expression_names done, pos={}, next bytes: {:02x?}",
            reader.position(), reader.remaining_slice().get(..20));

        // 18) cliff_settings
        let cliff_settings = CliffPlacementSettings::read(reader)?;
        #[cfg(test)]
        eprintln!("  MapGenSettings: cliff_settings done (name='{}'), pos={}",
            cliff_settings.name, reader.position());

        // 19) territory_settings
        let territory_settings = TerritorySettings::read(reader)?;
        #[cfg(test)]
        eprintln!("  MapGenSettings: territory_settings done, pos={}", reader.position());

        Ok(Self {
            autoplace_controls,
            autoplace_settings,
            default_enable_all_autoplace_controls,
            seed,
            width,
            height,
            unknown_0x78,
            unknown_0x7c,
            unknown_0x80,
            unknown_0x84,
            unknown_0x88,
            unknown_0x8a,
            starting_area,
            peaceful_mode,
            no_enemies_mode,
            starting_points,
            property_expression_names,
            cliff_settings,
            territory_settings,
        })
    }
}

// ============================================================================
// PropertyTree (doc lines 859-896)
// ============================================================================
fn skip_property_tree(reader: &mut BinaryReader) -> Result<()> {
    let pos_before = reader.position();
    let type_byte = reader.read_u8()?;
    let _flag = reader.read_u8()?;
    let type_code = type_byte & 0x1f;

    #[cfg(test)]
    if type_code > 7 {
        eprintln!("  PropertyTree: invalid type_code={} at pos={}, type_byte={:#04x}, next bytes: {:02x?}",
            type_code, pos_before, type_byte, reader.remaining_slice().get(..20));
    }

    match type_code {
        0 => {} // None
        1 => { reader.skip(1)?; } // bool
        2 => { reader.skip(8)?; } // double
        3 => {
            // String
            let is_null = reader.read_u8()?;
            if is_null == 0 {
                let len = reader.read_opt_u32()? as usize;
                reader.skip(len)?;
            }
        }
        4 | 5 => {
            // Dictionary
            let count = reader.read_u32_le()? as usize;
            for _ in 0..count {
                let is_null = reader.read_u8()?;
                if is_null == 0 {
                    let len = reader.read_opt_u32()? as usize;
                    reader.skip(len)?;
                }
                skip_property_tree(reader)?;
            }
        }
        6 | 7 => { reader.skip(8)?; }
        _ => return Err(Error::InvalidPacket(format!("unknown PropertyTree type: {}", type_code))),
    }
    Ok(())
}

fn skip_noise_program(reader: &mut BinaryReader) -> Result<()> {
    reader.read_u32_le()?; // field_0x00
    let op_count = reader.read_opt_u32()? as usize;
    if op_count != 0 {
        return Err(Error::InvalidPacket(
            "NoiseOperation parsing not implemented".into(),
        ));
    }
    Ok(())
}

fn skip_compiled_autoplacer_vector(reader: &mut BinaryReader) -> Result<()> {
    let count = reader.read_opt_u32()? as usize;
    for _ in 0..count {
        reader.read_u16_le()?; // prototype id
        reader.read_u32_le()?; // field_0x08
        reader.read_u32_le()?; // field_0x0c
    }
    Ok(())
}

fn skip_compiled_map_gen_settings(reader: &mut BinaryReader) -> Result<()> {
    let _map_gen = MapGenSettings::read(reader)?;
    skip_noise_program(reader)?;
    skip_noise_program(reader)?;
    reader.read_u32_le()?; // field_0x170
    skip_compiled_autoplacer_vector(reader)?;
    skip_compiled_autoplacer_vector(reader)?;
    skip_compiled_autoplacer_vector(reader)?;
    Ok(())
}

// ============================================================================
// Prototype table order (Space Age 2.0) (doc lines 1556-1601)
// ============================================================================
#[derive(Clone, Copy)]
enum IdSize {
    U8,
    U16,
    U32,
}

struct PrototypeTableSpec {
    name: &'static str,
    id_size: IdSize,
    gated_v2: bool,
    legacy_only: bool,
    discard: bool,
    tag_names: &'static [&'static str],
}

const PROTOTYPE_TABLES: &[PrototypeTableSpec] = &[
    PrototypeTableSpec { name: "CustomInput", id_size: IdSize::U16, gated_v2: false, legacy_only: false, discard: false, tag_names: &["custom-input"] },
    PrototypeTableSpec { name: "EquipmentGrid", id_size: IdSize::U8, gated_v2: false, legacy_only: false, discard: false, tag_names: &["equipment-grid"] },
    PrototypeTableSpec { name: "ItemPrototype", id_size: IdSize::U16, gated_v2: false, legacy_only: false, discard: false, tag_names: &["item", "item-prototype"] },
    PrototypeTableSpec { name: "CollisionLayer", id_size: IdSize::U16, gated_v2: true, legacy_only: false, discard: false, tag_names: &["collision-layer"] },
    PrototypeTableSpec { name: "AirbornePollutant", id_size: IdSize::U16, gated_v2: true, legacy_only: false, discard: false, tag_names: &["airborne-pollutant"] },
    PrototypeTableSpec { name: "TilePrototype", id_size: IdSize::U16, gated_v2: false, legacy_only: false, discard: false, tag_names: &["tile", "tile-prototype"] },
    PrototypeTableSpec { name: "Decorative", id_size: IdSize::U16, gated_v2: false, legacy_only: false, discard: false, tag_names: &["decorative", "optimized-decorative"] },
    PrototypeTableSpec { name: "Technology", id_size: IdSize::U16, gated_v2: false, legacy_only: false, discard: false, tag_names: &["technology"] },
    PrototypeTableSpec { name: "Entity", id_size: IdSize::U16, gated_v2: false, legacy_only: false, discard: false, tag_names: &["entity"] },
    PrototypeTableSpec { name: "Particle", id_size: IdSize::U16, gated_v2: false, legacy_only: false, discard: false, tag_names: &["particle", "optimized-particle"] },
    PrototypeTableSpec { name: "RecipeCategory", id_size: IdSize::U16, gated_v2: false, legacy_only: false, discard: false, tag_names: &["recipe-category"] },
    PrototypeTableSpec { name: "ItemSubGroup", id_size: IdSize::U16, gated_v2: false, legacy_only: false, discard: false, tag_names: &["item-subgroup"] },
    PrototypeTableSpec { name: "ItemGroup", id_size: IdSize::U8, gated_v2: false, legacy_only: false, discard: false, tag_names: &["item-group"] },
    PrototypeTableSpec { name: "Fluid", id_size: IdSize::U16, gated_v2: false, legacy_only: false, discard: false, tag_names: &["fluid"] },
    PrototypeTableSpec { name: "VirtualSignal", id_size: IdSize::U16, gated_v2: false, legacy_only: false, discard: false, tag_names: &["virtual-signal"] },
    PrototypeTableSpec { name: "AmmoCategory", id_size: IdSize::U8, gated_v2: false, legacy_only: false, discard: false, tag_names: &["ammo-category", "ammo"] },
    PrototypeTableSpec { name: "FuelCategory", id_size: IdSize::U8, gated_v2: false, legacy_only: false, discard: false, tag_names: &["fuel-category"] },
    PrototypeTableSpec { name: "ResourceCategory", id_size: IdSize::U8, gated_v2: false, legacy_only: false, discard: false, tag_names: &["resource-category"] },
    PrototypeTableSpec { name: "Equipment", id_size: IdSize::U16, gated_v2: false, legacy_only: false, discard: false, tag_names: &["equipment"] },
    PrototypeTableSpec { name: "_legacy_discard", id_size: IdSize::U16, gated_v2: false, legacy_only: true, discard: true, tag_names: &[] },
    PrototypeTableSpec { name: "NamedNoiseFunction", id_size: IdSize::U32, gated_v2: false, legacy_only: false, discard: true, tag_names: &["named-noise-function", "noise-function"] },
    PrototypeTableSpec { name: "NamedNoiseExpression", id_size: IdSize::U32, gated_v2: false, legacy_only: false, discard: true, tag_names: &["named-noise-expression", "noise-expression"] },
    PrototypeTableSpec { name: "AutoplaceControl", id_size: IdSize::U8, gated_v2: false, legacy_only: false, discard: false, tag_names: &["autoplace-control"] },
    PrototypeTableSpec { name: "DamageType", id_size: IdSize::U8, gated_v2: false, legacy_only: false, discard: false, tag_names: &["damage-type"] },
    PrototypeTableSpec { name: "Recipe", id_size: IdSize::U16, gated_v2: false, legacy_only: false, discard: false, tag_names: &["recipe"] },
    PrototypeTableSpec { name: "Achievement", id_size: IdSize::U16, gated_v2: false, legacy_only: false, discard: false, tag_names: &[] },
    PrototypeTableSpec { name: "ModuleCategory", id_size: IdSize::U8, gated_v2: false, legacy_only: false, discard: false, tag_names: &["module-category"] },
    PrototypeTableSpec { name: "EquipmentCategory", id_size: IdSize::U8, gated_v2: false, legacy_only: false, discard: false, tag_names: &["equipment-category"] },
    PrototypeTableSpec { name: "ModSetting", id_size: IdSize::U16, gated_v2: false, legacy_only: false, discard: false, tag_names: &["mod-setting"] },
    PrototypeTableSpec { name: "TrivialSmoke", id_size: IdSize::U8, gated_v2: false, legacy_only: false, discard: false, tag_names: &["trivial-smoke"] },
    PrototypeTableSpec { name: "AsteroidChunk", id_size: IdSize::U16, gated_v2: true, legacy_only: false, discard: false, tag_names: &["asteroid-chunk"] },
    PrototypeTableSpec { name: "Quality", id_size: IdSize::U8, gated_v2: false, legacy_only: false, discard: false, tag_names: &["quality"] },
    PrototypeTableSpec { name: "SurfaceProperty", id_size: IdSize::U16, gated_v2: true, legacy_only: false, discard: false, tag_names: &["surface-property"] },
    PrototypeTableSpec { name: "ProcessionLayerInheritanceGroup", id_size: IdSize::U8, gated_v2: true, legacy_only: false, discard: false, tag_names: &["procession-layer-inheritance-group"] },
    PrototypeTableSpec { name: "ProcessionPrototype", id_size: IdSize::U16, gated_v2: true, legacy_only: false, discard: false, tag_names: &["procession-prototype", "procession"] },
    PrototypeTableSpec { name: "SpaceLocation", id_size: IdSize::U16, gated_v2: false, legacy_only: false, discard: false, tag_names: &[] },
    PrototypeTableSpec { name: "SpaceConnection", id_size: IdSize::U16, gated_v2: false, legacy_only: false, discard: false, tag_names: &[] },
    PrototypeTableSpec { name: "ActiveTrigger", id_size: IdSize::U16, gated_v2: false, legacy_only: false, discard: false, tag_names: &["active-trigger"] },
    PrototypeTableSpec { name: "Shortcut", id_size: IdSize::U16, gated_v2: false, legacy_only: false, discard: false, tag_names: &["shortcut"] },
    PrototypeTableSpec { name: "BurnerUsage", id_size: IdSize::U16, gated_v2: true, legacy_only: false, discard: false, tag_names: &["burner-usage"] },
    PrototypeTableSpec { name: "SurfacePrototype", id_size: IdSize::U16, gated_v2: true, legacy_only: false, discard: false, tag_names: &["surface-prototype"] },
    PrototypeTableSpec { name: "ModData", id_size: IdSize::U32, gated_v2: true, legacy_only: false, discard: true, tag_names: &["mod-data"] },
    PrototypeTableSpec { name: "CustomEvent", id_size: IdSize::U16, gated_v2: true, legacy_only: false, discard: false, tag_names: &["custom-event"] },
];

/// Parse all prototype ID mapping tables (doc lines 1538-1601).
/// The loader reads a count (width matches ID size) then `string + id` pairs.
fn parse_all_prototype_mappings(reader: &mut BinaryReader, version: &MapVersion) -> Result<PrototypeMappings> {
    let mut mappings = PrototypeMappings::default();
    let debug = std::env::var("FACTORIO_DEBUG").is_ok();
    let mut consumed_space_connection = false;
    let mut consumed_active_trigger = false;

    fn read_grouped_table(
        reader: &mut BinaryReader,
        table_name: &str,
        store_groups: bool,
        mappings: &mut PrototypeMappings,
    ) -> Result<HashMap<u16, String>> {
        let group_count = reader.read_u16_le()? as usize;
        let debug = std::env::var("FACTORIO_DEBUG").is_ok();
        if debug {
            eprintln!(
                "[DEBUG] Prototype table {} grouped count={} pos={}",
                table_name,
                group_count,
                reader.position()
            );
        }
        let mut table = HashMap::new();
        for group_idx in 0..group_count {
            let group_name = reader.read_string().map_err(|e| {
                Error::InvalidPacket(format!(
                    "prototype {} group[{}] name: {}",
                    table_name, group_idx, e
                ))
            })?;
            let entry_count = reader.read_u16_le()? as usize;
            for entry_idx in 0..entry_count {
                let name = reader.read_string().map_err(|e| {
                    Error::InvalidPacket(format!(
                        "prototype {} group[{}] entry[{}] name: {}",
                        table_name, group_idx, entry_idx, e
                    ))
                })?;
                let id = reader.read_u16_le()?;
                table.insert(id, name);
                if store_groups {
                    mappings.entity_groups.insert(id, group_name.clone());
                }
            }
        }
        Ok(table)
    }

    for spec in PROTOTYPE_TABLES {
        if spec.gated_v2 && version.major < 2 {
            continue;
        }
        if spec.legacy_only && version.major >= 2 {
            continue;
        }

        maybe_skip_prototype_tag(reader, spec)?;

        let grouped = matches!(spec.name, "ItemPrototype" | "Entity" | "Equipment" | "ModSetting");
        if grouped && version.major >= 2 {
            if spec.discard {
                let group_count = reader.read_u16_le()? as usize;
                if debug {
                    eprintln!(
                        "[DEBUG] Prototype table {} grouped count={} pos={}",
                        spec.name,
                        group_count,
                        reader.position()
                    );
                }
                for _ in 0..group_count {
                    let _ = reader.read_string()?;
                    let entry_count = reader.read_u16_le()? as usize;
                    for _ in 0..entry_count {
                        let _ = reader.read_string()?;
                        let _ = reader.read_u16_le()?;
                    }
                }
                continue;
            }

            let table = read_grouped_table(
                reader,
                spec.name,
                spec.name == "Entity",
                &mut mappings,
            )?;
            mappings.tables.insert(spec.name.to_string(), table);
            continue;
        }

        if spec.name == "Achievement" {
            let module_tags = PROTOTYPE_TABLES
                .iter()
                .find(|t| t.name == "ModuleCategory")
                .map(|t| (t.tag_names, t.id_size))
                .unwrap_or((&[][..], IdSize::U8));
            let mut table = HashMap::new();
            while !module_tags.0.is_empty()
                && !has_tag_prefix(reader.remaining_slice(), module_tags.0, module_tags.1)
            {
                let name = reader.read_string().map_err(|e| {
                    Error::InvalidPacket(format!(
                        "prototype {} string: {}",
                        spec.name, e
                    ))
                })?;
                let id = reader.read_u16_le().map_err(|e| {
                    Error::InvalidPacket(format!(
                        "prototype {} id: {}",
                        spec.name, e
                    ))
                })?;
                table.insert(id, name);
            }
            mappings.tables.insert(spec.name.to_string(), table);
            continue;
        }

        if spec.name == "SpaceLocation" {
            let shortcut_tags = PROTOTYPE_TABLES
                .iter()
                .find(|t| t.name == "Shortcut")
                .map(|t| (t.tag_names, t.id_size))
                .unwrap_or((&[][..], IdSize::U16));

            let location_table = read_grouped_table(reader, "SpaceLocation", false, &mut mappings)?;

            let mut connection_table = HashMap::new();
            if !shortcut_tags.0.is_empty()
                && has_tag_prefix(reader.remaining_slice(), &["space-connection"], IdSize::U16)
            {
                // SpaceConnection is tagged in Space Age 2.0.
                maybe_skip_prototype_tag(reader, &PrototypeTableSpec {
                    name: "SpaceConnection",
                    id_size: IdSize::U16,
                    gated_v2: false,
                    legacy_only: false,
                    discard: false,
                    tag_names: &["space-connection"],
                })?;

                let count = reader.read_u16_le()? as usize;
                for idx in 0..count {
                    let name = reader.read_string().map_err(|e| {
                        Error::InvalidPacket(format!(
                            "prototype SpaceConnection[{}] string: {}",
                            idx, e
                        ))
                    })?;
                    let id = reader.read_u16_le().map_err(|e| {
                        Error::InvalidPacket(format!(
                            "prototype SpaceConnection[{}] id: {}",
                            idx, e
                        ))
                    })?;
                    connection_table.insert(id, name);
                }
            }

            if debug {
                eprintln!(
                    "[DEBUG] Prototype table SpaceLocation count={} pos={}",
                    location_table.len(),
                    reader.position()
                );
                eprintln!(
                    "[DEBUG] Prototype table SpaceConnection count={} pos={}",
                    connection_table.len(),
                    reader.position()
                );
            }
            mappings.tables.insert("SpaceLocation".to_string(), location_table);
            mappings.tables.insert("SpaceConnection".to_string(), connection_table);
            consumed_space_connection = true;
            continue;
        }
        if spec.name == "SpaceConnection" && consumed_space_connection {
            continue;
        }
        if spec.name == "ActiveTrigger" && consumed_active_trigger {
            continue;
        }
        if spec.name == "ActiveTrigger" && version.major >= 2 {
            let table = read_grouped_table(reader, "ActiveTrigger", false, &mut mappings)?;
            mappings.tables.insert("ActiveTrigger".to_string(), table);
            continue;
        }
        if (spec.name == "ModData" || spec.name == "CustomEvent")
            && !spec.tag_names.is_empty()
            && !has_tag_prefix(reader.remaining_slice(), spec.tag_names, spec.id_size)
        {
            continue;
        }

        let count = match spec.id_size {
            IdSize::U8 => reader.read_u8()? as u32,
            IdSize::U16 => reader.read_u16_le()? as u32,
            IdSize::U32 => reader.read_u32_le()?,
        };
        if debug {
            eprintln!(
                "[DEBUG] Prototype table {} count={} pos={}",
                spec.name,
                count,
                reader.position()
            );
        }

        if spec.discard {
            for _ in 0..count {
                let _ = reader.read_string().map_err(|e| {
                    Error::InvalidPacket(format!(
                        "prototype {} discard string: {}",
                        spec.name, e
                    ))
                })?;
                match spec.id_size {
                    IdSize::U8 => { let _ = reader.read_u8()?; }
                    IdSize::U16 => { let _ = reader.read_u16_le()?; }
                    IdSize::U32 => { let _ = reader.read_u32_le()?; }
                }
            }
            continue;
        }

        let mut table = HashMap::with_capacity(count as usize);
        for idx in 0..count {
            let name = reader.read_string().map_err(|e| {
                Error::InvalidPacket(format!(
                    "prototype {}[{}] string: {}",
                    spec.name, idx, e
                ))
            })?;
            let id = match spec.id_size {
                IdSize::U8 => reader.read_u8()? as u16,
                IdSize::U16 => reader.read_u16_le()?,
                IdSize::U32 => reader.read_u32_le()? as u16,
            };
            table.insert(id, name);
        }
        mappings.tables.insert(spec.name.to_string(), table);
    }

    Ok(mappings)
}

fn maybe_skip_prototype_tag(reader: &mut BinaryReader, spec: &PrototypeTableSpec) -> Result<()> {
    if spec.tag_names.is_empty() {
        return Ok(());
    }
    let remaining = reader.remaining_slice();
    if peek_tag_len(remaining, spec.tag_names, spec.id_size).is_some() {
        reader.read_string()?;
        return Ok(());
    }
    if remaining.first() == Some(&1) {
        if peek_tag_len(&remaining[1..], spec.tag_names, spec.id_size).is_some() {
            reader.read_u8()?;
            reader.read_string()?;
            return Ok(());
        }
        if remaining.get(1) == Some(&0) {
            if peek_tag_len(&remaining[2..], spec.tag_names, spec.id_size).is_some() {
                reader.read_u8()?;
                reader.read_u8()?;
                reader.read_string()?;
                return Ok(());
            }
            if remaining.get(2) == Some(&0) && remaining.get(3) == Some(&0) {
                if peek_tag_len(&remaining[4..], spec.tag_names, spec.id_size).is_some() {
                    reader.read_u8()?;
                    reader.read_u8()?;
                    reader.read_u8()?;
                    reader.read_u8()?;
                    reader.read_string()?;
                    return Ok(());
                }
            }
        }
    }
    if remaining.len() >= 3
        && remaining[0] == 0x02
        && remaining[1] == 0x01
        && remaining[2] == 0x00
        && peek_tag_len(&remaining[3..], spec.tag_names, spec.id_size).is_some()
    {
        reader.read_u8()?;
        reader.read_u8()?;
        reader.read_u8()?;
        reader.read_string()?;
        return Ok(());
    }
    Ok(())
}

fn has_tag_prefix(slice: &[u8], tags: &[&str], id_size: IdSize) -> bool {
    if peek_tag_len(slice, tags, id_size).is_some() {
        return true;
    }
    if slice.first() == Some(&1) {
        if peek_tag_len(&slice[1..], tags, id_size).is_some() {
            return true;
        }
        if slice.get(1) == Some(&0) {
            if peek_tag_len(&slice[2..], tags, id_size).is_some() {
                return true;
            }
            if slice.get(2) == Some(&0) && slice.get(3) == Some(&0) {
                if peek_tag_len(&slice[4..], tags, id_size).is_some() {
                    return true;
                }
            }
        }
    }
    if slice.len() >= 3
        && slice[0] == 0x02
        && slice[1] == 0x01
        && slice[2] == 0x00
        && peek_tag_len(&slice[3..], tags, id_size).is_some()
    {
        return true;
    }
    false
}

fn peek_tag_len(slice: &[u8], tags: &[&str], id_size: IdSize) -> Option<usize> {
    let (tag, consumed) = peek_string(slice)?;
    if !tags.iter().any(|t| *t == tag) {
        return None;
    }
    let count_offset = consumed;
    let count = read_count_from_slice(slice.get(count_offset.. )?, id_size)?;
    if count > 100_000 {
        return None;
    }
    Some(consumed)
}

fn read_count_from_slice(slice: &[u8], id_size: IdSize) -> Option<u32> {
    match id_size {
        IdSize::U8 => slice.get(0).copied().map(|v| v as u32),
        IdSize::U16 => {
            if slice.len() < 2 { return None; }
            Some(u16::from_le_bytes([slice[0], slice[1]]) as u32)
        }
        IdSize::U32 => {
            if slice.len() < 4 { return None; }
            Some(u32::from_le_bytes([slice[0], slice[1], slice[2], slice[3]]))
        }
    }
}

fn peek_string(slice: &[u8]) -> Option<(String, usize)> {
    let (len, consumed) = peek_opt_u32(slice)?;
    let len = len as usize;
    if len > 1024 * 1024 {
        return None;
    }
    let start = consumed;
    let end = start + len;
    let bytes = slice.get(start..end)?;
    let s = std::str::from_utf8(bytes).ok()?.to_string();
    Some((s, end))
}

fn peek_opt_u32(slice: &[u8]) -> Option<(u32, usize)> {
    let first = *slice.first()?;
    if first == 0xFF {
        if slice.len() < 5 {
            return None;
        }
        let v = u32::from_le_bytes([slice[1], slice[2], slice[3], slice[4]]);
        return Some((v, 5));
    }
    Some((first as u32, 1))
}


/// Skip prototype migration list (doc line 1413)
fn skip_prototype_migration_list(reader: &mut BinaryReader) -> Result<()> {
    // Space Age 2.0 observed layout:
    // string mod_name, u64 version/seed?, opt_u32 file_count, then pairs of strings.
    let debug = std::env::var("FACTORIO_DEBUG").is_ok();
    if debug {
        eprintln!("[DEBUG] migration list start pos={}", reader.position());
    }
    let _mod_name = reader.read_string()?;
    let _version = reader.read_u64_le()?;
    let count = reader.read_opt_u32()? as usize;
    if debug {
        eprintln!("[DEBUG] migration list count={} pos={}", count, reader.position());
    }
    for _ in 0..count {
        let _ = reader.read_string()?;
        let _ = reader.read_string()?;
    }
    if debug {
        eprintln!("[DEBUG] migration list end pos={}", reader.position());
    }
    Ok(())
}

fn skip_map_version_gated_block(reader: &mut BinaryReader, version: &MapVersion) -> Result<()> {
    // Map::loadData version-gated block right before MapModSettings.
    // For Space Age 2.0.x, the binary reads the full sequence below.
    if version.major < 2 {
        return Ok(());
    }

    // u32 map+0x80, u32 map+0x8c, u32 map+0x90, u32 map+0x94
    reader.read_u32_le()?;
    reader.read_u32_le()?;
    reader.read_u32_le()?;
    reader.read_u32_le()?;

    // u64 map+0x1d0, u64 map+0x1d8
    reader.read_u64_le()?;
    reader.read_u64_le()?;

    // u32 map+0x1e0, map+0x1e4, map+0x1ec, map+0x1f0, map+0x1f4
    reader.read_u32_le()?;
    reader.read_u32_le()?;
    reader.read_u32_le()?;
    reader.read_u32_le()?;
    reader.read_u32_le()?;

    // u64 map+0x1f8
    reader.read_u64_le()?;
    Ok(())
}

fn skip_map_mod_settings(reader: &mut BinaryReader) -> Result<()> {
    // MapModSettings::loadMapDeserialiser -> ModSettingsTemplate<scope 2> then <scope 3>
    // Each scope writes a u32 count then entries:
    // u8 setting_type, u16 prototype_id, then type-specific payload.
    fn skip_scope(reader: &mut BinaryReader, scope: u8) -> Result<bool> {
        let count = reader.read_u32_le()? as usize;
        let debug = std::env::var("FACTORIO_DEBUG").is_ok();
        if debug {
            eprintln!("[DEBUG] MapModSettings scope={} count={} pos={}", scope, count, reader.position());
        }
        for _ in 0..count {
            let setting_type = reader.read_u8()?;
            let _prototype_id = reader.read_u16_le()?;
            match setting_type {
                0 => {
                    // None/empty type - no payload
                }
                1 => {
                    reader.read_bool()?;
                }
                2 => {
                    reader.read_f64_le()?;
                }
                3 => {
                    let _ = reader.read_u64_le()?;
                }
                4 => {
                    let _ = reader.read_string()?;
                }
                5 => {
                    reader.read_f32_le()?;
                    reader.read_f32_le()?;
                    reader.read_f32_le()?;
                    reader.read_f32_le()?;
                }
                _ => {
                    return Err(Error::InvalidPacket(format!(
                        "MapModSettings scope={} invalid setting_type={}",
                        scope, setting_type
                    )));
                }
            }
        }
        Ok(true)
    }

    skip_scope(reader, 2)?;
    skip_scope(reader, 3)?;
    Ok(())
}

fn map_version_gt(version: &MapVersion, major: u16, minor: u16, patch: u16, build: u16) -> bool {
    (version.major, version.minor, version.patch, version.build) > (major, minor, patch, build)
}

fn skip_targeter_vector(reader: &mut BinaryReader) -> Result<()> {
    let count = reader.read_opt_u32()? as usize;
    for _ in 0..count {
        reader.read_u8()?;
    }
    Ok(())
}

fn skip_collision_mask(reader: &mut BinaryReader) -> Result<()> {
    let first = reader.read_u8()?;
    if first == 0xff {
        reader.read_u32_le()?; // collision mask index
        let layer_count = reader.read_opt_u32()? as usize;
        for _ in 0..layer_count {
            reader.read_u16_le()?; // CollisionLayerID
        }
    }
    Ok(())
}

fn skip_path(reader: &mut BinaryReader, version: &MapVersion) -> Result<()> {
    reader.read_u32_le()?; // field_0x00
    reader.read_u32_le()?; // field_0x04
    reader.read_u8()?; // field_0x08
    reader.read_u64_le()?; // field_0x10

    if map_version_gt(version, 1, 2, 1, 0x4b) {
        reader.read_u8()?; // field_0x18
    }

    if map_version_gt(version, 1, 2, 9, 0x0f) {
        skip_collision_mask(reader)?;
        reader.read_u8()?; // collision mask high bits
    }

    let waypoint_count = reader.read_u32_le()? as usize;
    let mut last = (0, 0);
    for _ in 0..waypoint_count {
        read_map_position_delta(reader, &mut last)?;
        reader.read_u8()?; // waypoint flag
    }

    reader.read_u32_le()?; // trailing field
    Ok(())
}

fn skip_path_cache(reader: &mut BinaryReader) -> Result<()> {
    let record_count = reader.read_u32_le()? as usize;
    for _ in 0..record_count {
        reader.read_u32_le()?; // record_field
        reader.read_u32_le()?; // path_index_plus_one
    }

    let map_entry_count = reader.read_u32_le()? as usize;
    for _ in 0..map_entry_count {
        reader.read_i32_le()?; // key_x
        reader.read_i32_le()?; // key_y
        let list_count = reader.read_u32_le()? as usize;
        for _ in 0..list_count {
            reader.read_u32_le()?; // path index
        }
    }

    let secondary_map_entry_count = reader.read_u32_le()? as usize;
    for _ in 0..secondary_map_entry_count {
        reader.read_i32_le()?; // key_x
        reader.read_i32_le()?; // key_y
        let list_count = reader.read_u32_le()? as usize;
        for _ in 0..list_count {
            reader.read_u32_le()?; // path index
        }
    }

    Ok(())
}

fn skip_pathfind_client_vector(reader: &mut BinaryReader) -> Result<()> {
    let count = reader.read_opt_u32()? as usize;
    for _ in 0..count {
        reader.read_u8()?; // TargetDeserialiser::loadTargeter (has_target)
    }
    Ok(())
}

fn skip_pathfind_client_cache_record_vector(reader: &mut BinaryReader) -> Result<()> {
    let count = reader.read_opt_u32()? as usize;
    for _ in 0..count {
        skip_map_position_delta(reader)?; // start position
        skip_map_position_delta(reader)?; // goal position
        reader.read_u8()?; // TargetDeserialiser::loadTargeter (has_target)
    }
    Ok(())
}

fn skip_optional_bounding_box(reader: &mut BinaryReader) -> Result<()> {
    let has_value = reader.read_u8()?;
    if has_value != 0 {
        skip_map_position_delta(reader)?;
        skip_map_position_delta(reader)?;
        reader.read_u16_le()?;
        reader.read_u16_le()?;
    }
    Ok(())
}

fn skip_chunk_position_map_tick(reader: &mut BinaryReader, version: &MapVersion) -> Result<()> {
    let count = reader.read_opt_u32()? as usize;
    for _ in 0..count {
        reader.read_i32_le()?; // chunk_x
        reader.read_i32_le()?; // chunk_y
        if map_version_gt(version, 1, 2, 1, 0x175) {
            reader.read_u64_le()?; // MapTick
        } else {
            reader.read_u32_le()?; // MapTick legacy
        }
    }
    Ok(())
}

fn skip_chunk_position_double_map(reader: &mut BinaryReader, count: u32) -> Result<()> {
    for _ in 0..(count as usize) {
        reader.read_i32_le()?; // chunk_x
        reader.read_i32_le()?; // chunk_y
        reader.read_f64_le()?; // value
    }
    Ok(())
}

fn skip_expansion_planner(reader: &mut BinaryReader, version: &MapVersion) -> Result<()> {
    if map_version_gt(version, 1, 2, 1, 0x175) {
        reader.read_u64_le()?;
    } else {
        reader.read_u32_le()?;
    }

    let count = reader.read_opt_u32()?;
    skip_chunk_position_double_map(reader, count)?;

    let has_bounds = reader.read_u8()?;
    if map_version_gt(version, 1, 2, 0x1e, 0x04) {
        skip_collision_mask(reader)?;
        reader.read_u8()?; // collision mask high bits
    }

    if has_bounds != 0 {
        reader.read_u32_le()?;
        reader.read_u32_le()?;
    }

    Ok(())
}

fn skip_path_find_subject(reader: &mut BinaryReader, version: &MapVersion) -> Result<()> {
    skip_map_position_delta(reader)?; // start
    skip_map_position_delta(reader)?; // goal
    reader.read_u16_le()?; // field_0x10
    reader.read_u16_le()?; // field_0x12
    skip_optional_bounding_box(reader)?;
    skip_collision_mask(reader)?;
    reader.read_u8()?; // field_0x37
    skip_map_position_delta(reader)?; // field_0x38
    reader.read_u8()?; // field_0x40
    skip_map_position_delta(reader)?; // field_0x44
    reader.read_u8()?; // TargetDeserialiser::loadTargeter (has_target)
    reader.read_u8()?; // field_0x70
    reader.read_u32_le()?; // field_0x74

    if map_version_gt(version, 1, 2, 0, 0x14b) {
        reader.read_u8()?; // field_0x78
    }
    if map_version_gt(version, 1, 2, 0, 0x1a4) {
        reader.read_u64_le()?; // field_0x80
    }

    Ok(())
}

fn skip_path_task_description(reader: &mut BinaryReader, version: &MapVersion) -> Result<()> {
    skip_map_position_delta(reader)?;
    skip_map_position_delta(reader)?;
    reader.read_f64_le()?;
    reader.read_f64_le()?;
    skip_path_find_subject(reader, version)?;
    reader.read_u32_le()?;
    Ok(())
}

fn skip_path_find_base_node(reader: &mut BinaryReader, version: &MapVersion) -> Result<()> {
    skip_map_position_delta(reader)?;
    reader.read_u8()?;
    reader.read_bytes(0x12)?;
    if map_version_gt(version, 1, 2, 0, 0x14c) {
        reader.read_u8()?;
    }
    reader.read_f64_le()?;
    Ok(())
}

fn skip_path_find_search_data(reader: &mut BinaryReader, version: &MapVersion) -> Result<()> {
    reader.read_u8()?;
    let count = reader.read_u32_le()? as usize;
    for _ in 0..count {
        skip_path_find_base_node(reader, version)?;
    }
    Ok(())
}

fn skip_path_find_algorithm(reader: &mut BinaryReader, version: &MapVersion) -> Result<()> {
    skip_path_task_description(reader, version)?;

    if !map_version_gt(version, 1, 2, 0, 0x139) {
        reader.read_f64_le()?;
    }

    reader.read_u32_le()?;
    reader.read_u32_le()?;
    reader.read_u32_le()?;

    if map_version_gt(version, 1, 2, 0, 0x175) {
        reader.read_u64_le()?;
    } else {
        reader.read_u32_le()?;
    }

    skip_path_find_search_data(reader, version)?;
    skip_path_find_search_data(reader, version)?;

    if map_version_gt(version, 1, 1, 0, 0x1c) {
        skip_map_position_delta(reader)?;
    }

    Ok(())
}

fn skip_extend_path_task(reader: &mut BinaryReader, version: &MapVersion) -> Result<()> {
    skip_path_find_algorithm(reader, version)?;
    skip_path_find_algorithm(reader, version)?;
    skip_path_find_algorithm(reader, version)?;

    reader.read_u32_le()?; // loadCurrent selector
    reader.read_u32_le()?;
    reader.read_u32_le()?;
    reader.read_u32_le()?;
    reader.read_u32_le()?;
    reader.read_u32_le()?;
    reader.read_u8()?; // field_0x62c

    if map_version_gt(version, 1, 2, 0, 0x175) {
        reader.read_u64_le()?;
    } else {
        reader.read_u32_le()?;
    }

    Ok(())
}

fn skip_path_find_task(reader: &mut BinaryReader, version: &MapVersion) -> Result<()> {
    let tag = reader.read_u8()?;
    match tag {
        0 => {
            skip_path_find_algorithm(reader, version)?;
            reader.read_u8()?; // field_0x210
            reader.read_u32_le()?; // field_0x214
        }
        1 => {
            skip_extend_path_task(reader, version)?;
        }
        2 => {
            skip_path_task_description(reader, version)?;
            reader.read_u8()?; // field_0xc8
            reader.read_u8()?; // TargetDeserialiser::loadTargeter (has_target)
            let has_task = reader.read_u8()?;
            if has_task != 0 {
                skip_path_find_task(reader, version)?;
            }
        }
        _ => {
            return Err(Error::InvalidPacket(format!(
                "PathFindTask unknown tag {}",
                tag
            )));
        }
    }
    Ok(())
}

fn skip_pathfind_client(reader: &mut BinaryReader, version: &MapVersion) -> Result<()> {
    reader.read_u8()?; // TargetDeserialiser::loadTargetable (has_target)
    let has_task = reader.read_u8()?;
    if has_task != 0 {
        skip_path_find_task(reader, version)?;
    }
    reader.read_u32_le()?; // field_0x40
    Ok(())
}

fn skip_script_pathfind_client_vector(reader: &mut BinaryReader, version: &MapVersion) -> Result<()> {
    let count = reader.read_opt_u32()? as usize;
    for _ in 0..count {
        skip_pathfind_client(reader, version)?;
        reader.read_u32_le()?; // ScriptPathFindClient::field_0x50
        reader.read_u8()?; // ScriptPathFindClient::field_0x54
    }
    Ok(())
}

fn skip_pathfinder(reader: &mut BinaryReader, version: &MapVersion) -> Result<()> {
    reader.read_u32_le()?; // field_0x18
    reader.read_u32_le()?; // field_0x1c
    reader.read_u32_le()?; // field_0x??
    reader.read_u32_le()?; // field_0x??

    let path_count = reader.read_u32_le()? as usize;
    for _ in 0..path_count {
        let has_path = reader.read_u8()?;
        if has_path != 0 {
            skip_path(reader, version)?;
        }
    }

    skip_path_cache(reader)?;
    skip_path_cache(reader)?;
    skip_path_cache(reader)?;

    for _ in 0..7 {
        skip_pathfind_client_vector(reader)?;
    }

    skip_pathfind_client_cache_record_vector(reader)?;
    skip_script_pathfind_client_vector(reader, version)?;

    let chunk_set_count = reader.read_opt_u32()? as usize;
    let mut last = (0, 0);
    for _ in 0..chunk_set_count {
        read_map_position_delta(reader, &mut last)?;
    }

    Ok(())
}

fn skip_active_entities(reader: &mut BinaryReader) -> Result<()> {
    let count = reader.read_opt_u32()? as usize;
    for _ in 0..count {
        reader.read_u8()?;
    }
    Ok(())
}

fn skip_active_entities_list(reader: &mut BinaryReader) -> Result<()> {
    let count = reader.read_u32_le()? as usize;
    for _ in 0..count {
        reader.read_u8()?; // RawPointerToTargetable::save
    }
    Ok(())
}

fn read_surface_index(reader: &mut BinaryReader) -> Result<u32> {
    let first = reader.read_u8()?;
    if first != 0xff {
        return Ok(first as u32);
    }
    reader.read_u32_le()
}

#[derive(Debug, Clone)]
struct SurfacePrelude {
    index: u32,
    chunks: Vec<ChunkPrelude>,
}

#[derive(Debug, Clone)]
struct ChunkPrelude {
    position: (i32, i32),
    status: u8,
}

fn parse_surface_preludes(reader: &mut BinaryReader, version: &MapVersion) -> Result<Vec<SurfacePrelude>> {
    let debug = std::env::var("FACTORIO_DEBUG").is_ok();
    let surface_count = reader.read_u32_le()? as usize;
    if debug {
        eprintln!("[DEBUG] parse_surface_preludes: surface_count={}", surface_count);
    }
    let mut surfaces = Vec::with_capacity(surface_count);

    for _ in 0..surface_count {
        let index = read_surface_index(reader)?;

        // ActiveEntitiesList::save
        if map_version_gt(version, 1, 1, 0, 0x28) {
            skip_active_entities_list(reader)?;
        }

        let chunk_count = reader.read_u32_le()? as usize;
        let mut chunks = Vec::with_capacity(chunk_count);
        for _ in 0..chunk_count {
            let x = reader.read_i32_le()?;
            let y = reader.read_i32_le()?;
            let status = reader.read_u8()?;
            if !map_version_gt(version, 1, 2, 0, 0xfd) {
                let active_list_count = reader.read_u8()? as usize;
                for _ in 0..active_list_count {
                    skip_active_entities(reader)?;
                }
            }
            chunks.push(ChunkPrelude {
                position: (x, y),
                status,
            });
        }

        if debug {
            eprintln!("[DEBUG] Surface {}: index={} chunk_count={}", surfaces.len(), index, chunks.len());
            if !chunks.is_empty() {
                let xs: Vec<i32> = chunks.iter().map(|c| c.position.0).collect();
                let ys: Vec<i32> = chunks.iter().map(|c| c.position.1).collect();
                eprintln!("[DEBUG]   Chunk range: x={}..{}, y={}..{}",
                    xs.iter().min().unwrap(), xs.iter().max().unwrap(),
                    ys.iter().min().unwrap(), ys.iter().max().unwrap());
            }
        }
        surfaces.push(SurfacePrelude { index, chunks });
    }

    Ok(surfaces)
}


fn skip_logistic_supply(reader: &mut BinaryReader) -> Result<()> {
    let count = reader.read_u16_le()? as usize;
    if count == 0 {
        return Ok(());
    }
    Err(Error::InvalidPacket(format!(
        "LogisticSupply parsing not implemented (count={})",
        count
    )))
}

fn skip_orbital_logistics(reader: &mut BinaryReader, version: &MapVersion) -> Result<()> {
    // Space Age 2.0: several Targeter vectors followed by LogisticSupply.
    skip_targeter_vector(reader)?; // Targeter<SpacePlatform>

    if map_version_gt(version, 2, 0, 0, 0x10) {
        skip_targeter_vector(reader)?; // Targeter<RocketSilo>
    }

    if map_version_gt(version, 2, 0, 0, 0x87) {
        skip_targeter_vector(reader)?; // Targeter<CargoLandingPad>
    }

    if map_version_gt(version, 2, 0, 0, 0x2a) {
        reader.read_u32_le()?; // unknown u32 at OrbitalLogistics+0xe8
    }

    skip_logistic_supply(reader)?;

    if map_version_gt(version, 2, 0, 0, 0xac) {
        return Err(Error::InvalidPacket(
            "OrbitalLogistics parsing not implemented for >= 2.0.0.172".into(),
        ));
    }

    Ok(())
}

fn skip_planets(reader: &mut BinaryReader, version: &MapVersion) -> Result<()> {
    // Planet::save writes a u16 count of non-null planets.
    let count = reader.read_u16_le()? as usize;
    let debug = std::env::var("FACTORIO_DEBUG").is_ok();
    if debug {
        eprintln!("[DEBUG] Planets count={} pos={}", count, reader.position());
    }

    for _ in 0..count {
        let _planet_proto_id = reader.read_u16_le()?;
        let _planet_index = reader.read_opt_u32()?;

        let logistics_count = reader.read_opt_u32()? as usize;
        for _ in 0..logistics_count {
            reader.read_u8()?; // ForceID
            skip_orbital_logistics(reader, version)?;
        }

        if map_version_gt(version, 2, 0, 0, 0xe1) {
            if map_version_gt(version, 2, 0, 0, 0x175) {
                reader.read_u64_le()?;
            } else {
                let _legacy = reader.read_u32_le()?;
            }
        }
    }

    Ok(())
}

fn skip_train_manager(reader: &mut BinaryReader) -> Result<()> {
    let _field_a = reader.read_u32_le()?;
    let _field_b = reader.read_u32_le()?;

    let rail_segment_count = reader.read_u32_le()? as usize;
    if rail_segment_count != 0 {
        return Err(Error::InvalidPacket(format!(
            "TrainManager rail segments not implemented (count={})",
            rail_segment_count
        )));
    }

    let train_count = reader.read_u32_le()? as usize;
    if train_count != 0 {
        return Err(Error::InvalidPacket(format!(
            "TrainManager trains not implemented (count={})",
            train_count
        )));
    }

    let _flag = reader.read_u8()?;

    let stop_ref_count = reader.read_u32_le()? as usize;
    for _ in 0..stop_ref_count {
        reader.read_u64_le()?;
    }

    let station_count = reader.read_u32_le()? as usize;
    for _ in 0..station_count {
        let _name = reader.read_string()?;
        let stop_count = reader.read_u32_le()? as usize;
        for _ in 0..stop_count {
            reader.read_u64_le()?;
        }
    }

    Ok(())
}

fn skip_map_generation_request(reader: &mut BinaryReader) -> Result<()> {
    reader.read_u32_le()?; // chunk_x
    reader.read_u32_le()?; // chunk_y
    reader.read_u8()?; // generation status
    Ok(())
}

fn skip_map_generation_request_in_progress(reader: &mut BinaryReader) -> Result<()> {
    skip_map_generation_request(reader)?;
    reader.read_u64_le()?; // field_0x10
    Ok(())
}

fn skip_map_generation_request_deque(reader: &mut BinaryReader, in_progress: bool) -> Result<()> {
    let count = reader.read_opt_u32()? as usize;
    for _ in 0..count {
        if in_progress {
            skip_map_generation_request_in_progress(reader)?;
        } else {
            skip_map_generation_request(reader)?;
        }
    }
    Ok(())
}

fn skip_map_generation_manager(reader: &mut BinaryReader) -> Result<()> {
    // MapGenerationManager::save (Space Age 2.0)
    skip_map_generation_request_deque(reader, false)?; // pending_basic_tiles
    skip_map_generation_request_deque(reader, false)?; // pending_basic_entities
    skip_map_generation_request_deque(reader, false)?; // pending_partial_entities
    skip_map_generation_request_deque(reader, false)?; // pending_full_entities

    // Optional set<MapGenerationRequest> is omitted in multiplayer map streams.
    // If encountered, this will desync; add detection once MapDeserialiser flags are known.

    skip_map_generation_request_deque(reader, true)?; // in_progress
    reader.read_u64_le()?; // field_0x218
    reader.read_u8()?; // field_0x220
    reader.read_u8()?; // field_0x222
    Ok(())
}

fn skip_planned_entity_updates(reader: &mut BinaryReader) -> Result<()> {
    let count = reader.read_opt_u32()? as usize;
    for _ in 0..count {
        skip_active_entities(reader)?;
    }
    Ok(())
}

fn skip_force_manager(reader: &mut BinaryReader) -> Result<u32> {
    let force_count = reader.read_u32_le()? as usize;
    for _ in 0..force_count {
        // ForceData::save (partial)
        let _force_id = reader.read_u8()?;
        let _force_name = reader.read_string()?;
        reader.read_u8()?; // field_0x28
        reader.read_u8()?; // field_0x29
        reader.read_u8()?; // field_0x2a
        skip_surface_evolution_map(reader)?;
        skip_recipes(reader)?;
        skip_technologies(reader)?;
        reader.read_u8()?; // field_0x58
        skip_research_manager(reader)?;

        let logistic_mgr_count = reader.read_u32_le()? as usize;
        for _ in 0..logistic_mgr_count {
            let has_value = reader.read_u8()?;
            if has_value != 0 {
                return Err(Error::InvalidPacket(
                    "LogisticManager parsing not implemented".into(),
                ));
            }
        }

        let construction_mgr_count = reader.read_u32_le()? as usize;
        for _ in 0..construction_mgr_count {
            let has_value = reader.read_u8()?;
            if has_value != 0 {
                return Err(Error::InvalidPacket(
                    "ConstructionManager parsing not implemented".into(),
                ));
            }
        }

        let space_platform_count = reader.read_opt_u32()? as usize;
        for _ in 0..space_platform_count {
            let has_value = reader.read_u8()?;
            if has_value != 0 {
                return Err(Error::InvalidPacket(
                    "SpacePlatform parsing not implemented".into(),
                ));
            }
        }

        let platform_delete_count = reader.read_opt_u32()? as usize;
        if platform_delete_count != 0 {
            return Err(Error::InvalidPacket(
                "SpacePlatformToBeDeleted parsing not implemented".into(),
            ));
        }

        reader.read_u32_le()?; // field_0x130

        let chart_tag_map_count = reader.read_opt_u32()? as usize;
        for _ in 0..chart_tag_map_count {
            let _surface_index = reader.read_opt_u32()?;
            let tag_count = reader.read_opt_u32()? as usize;
            if tag_count != 0 {
                return Err(Error::InvalidPacket(
                    "CustomChartTag parsing not implemented".into(),
                ));
            }
        }

        skip_f64_vector(reader)?;
        skip_f64_vector(reader)?;
        skip_f64_vector(reader)?;
        skip_u8_vector(reader)?;
        reader.read_u64_le()?; // field_0x228
        reader.read_u64_le()?; // field_0x240
        reader.read_u64_le()?; // field_0x258
        reader.read_u64_le()?; // field_0x270
        reader.read_u64_le()?; // field_0x288
        reader.read_u64_le()?; // field_0x2a0
        reader.read_u64_le()?; // field_0x2b8
    }

    let bitset_count = reader.read_u32_le()? as usize;
    for _ in 0..bitset_count {
        reader.read_u8()?;
        reader.read_u8()?;
    }

    Ok(force_count as u32)
}

fn skip_force_linked_inventories(reader: &mut BinaryReader, force_count: u32) -> Result<()> {
    for _ in 0..(force_count as usize) {
        // map<ID<EntityPrototype>, unique_ptr<LinkedInventory>>
        let inv_map_count = reader.read_opt_u32()? as usize;
        for _ in 0..inv_map_count {
            reader.read_u16_le()?; // entity prototype id
            let has_value = reader.read_u8()?;
            if has_value != 0 {
                return Err(Error::InvalidPacket(
                    "LinkedInventory payload parsing not implemented".into(),
                ));
            }
        }

        // map<ID<EntityPrototype>, map<u32, unique_ptr<LinkedInventory>>>
        let outer_count = reader.read_opt_u32()? as usize;
        for _ in 0..outer_count {
            reader.read_u16_le()?; // entity prototype id
            let inner_count = reader.read_opt_u32()? as usize;
            for _ in 0..inner_count {
                reader.read_u32_le()?; // key
                let has_value = reader.read_u8()?;
                if has_value != 0 {
                    return Err(Error::InvalidPacket(
                        "LinkedInventory payload parsing not implemented".into(),
                    ));
                }
            }
        }

        // set<pair<ID<EntityPrototype>, u32>>
        let set_count = reader.read_opt_u32()? as usize;
        for _ in 0..set_count {
            reader.read_u16_le()?;
            reader.read_u32_le()?;
        }
    }

    Ok(())
}

fn skip_surface_evolution_map(reader: &mut BinaryReader) -> Result<()> {
    let count = reader.read_opt_u32()? as usize;
    for _ in 0..count {
        let _surface_index = reader.read_opt_u32()?;
        skip_evolution_factors(reader)?;
    }
    Ok(())
}

fn skip_evolution_factors(reader: &mut BinaryReader) -> Result<()> {
    reader.read_f64_le()?; // evolution_factor
    reader.read_f64_le()?; // increased_by_pollution
    reader.read_f64_le()?; // increased_by_time
    reader.read_f64_le()?; // increased_by_killing_spawners
    Ok(())
}

fn skip_recipes(reader: &mut BinaryReader) -> Result<()> {
    let count = reader.read_u16_le()? as usize;
    reader.skip(count * 6)?;
    Ok(())
}

fn skip_technologies(reader: &mut BinaryReader) -> Result<()> {
    let count = reader.read_u16_le()? as usize;
    for _ in 0..count {
        reader.read_u8()?; // field_0x21
        reader.read_u8()?; // field_0x22
        let _ = reader.read_opt_u32()?; // field_0x24
        let has_progress = reader.read_u8()?;
        if has_progress != 0 {
            return Err(Error::InvalidPacket(
                "TechnologyProgress parsing not implemented".into(),
            ));
        }
    }
    Ok(())
}

fn skip_research_manager(reader: &mut BinaryReader) -> Result<()> {
    reader.read_u64_le()?; // field_0x50
    reader.read_u64_le()?; // field_0x58
    reader.read_u8()?;     // field_0x78
    reader.read_u16_le()?; // current tech id
    reader.read_u64_le()?; // field_0x68
    reader.read_u16_le()?; // previous tech id
    Ok(())
}

fn skip_f64_vector(reader: &mut BinaryReader) -> Result<()> {
    let count = reader.read_opt_u32()? as usize;
    reader.skip(count * 8)?;
    Ok(())
}

fn skip_u8_vector(reader: &mut BinaryReader) -> Result<()> {
    let count = reader.read_opt_u32()? as usize;
    reader.skip(count)?;
    Ok(())
}

fn skip_control_behavior_manager(reader: &mut BinaryReader) -> Result<()> {
    let count_a = reader.read_opt_u32()? as usize;
    for _ in 0..count_a {
        reader.read_u8()?;
    }
    let count_b = reader.read_opt_u32()? as usize;
    for _ in 0..count_b {
        reader.read_u8()?;
    }
    Ok(())
}

fn skip_circuit_network_manager(reader: &mut BinaryReader) -> Result<()> {
    reader.read_u32_le()?;
    let count = reader.read_u32_le()? as usize;
    if count != 0 {
        return Err(Error::InvalidPacket(format!(
            "CircuitNetwork parsing not implemented (count={})",
            count
        )));
    }
    Ok(())
}

fn skip_item_spoil_queue(reader: &mut BinaryReader) -> Result<()> {
    let queue_count = reader.read_u8()? as usize;
    for _ in 0..queue_count {
        let entry_count = reader.read_u32_le()? as usize;
        for _ in 0..entry_count {
            reader.read_u8()?; // TargeterBase::save
            reader.read_u64_le()?;
        }
    }
    Ok(())
}

fn skip_script_area_position_registry(reader: &mut BinaryReader) -> Result<()> {
    let count = reader.read_u32_le()? as usize;
    for _ in 0..count {
        reader.read_u8()?; // TargeterBase::save
        let _ = reader.read_string()?;
    }
    Ok(())
}

fn skip_script_areas_positions(reader: &mut BinaryReader) -> Result<()> {
    skip_script_area_position_registry(reader)?;
    reader.read_u32_le()?;
    reader.read_u32_le()?;

    let rect_map_count = reader.read_opt_u32()? as usize;
    for _ in 0..rect_map_count {
        let _surface_index = reader.read_opt_u32()?;
        let rect_count = reader.read_opt_u32()? as usize;
        for _ in 0..rect_count {
            let has_value = reader.read_u8()?;
            if has_value != 0 {
                return Err(Error::InvalidPacket(
                    "ScriptRectangle payload parsing not implemented".into(),
                ));
            }
        }
    }

    let pos_map_count = reader.read_opt_u32()? as usize;
    for _ in 0..pos_map_count {
        let _surface_index = reader.read_opt_u32()?;
        let pos_count = reader.read_opt_u32()? as usize;
        for _ in 0..pos_count {
            let has_value = reader.read_u8()?;
            if has_value != 0 {
                return Err(Error::InvalidPacket(
                    "ScriptPosition payload parsing not implemented".into(),
                ));
            }
        }
    }

    Ok(())
}

fn skip_object_destroyed_hooks(reader: &mut BinaryReader) -> Result<()> {
    reader.read_u64_le()?;
    let map_count = reader.read_opt_u32()? as usize;
    if map_count != 0 {
        return Err(Error::InvalidPacket(format!(
            "ObjectDestroyedHooks map parsing not implemented (count={})",
            map_count
        )));
    }
    let list_count = reader.read_u32_le()? as usize;
    for _ in 0..list_count {
        reader.read_u8()?; // TargeterBase::save
        reader.read_u64_le()?;
        reader.read_u8()?;
        reader.read_u64_le()?;
    }
    Ok(())
}

fn skip_script_rendering(reader: &mut BinaryReader) -> Result<()> {
    reader.read_u64_le()?;
    let object_map_count = reader.read_opt_u32()? as usize;
    if object_map_count != 0 {
        return Err(Error::InvalidPacket(format!(
            "ScriptRendering object map parsing not implemented (count={})",
            object_map_count
        )));
    }
    loop {
        let id = reader.read_u64_le()?;
        if id == 0 {
            break;
        }
    }
    loop {
        let id = reader.read_u64_le()?;
        if id == 0 {
            break;
        }
    }
    let flagged_count = reader.read_opt_u32()? as usize;
    for _ in 0..flagged_count {
        reader.read_u64_le()?;
    }
    reader.read_u32_le()?;
    let per_mod_count = reader.read_opt_u32()? as usize;
    if per_mod_count != 0 {
        return Err(Error::InvalidPacket(format!(
            "ScriptRendering per-mod list parsing not implemented (count={})",
            per_mod_count
        )));
    }
    Ok(())
}

fn skip_electric_network_manager(reader: &mut BinaryReader) -> Result<()> {
    let _next_sub_network_index = reader.read_u32_le()?;
    let primary_network_count = reader.read_u32_le()?;
    if primary_network_count != 0 {
        return Err(Error::InvalidPacket("ElectricNetworkManager parsing not implemented for non-zero networks".into()));
    }
    Ok(())
}

fn skip_map_position_delta(reader: &mut BinaryReader) -> Result<()> {
    let dx = reader.read_i16_le()?;
    if dx == 0x7fff {
        let _x = reader.read_i32_le()?;
        let _y = reader.read_i32_le()?;
    } else {
        let _dy = reader.read_i16_le()?;
    }
    Ok(())
}

fn skip_fluid(reader: &mut BinaryReader) -> Result<()> {
    let _prototype_id = reader.read_u16_le()?;
    let _amount = reader.read_f64_le()?;
    let _temperature = reader.read_f32_le()?;
    Ok(())
}

fn skip_fluid_buffer(reader: &mut BinaryReader) -> Result<()> {
    skip_fluid(reader)?;
    let _u64_field = reader.read_u64_le()?;
    let _u16_field = reader.read_u16_le()?;
    let _u32_field_0 = reader.read_u32_le()?;
    let _u32_field_1 = reader.read_u32_le()?;
    let _u32_field_2 = reader.read_u32_le()?;
    Ok(())
}

fn skip_fluid_segment(reader: &mut BinaryReader) -> Result<()> {
    let _segment_id = reader.read_u32_le()?;
    skip_fluid_buffer(reader)?;
    skip_map_position_delta(reader)?;
    skip_map_position_delta(reader)?;
    let _field_0x68 = reader.read_u16_le()?;
    let _field_0x6a = reader.read_u16_le()?;
    let _connection_count = reader.read_u32_le()?;
    let _field_0x6c = reader.read_u32_le()?;
    Ok(())
}

fn skip_fluid_segment_manager(reader: &mut BinaryReader) -> Result<()> {
    let segment_count = reader.read_u32_le()?;
    for _ in 0..segment_count {
        skip_fluid_segment(reader)?;
    }
    let _field_0x28 = reader.read_u32_le()?;
    Ok(())
}

fn skip_heat_buffer_manager(reader: &mut BinaryReader) -> Result<()> {
    let unsorted_buffer_count = reader.read_u32_le()?;
    let sorted_group_count = reader.read_u32_le()?;
    if unsorted_buffer_count != 0 || sorted_group_count != 0 {
        return Err(Error::InvalidPacket("HeatBufferManager parsing not implemented for non-zero buffers".into()));
    }
    Ok(())
}

fn skip_extra_script_data_inventories(reader: &mut BinaryReader) -> Result<()> {
    let mod_count = reader.read_u32_le()?;
    for _ in 0..mod_count {
        let _mod_name = reader.read_string()?;
        let inventory_count = reader.read_u32_le()?;
        if inventory_count != 0 {
            return Err(Error::InvalidPacket("ExtraScriptData inventory parsing not implemented for non-zero entries".into()));
        }
    }
    Ok(())
}

fn skip_map_runtime_counters(reader: &mut BinaryReader) -> Result<()> {
    let _active_entities_count = reader.read_u32_le()?;
    let _fully_active_segmented_units_count = reader.read_u32_le()?;
    let _minimally_active_segmented_units_count = reader.read_u32_le()?;
    let _asleep_segmented_units_count = reader.read_u32_le()?;
    let _next_unit_number = reader.read_u64_le()?;
    Ok(())
}

// ============================================================================
// Level.dat stream parsing (doc lines 1391-1444)
// ============================================================================
struct LevelDatStream {
    version: MapVersion,
    update_tick: u64,
    entity_tick: u64,
    ticks_played: u64,
    seed: u32,
    map_width: u32,
    map_height: u32,
    autoplace_controls: HashMap<String, FrequencySizeRichness>,
    prototype_mappings: PrototypeMappings,
    end_position: usize,
}

/// Skip the shared header block (doc lines 1269-1316)
/// Format: MapVersion + scenario strings + application version + mod list
fn skip_shared_header(reader: &mut BinaryReader) -> Result<MapVersion> {
    // 1) MapVersion
    let version = MapVersion::read(reader)?;
    #[cfg(test)]
    eprintln!("DEBUG: SharedHeader: MapVersion {}.{}.{}.{}, pos={}",
        version.major, version.minor, version.patch, version.build, reader.position());

    // 2) Empty string
    let s1 = reader.read_string()?;
    #[cfg(test)]
    eprintln!("DEBUG: SharedHeader: string1='{}', pos={}", s1, reader.position());

    // 3) Scenario name ("freeplay")
    let s2 = reader.read_string()?;
    #[cfg(test)]
    eprintln!("DEBUG: SharedHeader: string2='{}', pos={}", s2, reader.position());

    // 4) Mod name ("base")
    let s3 = reader.read_string()?;
    #[cfg(test)]
    eprintln!("DEBUG: SharedHeader: string3='{}', pos={}", s3, reader.position());

    // 5) Unknown u32 fields (doc line 1290: "u32 1, u32 0x01000000")
    let u1 = reader.read_u32_le()?;
    let u2 = reader.read_u32_le()?;
    #[cfg(test)]
    eprintln!("DEBUG: SharedHeader: u32s={}, {}, pos={}", u1, u2, reader.position());

    // 6) ApplicationVersion-like block (Space Age 2.0 observed as 12 bytes)
    // u16 major, u16 minor, u16 patch, u16 build, u32 unknown
    let app_major = reader.read_u16_le()?;
    let app_minor = reader.read_u16_le()?;
    let app_patch = reader.read_u16_le()?;
    let app_build = reader.read_u16_le()?;
    let _app_unknown = reader.read_u32_le()?;
    #[cfg(test)]
    eprintln!("DEBUG: SharedHeader: AppVersion {}.{}.{}.{}, pos={}",
        app_major, app_minor, app_patch, app_build, reader.position());

    // 7) Mod list
    let mod_count = reader.read_opt_u32()? as usize;
    #[cfg(test)]
    eprintln!("DEBUG: SharedHeader: mod_count={}, pos={}", mod_count, reader.position());

    // 8) Mod list (doc lines 1299-1307)
    for i in 0..mod_count {
        let name_len = reader.read_u8()? as usize;
        reader.skip(name_len)?; // name bytes
        reader.skip(3)?; // ver_major, ver_minor, ver_patch
        reader.skip(4)?; // crc
        #[cfg(test)]
        if i == 0 {
            eprintln!("DEBUG: SharedHeader: first mod done, pos={}", reader.position());
        }
    }
    #[cfg(test)]
    eprintln!("DEBUG: SharedHeader: all mods done, pos={}", reader.position());

    // 9) Scenario/freeplay victory/defeat messages block (Space Age 2.0)
    // Observed layout after mod list:
    // u32, u32, u16, then a sequence of LocalisedStrings + strings.
    if version.major >= 2 {
        let _scenario_u32_a = reader.read_u32_le()?;
        let _scenario_u32_b = reader.read_u32_le()?;
        let _scenario_u16 = reader.read_u16_le()?;

        skip_localised_string(reader)?; // victory title
        skip_localised_string(reader)?; // victory message

        let bullet_count = reader.read_u8()? as usize;
        for _ in 0..bullet_count {
            skip_localised_string(reader)?;
        }

        skip_localised_string(reader)?; // victory final message
        let _victory_image = reader.read_string()?;
        skip_localised_string(reader)?; // defeat title

        // Three flags (all zero observed) before defeat image
        reader.skip(3)?;
        let _defeat_image = reader.read_string()?;
        reader.skip(1)?; // trailing flag (0 observed)
    }

    Ok(version)
}

fn skip_localised_string(reader: &mut BinaryReader) -> Result<()> {
    let has_value = reader.read_u8()?;
    if has_value == 0 {
        return Ok(());
    }
    let _key = reader.read_string()?;
    let param_count = reader.read_u8()? as usize;
    for _ in 0..param_count {
        skip_localised_string(reader)?;
    }
    Ok(())
}

impl LevelDatStream {
    fn parse(data: &[u8]) -> Result<Self> {
        let mut reader = BinaryReader::new(data);
        let debug = std::env::var("FACTORIO_DEBUG").is_ok();

        // Parse shared header (doc lines 1269-1316)
        let version = skip_shared_header(&mut reader)?;

        // Now we're at MapSerialiser data (doc lines 1391-1444)
        // Note: MapSerialiser may or may not write its own MapVersion depending on flags

        // Check if next 9 bytes look like another MapVersion
        let peek_pos = reader.position();
        let peek_major = u16::from_le_bytes([data[peek_pos], data[peek_pos + 1]]);
        if peek_major == version.major {
            // Skip the redundant MapVersion
            let _ = MapVersion::read(&mut reader)?;
            #[cfg(test)]
            eprintln!("DEBUG: Skipped redundant MapVersion, pos={}", reader.position());
        }

        // 2) MapHeader (doc lines 1400-1403)
        let update_tick = reader.read_u64_le()?;
        let entity_tick = reader.read_u64_le()?;
        let ticks_played = reader.read_u64_le()?;
        if debug {
            eprintln!(
                "[DEBUG] MapHeader ticks: update={} entity={} played={} pos={}",
                update_tick,
                entity_tick,
                ticks_played,
                reader.position()
            );
        }

        // Sanity check: ticks should be reasonable values
        if update_tick > 100_000_000 || entity_tick > 100_000_000 {
            // Try to find the actual MapHeader by pattern matching
            if let Some(offset) = find_map_header_offset(data) {
                if debug {
                    eprintln!("[DEBUG] MapHeader invalid, falling back to parse_from_map_header at offset={}", offset);
                }
                reader = BinaryReader::new(&data[offset..]);
                let update_tick = reader.read_u64_le()?;
                let entity_tick = reader.read_u64_le()?;
                let ticks_played = reader.read_u64_le()?;
                return Self::parse_from_map_header(data, offset, version, update_tick, entity_tick, ticks_played);
            }
            return Err(Error::InvalidPacket("MapHeader tick values invalid".into()));
        }

        // 3) MapGenSettings (doc line 1404)
        if debug {
            eprintln!("[DEBUG] MapGenSettings start pos={}", reader.position());
        }
        let map_gen_settings = MapGenSettings::read(&mut reader)?;
        if debug {
            eprintln!("[DEBUG] MapGenSettings end pos={}", reader.position());
            eprintln!("[DEBUG] property_expression_names:");
            for (k, v) in &map_gen_settings.property_expression_names {
                eprintln!("[DEBUG]   {} = {}", k, v);
            }
            eprintln!("[DEBUG] autoplace_settings:");
            for (k, v) in &map_gen_settings.autoplace_settings {
                eprintln!("[DEBUG]   {} = {:?}", k, v);
            }
        }

        // 4) MapSettings (Space Age 2.0 flat format)
        if debug {
            eprintln!("[DEBUG] MapSettings start pos={}", reader.position());
        }
        skip_map_settings(&mut reader)?;
        if debug {
            eprintln!("[DEBUG] MapSettings end pos={}", reader.position());
        }

        // 5) Random generators - Space Age 2.0 observed as 86 bytes total.
        let rng_len = if version.major >= 2 { 86 } else { 5 * 12 };
        reader.skip(rng_len)?;
        #[cfg(test)]
        eprintln!("DEBUG: RNGs skipped, pos={}", reader.position());

        // 6) Unknown map fields (doc lines 1409-1411)
        let _unknown_bool = reader.read_bool()?;
        let _unknown_u32 = reader.read_u32_le()?;
        let _unknown_u16 = reader.read_u16_le()?;
        #[cfg(test)]
        eprintln!("DEBUG: Unknown fields done, pos={}", reader.position());

        // 7) Prototype ID mappings (doc line 1412)
        if debug {
            eprintln!("[DEBUG] Prototype mappings start pos={}", reader.position());
        }
        let prototype_mappings = parse_all_prototype_mappings(&mut reader, &version)?;
        if debug {
            eprintln!("[DEBUG] Prototype mappings end pos={}", reader.position());
        }

        // 8) Prototype migration list (doc line 1413)
        skip_prototype_migration_list(&mut reader)?;
        if debug {
            eprintln!("[DEBUG] after migration list pos={}", reader.position());
        }

        // 9) Map version-gated block + MapModSettings (doc lines 1660+)
        skip_map_version_gated_block(&mut reader, &version)?;
        skip_map_mod_settings(&mut reader)?;
        if debug {
            eprintln!("[DEBUG] after MapModSettings pos={}", reader.position());
        }

        // 10) Planets (Space Age)
        skip_planets(&mut reader, &version)?;
        if debug {
            eprintln!("[DEBUG] after Planets pos={}", reader.position());
        }

        // 11) Train manager
        skip_train_manager(&mut reader)?;
        if debug {
            eprintln!("[DEBUG] after TrainManager pos={}", reader.position());
        }

        // 12) Planned entity updates
        skip_planned_entity_updates(&mut reader)?;
        if debug {
            eprintln!("[DEBUG] after PlannedEntityUpdates pos={}", reader.position());
        }

        // 13) Force manager + linked inventories
        let force_count = skip_force_manager(&mut reader)?;
        skip_force_linked_inventories(&mut reader, force_count)?;
        if debug {
            eprintln!("[DEBUG] after ForceManager/LinkedInventories pos={}", reader.position());
        }

        // 14) Control behavior manager / circuit network / spoil queue
        skip_control_behavior_manager(&mut reader)?;
        skip_circuit_network_manager(&mut reader)?;
        skip_item_spoil_queue(&mut reader)?;
        if debug {
            eprintln!("[DEBUG] after Control/Circuit/Spoil pos={}", reader.position());
        }

        // 15) Script areas/positions + destroyed hooks + rendering
        skip_script_areas_positions(&mut reader)?;
        skip_object_destroyed_hooks(&mut reader)?;
        skip_script_rendering(&mut reader)?;
        if debug {
            eprintln!("[DEBUG] after Script blocks pos={}", reader.position());
        }

        // 16) Electric/Fluid/Heat/ExtraScript + runtime counters
        skip_electric_network_manager(&mut reader)?;
        skip_fluid_segment_manager(&mut reader)?;
        skip_heat_buffer_manager(&mut reader)?;
        skip_extra_script_data_inventories(&mut reader)?;
        skip_map_runtime_counters(&mut reader)?;
        if debug {
            eprintln!("[DEBUG] after Networks/Fluids/Heat/ExtraScript pos={}", reader.position());
        }

        let end_position = reader.position();
        Ok(Self {
            version,
            update_tick,
            entity_tick,
            ticks_played,
            seed: map_gen_settings.seed,
            map_width: map_gen_settings.width,
            map_height: map_gen_settings.height,
            autoplace_controls: map_gen_settings.autoplace_controls,
            prototype_mappings,
            end_position,
        })
    }

    fn parse_from_map_header(
        data: &[u8],
        header_offset: usize,
        version: MapVersion,
        update_tick: u64,
        entity_tick: u64,
        ticks_played: u64,
    ) -> Result<Self> {
        let mut reader = BinaryReader::new(&data[header_offset + 24..]); // Skip past the 3 u64 ticks
        let debug = std::env::var("FACTORIO_DEBUG").is_ok();

        // 3) MapGenSettings (doc line 1404)
        let map_gen_settings = MapGenSettings::read(&mut reader)?;

        // 4) MapSettings (Space Age 2.0 flat format)
        skip_map_settings(&mut reader)?;

        // 5) Random generators - Space Age 2.0 observed as 86 bytes total.
        let rng_len = if version.major >= 2 { 86 } else { 5 * 12 };
        reader.skip(rng_len)?;

        // 6) Unknown map fields
        let _unknown_bool = reader.read_bool()?;
        let _unknown_u32 = reader.read_u32_le()?;
        let _unknown_u16 = reader.read_u16_le()?;

        // 7) Prototype ID mappings (doc line 1443)
        #[cfg(test)]
        eprintln!("DEBUG: Starting prototype mappings at pos={}", reader.position());
        let prototype_mappings = parse_all_prototype_mappings(&mut reader, &version)?;
        #[cfg(test)]
        eprintln!("DEBUG: Prototype mappings done, pos={}", reader.position());

        // 8) Prototype migration list (doc line 1413)
        skip_prototype_migration_list(&mut reader)?;
        if debug {
            eprintln!(
                "[DEBUG] after migration list (fallback) pos={}",
                reader.position()
            );
        }

        // 9) Map version-gated block + MapModSettings (doc lines 1660+)
        skip_map_version_gated_block(&mut reader, &version)?;
        skip_map_mod_settings(&mut reader)?;
        if debug {
            eprintln!(
                "[DEBUG] after MapModSettings (fallback) pos={}",
                reader.position()
            );
        }

        // 10) Planets (Space Age)
        skip_planets(&mut reader, &version)?;
        if debug {
            eprintln!(
                "[DEBUG] after Planets (fallback) pos={}",
                reader.position()
            );
        }

        // 11) Train manager
        skip_train_manager(&mut reader)?;
        if debug {
            eprintln!(
                "[DEBUG] after TrainManager (fallback) pos={}",
                reader.position()
            );
        }

        // 12) Planned entity updates
        skip_planned_entity_updates(&mut reader)?;
        if debug {
            eprintln!(
                "[DEBUG] after PlannedEntityUpdates (fallback) pos={}",
                reader.position()
            );
        }

        if std::env::var("FACTORIO_PARSE_FORCE").is_ok() {
            skip_force_manager(&mut reader)?;
            if debug {
                eprintln!(
                    "[DEBUG] after ForceManager (fallback) pos={}",
                    reader.position()
                );
            }
        }

        if std::env::var("FACTORIO_PARSE_CONTROL").is_ok() {
            skip_control_behavior_manager(&mut reader)?;
            skip_circuit_network_manager(&mut reader)?;
            skip_item_spoil_queue(&mut reader)?;
            if debug {
                eprintln!(
                    "[DEBUG] after Control/Circuit/Spoil (fallback) pos={}",
                    reader.position()
                );
            }
        }

        if std::env::var("FACTORIO_PARSE_SCRIPT").is_ok() {
            skip_script_areas_positions(&mut reader)?;
            skip_object_destroyed_hooks(&mut reader)?;
            skip_script_rendering(&mut reader)?;
            if debug {
                eprintln!(
                    "[DEBUG] after Script blocks (fallback) pos={}",
                    reader.position()
                );
            }
        }

        if std::env::var("FACTORIO_PARSE_NETWORKS").is_ok() {
            skip_electric_network_manager(&mut reader)?;
            if debug {
                eprintln!(
                    "[DEBUG] after ElectricNetworkManager (fallback) pos={}",
                    reader.position()
                );
            }
        }

        if std::env::var("FACTORIO_PARSE_FLUIDS").is_ok() {
            skip_fluid_segment_manager(&mut reader)?;
            if debug {
                eprintln!(
                    "[DEBUG] after FluidSegmentManager (fallback) pos={}",
                    reader.position()
                );
            }
        }

        if std::env::var("FACTORIO_PARSE_HEAT").is_ok() {
            skip_heat_buffer_manager(&mut reader)?;
            if debug {
                eprintln!(
                    "[DEBUG] after HeatBufferManager (fallback) pos={}",
                    reader.position()
                );
            }
        }

        if std::env::var("FACTORIO_PARSE_EXTRA_SCRIPT").is_ok() {
            skip_extra_script_data_inventories(&mut reader)?;
            skip_map_runtime_counters(&mut reader)?;
            if debug {
                eprintln!(
                    "[DEBUG] after ExtraScriptData/runtime counters (fallback) pos={}",
                    reader.position()
                );
            }
        }

        let end_position = header_offset + 24 + reader.position();
        Ok(Self {
            version,
            update_tick,
            entity_tick,
            ticks_played,
            seed: map_gen_settings.seed,
            map_width: map_gen_settings.width,
            map_height: map_gen_settings.height,
            autoplace_controls: map_gen_settings.autoplace_controls,
            prototype_mappings,
            end_position,
        })
    }
}

/// Find the MapHeader by searching for 3 consecutive u64 tick values
fn find_map_header_offset(data: &[u8]) -> Option<usize> {
    let search_range = 2000.min(data.len());
    for offset in 0..search_range.saturating_sub(24) {
        let tick1 = u64::from_le_bytes([
            data[offset], data[offset+1], data[offset+2], data[offset+3],
            data[offset+4], data[offset+5], data[offset+6], data[offset+7],
        ]);
        let tick2 = u64::from_le_bytes([
            data[offset+8], data[offset+9], data[offset+10], data[offset+11],
            data[offset+12], data[offset+13], data[offset+14], data[offset+15],
        ]);
        let tick3 = u64::from_le_bytes([
            data[offset+16], data[offset+17], data[offset+18], data[offset+19],
            data[offset+20], data[offset+21], data[offset+22], data[offset+23],
        ]);

        if tick1 > 0 && tick1 < 10_000_000 && tick2 > 0 && tick2 < 10_000_000 && tick3 < 10_000_000 {
            let max = tick1.max(tick2).max(tick3.max(1));
            let min = tick1.min(tick2).min(if tick3 > 0 { tick3 } else { tick1 });
            if max <= min * 2 || (tick3 == tick1 && tick2 == tick1) {
                return Some(offset);
            }
        }
    }
    None
}

// ============================================================================
// Tile scanning (scan for "/T" markers to find tile blobs)
// ============================================================================

/// Decoded tile - either from save data or procedurally generated
#[derive(Clone)]
enum DecodedTile {
    FromSave(u16),
    Procedural(String),
}

fn tile_size_mask(tile_name: Option<&String>) -> u8 {
    match tile_name.map(|s| s.as_str()) {
        Some("out-of-map") => 0x01,
        Some("sand-1") | Some("sand-2") => 0x0F,
        _ => 0x07, // sizes 1, 2, 4
    }
}

/// Cross-chunk fill entry: a position in a neighbor chunk pre-filled by a large tile
struct CrossChunkFill {
    local_idx: usize, // y * 32 + x within target chunk
    tile_id: u16,
}

fn decode_tile_blob(
    data: &[u8],
    chunk_x: i32,
    chunk_y: i32,
    out_of_map_id: u16,
    terrain: &TerrainGenerator,
    prototype_mappings: &PrototypeMappings,
    prefilled: &[(usize, u16)], // (local_idx, tile_id) from neighbor large tiles
) -> Result<(Vec<DecodedTile>, Vec<((i32, i32), CrossChunkFill)>)> {
    let mut tile_ids = [out_of_map_id; 1024];
    let mut filled = [false; 1024];

    // Apply pre-fills from neighbor chunks' large tiles
    for &(idx, tile_id) in prefilled {
        if idx < 1024 {
            tile_ids[idx] = tile_id;
            filled[idx] = true;
        }
    }

    let mut cross_chunk_fills: Vec<((i32, i32), CrossChunkFill)> = Vec::new();
    let mut pos = 0usize;
    let mut remaining = 0u8;
    let mut current_tile_id = 0u16;

    // Column-major scan: x outer (0..31), y inner (0..31)
    'outer: for x in 0..32usize {
        for y in 0..32usize {
            let idx = y * 32 + x;

            if filled[idx] { continue; }

            if remaining == 0 {
                if pos + 3 > data.len() { break 'outer; }
                remaining = data[pos];
                if remaining == 0 { break 'outer; }
                current_tile_id = u16::from_le_bytes([data[pos + 1], data[pos + 2]]);
                pos += 3;
            }

            if pos >= data.len() { break 'outer; }
            let flag = data[pos];
            pos += 1;
            remaining -= 1;

            tile_ids[idx] = current_tile_id;
            filled[idx] = true;

            let masked = flag & 0xF0;
            let size_index = if masked & 0x10 != 0 {
                0
            } else {
                (masked >> 5) as u32 + 1
            };

            let size_mask = tile_size_mask(prototype_mappings.tile_name(current_tile_id));
            if size_index > 0 && (size_mask >> size_index) & 1 != 0 {
                let side = 1usize << size_index;
                for dx in 0..side {
                    let dy_start = if dx == 0 { 1 } else { 0 };
                    for dy in dy_start..side {
                        let gx = x + dx;
                        let gy = y + dy;
                        if gx < 32 && gy < 32 {
                            let fidx = gy * 32 + gx;
                            tile_ids[fidx] = current_tile_id;
                            filled[fidx] = true;
                        } else {
                            // Cross-chunk: compute target chunk and local position
                            let target_cx = chunk_x + (gx / 32) as i32;
                            let target_cy = chunk_y + (gy / 32) as i32;
                            let local_x = gx % 32;
                            let local_y = gy % 32;
                            let local_idx = local_y * 32 + local_x;
                            cross_chunk_fills.push((
                                (target_cx, target_cy),
                                CrossChunkFill { local_idx, tile_id: current_tile_id },
                            ));
                        }
                    }
                }
            }
        }
    }

    let chunk_tiles = terrain.compute_chunk(chunk_x, chunk_y);
    let tiles: Vec<DecodedTile> = (0..1024).map(|i| {
        if filled[i] {
            DecodedTile::FromSave(tile_ids[i])
        } else {
            DecodedTile::Procedural(terrain.tile_name(chunk_tiles[i]).to_string())
        }
    }).collect();

    Ok((tiles, cross_chunk_fills))
}

/// Scan for tile data by finding "/T" markers and working backwards
fn scan_for_tiles(
    data: &[u8],
    prototype_mappings: &PrototypeMappings,
    chunk_positions: Option<&Vec<ChunkPrelude>>,
    seed: u32,
    autoplace_controls: &HashMap<String, FrequencySizeRichness>,
) -> Vec<MapTile> {
    let debug = std::env::var("FACTORIO_DEBUG").is_ok();
    let mut all_tiles = Vec::new();
    let mut prelude_hits = 0usize;
    let mut prelude_misses = 0usize;
    let mut decode_failures = 0usize;
    let mut skipped_not_ready = 0usize;
    let mut max_chunk_index = 0usize;
    let mut min_chunk_index = usize::MAX;
    let mut all_indices = Vec::new();
    let mut used_positions: std::collections::HashSet<(i32, i32)> = std::collections::HashSet::new();
    let mut tiles_per_chunk: std::collections::HashMap<usize, usize> = std::collections::HashMap::new();
    let mut blob_lens: Vec<usize> = Vec::new();
    let mut blob_lens_by_idx: std::collections::HashMap<usize, usize> = std::collections::HashMap::new();

    // Look up the out-of-map tile ID from prototype mappings
    let out_of_map_id = prototype_mappings.tile_id_by_name("out-of-map").unwrap_or(143);

    // Convert autoplace_controls to compiler control variable format
    let mut controls = HashMap::new();
    for (name, fsr) in autoplace_controls {
        controls.insert(format!("control:{}:frequency", name), fsr.frequency);
        controls.insert(format!("control:{}:size", name), fsr.size);
    }

    let terrain = match TerrainGenerator::new_with_controls(seed, &controls) {
        Ok(gen) => gen,
        Err(e) => {
            eprintln!("Failed to create TerrainGenerator: {}", e);
            return Vec::new();
        }
    };

    if debug {
        if let Some(positions) = chunk_positions {
            eprintln!(
                "[DEBUG] Using parsed surface prelude: {} chunk positions",
                positions.len()
            );
        } else {
            eprintln!("[DEBUG] No surface prelude available, using fallback positions");
        }
    }

    let mut tile_chunks = Vec::new();
    for i in 0..data.len().saturating_sub(10) {
        if data[i] != 0x43 || data[i + 1] != 0x3a {
            continue;
        }

        let chunk_index = u32::from_le_bytes([
            data[i + 2],
            data[i + 3],
            data[i + 4],
            data[i + 5],
        ]) as usize;
        let tile_blob_len =
            u16::from_le_bytes([data[i + 6], data[i + 7]]) as usize;

        if tile_blob_len == 0 || tile_blob_len > 0x1000 {
            continue;
        }

        let tile_end = i + 8 + tile_blob_len;
        if tile_end + 2 > data.len() {
            continue;
        }
        if data[tile_end] != 0x2f || data[tile_end + 1] != 0x54 {
            continue;
        }

        let tile_blob = &data[i + 8..tile_end];
        tile_chunks.push((i, chunk_index, tile_blob_len, tile_blob));
    }

    if debug {
        eprintln!(
            "[DEBUG] Found {} C:/T tile chunks in {} bytes",
            tile_chunks.len(),
            data.len()
        );
    }

    // Sort by byte position in stream to match prelude order
    tile_chunks.sort_by_key(|(chunk_start, _, _, _)| *chunk_start);

    let total_chunks = tile_chunks.len();

    // Cross-chunk fill map: positions pre-filled by previous chunks' large tiles
    let mut cross_chunk_map: std::collections::HashMap<(i32, i32), Vec<(usize, u16)>> =
        std::collections::HashMap::new();

    for (ordinal, (_chunk_start, chunk_index, tile_blob_len, tile_blob)) in tile_chunks.into_iter().enumerate() {
        blob_lens.push(tile_blob_len);
        blob_lens_by_idx.insert(chunk_index, tile_blob_len);

        let from_prelude = chunk_positions.and_then(|positions| {
            positions.get(ordinal)
        });
        if from_prelude.is_some() {
            prelude_hits += 1;
        } else {
            prelude_misses += 1;
        }

        let should_render = from_prelude
            .map(|chunk| chunk.status >= 0x0a)
            .unwrap_or(true);
        if !should_render {
            skipped_not_ready += 1;
            continue;
        }

        let (chunk_x, chunk_y) = from_prelude
            .map(|chunk| chunk.position)
            .unwrap_or_else(|| {
                let side = (total_chunks as f64).sqrt().ceil() as i32;
                let half = side / 2;
                ((ordinal as i32 / side) - half, (ordinal as i32 % side) - half)
            });

        if debug && (ordinal < 5 || (chunk_x >= -2 && chunk_x <= 1 && chunk_y >= -2 && chunk_y <= 1)) {
            eprintln!(
                "[DEBUG] Tile chunk idx={} ord={} pos=({},{})",
                chunk_index, ordinal, chunk_x, chunk_y
            );
        }

        let prefilled = cross_chunk_map.remove(&(chunk_x, chunk_y)).unwrap_or_default();
        let status = from_prelude.map(|chunk| chunk.status).unwrap_or(0);
        match decode_tile_blob(tile_blob, chunk_x, chunk_y, out_of_map_id, &terrain, prototype_mappings, &prefilled) {
        Ok((chunk_tiles, cross_fills)) => {
            for ((target_cx, target_cy), fill) in cross_fills {
                cross_chunk_map.entry((target_cx, target_cy))
                    .or_default()
                    .push((fill.local_idx, fill.tile_id));
            }
            tiles_per_chunk.insert(chunk_index, chunk_tiles.len());

            max_chunk_index = max_chunk_index.max(chunk_index);
            min_chunk_index = min_chunk_index.min(chunk_index);
            all_indices.push(chunk_index);

            used_positions.insert((chunk_x, chunk_y));
            let base_x = chunk_x * 32;
            let base_y = chunk_y * 32;

            for (idx, decoded_tile) in chunk_tiles.iter().enumerate() {
                let local_x = (idx % 32) as i32;
                let local_y = (idx / 32) as i32;

                let (name, procedural) = match decoded_tile {
                    DecodedTile::FromSave(tile_id) => {
                        let n = prototype_mappings.tile_name(*tile_id)
                            .cloned()
                            .unwrap_or_else(|| "unknown".to_string());
                        (n, false)
                    }
                    DecodedTile::Procedural(tile_name) => (tile_name.clone(), true),
                };

                all_tiles.push(MapTile {
                    name,
                    x: base_x + local_x,
                    y: base_y + local_y,
                    procedural,
                });
            }
        }
        Err(e) => {
            decode_failures += 1;
            // Always log decode failures to help debug
            let first_bytes: Vec<String> = tile_blob.iter().take(32).map(|b| format!("{:02x}", b)).collect();
            eprintln!(
                "[TILES] Decode failed: ord={} idx={} pos=({},{}) len={} status=0x{:02x} err={}\n        first 32 bytes: {}",
                ordinal, chunk_index, chunk_x, chunk_y, tile_blob_len, status, e, first_bytes.join(" ")
            );
        }
        }
    }

    // Count tiles by type
    let out_of_map_count = all_tiles.iter().filter(|t| t.name.contains("out-of-map")).count();
    let water_count = all_tiles.iter().filter(|t| t.name.contains("water")).count();
    let grass_count = all_tiles.iter().filter(|t| t.name.starts_with("grass")).count();
    let dirt_count = all_tiles.iter().filter(|t| t.name.starts_with("dirt") || t.name.starts_with("dry-dirt")).count();
    let sand_count = all_tiles.iter().filter(|t| t.name.starts_with("sand")).count();
    let red_desert_count = all_tiles.iter().filter(|t| t.name.starts_with("red-desert")).count();

    // Always print tile parsing summary for debugging
    let total_chunks = prelude_hits + prelude_misses + decode_failures + skipped_not_ready;
    eprintln!("[TILES] Chunks: {} total, {} ok, {} decode failed, {} skipped (not ready), {} no prelude",
        total_chunks, used_positions.len(), decode_failures, skipped_not_ready, prelude_misses);
    eprintln!("[TILES] Tiles: {} (water={}, grass={}, dirt={}, sand={}, red-desert={}, out-of-map={})",
        all_tiles.len(), water_count, grass_count, dirt_count, sand_count, red_desert_count, out_of_map_count);
    if !used_positions.is_empty() {
        let xs: Vec<i32> = used_positions.iter().map(|(x, _)| *x).collect();
        let ys: Vec<i32> = used_positions.iter().map(|(_, y)| *y).collect();
        eprintln!("[TILES] Chunk range: x {}..{}, y {}..{}",
            xs.iter().min().unwrap(), xs.iter().max().unwrap(),
            ys.iter().min().unwrap(), ys.iter().max().unwrap()
        );
    }

    all_tiles
}

// ============================================================================
// ZIP parsing
// ============================================================================
fn parse_zip_map(data: &[u8]) -> Result<MapData> {
    let cursor = Cursor::new(data);
    let mut archive = zip::ZipArchive::new(cursor)
        .map_err(|e| Error::InvalidPacket(format!("ZIP error: {}", e)))?;

    let mut file_names = Vec::new();
    for i in 0..archive.len() {
        if let Ok(file) = archive.by_index(i) {
            file_names.push(file.name().to_string());
        }
    }

    // Collect all level.dat chunks and concatenate
    let mut level_dat_chunks: Vec<(usize, Vec<u8>)> = Vec::new();
    for i in 0..20 {
        if let Some(chunk_data) = read_level_dat(&mut archive, i) {
            if let Ok(decompressed) = decompress_if_needed(&chunk_data) {
                level_dat_chunks.push((i, decompressed));
            }
        }
    }

    level_dat_chunks.sort_by_key(|(idx, _)| *idx);
    let full_stream: Vec<u8> = level_dat_chunks
        .into_iter()
        .flat_map(|(_, data)| data)
        .collect();

    let mut stream = match LevelDatStream::parse(&full_stream) {
        Ok(s) => s,
        Err(e) => return Err(e),
    };
    let debug = std::env::var("FACTORIO_DEBUG").is_ok();
    if debug {
        let start = stream.end_position;
        let end = (start + 64).min(full_stream.len());
        let preview = &full_stream[start..end];
        eprintln!(
            "[DEBUG] level.dat stream: len={} end_position={} next_bytes={:02x?}",
            full_stream.len(),
            stream.end_position,
            preview
        );
    }

    let mut surface_reader = BinaryReader::new(&full_stream[stream.end_position..]);
    let mut surface_preludes = parse_surface_preludes(&mut surface_reader, &stream.version)?;

    let entity_prototypes = stream.prototype_mappings.tables.get("Entity").cloned().unwrap_or_default();
    let item_prototypes = stream.prototype_mappings.tables.get("ItemPrototype").cloned().unwrap_or_default();
    let recipe_prototypes = stream.prototype_mappings.tables.get("Recipe").cloned().unwrap_or_default();

    let entities = Vec::new();

    let seed = stream.seed;
    if seed != 0 {
        eprintln!("[MAP] Using map seed {} for procedural terrain", seed);
    }

    let first_surface_positions = surface_preludes.get(0).map(|s| &s.chunks);
    let mut tiles = scan_for_tiles(
        &full_stream,
        &stream.prototype_mappings,
        first_surface_positions,
        seed,
        &stream.autoplace_controls,
    );

    // Discard tiles outside the known map bounds
    let half_w = (stream.map_width / 2) as i32;
    let half_h = (stream.map_height / 2) as i32;
    if half_w > 0 && half_h > 0 {
        let before = tiles.len();
        tiles.retain(|t| t.x >= -half_w && t.x < half_w && t.y >= -half_h && t.y < half_h);
        let removed = before - tiles.len();
        if removed > 0 {
            eprintln!("[TILES] Filtered {} tiles outside map bounds (±{}, ±{})", removed, half_w, half_h);
        }
    }

    let surfaces = surface_preludes
        .into_iter()
        .map(|surface| {
            let chunks = surface
                .chunks
                .into_iter()
                .map(|chunk| ChunkData {
                    position: chunk.position,
                    entities: Vec::new(),
                    tiles: Vec::new(),
                    decoratives: Vec::new(),
                })
                .collect();
            SurfaceData {
                name: String::new(),
                index: surface.index as u16,
                chunks,
            }
        })
        .collect();

    Ok(MapData {
        version: stream.version,
        scenario_name: String::new(),
        scenario_mod: String::new(),
        seed,
        ticks_played: stream.ticks_played as u32,
        entities,
        tiles,
        player_spawn: (0.0, 0.0),
        raw_files: file_names,
        item_prototypes,
        recipe_prototypes,
        entity_prototypes,
        resource_counts: HashMap::new(),
        prototype_mappings: stream.prototype_mappings,
        surfaces,
    })
}

fn read_level_dat(archive: &mut zip::ZipArchive<Cursor<&[u8]>>, chunk: usize) -> Option<Vec<u8>> {
    let name = format!("level.dat{}", chunk);
    for i in 0..archive.len() {
        if let Ok(mut file) = archive.by_index(i) {
            if file.name().ends_with(&name) || file.name().contains(&format!("/{}", name)) {
                let mut data = Vec::new();
                file.read_to_end(&mut data).ok()?;
                return Some(data);
            }
        }
    }
    None
}

fn decompress_if_needed(data: &[u8]) -> Result<Vec<u8>> {
    if data.len() < 2 {
        return Ok(data.to_vec());
    }

    if data[0] == 0x78 {
        let mut decoder = ZlibDecoder::new(data);
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed)
            .map_err(|e| Error::InvalidPacket(format!("zlib error: {}", e)))?;
        return Ok(decompressed);
    }

    if data.len() >= 4 && &data[0..4] == &[0x28, 0xB5, 0x2F, 0xFD] {
        let decompressed = zstd::decode_all(data)
            .map_err(|e| Error::InvalidPacket(format!("zstd error: {}", e)))?;
        return Ok(decompressed);
    }

    Ok(data.to_vec())
}

// ============================================================================
// Public types
// ============================================================================

/// Parsed map data
#[derive(Debug, Clone)]
pub struct MapData {
    pub version: MapVersion,
    pub scenario_name: String,
    pub scenario_mod: String,
    pub seed: u32,
    pub ticks_played: u32,
    pub entities: Vec<MapEntity>,
    pub tiles: Vec<MapTile>,
    pub player_spawn: (f64, f64),
    pub raw_files: Vec<String>,
    pub item_prototypes: HashMap<u16, String>,
    pub recipe_prototypes: HashMap<u16, String>,
    pub entity_prototypes: HashMap<u16, String>,
    pub resource_counts: HashMap<String, u32>,
    pub prototype_mappings: PrototypeMappings,
    pub surfaces: Vec<SurfaceData>,
}

impl MapData {
    pub fn character_positions(&self) -> Vec<(f64, f64)> {
        self.entities
            .iter()
            .filter(|e| e.name == "character")
            .map(|e| (e.x, e.y))
            .collect()
    }

    pub fn character_speed(&self) -> f64 {
        self.prototype_mappings.character_speed()
    }

    pub fn parse(data: &[u8]) -> Result<Self> {
        let mut reader = BinaryReader::new(data);

        let version = MapVersion::read(&mut reader).unwrap_or_default();
        let scenario_name = reader.read_string().unwrap_or_default();
        let scenario_mod = reader.read_string().unwrap_or_default();
        let seed = reader.read_u32_le().unwrap_or(0);
        let ticks_played = reader.read_u32_le().unwrap_or(0);

        Ok(Self {
            version,
            scenario_name,
            scenario_mod,
            seed,
            ticks_played,
            entities: Vec::new(),
            tiles: Vec::new(),
            player_spawn: (0.0, 0.0),
            raw_files: Vec::new(),
            item_prototypes: HashMap::new(),
            recipe_prototypes: HashMap::new(),
            entity_prototypes: HashMap::new(),
            resource_counts: HashMap::new(),
            prototype_mappings: PrototypeMappings::default(),
            surfaces: Vec::new(),
        })
    }
}

use super::map_types::{MapEntity, MapTile, MapVersion};

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_map_transfer_tracking() {
        let mut transfer = MapTransfer::new(100);
        assert!(!transfer.is_complete());

        transfer.add_block(0, vec![0; 50]);
        assert!(!transfer.is_complete());

        transfer.add_block(1, vec![0; 50]);
        assert!(transfer.is_complete());
    }

    fn load_test_map() -> Option<Vec<u8>> {
        let path = std::env::current_dir().ok()?.join("captured_map.zip");
        fs::read(path).ok()
    }

    #[test]
    fn test_parse_captured_map() {
        let data = match load_test_map() {
            Some(d) => d,
            None => return,
        };

        let result = parse_map_data(&data);
        match &result {
            Ok(map) => {
                println!("Version: {}.{}.{}.{}", map.version.major, map.version.minor, map.version.patch, map.version.build);
                println!("Entity prototypes: {}", map.entity_prototypes.len());
            }
            Err(e) => {
                println!("Parse error: {:?}", e);
            }
        }
        assert!(result.is_ok());
    }

    #[test]
    fn test_delta_position_encoding() {
        let delta_data = [0x0A, 0x00, 0x14, 0x00];
        let mut reader = BinaryReader::new(&delta_data);
        let dx = reader.read_i16_le().unwrap();
        let dy = reader.read_i16_le().unwrap();
        assert_eq!(dx, 10);
        assert_eq!(dy, 20);

        let escape_data = [
            0xFF, 0x7F,
            0x00, 0x10, 0x00, 0x00,
            0x00, 0x20, 0x00, 0x00,
        ];
        let mut reader = BinaryReader::new(&escape_data);
        let escape = reader.read_i16_le().unwrap();
        assert_eq!(escape, 0x7FFF);
        let x = reader.read_i32_le().unwrap();
        let y = reader.read_i32_le().unwrap();
        assert_eq!(x, 4096);
        assert_eq!(y, 8192);
    }

    #[test]
    fn test_entity_prototype_lookups() {
        let data = match load_test_map() {
            Some(d) => d,
            None => return,
        };

        let result = parse_map_data(&data);
        let map = result.expect("Should parse map successfully");

        // Verify Entity table has reasonable content
        assert!(map.entity_prototypes.len() > 500, "Should have many entities");

        // Check that common entities exist (IDs vary by game version/mods)
        let entity_names: Vec<&String> = map.entity_prototypes.values().collect();
        assert!(entity_names.iter().any(|n| *n == "coal"), "Should have coal entity");
        assert!(entity_names.iter().any(|n| *n == "iron-ore"), "Should have iron-ore entity");
        assert!(entity_names.iter().any(|n| *n == "transport-belt"), "Should have transport-belt entity");
        assert!(entity_names.iter().any(|n| *n == "accumulator"), "Should have accumulator entity");
    }
}
