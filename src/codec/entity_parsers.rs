use super::BinaryReader;
use super::map_transfer::read_map_position_delta;
use crate::error::Result;

pub struct EntityParseResult {
    pub position: (i32, i32),
    pub proto_id: u16,
    pub name: String,
}

#[derive(Debug, Clone, Copy)]
enum EntityClass {
    Entity,
    EntityWithHealth,
    EntityWithOwner,
}

fn entity_class(name: &str) -> EntityClass {
    match name {
        // Entity base (no health/owner fields) - ResourceEntity
        n if n.ends_with("-ore") || n == "crude-oil" || n == "uranium-ore"
            || n == "coal" || n == "stone" => EntityClass::Entity,

        // EntityWithOwner types
        n if n.contains("inserter")
            || n.contains("assembling-machine")
            || n.contains("furnace")
            || n.contains("mining-drill")
            || n.contains("transport-belt")
            || n.contains("underground-belt")
            || n.contains("splitter")
            || n.contains("loader")
            || n.contains("pipe")
            || n.contains("pump")
            || n.contains("boiler")
            || n.contains("generator")
            || n.contains("solar-panel")
            || n.contains("accumulator")
            || n.contains("reactor")
            || n.contains("heat-pipe")
            || n.contains("roboport")
            || n.contains("radar")
            || n.contains("turret")
            || n.contains("-wall")
            || n == "stone-wall"
            || n.contains("gate")
            || n.contains("electric-pole")
            || n.contains("substation")
            || n.contains("lamp")
            || n.contains("combinator")
            || n.contains("speaker")
            || n.contains("power-switch")
            || n.contains("rail")
            || n.contains("train-stop")
            || n.contains("rail-signal")
            || n.contains("rail-chain-signal")
            || n.contains("locomotive")
            || n.contains("wagon")
            || n.contains("car") || n == "tank" || n.contains("spidertron")
            || n.contains("chest")
            || n.contains("container")
            || n.contains("rocket-silo")
            || n.contains("lab")
            || n.contains("beacon")
            || n.contains("offshore-pump")
            || n == "character"
            || n.contains("simple-entity-with") => EntityClass::EntityWithOwner,

        // EntityWithHealth: trees, rocks, fish, remnants
        n if n.starts_with("tree-")
            || n.starts_with("dead-")
            || n.contains("rock")
            || n == "fish"
            || n.contains("remnants")
            || n.starts_with("simple-entity") => EntityClass::EntityWithHealth,

        // Default: EntityWithHealth is safest for unknown types with destructible bodies
        _ => EntityClass::EntityWithHealth,
    }
}

fn skip_entity_with_health(reader: &mut BinaryReader, flags: u16) -> Result<()> {
    // If flags bit 13 set: health (f32) + cumulative damage (f32)
    if flags & (1 << 13) != 0 {
        reader.skip(8)?;
    }
    Ok(())
}

fn skip_entity_with_owner(reader: &mut BinaryReader) -> Result<()> {
    // u8 quality_id
    let _quality = reader.read_u8()?;
    Ok(())
}

/// Skip type-specific entity data. Returns false if the type is unknown.
fn skip_type_specific(reader: &mut BinaryReader, name: &str, _flags: u16) -> Result<bool> {
    match name {
        // ResourceEntity: u32 amount + bool infinite + opt(u32 initial_amount) + u8 stage
        n if n.ends_with("-ore") || n == "crude-oil" || n == "uranium-ore"
            || n == "coal" || n == "stone" => {
            reader.skip(4)?; // amount
            let infinite = reader.read_bool()?;
            if infinite {
                reader.skip(4)?; // initial_amount
            }
            reader.skip(1)?; // stage
            Ok(true)
        }

        // Tree: u8 variation + u16 stage_info
        // Names ending in "-tree" are Tree type (including dead-dry-hairy-tree)
        n if n.starts_with("tree-") || n.ends_with("-tree") => {
            reader.skip(3)?;
            Ok(true)
        }

        // Dead tree variants (dead-grey-trunk, dead-tree-desert): same as Tree (u8 variation + u16 stage_info)
        n if n.starts_with("dead-") => {
            reader.skip(3)?;
            Ok(true)
        }

        // SimpleEntity: u8 graphics_variation
        n if n.starts_with("simple-entity") && !n.contains("with") => {
            reader.skip(1)?;
            Ok(true)
        }

        // SimpleEntityWithOwner: 4*u8 + u16 + u8 = 7 bytes
        n if n.contains("simple-entity-with") => {
            reader.skip(7)?;
            Ok(true)
        }

        // Fish: u8 direction
        "fish" => {
            reader.skip(1)?;
            Ok(true)
        }

        // Rocks (various types): u8 graphics_variation (SimpleEntity)
        n if n.contains("rock") && !n.contains("rocket") => {
            reader.skip(1)?;
            Ok(true)
        }

        _ => Ok(false),
    }
}

/// Parse a single entity from the stream.
/// Returns None if the entity type is unknown (caller should stop parsing this chunk).
pub fn parse_entity(
    reader: &mut BinaryReader,
    proto_id: u16,
    entity_name: &str,
    last_pos: &mut (i32, i32),
) -> Result<Option<EntityParseResult>> {
    let position = read_map_position_delta(reader, last_pos)?;
    let flags = reader.read_u16_le()?;

    let class = entity_class(entity_name);

    match class {
        EntityClass::EntityWithHealth => {
            skip_entity_with_health(reader, flags)?;
        }
        EntityClass::EntityWithOwner => {
            skip_entity_with_health(reader, flags)?;
            skip_entity_with_owner(reader)?;
        }
        EntityClass::Entity => {}
    }

    if !skip_type_specific(reader, entity_name, flags)? {
        return Ok(None);
    }

    Ok(Some(EntityParseResult {
        position,
        proto_id,
        name: entity_name.to_string(),
    }))
}

/// Skip the military targets and active entities sections between "/T" and the entity loop.
pub fn skip_pre_entity_sections(reader: &mut BinaryReader) -> Result<()> {
    // military_target_count
    let mil_count = reader.read_opt_u32()? as usize;
    for _ in 0..mil_count {
        let n = reader.read_opt_u32()? as usize;
        reader.skip(n)?;
    }

    // active_enemies_count
    let enemy_count = reader.read_opt_u32()? as usize;
    for _ in 0..enemy_count {
        let n = reader.read_opt_u32()? as usize;
        reader.skip(n)?;
    }

    // ActiveEntities mode=2
    let n = reader.read_opt_u32()? as usize;
    reader.skip(n)?;

    // ActiveEntities mode=4
    let n = reader.read_opt_u32()? as usize;
    reader.skip(n)?;

    Ok(())
}

/// Parse all entities from a chunk's entity section (data starting right after "/T" marker).
/// Skips unhandled entity types by scanning for the next valid proto_id.
pub fn parse_chunk_entities(
    data: &[u8],
    chunk_x: i32,
    chunk_y: i32,
    entity_prototypes: &std::collections::HashMap<u16, String>,
) -> Vec<EntityParseResult> {
    let mut reader = BinaryReader::new(data);
    let mut entities = Vec::new();

    if skip_pre_entity_sections(&mut reader).is_err() {
        return entities;
    }

    let mut last_pos = (chunk_x * 32 * 256, chunk_y * 32 * 256);

    loop {
        if reader.remaining() < 2 {
            break;
        }
        let proto_id = match reader.read_u16_le() {
            Ok(id) => id,
            Err(_) => break,
        };
        if proto_id == 0 {
            break;
        }

        let entity_name = match entity_prototypes.get(&proto_id) {
            Some(name) => name.clone(),
            None => {
                entities.push(EntityParseResult {
                    position: last_pos,
                    proto_id,
                    name: format!("?proto_{}", proto_id),
                });
                if !recover_to_next_entity(&mut reader, data, entity_prototypes) {
                    break;
                }
                continue;
            }
        };

        match parse_entity(&mut reader, proto_id, &entity_name, &mut last_pos) {
            Ok(Some(result)) => entities.push(result),
            Ok(None) => {
                // Unknown type-specific format - record entity with "?" name and scan forward
                entities.push(EntityParseResult {
                    position: last_pos,
                    proto_id,
                    name: format!("?{}", entity_name),
                });
                if !recover_to_next_entity(&mut reader, data, entity_prototypes) {
                    break;
                }
            }
            Err(_) => break,
        }
    }

    entities
}

/// Scan forward from current position to find the next valid proto_id.
/// Returns true if recovery succeeded and reader is positioned at the byte after the proto_id.
fn recover_to_next_entity(
    reader: &mut BinaryReader,
    data: &[u8],
    entity_prototypes: &std::collections::HashMap<u16, String>,
) -> bool {
    let start = reader.position();
    // Scan up to 64 bytes ahead for a valid proto_id
    for skip in 0..64 {
        let pos = start + skip;
        if pos + 2 > data.len() {
            break;
        }
        let candidate = u16::from_le_bytes([data[pos], data[pos + 1]]);
        if candidate == 0 || entity_prototypes.contains_key(&candidate) {
            reader.set_position(pos);
            return true;
        }
    }
    false
}
