use std::collections::HashMap;
use super::BinaryReader;
use super::map_transfer::read_map_position_delta;
use crate::error::Result;

pub struct EntityParseResult {
    pub position: (i32, i32),
    pub proto_id: u16,
    pub name: String,
    pub resource_amount: Option<u32>,
    pub resource_infinite: bool,
    pub underground_type: Option<u8>,
}

#[derive(Debug, Clone, Copy)]
enum EntityClass {
    Entity,
    EntityWithHealth,
    EntityWithOwner,
}

fn entity_class_for_type(entity_type: &str) -> EntityClass {
    match entity_type {
        "resource" | "cliff" | "corpse" | "character-corpse"
            | "rail-remnants" | "deconstructible-tile-proxy"
            | "item-entity" => EntityClass::Entity,
        "tree" | "simple-entity" | "fish" => EntityClass::EntityWithHealth,
        _ => EntityClass::EntityWithOwner,
    }
}

fn skip_entity_with_health(reader: &mut BinaryReader, flags: u16) -> Result<()> {
    if flags & (1 << 13) != 0 {
        reader.skip(8)?;
    }
    Ok(())
}

fn skip_entity_with_owner(reader: &mut BinaryReader) -> Result<()> {
    let _force = reader.read_u8()?;
    let quality = reader.read_u8()?;
    reader.skip(8)?; // version >= 2.0.31 data
    if quality != 0 {
        let opt_byte = reader.read_u8()?;
        if opt_byte == 0xFF {
            reader.skip(2)?;
        }
    }
    let military = reader.read_bool()?;
    if military {
        return Err(crate::error::Error::InvalidPacket("military target".into()));
    }
    Ok(())
}

fn skip_item_stack(reader: &mut BinaryReader) -> Result<()> {
    let proto_id = reader.read_u16_le()?;
    if proto_id == 0 {
        return Ok(());
    }
    let _quality = reader.read_u8()?;
    let has_data = reader.read_bool()?;
    if has_data {
        return Err(crate::error::Error::InvalidPacket("complex item data".into()));
    }
    let _count = reader.read_opt_u32()?;
    Ok(())
}

fn skip_inventory(reader: &mut BinaryReader) -> Result<()> {
    let inv_type = reader.read_u8()?;
    let slot_count = reader.read_u16_le()?;
    for _ in 0..slot_count {
        skip_item_stack(reader)?;
    }
    let _active_index = reader.read_u16_le()?;
    if inv_type == 1 || inv_type == 3 {
        let filter_count = reader.read_opt_u32()?;
        reader.skip(filter_count as usize * 4)?;
    }
    if inv_type == 0 || inv_type == 1 {
        reader.read_u16_le()?;
    }
    Ok(())
}

fn skip_wake_up_list(reader: &mut BinaryReader) -> Result<()> {
    let count = reader.read_opt_u32()?;
    for _ in 0..count {
        let _has_entity = reader.read_bool()?;
    }
    Ok(())
}

fn skip_container(reader: &mut BinaryReader) -> Result<()> {
    skip_inventory(reader)?;
    skip_wake_up_list(reader)?;
    skip_wake_up_list(reader)?;
    let has_cb = reader.read_bool()?;
    if has_cb {
        return Err(crate::error::Error::InvalidPacket("container control behavior".into()));
    }
    Ok(())
}

fn skip_corpse(reader: &mut BinaryReader) -> Result<()> {
    reader.skip(2)?; // UpdatableEntity.load: state + byte
    reader.skip(4)?; // RealOrientation (f32)
    reader.skip(4)?; // time1 (f32)
    reader.skip(4)?; // time2 (f32)
    reader.skip(4)?; // dying_speed, ground_patches, variation, byte
    reader.skip(8)?; // age_in_ticks (u64, version >= 1.2.0.373)
    let flags = reader.read_u8()?;
    if flags & 2 != 0 {
        reader.skip(4)?; // RGBA color
    }
    Ok(())
}

/// Skip type-specific entity data. Returns false if the type is unknown.
fn skip_type_specific(reader: &mut BinaryReader, entity_type: &str) -> Result<(bool, Option<u32>, bool, Option<u8>)> {
    match entity_type {
        "resource" => {
            let amount = reader.read_u32_le()?;
            let infinite = reader.read_bool()?;
            if infinite {
                reader.read_u32_le()?;
            }
            reader.skip(1)?;
            Ok((true, Some(amount), infinite, None))
        }
        "tree" => {
            reader.skip(3)?;
            Ok((true, None, false, None))
        }
        "simple-entity" => {
            reader.skip(1)?;
            Ok((true, None, false, None))
        }
        "simple-entity-with-owner" | "simple-entity-with-force" => {
            reader.skip(16)?; // 4×u8 + u16 + u8 graphics + 8 bytes color + u8 direction
            Ok((true, None, false, None))
        }
        "fish" => {
            reader.skip(2)?; // UpdatableEntity
            reader.skip(1)?; // variation
            reader.skip(4)?; // RealOrientation (float)
            reader.skip(8)?; // speed (double)
            reader.skip(4)?; // tick counter (u32)
            Ok((true, None, false, None))
        }
        "cliff" => {
            reader.skip(2)?; // orientation + variant
            Ok((true, None, false, None))
        }
        "corpse" => {
            skip_corpse(reader)?;
            Ok((true, None, false, None))
        }
        "rail-remnants" => {
            skip_corpse(reader)?;
            reader.skip(1)?; // Direction
            Ok((true, None, false, None))
        }
        "item-entity" => {
            skip_item_stack(reader)?;
            reader.skip(1)?;
            Ok((true, None, false, None))
        }
        "deconstructible-tile-proxy" => {
            reader.skip(4)?; // u8 + u16 + u8
            Ok((true, None, false, None))
        }
        "container" | "logistic-container" => {
            Ok((skip_container(reader).is_ok(), None, false, None))
        }
        "underground-belt" => {
            let belt_type = reader.read_u8()?;
            Ok((true, None, false, Some(belt_type)))
        }
        _ => Ok((false, None, false, None)),
    }
}

/// Parse a single entity from the stream.
/// Returns None if the entity type is unknown (caller should use recovery).
fn parse_entity(
    reader: &mut BinaryReader,
    entity_type: &str,
    last_pos: &mut (i32, i32),
) -> Result<Option<((i32, i32), Option<u32>, bool, Option<u8>)>> {
    let position = read_map_position_delta(reader, last_pos)?;
    let flags = reader.read_u16_le()?;

    let class = entity_class_for_type(entity_type);
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

    let (ok, amount, infinite, underground_type) = skip_type_specific(reader, entity_type)?;
    if !ok {
        return Ok(None);
    }

    Ok(Some((position, amount, infinite, underground_type)))
}

/// Skip the military targets and active entities sections between "/T" and the entity loop.
pub fn skip_pre_entity_sections(reader: &mut BinaryReader) -> Result<()> {
    let mil_count = reader.read_opt_u32()? as usize;
    for _ in 0..mil_count {
        let n = reader.read_opt_u32()? as usize;
        reader.skip(n)?;
    }

    let enemy_count = reader.read_opt_u32()? as usize;
    for _ in 0..enemy_count {
        let n = reader.read_opt_u32()? as usize;
        reader.skip(n)?;
    }

    let n = reader.read_opt_u32()? as usize;
    reader.skip(n)?;

    let n = reader.read_opt_u32()? as usize;
    reader.skip(n)?;

    Ok(())
}

/// Parse all entities from a chunk's entity section (data starting right after "/T" marker).
pub fn parse_chunk_entities(
    data: &[u8],
    chunk_x: i32,
    chunk_y: i32,
    entity_prototypes: &HashMap<u16, String>,
    entity_groups: &HashMap<u16, String>,
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
                if !recover_to_next_entity(&mut reader, data, &last_pos, entity_prototypes, chunk_x, chunk_y) {
                    break;
                }
                continue;
            }
        };

        let entity_type = entity_groups.get(&proto_id)
            .map(|s| s.as_str())
            .unwrap_or("");

        match parse_entity(&mut reader, entity_type, &mut last_pos) {
            Ok(Some((position, resource_amount, resource_infinite, underground_type))) => {
                entities.push(EntityParseResult {
                    position,
                    proto_id,
                    name: entity_name,
                    resource_amount,
                    resource_infinite,
                    underground_type,
                });
            }
            Ok(None) => {
                entities.push(EntityParseResult {
                    position: last_pos,
                    proto_id,
                    name: entity_name,
                    resource_amount: None,
                    resource_infinite: false,
                    underground_type: None,
                });
                if !recover_to_next_entity(&mut reader, data, &last_pos, entity_prototypes, chunk_x, chunk_y) {
                    break;
                }
            }
            Err(_) => break,
        }
    }

    entities
}

/// Scan forward from current position to find the next valid proto_id with position validation.
fn recover_to_next_entity(
    reader: &mut BinaryReader,
    data: &[u8],
    last_pos: &(i32, i32),
    entity_prototypes: &HashMap<u16, String>,
    chunk_x: i32,
    chunk_y: i32,
) -> bool {
    let start = reader.position();
    let chunk_center_x = chunk_x * 32 * 256 + 16 * 256;
    let chunk_center_y = chunk_y * 32 * 256 + 16 * 256;

    for offset in 0..256 {
        let pos = start + offset;
        if pos + 2 > data.len() {
            break;
        }
        let candidate = u16::from_le_bytes([data[pos], data[pos + 1]]);

        if candidate == 0 {
            reader.set_position(pos);
            return true;
        }
        if !entity_prototypes.contains_key(&candidate) {
            continue;
        }

        if pos + 6 > data.len() {
            continue;
        }
        let dx = i16::from_le_bytes([data[pos + 2], data[pos + 3]]);
        if dx == 0x7FFF {
            if pos + 12 > data.len() {
                continue;
            }
            let x = i32::from_le_bytes([data[pos + 4], data[pos + 5], data[pos + 6], data[pos + 7]]);
            let y = i32::from_le_bytes([data[pos + 8], data[pos + 9], data[pos + 10], data[pos + 11]]);
            if (x - chunk_center_x).abs() > 64 * 256 {
                continue;
            }
            if (y - chunk_center_y).abs() > 64 * 256 {
                continue;
            }
        } else {
            let dy = i16::from_le_bytes([data[pos + 4], data[pos + 5]]);
            let new_x = last_pos.0.wrapping_add(dx as i32);
            let new_y = last_pos.1.wrapping_add(dy as i32);
            if (new_x - chunk_center_x).abs() > 64 * 256 {
                continue;
            }
            if (new_y - chunk_center_y).abs() > 64 * 256 {
                continue;
            }
        }

        reader.set_position(pos);
        return true;
    }
    false
}
