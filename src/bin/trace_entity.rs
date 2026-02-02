use std::io::{Cursor, Read};
use factorio_client::codec::{BinaryReader, parse_map_data};
use factorio_client::codec::entity_parsers::skip_pre_entity_sections;

mod bin_util;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    unsafe { std::env::set_var("FACTORIO_NO_PROCEDURAL", "1"); }
    let data = std::fs::read("downloaded_map.zip")?;
    let map_data = parse_map_data(&data)?;

    let entity_protos = map_data.prototype_mappings.tables.get("Entity").unwrap();
    let entity_groups = &map_data.prototype_mappings.entity_groups;

    // Load full stream
    let cursor = Cursor::new(&data);
    let mut archive = zip::ZipArchive::new(cursor)?;
    let mut full_stream = Vec::new();
    for i in 0..20 {
        for idx in 0..archive.len() {
            let mut file = archive.by_index(idx)?;
            let name = file.name().to_string();
            if name.contains(&format!("level.dat{}", i)) && !name.contains("level-init") {
                let mut raw = Vec::new();
                file.read_to_end(&mut raw)?;
                if let Ok(decompressed) = decompress(&raw) {
                    full_stream.extend_from_slice(&decompressed);
                } else {
                    full_stream.extend_from_slice(&raw);
                }
                break;
            }
        }
    }

    // Find chunks
    let mut chunks = Vec::new();
    for i in 0..full_stream.len().saturating_sub(10) {
        if full_stream[i] != 0x43 || full_stream[i + 1] != 0x3a { continue; }
        let chunk_index = u32::from_le_bytes([full_stream[i+2], full_stream[i+3], full_stream[i+4], full_stream[i+5]]);
        let tile_blob_len = u16::from_le_bytes([full_stream[i + 6], full_stream[i + 7]]) as usize;
        if tile_blob_len == 0 || tile_blob_len > 0x1000 { continue; }
        let tile_end = i + 8 + tile_blob_len;
        if tile_end + 2 > full_stream.len() { continue; }
        if full_stream[tile_end] != 0x2f || full_stream[tile_end + 1] != 0x54 { continue; }
        chunks.push((i, tile_end + 2, chunk_index));
    }
    chunks.sort_by_key(|(start, _, _)| *start);

    // Find chunk 73
    for (idx, &(_, entity_start, chunk_index)) in chunks.iter().enumerate() {
        if chunk_index != 73 { continue; }
        let entity_end = chunks.get(idx + 1)
            .map(|(next_start, _, _)| *next_start)
            .unwrap_or(full_stream.len());
        let entity_data = &full_stream[entity_start..entity_end];

        println!("=== Chunk 73 entity section: {} bytes ===", entity_data.len());
        let mut reader = BinaryReader::new(entity_data);
        let _ = skip_pre_entity_sections(&mut reader);
        let start = reader.position();
        println!("Entity loop starts at byte {}", start);

        let data_slice = &entity_data[start..];
        println!("Total entity data: {} bytes\n", data_slice.len());

        // Dump first 300 bytes with proto ID annotations
        let dump_len = data_slice.len().min(300);
        for row in 0..(dump_len + 15) / 16 {
            let s = row * 16;
            let e = (s + 16).min(dump_len);
            print!("{:4}: ", s);
            for i in s..e { print!("{:02x} ", data_slice[i]); }
            for _ in e..s+16 { print!("   "); }
            print!(" |");
            for i in s..e {
                let c = data_slice[i];
                if c >= 0x20 && c < 0x7f { print!("{}", c as char); }
                else { print!("."); }
            }
            println!("|");
        }

        // Search for proto IDs
        println!("\nAll proto IDs found in first 300 bytes:");
        for offset in 0..dump_len.saturating_sub(1) {
            let val = u16::from_le_bytes([data_slice[offset], data_slice[offset+1]]);
            if let Some(name) = entity_protos.get(&val) {
                let group = entity_groups.get(&val).map(|s| s.as_str()).unwrap_or("?");
                println!("  offset {:4}: proto {:4} = {} [{}]", offset, val, name, group);
            }
        }

        // Now try to manually parse, printing every byte
        println!("\n=== Manual parse trace ===");
        let mut pos = 0usize;
        let mut last_pos: (i32, i32) = (-1 * 32 * 256, 0 * 32 * 256); // chunk (-1, 0)
        let mut ent_num = 0;

        while pos + 2 <= data_slice.len() && ent_num < 10 {
            let proto_id = u16::from_le_bytes([data_slice[pos], data_slice[pos+1]]);
            if proto_id == 0 { println!("  [terminator at {}]", pos); break; }

            let name = entity_protos.get(&proto_id).map(|s| s.as_str()).unwrap_or("???");
            let group = entity_groups.get(&proto_id).map(|s| s.as_str()).unwrap_or("?");
            println!("\n--- Entity {} at offset {} ---", ent_num, pos);
            println!("  proto_id={} name={} group={}", proto_id, name, group);
            pos += 2;

            // Position delta
            let dx = i16::from_le_bytes([data_slice[pos], data_slice[pos+1]]);
            pos += 2;
            if dx == 0x7FFF {
                let x = i32::from_le_bytes([data_slice[pos], data_slice[pos+1], data_slice[pos+2], data_slice[pos+3]]);
                pos += 4;
                let y = i32::from_le_bytes([data_slice[pos], data_slice[pos+1], data_slice[pos+2], data_slice[pos+3]]);
                pos += 4;
                last_pos = (x, y);
                println!("  pos: absolute ({}, {}) = ({:.2}, {:.2})", x, y, x as f64/256.0, y as f64/256.0);
            } else {
                let dy = i16::from_le_bytes([data_slice[pos], data_slice[pos+1]]);
                pos += 2;
                let x = last_pos.0.wrapping_add(dx as i32);
                let y = last_pos.1.wrapping_add(dy as i32);
                last_pos = (x, y);
                println!("  pos: delta ({}, {}) -> ({}, {}) = ({:.2}, {:.2})", dx, dy, x, y, x as f64/256.0, y as f64/256.0);
            }

            // Flags
            let flags = u16::from_le_bytes([data_slice[pos], data_slice[pos+1]]);
            pos += 2;
            println!("  flags: 0x{:04x} (bit13={})", flags, (flags >> 13) & 1);

            // Remaining bytes until next proto or end
            let mut next_entity = data_slice.len();
            for scan in (pos+2)..data_slice.len().min(pos+300) {
                if scan + 1 >= data_slice.len() { break; }
                let candidate = u16::from_le_bytes([data_slice[scan], data_slice[scan+1]]);
                if candidate == 0 || entity_protos.contains_key(&candidate) {
                    // Validate position
                    if candidate == 0 { next_entity = scan; break; }
                    if scan + 6 > data_slice.len() { continue; }
                    let cdx = i16::from_le_bytes([data_slice[scan+2], data_slice[scan+3]]);
                    if cdx == 0x7FFF {
                        if scan + 12 > data_slice.len() { continue; }
                        let cx = i32::from_le_bytes([data_slice[scan+4], data_slice[scan+5], data_slice[scan+6], data_slice[scan+7]]);
                        let cy = i32::from_le_bytes([data_slice[scan+8], data_slice[scan+9], data_slice[scan+10], data_slice[scan+11]]);
                        if (cx + 8192).abs() < 20000 && cy.abs() < 20000 {
                            next_entity = scan;
                            break;
                        }
                    } else {
                        let cdy = i16::from_le_bytes([data_slice[scan+4], data_slice[scan+5]]);
                        let nx = last_pos.0.wrapping_add(cdx as i32);
                        let ny = last_pos.1.wrapping_add(cdy as i32);
                        if (nx + 8192).abs() < 20000 && ny.abs() < 20000 {
                            next_entity = scan;
                            break;
                        }
                    }
                }
            }

            let remaining = next_entity - pos;
            println!("  remaining bytes until next entity: {} (next at offset {})", remaining, next_entity);
            print!("  raw: ");
            for i in pos..next_entity.min(pos+60) {
                print!("{:02x} ", data_slice[i]);
            }
            println!();

            pos = next_entity;
            ent_num += 1;
        }
    }

    Ok(())
}

fn decompress(data: &[u8]) -> Result<Vec<u8>, std::io::Error> {
    bin_util::decompress_zlib(data)
}
