use std::io::{Read, Cursor};
use std::collections::HashMap;
use flate2::read::ZlibDecoder;
use factorio_client::codec::{BinaryReader, parse_map_data};
use factorio_client::codec::entity_parsers::skip_pre_entity_sections;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    unsafe { std::env::set_var("FACTORIO_NO_PROCEDURAL", "1"); }
    let data = std::fs::read("downloaded_map.zip")?;
    let map_data = parse_map_data(&data)?;
    
    let entity_protos = map_data.prototype_mappings.tables.get("Entity").unwrap();
    let entity_groups = &map_data.prototype_mappings.entity_groups;
    
    // Print proto IDs for the entities we expect in this chunk
    for (id, name) in entity_protos.iter() {
        if name.contains("crash-site") || name == "stone-furnace" || name == "character" {
            let group = entity_groups.get(id).map(|s| s.as_str()).unwrap_or("?");
            println!("proto {:4} = {:40} group={}", id, name, group);
        }
    }
    
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
    
    // Find chunk 73 (crash-site chunk)
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
        
        println!("\n=== Chunk 73 entity section: {} bytes ===", entity_data.len());
        let mut reader = BinaryReader::new(entity_data);
        let _ = skip_pre_entity_sections(&mut reader);
        let start = reader.position();
        println!("Entity loop starts at byte {}", start);
        
        // Dump all entity data (hex + ascii)
        let data_slice = &entity_data[start..];
        println!("Dumping {} bytes of entity data:", data_slice.len());
        for row in 0..(data_slice.len() + 31) / 32 {
            let s = row * 32;
            let e = (s + 32).min(data_slice.len());
            print!("{:4}: ", s);
            for i in s..e { print!("{:02x} ", data_slice[i]); }
            for _ in e..s+32 { print!("   "); }
            print!(" |");
            for i in s..e {
                let c = data_slice[i];
                if c >= 0x20 && c < 0x7f { print!("{}", c as char); }
                else { print!("."); }
            }
            println!("|");
            if row > 30 { println!("...truncated"); break; }
        }
        
        // Search for known proto IDs in the data
        println!("\nSearching for known proto IDs in entity data:");
        for offset in 0..data_slice.len()-1 {
            let val = u16::from_le_bytes([data_slice[offset], data_slice[offset+1]]);
            if let Some(name) = entity_protos.get(&val) {
                if name.contains("crash-site") || name == "stone-furnace" || name == "character" 
                    || name == "iron-plate" || name == "wooden-chest" {
                    println!("  offset {:4}: u16={:4} (0x{:04x}) = {}", offset, val, val, name);
                }
            }
        }
    }
    
    Ok(())
}

fn decompress(data: &[u8]) -> Result<Vec<u8>, std::io::Error> {
    let mut decoder = ZlibDecoder::new(data);
    let mut out = Vec::new();
    decoder.read_to_end(&mut out)?;
    Ok(out)
}
