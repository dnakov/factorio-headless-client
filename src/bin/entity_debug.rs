use std::io::{Read, Cursor, Write as IoWrite};
use std::net::TcpStream;
use std::collections::HashMap;
use flate2::read::ZlibDecoder;
use factorio_client::codec::{BinaryReader, parse_map_data};
use factorio_client::codec::entity_parsers::parse_chunk_entities;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let map_path = args.get(1).map(|s| s.as_str()).unwrap_or("server_map.zip");

    unsafe { std::env::set_var("FACTORIO_NO_PROCEDURAL", "1"); }

    println!("=== Entity Debug: {} ===\n", map_path);

    let data = std::fs::read(map_path)?;
    let map_data = parse_map_data(&data)?;

    println!("Map seed: {}", map_data.seed);
    println!("Parsed entities: {}", map_data.entities.len());
    println!("Parsed tiles: {}", map_data.tiles.len());

    // Count entities by type
    let mut by_type: HashMap<&str, usize> = HashMap::new();
    for ent in &map_data.entities {
        *by_type.entry(&ent.name).or_default() += 1;
    }
    let mut type_counts: Vec<_> = by_type.iter().collect();
    type_counts.sort_by(|a, b| b.1.cmp(a.1));
    println!("\nEntity counts by type:");
    for (name, count) in &type_counts {
        println!("  {:30} {}", name, count);
    }

    // Now do detailed chunk-level analysis
    println!("\n=== Chunk-level analysis ===\n");
    let (full_stream, dat_offsets) = load_full_stream_with_offsets(&data)?;
    let entity_protos = map_data.prototype_mappings.tables.get("Entity")
        .cloned().unwrap_or_default();
    let entity_groups = &map_data.prototype_mappings.entity_groups;
    println!("Entity prototypes found: {}", entity_protos.len());
    println!("level.dat file offsets in stream:");
    for (idx, offset, len) in &dat_offsets {
        println!("  level.dat{}: offset={} len={}", idx, offset, len);
    }
    // Print groups present in map
    let mut groups_seen: HashMap<&str, usize> = HashMap::new();
    for (id, _) in &entity_protos {
        let group = entity_groups.get(id).map(|s| s.as_str()).unwrap_or("NO GROUP");
        *groups_seen.entry(group).or_default() += 1;
    }
    let mut group_list: Vec<_> = groups_seen.iter().collect();
    group_list.sort_by(|a, b| b.1.cmp(a.1));
    println!("Entity groups:");
    for (group, count) in group_list.iter().take(30) {
        println!("  {:40} {} protos", group, count);
    }

    // Find chunk boundaries
    let chunks = find_chunk_boundaries(&full_stream);
    println!("Chunks found: {}", chunks.len());
    // Show which dat files chunks are in
    if let Some(first) = chunks.first() {
        let dat_file = dat_offsets.iter().rev()
            .find(|(_, off, _)| first.entity_start >= *off)
            .map(|(idx, _, _)| *idx).unwrap_or(0);
        println!("  First chunk at stream offset {} (level.dat{})", chunks[0].entity_start - 2, dat_file);
    }
    if let Some(last) = chunks.last() {
        let dat_file = dat_offsets.iter().rev()
            .find(|(_, off, _)| last.entity_start >= *off)
            .map(|(idx, _, _)| *idx).unwrap_or(0);
        println!("  Last chunk at stream offset {} (level.dat{})", last.entity_start, dat_file);
    }
    // Count chunks per dat file
    for (idx, off, len) in &dat_offsets {
        let count = chunks.iter().filter(|c| c.entity_start >= *off && c.entity_start < off + len).count();
        if count > 0 {
            println!("  level.dat{}: {} chunks", idx, count);
        }
    }

    let mut total_entities = 0usize;
    let mut chunks_with_entities = 0usize;
    let mut chunks_empty = 0usize;
    let mut per_chunk_details: Vec<ChunkDebug> = Vec::new();

    for (i, chunk) in chunks.iter().enumerate() {
        let entity_data = &full_stream[chunk.entity_start..chunk.entity_end];
        let parsed = parse_chunk_entities(entity_data, chunk.x, chunk.y, &entity_protos, entity_groups);

        let detail = ChunkDebug {
            index: i,
            x: chunk.x,
            y: chunk.y,
            entity_section_len: entity_data.len(),
            entities_parsed: parsed.len(),
            first_entity: parsed.first().map(|e| e.name.clone()),
        };

        if parsed.is_empty() {
            chunks_empty += 1;
        } else {
            chunks_with_entities += 1;
            total_entities += parsed.len();
        }
        per_chunk_details.push(detail);
    }

    println!("\nResults:");
    println!("  Chunks with entities: {}", chunks_with_entities);
    println!("  Chunks empty: {}", chunks_empty);
    println!("  Total entities parsed: {}", total_entities);

    // Show chunks where parsing stops way too early (large section, few entities)
    println!("\nChunks with early parse stops (section>500 but <10 entities):");
    for detail in &per_chunk_details {
        if detail.entity_section_len > 500 && detail.entities_parsed < 10 && detail.entities_parsed > 0 {
            let entity_data = &full_stream[chunks[detail.index].entity_start..chunks[detail.index].entity_end];
            let blocker = find_parse_blocker_verbose(entity_data, detail.x, detail.y, &entity_protos);
            println!("  chunk({:3},{:3}): {} entities, section={} bytes, first={}, blocker={}",
                detail.x, detail.y, detail.entities_parsed,
                detail.entity_section_len,
                detail.first_entity.as_deref().unwrap_or("?"),
                blocker);
        }
    }

    // Dump bytes around terminator for early-stopping chunks
    println!("\n=== Bytes after terminator (first 3 early-stop chunks) ===");
    let mut shown_term = 0;
    for detail in &per_chunk_details {
        if detail.entity_section_len > 500 && detail.entities_parsed > 0 && detail.entities_parsed < 5 && shown_term < 3 {
            let entity_data = &full_stream[chunks[detail.index].entity_start..chunks[detail.index].entity_end];
            // Find terminator position
            let term_pos = find_terminator_pos(entity_data, detail.x, detail.y, &entity_protos);
            if let Some(pos) = term_pos {
                println!("  chunk({},{}) term at byte {}, section={} bytes:", detail.x, detail.y, pos, entity_data.len());
                let start = pos;
                let end = (start + 48).min(entity_data.len());
                print!("    ");
                for b in &entity_data[start..end] { print!("{:02x} ", b); }
                println!();
                // Check if there's a /E marker nearby
                for i in pos..entity_data.len().min(pos + 100) {
                    if i + 1 < entity_data.len() && entity_data[i] == 0x2f && entity_data[i+1] == 0x45 {
                        println!("    /E marker at byte {} (offset +{} from terminator)", i, i - pos);
                        // What's after /E?
                        let after_e = (i + 2..entity_data.len().min(i + 30)).map(|j| format!("{:02x}", entity_data[j])).collect::<Vec<_>>().join(" ");
                        println!("    after /E: {}", after_e);
                        break;
                    }
                }
            }
            shown_term += 1;
        }
    }

    // Show chunks where parsing likely failed (large section but 0 entities)
    println!("\nChunks with large entity sections but 0 entities (likely parse failures):");
    let mut failures: Vec<_> = per_chunk_details.iter()
        .filter(|d| d.entities_parsed == 0 && d.entity_section_len > 50)
        .collect();
    failures.sort_by(|a, b| b.entity_section_len.cmp(&a.entity_section_len));
    for detail in failures.iter().take(10) {
        let entity_data = &full_stream[chunks[detail.index].entity_start..chunks[detail.index].entity_end];
        println!("  chunk({:3},{:3}): section={} bytes", detail.x, detail.y, detail.entity_section_len);
        // Dump first 48 bytes
        let preview = &entity_data[..entity_data.len().min(48)];
        print!("    hex: ");
        for b in preview { print!("{:02x} ", b); }
        println!();
        // Try to show what skip_pre_entity_sections reads
        dump_pre_entity_parse(entity_data);
    }

    // Byte-level analysis: dump first 200 bytes of a tree chunk and a resource chunk
    println!("\n=== Raw hex: first tree chunk ===");
    if let Some(detail) = per_chunk_details.iter().find(|d| d.entities_parsed > 0 && d.first_entity.as_deref().map(|n| n.starts_with("tree-")).unwrap_or(false)) {
        let entity_data = &full_stream[chunks[detail.index].entity_start..chunks[detail.index].entity_end];
        println!("chunk({},{}) section={} bytes, parsed {} entities", detail.x, detail.y, entity_data.len(), detail.entities_parsed);
        dump_hex_annotated(entity_data, detail.x, detail.y, &entity_protos);
    }

    println!("\n=== Raw hex: first resource chunk ===");
    if let Some(detail) = per_chunk_details.iter().find(|d| d.entities_parsed > 5 && d.first_entity.as_deref().map(|n| n.ends_with("-ore")).unwrap_or(false)) {
        let entity_data = &full_stream[chunks[detail.index].entity_start..chunks[detail.index].entity_end];
        println!("chunk({},{}) section={} bytes, parsed {} entities", detail.x, detail.y, entity_data.len(), detail.entities_parsed);
        dump_hex_annotated(entity_data, detail.x, detail.y, &entity_protos);
    }

    // Dump bytes for container entity chunks
    println!("\n=== Container entity bytes ===");
    for detail in &per_chunk_details {
        if detail.entities_parsed > 0 {
            let first = detail.first_entity.as_deref().unwrap_or("");
            if first.contains("crash-site") || first.contains("container") {
                let entity_data = &full_stream[chunks[detail.index].entity_start..chunks[detail.index].entity_end];
                println!("chunk({},{}) section={} bytes, parsed={}", detail.x, detail.y, entity_data.len(), detail.entities_parsed);
                // Show first 200 bytes after pre-entity sections
                let mut r = BinaryReader::new(entity_data);
                let _ = factorio_client::codec::entity_parsers::skip_pre_entity_sections(&mut r);
                let start = r.position();
                let end = (start + 200).min(entity_data.len());
                for row in 0..((end - start + 15) / 16) {
                    let s = start + row * 16;
                    let e = (s + 16).min(end);
                    print!("  {:4}: ", s);
                    for i in s..e { print!("{:02x} ", entity_data[i]); }
                    println!();
                }
            }
        }
    }

    // Identify which entity types cause parsing to stop
    println!("\n=== Entity types causing parse stops ===");
    let mut stop_causes: HashMap<String, usize> = HashMap::new();
    let mut total_lost = 0usize;
    for detail in &per_chunk_details {
        let entity_data = &full_stream[chunks[detail.index].entity_start..chunks[detail.index].entity_end];
        if let Some((blocker, remaining)) = find_parse_blocker(entity_data, detail.x, detail.y, &entity_protos) {
            *stop_causes.entry(blocker).or_default() += 1;
            total_lost += remaining;
        }
    }
    let mut causes: Vec<_> = stop_causes.iter().collect();
    causes.sort_by(|a, b| b.1.cmp(a.1));
    println!("Entities lost to unknown types: ~{}", total_lost);
    for (name, count) in &causes {
        println!("  {:40} stops {} chunks", name, count);
    }

    // Dump all unhandled entity prototypes
    println!("\n=== Unhandled Entity Prototypes ===\n");
    let mut handled = Vec::new();
    let mut unhandled = Vec::new();
    for (id, name) in &entity_protos {
        if is_type_handled(name) {
            handled.push((*id, name.as_str()));
        } else {
            unhandled.push((*id, name.as_str()));
        }
    }
    unhandled.sort_by_key(|(_, name)| *name);
    println!("Handled: {} types, Unhandled: {} types", handled.len(), unhandled.len());
    for (id, name) in &unhandled {
        println!("  {:4}  {}", id, name);
    }

    // RCON comparison
    println!("\n=== RCON Ground Truth ===\n");
    // Check server chunk count
    match rcon_command("/c local s=game.surfaces[1] local n=0 for _ in s.get_chunks() do n=n+1 end rcon.print(n)") {
        Ok(resp) => println!("Server chunks: {} (map has {})", resp.trim(), chunks.len()),
        Err(e) => println!("  chunk query failed: {}", e),
    }
    // Count entities only within our map bounds (±100 tiles)
    match rcon_command("/c local s=game.surfaces[1] local n=0 for _,e in pairs(s.find_entities_filtered{area={{-100,-100},{100,100}}}) do n=n+1 end rcon.print(n)") {
        Ok(resp) => println!("Entities within ±100 tiles: {} (our parse: {})", resp.trim(), total_entities),
        Err(e) => println!("  bounded query failed: {}", e),
    }
    match rcon_entity_counts() {
        Ok(counts) => {
            println!("Server entity counts:");
            let mut rcon_types: Vec<_> = counts.iter().collect();
            rcon_types.sort_by(|a, b| b.1.cmp(a.1));
            for (name, count) in rcon_types.iter().take(20) {
                let parsed = by_type.get(name.as_str()).copied().unwrap_or(0);
                let pct = if *count > &0 { parsed as f64 / **count as f64 * 100.0 } else { 0.0 };
                println!("  {:30} server={:5}  parsed={:5}  ({:.0}%)", name, count, parsed, pct);
            }
        }
        Err(e) => println!("  RCON failed: {} (server may not be running)", e),
    }

    Ok(())
}

struct ChunkInfo {
    x: i32,
    y: i32,
    entity_start: usize,
    entity_end: usize,
}

struct ChunkDebug {
    index: usize,
    x: i32,
    y: i32,
    entity_section_len: usize,
    entities_parsed: usize,
    first_entity: Option<String>,
}

fn load_full_stream_with_offsets(zip_data: &[u8]) -> Result<(Vec<u8>, Vec<(usize, usize, usize)>), Box<dyn std::error::Error>> {
    let cursor = Cursor::new(zip_data);
    let mut archive = zip::ZipArchive::new(cursor)?;

    let mut chunks: Vec<(usize, Vec<u8>)> = Vec::new();
    for i in 0..20 {
        for idx in 0..archive.len() {
            let mut file = archive.by_index(idx)?;
            let name = file.name().to_string();
            if name.contains(&format!("level.dat{}", i)) && !name.contains("level-init") {
                let mut raw = Vec::new();
                file.read_to_end(&mut raw)?;
                if let Ok(decompressed) = decompress(&raw) {
                    chunks.push((i, decompressed));
                } else {
                    chunks.push((i, raw));
                }
                break;
            }
        }
    }
    chunks.sort_by_key(|(idx, _)| *idx);
    let mut offsets = Vec::new();
    let mut pos = 0usize;
    for (idx, data) in &chunks {
        offsets.push((*idx, pos, data.len()));
        pos += data.len();
    }
    Ok((chunks.into_iter().flat_map(|(_, data)| data).collect(), offsets))
}

fn decompress(data: &[u8]) -> Result<Vec<u8>, std::io::Error> {
    let mut decoder = ZlibDecoder::new(data);
    let mut out = Vec::new();
    decoder.read_to_end(&mut out)?;
    Ok(out)
}

fn extract_entity_prototypes(data: &[u8]) -> HashMap<u16, String> {
    // Look for the entity prototype table by finding known entity names
    // Format: opt_u32 count, then [opt_u32 name_len, name bytes, u16 id] repeated
    let mut protos = HashMap::new();

    // Scan for "Entity" table marker - look for prototype table format
    for i in 0..data.len().saturating_sub(20) {
        // Look for length-prefixed strings followed by u16 IDs
        // Entity names we know: "tree-01", "iron-ore", "coal", etc.
        if i + 10 < data.len() {
            let len = data[i] as usize;
            if len >= 3 && len <= 50 && i + 1 + len + 2 <= data.len() {
                let name_bytes = &data[i + 1..i + 1 + len];
                if name_bytes.iter().all(|b| b.is_ascii_alphanumeric() || *b == b'-' || *b == b'_') {
                    let name = String::from_utf8_lossy(name_bytes).to_string();
                    if is_known_entity(&name) {
                        let id = u16::from_le_bytes([data[i + 1 + len], data[i + 1 + len + 1]]);
                        if id > 0 && id < 1000 {
                            protos.insert(id, name);
                        }
                    }
                }
            }
        }
    }
    protos
}

fn is_known_entity(name: &str) -> bool {
    name.ends_with("-ore") || name.starts_with("tree-") || name.starts_with("dead-")
        || name.contains("rock") || name == "coal" || name == "stone"
        || name == "fish" || name == "crude-oil" || name == "uranium-ore"
        || name.contains("remnants") || name.starts_with("simple-entity")
        || name.contains("inserter") || name.contains("belt")
        || name.contains("furnace") || name.contains("assembling")
        || name.contains("chest") || name.contains("pipe")
        || name.contains("pole") || name.contains("lamp")
        || name.contains("splitter") || name.contains("loader")
        || name == "character" || name.contains("biter")
        || name.contains("spitter") || name.contains("worm")
        || name.contains("spawner")
}

fn find_chunk_boundaries(data: &[u8]) -> Vec<ChunkInfo> {
    let mut chunks = Vec::new();
    let mut chunk_starts = Vec::new();

    for i in 0..data.len().saturating_sub(10) {
        if data[i] != 0x43 || data[i + 1] != 0x3a {
            continue;
        }
        let chunk_index = u32::from_le_bytes([data[i+2], data[i+3], data[i+4], data[i+5]]);
        let tile_blob_len = u16::from_le_bytes([data[i + 6], data[i + 7]]) as usize;
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
        chunk_starts.push((i, tile_end + 2, chunk_index));
    }

    chunk_starts.sort_by_key(|(start, _, _)| *start);

    for ordinal in 0..chunk_starts.len() {
        let (_chunk_start, entity_start, chunk_index) = chunk_starts[ordinal];
        let entity_end = chunk_starts.get(ordinal + 1)
            .map(|(next_start, _, _)| *next_start)
            .unwrap_or(data.len());

        // Derive chunk x/y from index (assume square grid)
        let total = chunk_starts.len();
        let side = (total as f64).sqrt().ceil() as i32;
        let half = side / 2;
        let x = (ordinal as i32 / side) - half;
        let y = (ordinal as i32 % side) - half;

        chunks.push(ChunkInfo {
            x,
            y,
            entity_start,
            entity_end,
        });
        let _ = chunk_index; // suppress warning
    }

    chunks
}

fn dump_pre_entity_parse(data: &[u8]) {
    let mut reader = BinaryReader::new(data);
    // Try to read pre-entity sections
    match reader.read_opt_u32() {
        Ok(mil_count) => {
            print!("    mil_targets={}", mil_count);
            let mut ok = true;
            for _ in 0..mil_count {
                match reader.read_opt_u32() {
                    Ok(n) => {
                        if reader.remaining() < n as usize {
                            print!(" (overflow at mil vec n={})", n);
                            ok = false;
                            break;
                        }
                        let _ = reader.skip(n as usize);
                    }
                    Err(_) => { ok = false; break; }
                }
            }
            if !ok { println!(); return; }

            match reader.read_opt_u32() {
                Ok(enemy_count) => {
                    print!(" enemies={}", enemy_count);
                    for _ in 0..enemy_count {
                        match reader.read_opt_u32() {
                            Ok(n) => {
                                if reader.remaining() < n as usize {
                                    print!(" (overflow at enemy vec n={})", n);
                                    println!();
                                    return;
                                }
                                let _ = reader.skip(n as usize);
                            }
                            Err(_) => { println!(); return; }
                        }
                    }
                }
                Err(_) => { println!(); return; }
            }

            // ActiveEntities mode=2
            match reader.read_opt_u32() {
                Ok(n) => {
                    print!(" active2={}", n);
                    if reader.remaining() >= n as usize {
                        let _ = reader.skip(n as usize);
                    } else {
                        print!(" (overflow)");
                        println!();
                        return;
                    }
                }
                Err(_) => { println!(); return; }
            }

            // ActiveEntities mode=4
            match reader.read_opt_u32() {
                Ok(n) => {
                    print!(" active4={}", n);
                    if reader.remaining() >= n as usize {
                        let _ = reader.skip(n as usize);
                    } else {
                        print!(" (overflow)");
                        println!();
                        return;
                    }
                }
                Err(_) => { println!(); return; }
            }

            println!(" | pos={} remaining={}", reader.position(), reader.remaining());
            // Show first u16 after pre-entity
            if reader.remaining() >= 2 {
                let proto = reader.read_u16_le().unwrap();
                println!("    first proto_id={} ({})", proto,
                    if proto == 0 { "terminator".to_string() }
                    else { format!("unknown/valid") });
            }
        }
        Err(e) => println!("    pre-entity parse failed at byte 0: {:?}", e),
    }
}

fn dump_successful_parse(data: &[u8], chunk_x: i32, chunk_y: i32, protos: &HashMap<u16, String>) {
    let mut reader = BinaryReader::new(data);
    println!("  chunk({},{}) section_len={}", chunk_x, chunk_y, data.len());

    // Read pre-entity sections
    let mil = reader.read_opt_u32().unwrap_or(0);
    print!("  pre-entity: mil={}", mil);
    for _ in 0..mil {
        let n = reader.read_opt_u32().unwrap_or(0);
        let _ = reader.skip(n as usize);
    }
    let enemies = reader.read_opt_u32().unwrap_or(0);
    print!(" enemies={}", enemies);
    for _ in 0..enemies {
        let n = reader.read_opt_u32().unwrap_or(0);
        let _ = reader.skip(n as usize);
    }
    let a2 = reader.read_opt_u32().unwrap_or(0);
    print!(" active2={}", a2);
    let _ = reader.skip(a2 as usize);
    let a4 = reader.read_opt_u32().unwrap_or(0);
    print!(" active4={}", a4);
    let _ = reader.skip(a4 as usize);
    println!(" | entity_loop starts at byte {}", reader.position());

    // Parse entities one by one with byte tracking
    let mut last_pos = (chunk_x * 32 * 256, chunk_y * 32 * 256);
    let mut count = 0;
    loop {
        if reader.remaining() < 2 { break; }
        let before = reader.position();
        let proto_id = reader.read_u16_le().unwrap();
        if proto_id == 0 { println!("  terminator at byte {}", before); break; }

        let name = protos.get(&proto_id).cloned().unwrap_or_else(|| format!("?proto_{}", proto_id));

        // Read position
        let pos_before = reader.position();
        let dx = reader.read_i16_le().unwrap_or(0);
        let pos = if dx == 0x7FFF {
            let x = reader.read_i32_le().unwrap_or(0);
            let y = reader.read_i32_le().unwrap_or(0);
            last_pos = (x, y);
            (x, y)
        } else {
            let dy = reader.read_i16_le().unwrap_or(0);
            let x = last_pos.0.wrapping_add(dx as i32);
            let y = last_pos.1.wrapping_add(dy as i32);
            last_pos = (x, y);
            (x, y)
        };

        let flags = reader.read_u16_le().unwrap_or(0);
        let after = reader.position();

        if count < 5 {
            println!("  [{}] proto={} name={:20} pos=({:.1},{:.1}) flags=0x{:04x} bytes={}-{}",
                count, proto_id, name,
                pos.0 as f64 / 256.0, pos.1 as f64 / 256.0,
                flags, pos_before, after);
        }
        count += 1;

        // Skip remainder (health, type-specific) - just break since we're debugging
        // Actually let's try to parse fully
        let class = entity_class_name(&name);
        if class >= 1 && flags & (1 << 13) != 0 {
            let _ = reader.skip(8); // health + damage
        }
        if class >= 2 {
            let _ = reader.skip(1); // quality
        }
        // type-specific
        if !skip_type_debug(&mut reader, &name) {
            println!("  STOPPED at entity {} ({}) - unknown type, byte {}", count, name, reader.position());
            break;
        }
    }
    println!("  total entities in chunk: {}, consumed {}/{} bytes",
        count, reader.position(), data.len());
}

fn entity_class_name(name: &str) -> u8 {
    // 0=Entity, 1=EntityWithHealth, 2=EntityWithOwner
    match name {
        n if n.ends_with("-ore") || n == "crude-oil" || n == "uranium-ore"
            || n == "coal" || n == "stone" => 0,
        n if n.contains("inserter") || n.contains("belt") || n.contains("furnace")
            || n.contains("assembling") || n.contains("chest") || n.contains("pipe")
            || n.contains("pole") || n.contains("drill") || n.contains("pump")
            || n.contains("turret") || n.contains("wall") || n.contains("gate")
            || n.contains("radar") || n.contains("roboport") || n.contains("lab")
            || n.contains("beacon") || n.contains("reactor") || n.contains("lamp")
            || n.contains("combinator") || n.contains("rail") || n.contains("wagon")
            || n.contains("car") || n.contains("locomotive") || n.contains("splitter")
            || n.contains("loader") || n.contains("substation") || n.contains("accumulator")
            || n.contains("solar-panel") || n.contains("generator") || n.contains("boiler")
            || n.contains("heat-pipe") || n.contains("speaker") || n.contains("power-switch")
            || n.contains("rocket-silo") || n.contains("offshore-pump")
            || n.contains("simple-entity-with") || n == "character"
            || n == "tank" || n.contains("spidertron") || n.contains("container") => 2,
        _ => 1,
    }
}

fn skip_type_debug(reader: &mut BinaryReader, name: &str) -> bool {
    match name {
        n if n.ends_with("-ore") || n == "crude-oil" || n == "uranium-ore"
            || n == "coal" || n == "stone" => {
            let _ = reader.skip(4); // amount
            if let Ok(infinite) = reader.read_u8() {
                if infinite != 0 { let _ = reader.skip(4); }
            }
            let _ = reader.skip(1); // stage
            true
        }
        n if n.starts_with("tree-") || n.ends_with("-tree") => {
            let _ = reader.skip(3); // u8 variation + u16 stage_info
            true
        }
        n if n.starts_with("dead-") => {
            let _ = reader.skip(1); // u8 graphics_variation
            true
        }
        n if n.starts_with("simple-entity") && !n.contains("with") => {
            let _ = reader.skip(1);
            true
        }
        n if n.contains("simple-entity-with") => {
            let _ = reader.skip(7);
            true
        }
        "fish" => {
            let _ = reader.skip(1);
            true
        }
        n if n.contains("rock") && !n.contains("rocket") => {
            let _ = reader.skip(1);
            true
        }
        _ => false,
    }
}

/// Find the byte position of the terminator (proto_id=0) in a chunk's entity section
fn find_terminator_pos(data: &[u8], chunk_x: i32, chunk_y: i32, protos: &HashMap<u16, String>) -> Option<usize> {
    let mut reader = BinaryReader::new(data);
    let _ = reader.read_opt_u32().ok()?;
    let _ = reader.read_opt_u32().ok()?;
    let _ = reader.read_opt_u32().ok()?;
    let _ = reader.read_opt_u32().ok()?;

    let mut last_pos = (chunk_x * 32 * 256, chunk_y * 32 * 256);
    loop {
        if reader.remaining() < 2 { return None; }
        let pos = reader.position();
        let proto_id = reader.read_u16_le().ok()?;
        if proto_id == 0 { return Some(pos); }

        let name = protos.get(&proto_id).cloned().unwrap_or_default();
        let dx = reader.read_i16_le().ok()?;
        if dx == 0x7FFF {
            let x = reader.read_i32_le().ok()?;
            let y = reader.read_i32_le().ok()?;
            last_pos = (x, y);
        } else {
            let dy = reader.read_i16_le().ok()?;
            last_pos = (last_pos.0.wrapping_add(dx as i32), last_pos.1.wrapping_add(dy as i32));
        }
        let flags = reader.read_u16_le().ok()?;
        let class = entity_class_name(&name);
        if class >= 1 && (flags & (1 << 13)) != 0 { let _ = reader.skip(8); }
        if class >= 2 { let _ = reader.read_u8(); }
        if !skip_type_debug(&mut reader, &name) { return None; }
    }
}

/// Verbose version - returns a string description of what stops parsing
fn find_parse_blocker_verbose(data: &[u8], chunk_x: i32, chunk_y: i32, protos: &HashMap<u16, String>) -> String {
    let mut reader = BinaryReader::new(data);
    // Skip pre-entity
    if reader.read_opt_u32().is_err() { return "pre-entity read failed".to_string(); }
    if reader.read_opt_u32().is_err() { return "pre-entity read failed".to_string(); }
    if reader.read_opt_u32().is_err() { return "pre-entity read failed".to_string(); }
    if reader.read_opt_u32().is_err() { return "pre-entity read failed".to_string(); }

    let mut last_pos = (chunk_x * 32 * 256, chunk_y * 32 * 256);
    let mut count = 0;
    loop {
        if reader.remaining() < 2 { return format!("end of data after {} entities", count); }
        let proto_id = match reader.read_u16_le() {
            Ok(id) => id,
            Err(_) => return format!("read error after {} entities", count),
        };
        if proto_id == 0 { return format!("terminator after {} entities (byte {})", count, reader.position() - 2); }

        let name = protos.get(&proto_id).cloned()
            .unwrap_or_else(|| format!("unknown_proto_{}", proto_id));

        // Position
        if reader.remaining() < 2 { return format!("pos read fail on '{}'", name); }
        let dx = reader.read_i16_le().unwrap_or(0);
        if dx == 0x7FFF {
            if reader.remaining() < 8 { return format!("abs pos read fail on '{}'", name); }
            let x = reader.read_i32_le().unwrap_or(0);
            let y = reader.read_i32_le().unwrap_or(0);
            last_pos = (x, y);
        } else {
            if reader.remaining() < 2 { return format!("dy read fail on '{}'", name); }
            let dy = reader.read_i16_le().unwrap_or(0);
            last_pos = (last_pos.0.wrapping_add(dx as i32), last_pos.1.wrapping_add(dy as i32));
        }

        if reader.remaining() < 2 { return format!("flags read fail on '{}'", name); }
        let flags = reader.read_u16_le().unwrap_or(0);

        let class = entity_class_name(&name);
        if class >= 1 && (flags & (1 << 13)) != 0 {
            if reader.remaining() < 8 { return format!("health skip fail on '{}'", name); }
            let _ = reader.skip(8);
        }
        if class >= 2 {
            if reader.remaining() < 1 { return format!("quality read fail on '{}'", name); }
            let _ = reader.read_u8();
        }

        if !skip_type_debug(&mut reader, &name) {
            return format!("unhandled type '{}' (proto={}) at byte {}", name, proto_id, reader.position());
        }
        count += 1;
    }
}

/// Find the entity type that causes parsing to stop in a chunk.
/// Returns (entity_name, estimated_remaining_entities) or None if chunk parsed fully.
fn find_parse_blocker(data: &[u8], chunk_x: i32, chunk_y: i32, protos: &HashMap<u16, String>) -> Option<(String, usize)> {
    let mut reader = BinaryReader::new(data);
    // Skip pre-entity sections
    for _ in 0..4 {
        let n = reader.read_opt_u32().ok()? as usize;
        if n > 0 {
            for _ in 0..n {
                let inner = reader.read_opt_u32().ok()? as usize;
                if reader.remaining() < inner { return None; }
                let _ = reader.skip(inner);
            }
        }
    }

    let mut last_pos = (chunk_x * 32 * 256, chunk_y * 32 * 256);
    loop {
        if reader.remaining() < 2 { return None; }
        let proto_id = reader.read_u16_le().ok()?;
        if proto_id == 0 { return None; } // Normal termination

        let name = protos.get(&proto_id).cloned()
            .unwrap_or_else(|| format!("unknown_proto_{}", proto_id));

        // Try to parse position
        if reader.remaining() < 4 { return None; }
        let dx = reader.read_i16_le().ok()?;
        if dx == 0x7FFF {
            let x = reader.read_i32_le().ok()?;
            let y = reader.read_i32_le().ok()?;
            last_pos = (x, y);
        } else {
            let dy = reader.read_i16_le().ok()?;
            last_pos = (last_pos.0.wrapping_add(dx as i32), last_pos.1.wrapping_add(dy as i32));
        }

        if reader.remaining() < 2 { return None; }
        let flags = reader.read_u16_le().ok()?;

        let class = entity_class_name(&name);
        if class >= 1 && (flags & (1 << 13)) != 0 {
            if reader.remaining() < 8 { return None; }
            let _ = reader.skip(8);
        }
        if class >= 2 {
            let _ = reader.read_u8().ok()?;
        }

        if !skip_type_debug(&mut reader, &name) {
            // Estimate remaining entities by remaining bytes / avg entity size
            let remaining_est = reader.remaining() / 11;
            return Some((name, remaining_est));
        }
    }
}

fn dump_hex_annotated(data: &[u8], chunk_x: i32, chunk_y: i32, protos: &HashMap<u16, String>) {
    let dump_len = data.len().min(200);
    // Print raw hex with offset
    for row in 0..(dump_len + 15) / 16 {
        let start = row * 16;
        let end = (start + 16).min(dump_len);
        print!("  {:4}: ", start);
        for i in start..end {
            print!("{:02x} ", data[i]);
        }
        // ASCII
        for _ in end..start + 16 { print!("   "); }
        print!(" |");
        for i in start..end {
            let c = data[i];
            if c >= 0x20 && c < 0x7f { print!("{}", c as char); }
            else { print!("."); }
        }
        println!("|");
    }

    // Now try to manually parse and annotate
    println!("  --- Annotation ---");
    let mut reader = BinaryReader::new(data);

    // Pre-entity sections
    let mil = reader.read_opt_u32().unwrap_or(999);
    println!("  byte {}: mil_targets = {}", 0, mil);
    if mil > 100 { println!("  ABORT: mil_targets too large"); return; }
    for i in 0..mil {
        let n = reader.read_opt_u32().unwrap_or(0);
        println!("  byte {}: mil_vec[{}] len={}", reader.position() - 1, i, n);
        if reader.remaining() < n as usize { return; }
        let _ = reader.skip(n as usize);
    }
    let enemies = reader.read_opt_u32().unwrap_or(999);
    println!("  byte {}: enemies = {}", reader.position() - 1, enemies);
    if enemies > 100 { return; }
    for i in 0..enemies {
        let n = reader.read_opt_u32().unwrap_or(0);
        println!("  byte {}: enemy_vec[{}] len={}", reader.position() - 1, i, n);
        if reader.remaining() < n as usize { return; }
        let _ = reader.skip(n as usize);
    }
    let a2 = reader.read_opt_u32().unwrap_or(999);
    println!("  byte {}: active2 len={}", reader.position() - 1, a2);
    if reader.remaining() < a2 as usize { return; }
    let _ = reader.skip(a2 as usize);
    let a4 = reader.read_opt_u32().unwrap_or(999);
    println!("  byte {}: active4 len={}", reader.position() - 1, a4);
    if reader.remaining() < a4 as usize { return; }
    let _ = reader.skip(a4 as usize);

    println!("  --- Entity loop at byte {} ---", reader.position());
    let base_x = chunk_x * 32 * 256;
    let base_y = chunk_y * 32 * 256;
    let mut last_pos = (base_x, base_y);

    for entity_idx in 0..10 {
        if reader.remaining() < 2 { break; }
        let proto_pos = reader.position();
        let proto_id = reader.read_u16_le().unwrap_or(0);
        if proto_id == 0 {
            println!("  byte {}: proto_id=0 (TERMINATOR)", proto_pos);
            break;
        }
        let name = protos.get(&proto_id).cloned().unwrap_or_else(|| format!("?{}", proto_id));
        println!("  byte {}: proto_id={} ({})", proto_pos, proto_id, name);

        // Position
        let pos_start = reader.position();
        if reader.remaining() < 2 { break; }
        let dx = reader.read_i16_le().unwrap_or(0);
        let (px, py) = if dx == 0x7FFF {
            let x = reader.read_i32_le().unwrap_or(0);
            let y = reader.read_i32_le().unwrap_or(0);
            last_pos = (x, y);
            println!("  byte {}: ABSOLUTE pos x={} y={} (tiles: {:.1}, {:.1})",
                pos_start, x, y, x as f64 / 256.0, y as f64 / 256.0);
            (x, y)
        } else {
            let dy = reader.read_i16_le().unwrap_or(0);
            let x = last_pos.0.wrapping_add(dx as i32);
            let y = last_pos.1.wrapping_add(dy as i32);
            last_pos = (x, y);
            println!("  byte {}: DELTA dx={} dy={} -> pos ({:.1}, {:.1})",
                pos_start, dx, dy, x as f64 / 256.0, y as f64 / 256.0);
            (x, y)
        };

        // Flags
        if reader.remaining() < 2 { break; }
        let flags_pos = reader.position();
        let flags = reader.read_u16_le().unwrap_or(0);
        println!("  byte {}: flags=0x{:04x} (bit13={})", flags_pos, flags, (flags >> 13) & 1);

        // Show next 20 bytes raw for manual inspection
        let remaining_start = reader.position();
        let show = reader.remaining().min(20);
        if show > 0 {
            let bytes = &data[remaining_start..remaining_start + show];
            print!("  byte {}: next {} bytes: ", remaining_start, show);
            for b in bytes { print!("{:02x} ", b); }
            println!();
        }

        // Try to consume entity (but show what we'd skip)
        let class = entity_class_name(&name);
        if class >= 1 && (flags & (1 << 13)) != 0 {
            println!("  byte {}: skip health+damage (8 bytes)", reader.position());
            let _ = reader.skip(8);
        }
        if class >= 2 {
            let q = reader.read_u8().unwrap_or(0);
            println!("  byte {}: quality={}", reader.position() - 1, q);
        }
        let type_ok = skip_type_debug(&mut reader, &name);
        println!("  byte {}: after type-specific (ok={})", reader.position(), type_ok);
        if !type_ok {
            println!("  STOPPED: unknown type");
            break;
        }
        let _ = (px, py, entity_idx);
    }
}

fn rcon_command(cmd: &str) -> Result<String, Box<dyn std::error::Error>> {
    let mut stream = TcpStream::connect("127.0.0.1:27015")?;
    stream.set_read_timeout(Some(std::time::Duration::from_secs(5)))?;
    rcon_send(&mut stream, 1, 3, "factorio123")?;
    let _ = rcon_recv(&mut stream)?;
    rcon_send(&mut stream, 2, 2, cmd)?;
    Ok(rcon_recv(&mut stream)?)
}

// Simple RCON client for Factorio
fn rcon_entity_counts() -> Result<HashMap<String, usize>, Box<dyn std::error::Error>> {
    let mut stream = TcpStream::connect("127.0.0.1:27015")?;
    stream.set_read_timeout(Some(std::time::Duration::from_secs(5)))?;

    // Auth
    rcon_send(&mut stream, 1, 3, "factorio123")?;
    let _ = rcon_recv(&mut stream)?;

    // Get entity counts by type
    let cmd = r#"/c local counts = {} for _, e in pairs(game.surfaces[1].find_entities_filtered{}) do counts[e.name] = (counts[e.name] or 0) + 1 end local s = "" for k, v in pairs(counts) do s = s .. k .. "=" .. v .. "\n" end rcon.print(s)"#;
    rcon_send(&mut stream, 2, 2, cmd)?;
    let response = rcon_recv(&mut stream)?;

    let mut counts = HashMap::new();
    for line in response.lines() {
        if let Some((name, count_str)) = line.split_once('=') {
            if let Ok(count) = count_str.trim().parse::<usize>() {
                counts.insert(name.to_string(), count);
            }
        }
    }
    Ok(counts)
}

fn rcon_send(stream: &mut TcpStream, id: i32, msg_type: i32, body: &str) -> std::io::Result<()> {
    let body_bytes = body.as_bytes();
    let length = 4 + 4 + body_bytes.len() as i32 + 2; // id + type + body + 2 nulls
    stream.write_all(&length.to_le_bytes())?;
    stream.write_all(&id.to_le_bytes())?;
    stream.write_all(&msg_type.to_le_bytes())?;
    stream.write_all(body_bytes)?;
    stream.write_all(&[0, 0])?;
    stream.flush()
}

fn rcon_recv(stream: &mut TcpStream) -> std::io::Result<String> {
    let mut len_buf = [0u8; 4];
    std::io::Read::read_exact(stream, &mut len_buf)?;
    let length = i32::from_le_bytes(len_buf) as usize;
    let mut buf = vec![0u8; length];
    std::io::Read::read_exact(stream, &mut buf)?;
    // Skip id(4) + type(4), body is the rest minus 2 trailing nulls
    let body = &buf[8..buf.len().saturating_sub(2)];
    Ok(String::from_utf8_lossy(body).to_string())
}

fn is_type_handled(name: &str) -> bool {
    // Mirrors skip_type_specific logic in entity_parsers.rs
    match name {
        n if n.ends_with("-ore") || n == "crude-oil" || n == "uranium-ore"
            || n == "coal" || n == "stone" => true,
        n if n.starts_with("tree-") || n.ends_with("-tree") => true,
        n if n.starts_with("dead-") => true,
        n if n.contains("simple-entity-with") => true,
        "fish" => true,
        n if n.contains("rock") && !n.contains("rocket") => true,
        n if n.starts_with("simple-entity") => true,
        _ => false,
    }
}
