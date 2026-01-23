use std::io::Read;
use factorio_client::codec::{parse_map_data, PrototypeMappings};
use factorio_mapgen::TerrainGenerator;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let path = args.get(1).map(|s| s.as_str()).unwrap_or("server_map.zip");

    let zip_data = std::fs::read(path)?;

    // Use library to get seed + prototype mappings
    let map_data = parse_map_data(&zip_data)?;
    let seed = map_data.seed;
    let mappings = &map_data.prototype_mappings;
    eprintln!("seed={}, tile prototypes={}", seed, mappings.tables.get("TilePrototype").map(|t| t.len()).unwrap_or(0));

    let out_of_map_id = mappings.tile_id_by_name("out-of-map").unwrap_or(143);
    eprintln!("out_of_map_id={}", out_of_map_id);
    eprintln!("surfaces={}, chunks_in_first={}",
        map_data.surfaces.len(),
        map_data.surfaces.first().map(|s| s.chunks.len()).unwrap_or(0));
    if let Some(s) = map_data.surfaces.first() {
        for (i, c) in s.chunks.iter().take(5).enumerate() {
            eprintln!("  prelude[{}]: pos=({},{})", i, c.position.0, c.position.1);
        }
    }

    // Also extract raw blobs from ZIP
    let cursor = std::io::Cursor::new(&zip_data);
    let mut archive = zip::ZipArchive::new(cursor)?;
    let mut all_data = Vec::new();
    for i in 0..archive.len() {
        let mut entry = archive.by_index(i)?;
        let name = entry.name().to_string();
        if !name.contains("level.dat") { continue; }
        let mut raw = Vec::new();
        entry.read_to_end(&mut raw)?;
        let decompressed = decompress(&raw);
        all_data.extend_from_slice(&decompressed);
    }
    eprintln!("total decompressed: {} bytes", all_data.len());

    // Find C:/T blobs
    let mut blobs: Vec<(usize, usize, Vec<u8>)> = Vec::new(); // (chunk_index, offset, blob)
    let mut i = 0;
    while i + 10 < all_data.len() {
        if all_data[i] == 0x43 && all_data[i + 1] == 0x3a {
            let chunk_index = u32::from_le_bytes([all_data[i+2], all_data[i+3], all_data[i+4], all_data[i+5]]) as usize;
            let blob_len = u16::from_le_bytes([all_data[i+6], all_data[i+7]]) as usize;
            if blob_len > 0 && blob_len <= 0x1000 {
                let blob_end = i + 8 + blob_len;
                if blob_end + 2 <= all_data.len() && all_data[blob_end] == 0x2f && all_data[blob_end + 1] == 0x54 {
                    blobs.push((chunk_index, i, all_data[i+8..blob_end].to_vec()));
                    i = blob_end + 2;
                    continue;
                }
            }
        }
        i += 1;
    }
    blobs.sort_by_key(|(_, offset, _)| *offset);
    eprintln!("found {} C:/T blobs", blobs.len());

    // Create terrain generator
    let terrain = TerrainGenerator::new(seed)?;

    // Get chunk positions from surface prelude
    let chunk_positions: Vec<(i32, i32)> = if !map_data.surfaces.is_empty() && !map_data.surfaces[0].chunks.is_empty() {
        let positions: Vec<_> = map_data.surfaces[0].chunks.iter().map(|c| c.position).collect();
        eprintln!("using {} prelude positions", positions.len());
        positions
    } else {
        let total = blobs.len();
        let side = (total as f64).sqrt().ceil() as i32;
        let half = side / 2;
        eprintln!("no prelude, using grid: {}x{}, half={}", side, side, half);
        (0..total).map(|ord| ((ord as i32 / side) - half, (ord as i32 % side) - half)).collect()
    };

    // Try both ordinal-based and chunk_index-based position mappings
    let side = (blobs.len() as f64).sqrt().ceil() as i32;
    let half = side / 2;

    // For chunk_index mapping, try both row-major and column-major
    let pos_by_idx_rowmaj = |idx: usize| -> (i32, i32) {
        ((idx as i32 % side) - half, (idx as i32 / side) - half)
    };
    let pos_by_idx_colmaj = |idx: usize| -> (i32, i32) {
        ((idx as i32 / side) - half, (idx as i32 % side) - half)
    };

    // Test ALL chunks, comparing blob decode vs procedural with different position mappings
    println!("ord  idx  pos_ord    pos_idx_rm pos_idx_cm blob  col_ord  col_irm  col_icm");
    let mut total_col_ord = 0usize;
    let mut total_col_irm = 0usize;
    let mut total_col_icm = 0usize;
    let mut total_filled = 0usize;
    let mut inner_col_ord = 0usize;
    let mut inner_col_irm = 0usize;
    let mut inner_col_icm = 0usize;
    let mut inner_filled = 0usize;

    for (ordinal, (chunk_index, _offset, blob)) in blobs.iter().enumerate() {
        let (chunk_x, chunk_y) = chunk_positions.get(ordinal).copied().unwrap_or((0, 0));

        let (ids_col, filled_col, _) = decode_blob_column_major(&blob, out_of_map_id, mappings);
        let filled_count = filled_col.iter().filter(|&&f| f).count();

        // Try 3 position mappings for this blob
        let pos_ord = (chunk_x, chunk_y);
        let pos_irm = pos_by_idx_rowmaj(*chunk_index);
        let pos_icm = pos_by_idx_colmaj(*chunk_index);

        let proc_ord = terrain.compute_chunk(pos_ord.0, pos_ord.1);
        let proc_irm = terrain.compute_chunk(pos_irm.0, pos_irm.1);
        let proc_icm = terrain.compute_chunk(pos_icm.0, pos_icm.1);

        let match_ord = count_matches(&ids_col, &filled_col, &proc_ord, mappings, &terrain);
        let match_irm = count_matches(&ids_col, &filled_col, &proc_irm, mappings, &terrain);
        let match_icm = count_matches(&ids_col, &filled_col, &proc_icm, mappings, &terrain);

        total_col_ord += match_ord;
        total_col_irm += match_irm;
        total_col_icm += match_icm;
        total_filled += filled_count;

        // Only count chunks with actual terrain (not pure out-of-map)
        if blob.len() < 1039 {
            inner_col_ord += match_ord;
            inner_col_irm += match_irm;
            inner_col_icm += match_icm;
            inner_filled += filled_count;
        }

        if blob.len() < 1039 || ordinal < 5 {
            println!("{:3}  {:3}  ({:3},{:3})  ({:3},{:3})  ({:3},{:3})  {:4}  {:4}/{:4} {:4}/{:4} {:4}/{:4}",
                ordinal, chunk_index, pos_ord.0, pos_ord.1,
                pos_irm.0, pos_irm.1, pos_icm.0, pos_icm.1,
                blob.len(), match_ord, filled_count, match_irm, filled_count, match_icm, filled_count);
        }
    }

    println!("\nAll chunks: ord={}/{} ({:.1}%), irm={}/{} ({:.1}%), icm={}/{} ({:.1}%)",
        total_col_ord, total_filled, 100.0 * total_col_ord as f64 / total_filled as f64,
        total_col_irm, total_filled, 100.0 * total_col_irm as f64 / total_filled as f64,
        total_col_icm, total_filled, 100.0 * total_col_icm as f64 / total_filled as f64);
    println!("Inner only: ord={}/{} ({:.1}%), irm={}/{} ({:.1}%), icm={}/{} ({:.1}%)",
        inner_col_ord, inner_filled, 100.0 * inner_col_ord as f64 / inner_filled.max(1) as f64,
        inner_col_irm, inner_filled, 100.0 * inner_col_irm as f64 / inner_filled.max(1) as f64,
        inner_col_icm, inner_filled, 100.0 * inner_col_icm as f64 / inner_filled.max(1) as f64);

    // Find chunks with water and dump their visual to check spatial coherence
    let water_id = mappings.tile_id_by_name("water").unwrap_or(11);
    let deepwater_id = mappings.tile_id_by_name("deepwater").unwrap_or(12);
    println!("\n=== Water chunk analysis ===");
    for (ordinal, (chunk_index, _offset, blob)) in blobs.iter().enumerate() {
        let (ids, filled, _) = decode_blob_column_major(blob, out_of_map_id, mappings);
        let water_count = (0..1024).filter(|&i| filled[i] && (ids[i] == water_id || ids[i] == deepwater_id)).count();
        if water_count > 50 {
            let (cx, cy) = chunk_positions.get(ordinal).copied().unwrap_or((0,0));
            println!("Chunk ord={} idx={} pos=({},{}) water_tiles={}", ordinal, chunk_index, cx, cy, water_count);
            // Visual: W=water, D=deep, .=other, _=unfilled
            for y in 0..32 {
                let mut row = String::new();
                for x in 0..32 {
                    let idx = y * 32 + x;
                    if !filled[idx] { row.push('_'); }
                    else if ids[idx] == water_id { row.push('W'); }
                    else if ids[idx] == deepwater_id { row.push('D'); }
                    else { row.push('.'); }
                }
                println!("  {}", row);
            }
            // Also show row-major decode for comparison
            let (ids_rm, filled_rm, _) = decode_blob_row_major(blob, out_of_map_id, mappings);
            let water_rm = (0..1024).filter(|&i| filled_rm[i] && (ids_rm[i] == water_id || ids_rm[i] == deepwater_id)).count();
            println!("  Row-major water={}", water_rm);
            for y in 0..32 {
                let mut row = String::new();
                for x in 0..32 {
                    let idx = y * 32 + x;
                    if !filled_rm[idx] { row.push('_'); }
                    else if ids_rm[idx] == water_id { row.push('W'); }
                    else if ids_rm[idx] == deepwater_id { row.push('D'); }
                    else { row.push('.'); }
                }
                println!("  {}", row);
            }
            break; // Just show first water chunk
        }
    }

    // Detailed dump of chunk (0,0) if it exists
    let target_ordinal = chunk_positions.iter().position(|&(x, y)| x == 0 && y == 0).unwrap_or(0);
    if target_ordinal < blobs.len() {
        let (_, _, ref blob) = blobs[target_ordinal];
        let (chunk_x, chunk_y) = chunk_positions[target_ordinal];
        println!("\n=== Detailed: chunk ({},{}) ordinal {} ===", chunk_x, chunk_y, target_ordinal);

        let proc_tiles = terrain.compute_chunk(chunk_x, chunk_y);
        let (ids, filled, _) = decode_blob_column_major(blob, out_of_map_id, mappings);

        println!("First 30 mismatches:");
        let mut mismatch_count = 0;
        for idx in 0..1024 {
            if !filled[idx] { continue; }
            let blob_name = mappings.tile_name(ids[idx]).cloned().unwrap_or_else(|| format!("id:{}", ids[idx]));
            let proc_name = terrain.tile_name(proc_tiles[idx]);
            if blob_name != proc_name {
                if mismatch_count < 30 {
                    let x = idx % 32;
                    let y = idx / 32;
                    println!("  idx={:4} ({:2},{:2}) blob={:<20} proc={}", idx, x, y, blob_name, proc_name);
                }
                mismatch_count += 1;
            }
        }
        println!("Total mismatches: {}/1024", mismatch_count);

        // Visual grid of blob-decoded tiles (first char of tile name)
        println!("\nBlob decode visual (first 2 chars of tile name):");
        for y in 0..32 {
            let mut row = String::new();
            for x in 0..32 {
                let idx = y * 32 + x;
                if !filled[idx] {
                    row.push_str("..");
                } else {
                    let name = mappings.tile_name(ids[idx]).cloned().unwrap_or_default();
                    let ch = match name.as_str() {
                        "grass-2" => "g2",
                        "grass-3" => "g3",
                        "grass-4" => "g4",
                        "red-desert-0" => "r0",
                        "red-desert-1" => "r1",
                        "dirt-4" => "d4",
                        "dirt-6" => "d6",
                        "water" => "ww",
                        "deepwater" => "dw",
                        "out-of-map" => "OM",
                        _ => &name[..2.min(name.len())],
                    };
                    row.push_str(ch);
                }
            }
            println!("  y={:2}: {}", y, row);
        }

        // Dump first 15 RLE runs
        println!("\nFirst 15 RLE runs:");
        let mut dpos = 0;
        for run in 0..15 {
            if dpos + 3 > blob.len() { break; }
            let run_len = blob[dpos] as usize;
            if run_len == 0 { break; }
            let tid = u16::from_le_bytes([blob[dpos+1], blob[dpos+2]]);
            let tname = mappings.tile_name(tid).cloned().unwrap_or_else(|| format!("id:{}", tid));
            dpos += 3;
            let end = (dpos + run_len).min(blob.len());
            let flags: Vec<u8> = blob[dpos..end].to_vec();
            dpos = end;
            let sizes: Vec<usize> = flags.iter().map(|&f| flag_to_size(f, &tname)).collect();
            println!("  run {:2}: len={:3} tile={:<20} flags={:02x?} sizes={:?}",
                run, run_len, tname, &flags[..flags.len().min(8)], &sizes[..sizes.len().min(8)]);
        }
    }

    // === Cross-chunk comparison ===
    // Process all chunks in stream order with cross-chunk fills
    println!("\n=== Cross-chunk fill comparison ===");
    let mut cross_map: std::collections::HashMap<(i32, i32), Vec<(usize, u16)>> = std::collections::HashMap::new();
    let mut xc_match = 0usize;
    let mut xc_filled = 0usize;
    let mut xc_inner_match = 0usize;
    let mut xc_inner_filled = 0usize;
    let mut total_prefills = 0usize;
    let mut total_cross_gen = 0usize;

    for (ordinal, (_, _, blob)) in blobs.iter().enumerate() {
        let (chunk_x, chunk_y) = chunk_positions.get(ordinal).copied().unwrap_or((0, 0));
        let prefilled = cross_map.remove(&(chunk_x, chunk_y)).unwrap_or_default();
        total_prefills += prefilled.len();
        let (ids, filled, cross_fills) = decode_blob_with_prefill(blob, out_of_map_id, mappings, &prefilled, chunk_x, chunk_y);

        total_cross_gen += cross_fills.len();
        for ((tcx, tcy), idx, tid) in cross_fills {
            cross_map.entry((tcx, tcy)).or_default().push((idx, tid));
        }

        let proc_tiles = terrain.compute_chunk(chunk_x, chunk_y);
        let matches = count_matches(&ids, &filled, &proc_tiles, mappings, &terrain);
        let fc = filled.iter().filter(|&&f| f).count();
        xc_match += matches;
        xc_filled += fc;
        if blob.len() < 1039 {
            xc_inner_match += matches;
            xc_inner_filled += fc;
        }
    }
    println!("Prefills applied: {}, Cross-fills generated: {}", total_prefills, total_cross_gen);
    println!("With cross-chunk: all={}/{} ({:.1}%), inner={}/{} ({:.1}%)",
        xc_match, xc_filled, 100.0 * xc_match as f64 / xc_filled.max(1) as f64,
        xc_inner_match, xc_inner_filled, 100.0 * xc_inner_match as f64 / xc_inner_filled.max(1) as f64);
    println!("Without cross-chunk: all={}/{} ({:.1}%), inner={}/{} ({:.1}%)",
        total_col_ord, total_filled, 100.0 * total_col_ord as f64 / total_filled.max(1) as f64,
        inner_col_ord, inner_filled, 100.0 * inner_col_ord as f64 / inner_filled.max(1) as f64);

    // === Mismatch breakdown ===
    println!("\n=== Mismatch breakdown (save -> procedural) ===");
    let mut mismatch_pairs: std::collections::HashMap<(String, String), usize> = std::collections::HashMap::new();
    let mut per_chunk_rates: Vec<(i32, i32, usize, usize)> = Vec::new();
    {
        let mut cross_map3: std::collections::HashMap<(i32, i32), Vec<(usize, u16)>> = std::collections::HashMap::new();
        for (ordinal, (_, _, blob)) in blobs.iter().enumerate() {
            let (chunk_x, chunk_y) = chunk_positions.get(ordinal).copied().unwrap_or((0, 0));
            let prefilled = cross_map3.remove(&(chunk_x, chunk_y)).unwrap_or_default();
            let (ids, filled, cross_fills) = decode_blob_with_prefill(blob, out_of_map_id, mappings, &prefilled, chunk_x, chunk_y);
            for ((tcx, tcy), idx, tid) in cross_fills {
                cross_map3.entry((tcx, tcy)).or_default().push((idx, tid));
            }
            if blob.len() >= 1039 { continue; } // inner only
            let proc_tiles = terrain.compute_chunk(chunk_x, chunk_y);
            let mut chunk_match = 0usize;
            let mut chunk_filled = 0usize;
            for i in 0..1024 {
                if !filled[i] { continue; }
                chunk_filled += 1;
                let save_name = mappings.tile_name(ids[i]).map(|s| s.as_str()).unwrap_or("??");
                let proc_name = terrain.tile_name(proc_tiles[i]);
                if save_name == proc_name { chunk_match += 1; }
                else { *mismatch_pairs.entry((save_name.to_string(), proc_name.to_string())).or_default() += 1; }
            }
            per_chunk_rates.push((chunk_x, chunk_y, chunk_match, chunk_filled));
        }
    }
    let mut pairs: Vec<_> = mismatch_pairs.into_iter().collect();
    pairs.sort_by(|a, b| b.1.cmp(&a.1));
    for ((save, proc), count) in pairs.iter().take(20) {
        println!("  {:>20} -> {:<20} : {}", save, proc, count);
    }
    println!("\nWorst inner chunks:");
    per_chunk_rates.sort_by(|a, b| {
        let ra = a.2 as f64 / a.3.max(1) as f64;
        let rb = b.2 as f64 / b.3.max(1) as f64;
        ra.partial_cmp(&rb).unwrap()
    });
    for &(cx, cy, m, f) in per_chunk_rates.iter().take(10) {
        println!("  ({:3},{:3}): {}/{} ({:.1}%)", cx, cy, m, f, 100.0 * m as f64 / f.max(1) as f64);
    }

    // Show chunk (0,0) with cross-chunk fills for comparison
    let mut cross_map2: std::collections::HashMap<(i32, i32), Vec<(usize, u16)>> = std::collections::HashMap::new();
    for (ordinal, (_, _, blob)) in blobs.iter().enumerate() {
        let (cx, cy) = chunk_positions.get(ordinal).copied().unwrap_or((0, 0));
        let prefilled = cross_map2.remove(&(cx, cy)).unwrap_or_default();
        let (ids, filled, cross_fills) = decode_blob_with_prefill(blob, out_of_map_id, mappings, &prefilled, cx, cy);
        for ((tcx, tcy), idx, tid) in cross_fills {
            cross_map2.entry((tcx, tcy)).or_default().push((idx, tid));
        }
        if cx == 0 && cy == 0 {
            println!("\n=== Chunk (0,0) WITH cross-chunk fills (prefilled={}) ===", prefilled.len());
            let proc_tiles = terrain.compute_chunk(0, 0);
            let mut mismatch = 0;
            for i in 0..1024 { if filled[i] && mappings.tile_name(ids[i]).map(|s| s.as_str()) != Some(terrain.tile_name(proc_tiles[i])) { mismatch += 1; } }
            println!("Mismatches: {}/1024", mismatch);
            for y in 0..32 {
                let mut row = String::new();
                for x in 0..32 {
                    let idx = y * 32 + x;
                    if !filled[idx] { row.push_str(".."); }
                    else {
                        let name = mappings.tile_name(ids[idx]).map(|s| s.as_str()).unwrap_or("??");
                        row.push_str(match name { "grass-2" => "g2", "red-desert-0" => "r0", "water" => "ww", "deepwater" => "dw", _ => &name[..2.min(name.len())] });
                    }
                }
                println!("  y={:2}: {}", y, row);
            }
        }
    }

    Ok(())
}

fn count_matches(ids: &[u16; 1024], filled: &[bool; 1024], proc_tiles: &[u8; 1024], mappings: &PrototypeMappings, terrain: &TerrainGenerator) -> usize {
    let mut matches = 0;
    for idx in 0..1024 {
        if !filled[idx] { continue; }
        let blob_name = mappings.tile_name(ids[idx]).cloned().unwrap_or_default();
        let proc_name = terrain.tile_name(proc_tiles[idx]);
        if blob_name == proc_name { matches += 1; }
    }
    matches
}

fn flag_to_size(flag: u8, tile_name: &str) -> usize {
    let masked = flag & 0xF0;
    let size_index = if masked & 0x10 != 0 { 0 } else { (masked >> 5) as u32 + 1 };
    let size_mask: u8 = match tile_name {
        "out-of-map" => 0x01,
        "sand-1" | "sand-2" => 0x0F,
        _ => 0x07,
    };
    if size_index > 0 && (size_mask >> size_index) & 1 != 0 {
        1 << size_index
    } else {
        1
    }
}

fn decode_blob_column_major(data: &[u8], out_of_map_id: u16, mappings: &PrototypeMappings) -> ([u16; 1024], [bool; 1024], usize) {
    let mut tile_ids = [out_of_map_id; 1024];
    let mut filled = [false; 1024];
    let mut pos = 0usize;
    let mut remaining = 0u8;
    let mut current_tile_id = 0u16;

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

            let tname = mappings.tile_name(current_tile_id).map(|s| s.as_str()).unwrap_or("");
            let side = flag_to_size(flag, tname);
            if side > 1 {
                for dx in 0..side {
                    let dy_start = if dx == 0 { 1 } else { 0 };
                    for dy in dy_start..side {
                        let fx = x + dx;
                        let fy = y + dy;
                        if fx < 32 && fy < 32 {
                            let fidx = fy * 32 + fx;
                            tile_ids[fidx] = current_tile_id;
                            filled[fidx] = true;
                        }
                    }
                }
            }
        }
    }
    (tile_ids, filled, pos)
}

fn decode_blob_row_major(data: &[u8], out_of_map_id: u16, mappings: &PrototypeMappings) -> ([u16; 1024], [bool; 1024], usize) {
    let mut tile_ids = [out_of_map_id; 1024];
    let mut filled = [false; 1024];
    let mut pos = 0usize;
    let mut remaining = 0u8;
    let mut current_tile_id = 0u16;

    'outer: for y in 0..32usize {
        for x in 0..32usize {
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

            let tname = mappings.tile_name(current_tile_id).map(|s| s.as_str()).unwrap_or("");
            let side = flag_to_size(flag, tname);
            if side > 1 {
                for dy in 0..side {
                    let dx_start = if dy == 0 { 1 } else { 0 };
                    for dx in dx_start..side {
                        let fx = x + dx;
                        let fy = y + dy;
                        if fx < 32 && fy < 32 {
                            let fidx = fy * 32 + fx;
                            tile_ids[fidx] = current_tile_id;
                            filled[fidx] = true;
                        }
                    }
                }
            }
        }
    }
    (tile_ids, filled, pos)
}

fn decode_blob_no_large(data: &[u8], out_of_map_id: u16) -> ([u16; 1024], [bool; 1024], usize) {
    let mut tile_ids = [out_of_map_id; 1024];
    let mut filled = [false; 1024];
    let mut pos = 0usize;
    let mut remaining = 0u8;
    let mut current_tile_id = 0u16;

    'outer: for x in 0..32usize {
        for y in 0..32usize {
            let idx = y * 32 + x;

            if remaining == 0 {
                if pos + 3 > data.len() { break 'outer; }
                remaining = data[pos];
                if remaining == 0 { break 'outer; }
                current_tile_id = u16::from_le_bytes([data[pos + 1], data[pos + 2]]);
                pos += 3;
            }

            if pos >= data.len() { break 'outer; }
            pos += 1;
            remaining -= 1;

            tile_ids[idx] = current_tile_id;
            filled[idx] = true;
        }
    }
    (tile_ids, filled, pos)
}

fn decode_blob_with_prefill(
    data: &[u8], out_of_map_id: u16, mappings: &PrototypeMappings,
    prefilled: &[(usize, u16)], chunk_x: i32, chunk_y: i32,
) -> ([u16; 1024], [bool; 1024], Vec<((i32, i32), usize, u16)>) {
    let mut tile_ids = [out_of_map_id; 1024];
    let mut filled = [false; 1024];
    let mut cross_fills: Vec<((i32, i32), usize, u16)> = Vec::new();

    for &(idx, tile_id) in prefilled {
        if idx < 1024 {
            tile_ids[idx] = tile_id;
            filled[idx] = true;
        }
    }

    let mut pos = 0usize;
    let mut remaining = 0u8;
    let mut current_tile_id = 0u16;

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

            let tname = mappings.tile_name(current_tile_id).map(|s| s.as_str()).unwrap_or("");
            let side = flag_to_size(flag, tname);
            if side > 1 {
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
                            let target_cx = chunk_x + (gx / 32) as i32;
                            let target_cy = chunk_y + (gy / 32) as i32;
                            let local_x = gx % 32;
                            let local_y = gy % 32;
                            let local_idx = local_y * 32 + local_x;
                            cross_fills.push(((target_cx, target_cy), local_idx, current_tile_id));
                        }
                    }
                }
            }
        }
    }
    (tile_ids, filled, cross_fills)
}

fn decompress(data: &[u8]) -> Vec<u8> {
    use flate2::read::ZlibDecoder;
    let mut decoder = ZlibDecoder::new(data);
    let mut out = Vec::new();
    decoder.read_to_end(&mut out).unwrap_or_default();
    out
}
