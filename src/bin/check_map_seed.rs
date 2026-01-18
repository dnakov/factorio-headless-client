use factorio_client::codec::parse_map_data;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <map.zip>", args[0]);
        std::process::exit(1);
    }

    unsafe { std::env::set_var("FACTORIO_NO_PROCEDURAL", "1"); }

    let data = std::fs::read(&args[1])?;
    let map_data = parse_map_data(&data)?;

    println!("Map seed: {}", map_data.seed);

    let water_tiles: Vec<_> = map_data.tiles.iter()
        .filter(|t| t.name.contains("water"))
        .collect();
    println!("Water tiles: {}", water_tiles.len());

    if !water_tiles.is_empty() {
        let min_x = water_tiles.iter().map(|t| t.x).min().unwrap();
        let max_x = water_tiles.iter().map(|t| t.x).max().unwrap();
        let min_y = water_tiles.iter().map(|t| t.y).min().unwrap();
        let max_y = water_tiles.iter().map(|t| t.y).max().unwrap();
        println!("Water bounds: x=[{},{}] y=[{},{}]", min_x, max_x, min_y, max_y);
    }

    let real_tiles: Vec<_> = map_data.tiles.iter()
        .filter(|t| !t.name.contains("out-of-map"))
        .collect();
    if !real_tiles.is_empty() {
        let min_x = real_tiles.iter().map(|t| t.x).min().unwrap();
        let max_x = real_tiles.iter().map(|t| t.x).max().unwrap();
        let min_y = real_tiles.iter().map(|t| t.y).min().unwrap();
        let max_y = real_tiles.iter().map(|t| t.y).max().unwrap();
        println!("Real tile bounds: x=[{},{}] y=[{},{}] ({}x{} tiles)",
            min_x, max_x, min_y, max_y,
            max_x - min_x + 1, max_y - min_y + 1);
    }

    // Chunk grid minimap
    println!("\nChunk grid (W=water, .=terrain, _=out-of-map):");
    println!("       x: -7-6-5-4-3-2-1 0 1 2 3 4 5 6");
    for cy in -7..=6 {
        print!("  y={:3}:  ", cy);
        for cx in -7..=6 {
            let base_x = cx * 32;
            let base_y = cy * 32;
            let has_water = map_data.tiles.iter().any(|t|
                t.name.contains("water") &&
                t.x >= base_x && t.x < base_x + 32 &&
                t.y >= base_y && t.y < base_y + 32
            );
            let has_real = map_data.tiles.iter().any(|t|
                !t.name.contains("out-of-map") &&
                t.x >= base_x && t.x < base_x + 32 &&
                t.y >= base_y && t.y < base_y + 32
            );
            if has_water { print!("W "); }
            else if has_real { print!(". "); }
            else { print!("_ "); }
        }
        println!();
    }

    Ok(())
}
