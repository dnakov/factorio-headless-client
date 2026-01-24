use factorio_client::codec::parse_map_data;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    unsafe { std::env::set_var("FACTORIO_NO_PROCEDURAL", "1"); }
    let data = std::fs::read("downloaded_map.zip")?;
    let map_data = parse_map_data(&data)?;
    
    let entity_protos = map_data.prototype_mappings.tables.get("Entity").unwrap();
    let entity_groups = &map_data.prototype_mappings.entity_groups;
    
    // Check stone-furnace group
    for (id, name) in entity_protos.iter() {
        if name.contains("furnace") {
            let group = entity_groups.get(id).map(|s| s.as_str()).unwrap_or("NO GROUP");
            println!("proto {:4} = {:40} group={}", id, name, group);
        }
    }
    
    // Check which chunk the furnace is in
    // RCON says the player placed it, so check character position
    println!("\ncharacter/player entities:");
    for ent in &map_data.entities {
        if ent.name == "character" || ent.name.contains("furnace") {
            println!("  {} at ({:.1}, {:.1})", ent.name, ent.x, ent.y);
        }
    }
    
    Ok(())
}
