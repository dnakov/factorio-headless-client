use crate::codec::{InputAction, Direction, MapPosition};
use crate::error::Result;
use crate::state::{GameWorld, Inventory, ItemStack, player::Player};
use crate::lua::prototype::Prototypes;

/// Convert direction vector (x, y) to Direction enum
fn direction_from_vector(x: f64, y: f64) -> Direction {
    let angle = y.atan2(x);
    let octant = ((angle + std::f64::consts::PI) / (std::f64::consts::PI / 4.0)).round() as i32 % 8;
    match octant {
        0 => Direction::West,
        1 => Direction::NorthWest,
        2 => Direction::North,
        3 => Direction::NorthEast,
        4 => Direction::East,
        5 => Direction::SouthEast,
        6 => Direction::South,
        7 => Direction::SouthWest,
        _ => Direction::North,
    }
}

/// Executes input actions against the game world
pub struct ActionExecutor {
    /// Pending crafts that will complete (tick, player_index, recipe_name, count)
    pending_crafts: Vec<(u32, u16, String, u32)>,
}

impl ActionExecutor {
    pub fn new() -> Self {
        Self {
            pending_crafts: Vec::new(),
        }
    }

    /// Process pending crafts that have completed
    pub fn tick(&mut self, world: &mut GameWorld) {
        let current_tick = world.tick;
        let mut completed = Vec::new();

        // Find completed crafts
        for (i, (finish_tick, player_index, recipe_name, count)) in self.pending_crafts.iter().enumerate() {
            if current_tick >= *finish_tick {
                completed.push((i, *player_index, recipe_name.clone(), *count));
            }
        }

        // Process completed crafts (reverse order to not mess up indices)
        for (idx, player_index, recipe_name, count) in completed.into_iter().rev() {
            self.pending_crafts.remove(idx);

            // Add products to player inventory
            if let Some(recipe) = world.recipes.get_by_name(&recipe_name) {
                if let Some(player) = world.players.get_mut(&player_index) {
                    // Ensure player has main inventory
                    if player.main_inventory.is_none() {
                        player.main_inventory = Some(Inventory::new(80));
                    }

                    if let Some(ref mut inv) = player.main_inventory {
                        for product in &recipe.products {
                            let stack = ItemStack::new(&product.name, product.amount * count);
                            inv.insert(stack);
                        }
                    }

                    // Decrement crafting queue
                    player.crafting_queue_size = player.crafting_queue_size.saturating_sub(count);
                }
            }
        }

        // Advance research progress (very rough approximation)
        if let Some(current) = world.research.current_research.clone() {
            world.research.progress += 1.0 / 6000.0;
            if world.research.progress >= 1.0 {
                world.research.progress = 0.0;
                world.research.researched.push(current);
                world.research.current_research = world.research.queue.first().cloned();
            }
        }

        // Advance machine crafting progress (simplified)
        let recipes = world.recipes.clone();
        if let Some(surface) = world.nauvis_mut() {
            let drill_ids: Vec<u32> = surface
                .entities
                .iter()
                .filter_map(|(id, e)| {
                    if matches!(e.data, crate::state::entity::EntityData::MiningDrill(_)) {
                        Some(*id)
                    } else {
                        None
                    }
                })
                .collect();
            let inserter_ids: Vec<u32> = surface
                .entities
                .iter()
                .filter_map(|(id, e)| {
                    if matches!(e.data, crate::state::entity::EntityData::Inserter(_)) {
                        Some(*id)
                    } else {
                        None
                    }
                })
                .collect();
            let belt_ids: Vec<u32> = surface
                .entities
                .iter()
                .filter_map(|(id, e)| {
                    if matches!(e.data, crate::state::entity::EntityData::TransportBelt(_)) {
                        Some(*id)
                    } else {
                        None
                    }
                })
                .collect();
            for entity in surface.entities.values_mut() {
                match &mut entity.data {
                    crate::state::entity::EntityData::AssemblingMachine(data) => {
                        let Some(recipe_name) = data.recipe.clone() else { continue; };
                        let Some(recipe) = recipes.get_by_name(&recipe_name) else { continue; };
                        let speed = if data.crafting_speed > 0.0 { data.crafting_speed as f64 } else { 1.0 };
                        let craft_ticks = (recipe.crafting_time * 60.0 / speed).max(1.0);
                        data.crafting_progress += 1.0 / craft_ticks as f32;
                        if data.crafting_progress >= 1.0 {
                            data.crafting_progress -= 1.0;
                            let can_craft = {
                                let input = self.ensure_entity_inventory(entity, "input", 6);
                                let available = input.contents();
                                recipe.can_craft(&available, 1)
                            };
                            if can_craft {
                                {
                                    let input = self.ensure_entity_inventory(entity, "input", 6);
                                    for ing in &recipe.ingredients {
                                        input.remove(&ing.name, ing.amount);
                                    }
                                }
                                {
                                    let output = self.ensure_entity_inventory(entity, "output", 6);
                                    for product in &recipe.products {
                                        output.insert(ItemStack::new(&product.name, product.amount));
                                    }
                                }
                            }
                        }
                    }
                    crate::state::entity::EntityData::Furnace(data) => {
                        let Some(recipe_name) = data.smelting_recipe.clone() else { continue; };
                        let Some(recipe) = recipes.get_by_name(&recipe_name) else { continue; };
                        let speed = if data.crafting_speed > 0.0 { data.crafting_speed as f64 } else { 1.0 };
                        let craft_ticks = (recipe.crafting_time * 60.0 / speed).max(1.0);
                        data.crafting_progress += 1.0 / craft_ticks as f32;
                        if data.crafting_progress >= 1.0 {
                            data.crafting_progress -= 1.0;
                            let can_craft = {
                                let input = self.ensure_entity_inventory(entity, "source", 2);
                                let available = input.contents();
                                recipe.can_craft(&available, 1)
                            };
                            if can_craft {
                                {
                                    let input = self.ensure_entity_inventory(entity, "source", 2);
                                    for ing in &recipe.ingredients {
                                        input.remove(&ing.name, ing.amount);
                                    }
                                }
                                {
                                    let output = self.ensure_entity_inventory(entity, "result", 2);
                                    for product in &recipe.products {
                                        output.insert(ItemStack::new(&product.name, product.amount));
                                    }
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }

            for drill_id in drill_ids {
                let (drill_pos, mut progress) = match surface.entities.get(&drill_id) {
                    Some(entity) => {
                        if let crate::state::entity::EntityData::MiningDrill(data) = &entity.data {
                            (entity.position, data.mining_progress)
                        } else {
                            continue;
                        }
                    }
                    None => continue,
                };

                let target = {
                    let surface_ref: &crate::state::surface::Surface = &*surface;
                    self.find_resource_near(surface_ref, drill_pos, 2.0)
                };

                let Some((res_id, res_name, res_amount, res_infinite, res_mining_time)) = target else {
                    if let Some(entity) = surface.get_entity_mut(drill_id) {
                        if let crate::state::entity::EntityData::MiningDrill(data) = &mut entity.data {
                            data.mining_target = None;
                        }
                    }
                    continue;
                };

                if !res_infinite && res_amount == 0 {
                    continue;
                }

                let mut mined = false;
                if let Some(entity) = surface.get_entity_mut(drill_id) {
                    let mining_speed = match &entity.data {
                        crate::state::entity::EntityData::MiningDrill(data) => data.mining_speed,
                        _ => 0.0,
                    };
                    let mining_speed = if mining_speed > 0.0 { mining_speed } else { 0.5 };
                    let mining_ticks = (res_mining_time as f64 * 60.0 / mining_speed as f64).max(1.0);
                    progress += 1.0 / mining_ticks as f32;
                    if progress >= 1.0 {
                        let output = self.ensure_entity_inventory(entity, "output", 6);
                        if output.insert(ItemStack::new(&res_name, 1)).is_none() {
                            mined = true;
                            progress -= 1.0;
                        } else {
                            progress = 1.0;
                        }
                    }
                    if let crate::state::entity::EntityData::MiningDrill(data) = &mut entity.data {
                        data.mining_target = Some(res_name.clone());
                        data.mining_progress = progress;
                    }
                }

                if mined && !res_infinite {
                    if let Some(entity) = surface.get_entity_mut(res_id) {
                        if let crate::state::entity::EntityData::Resource(ref mut data) = entity.data {
                            if data.amount > 0 {
                                data.amount = data.amount.saturating_sub(1);
                            }
                            if data.amount == 0 {
                                surface.remove_entity(res_id);
                            }
                        }
                    }
                }
            }

            for inserter_id in inserter_ids {
                let (pickup_pos, drop_pos, mut cooldown, filters, stack_size, speed_ticks) = match surface.entities.get(&inserter_id) {
                    Some(entity) => {
                        let (px, py) = entity.position.to_tiles();
                        let (default_pickup, default_drop) = self.default_inserter_positions(entity, px, py);
                        if let crate::state::entity::EntityData::Inserter(data) = &entity.data {
                            let (stack_size, speed_ticks) = self.inserter_stack_and_speed(entity, data.stack_size_override);
                            (
                                data.pickup_position.unwrap_or(default_pickup),
                                data.drop_position.unwrap_or(default_drop),
                                data.cooldown,
                                data.filters.clone(),
                                stack_size,
                                speed_ticks,
                            )
                        } else {
                            continue;
                        }
                    }
                    None => continue,
                };

                if cooldown > 0 {
                    cooldown = cooldown.saturating_sub(1);
                    if let Some(entity) = surface.get_entity_mut(inserter_id) {
                        if let crate::state::entity::EntityData::Inserter(data) = &mut entity.data {
                            data.cooldown = cooldown;
                        }
                    }
                    continue;
                }

                let source = self.find_entity_at_surface(surface, pickup_pos, 1);
                let dest = self.find_entity_at_surface(surface, drop_pos, 1);
                if let Some(source_id) = source {
                    if dest == Some(source_id) {
                        continue;
                    }
                    let taken = if let Some(entity) = surface.get_entity_mut(source_id) {
                        self.take_items_from_entity(entity, stack_size, &filters)
                    } else {
                        None
                    };

                    if let Some(stack) = taken {
                        let mut remainder = Some(stack);
                        if let Some(dest_id) = dest {
                            if let Some(entity) = surface.get_entity_mut(dest_id) {
                                remainder = self.insert_stack_into_entity(entity, remainder.take().unwrap());
                            }
                        }
                        if remainder.is_none() {
                            if let Some(entity) = surface.get_entity_mut(inserter_id) {
                                if let crate::state::entity::EntityData::Inserter(data) = &mut entity.data {
                                    data.cooldown = speed_ticks;
                                }
                            }
                        } else if let Some(entity) = surface.get_entity_mut(source_id) {
                            self.put_back_item(entity, remainder.unwrap());
                        }
                    }
                }
            }

            const BELT_LANE_CAP: usize = 4;
            for belt_id in belt_ids {
                let (belt_pos, belt_dir, mut lanes, mut progress, belt_speed, underground_max, is_underground, underground_type) =
                    match surface.entities.get(&belt_id) {
                    Some(entity) => {
                        if let crate::state::entity::EntityData::TransportBelt(data) = &entity.data {
                            let mut lanes = data.lane_items.clone();
                            if lanes[0].is_empty() && lanes[1].is_empty() && !data.line_contents.is_empty() {
                                lanes[0] = data.line_contents.clone();
                            }
                            let speed = Prototypes::global()
                                .and_then(|p| p.entity(&entity.name))
                                .and_then(|p| p.belt_speed)
                                .unwrap_or(0.03125);
                            let max_distance = Prototypes::global()
                                .and_then(|p| p.entity(&entity.name))
                                .and_then(|p| p.underground_max_distance);
                            (
                                entity.position,
                                entity.direction,
                                lanes,
                                data.lane_progress,
                                speed,
                                max_distance,
                                data.is_underground,
                                data.underground_type,
                            )
                        } else {
                            continue;
                        }
                    }
                    None => continue,
                };

                let is_input = underground_type == Some(0);
                let is_output = underground_type == Some(1);
                if !is_output && (lanes[0].len() < BELT_LANE_CAP || lanes[1].len() < BELT_LANE_CAP) {
                    let (dx, dy) = belt_dir.to_vector();
                    let (bx, by) = belt_pos.to_tiles();
                    let behind_pos = crate::codec::MapPosition::from_tiles(bx - dx, by - dy);
                    let behind_id = self.find_entity_at_surface(surface, behind_pos, 1);
                    if let Some(source_id) = behind_id {
                        if let Some(source) = surface.get_entity_mut(source_id) {
                            if let Some(stack) = self.take_items_from_entity(source, 1, &[]) {
                                if lanes[0].len() <= lanes[1].len() && lanes[0].len() < BELT_LANE_CAP {
                                    lanes[0].push(stack.name);
                                } else if lanes[1].len() < BELT_LANE_CAP {
                                    lanes[1].push(stack.name);
                                }
                            }
                        }
                    }
                }

                if lanes[0].is_empty() && lanes[1].is_empty() {
                    continue;
                }
                let (dx, dy) = belt_dir.to_vector();
                let (bx, by) = belt_pos.to_tiles();
                let target_pos = crate::codec::MapPosition::from_tiles(bx + dx, by + dy);
                let mut target_id = self.find_entity_at_surface(surface, target_pos, 1);
                if is_underground && is_input {
                    if let Some(max_dist) = underground_max {
                        let mut best = None;
                        for dist in 1..=max_dist as i32 {
                            let tx = bx + dx * dist as f64;
                            let ty = by + dy * dist as f64;
                            let pos = crate::codec::MapPosition::from_tiles(tx, ty);
                            if let Some(id) = self.find_entity_at_surface(surface, pos, 1) {
                                if let Some(ent) = surface.get_entity(id) {
                                    if ent.entity_type == crate::state::entity::EntityType::UndergroundBelt {
                                        if let crate::state::entity::EntityData::TransportBelt(data) = &ent.data {
                                            if data.underground_type == Some(1) && ent.direction == belt_dir {
                                                best = Some(id);
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        target_id = best;
                    }
                }
                if is_underground && is_output {
                    if let Some(max_dist) = underground_max {
                        let mut best = None;
                        for dist in 1..=max_dist as i32 {
                            let tx = bx - dx * dist as f64;
                            let ty = by - dy * dist as f64;
                            let pos = crate::codec::MapPosition::from_tiles(tx, ty);
                            if let Some(id) = self.find_entity_at_surface(surface, pos, 1) {
                                if let Some(ent) = surface.get_entity(id) {
                                    if ent.entity_type == crate::state::entity::EntityType::UndergroundBelt {
                                        if let crate::state::entity::EntityData::TransportBelt(data) = &ent.data {
                                            if data.underground_type == Some(0) && ent.direction == belt_dir {
                                                best = Some(id);
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        target_id = best.or(target_id);
                    }
                }

                for lane in 0..2 {
                    if lanes[lane].is_empty() {
                        progress[lane] = 0.0;
                        continue;
                    }
                    progress[lane] += belt_speed;
                    if progress[lane] < 1.0 {
                        continue;
                    }
                    let item = lanes[lane][0].clone();
                    let mut moved = false;
                    if let Some(target_id) = target_id {
                        if let Some(target) = surface.get_entity_mut(target_id) {
                            moved = self.insert_into_entity(target, ItemStack::new(item.clone(), 1));
                        }
                    }
                    if moved {
                        lanes[lane].remove(0);
                        progress[lane] -= 1.0;
                    } else {
                        progress[lane] = 1.0;
                    }
                }

                if let Some(entity) = surface.get_entity_mut(belt_id) {
                    if let crate::state::entity::EntityData::TransportBelt(data) = &mut entity.data {
                        data.lane_items = lanes;
                        data.lane_progress = progress;
                        data.line_contents = data
                            .lane_items
                            .iter()
                            .flat_map(|lane| lane.iter().cloned())
                            .collect();
                    }
                }
            }
        }

        self.tick_trains(world);
    }

    /// Execute an input action for a player
    pub fn execute(&mut self, world: &mut GameWorld, player_index: u16, action: InputAction) -> Result<()> {
        match action {
            InputAction::Nothing => Ok(()),

            InputAction::StartWalking { direction_x, direction_y } => {
                self.execute_start_walking(world, player_index, direction_x, direction_y)
            }

            InputAction::StopWalking => {
                self.execute_stop_walking(world, player_index)
            }

            InputAction::BeginMining { position, .. } => {
                self.execute_begin_mining(world, player_index, position)
            }

            InputAction::StopMining => {
                self.execute_stop_mining(world, player_index)
            }

            InputAction::Craft { recipe_id, count } => {
                self.execute_craft(world, player_index, recipe_id, count)
            }

            InputAction::WriteToConsole { message } => {
                self.execute_chat(world, player_index, message)
            }

            InputAction::ChangeRidingState { acceleration, direction } => {
                self.execute_change_riding_state(world, player_index, acceleration, direction)
            }

            InputAction::ChangeShootingState { state: _, position: _ } => {
                // Update player shooting state
                Ok(())
            }

            InputAction::ClearCursor => {
                if let Some(player) = world.players.get_mut(&player_index) {
                    player.clear_cursor();
                }
                Ok(())
            }
            InputAction::CursorTransfer { location } => {
                let item_name = if location.item_id != 0 {
                    Some(self.item_name_from_id(world, location.item_id))
                } else {
                    None
                };
                if let Some(player) = world.players.get_mut(&player_index) {
                    let prev_cursor = player.cursor_stack.take();
                    let inv = self.ensure_player_inventory(player, location.location.inventory_index);
                    let slot_idx = location.location.slot_index as usize;
                    let slot_stack = inv.get(slot_idx).cloned();
                    if let Some(prev) = prev_cursor {
                        inv.set(slot_idx, Some(prev));
                    } else {
                        inv.set(slot_idx, None);
                    }
                    let new_cursor = if let Some(stack) = slot_stack {
                        Some(stack)
                    } else if let Some(name) = item_name {
                        Some(ItemStack::new(name, 1))
                    } else {
                        None
                    };
                    player.cursor_stack = new_cursor;
                }
                Ok(())
            }
            InputAction::CursorSplit { location } => {
                if let Some(player) = world.players.get_mut(&player_index) {
                    let inv = self.ensure_player_inventory(player, location.location.inventory_index);
                    let slot_idx = location.location.slot_index as usize;
                    if let Some(mut stack) = inv.get(slot_idx).cloned() {
                        let take = (stack.count + 1) / 2;
                        stack.count = stack.count.saturating_sub(take);
                        if stack.count == 0 {
                            inv.set(slot_idx, None);
                        } else {
                            inv.set(slot_idx, Some(stack.clone()));
                        }
                        player.cursor_stack = Some(ItemStack::new(stack.name, take));
                    }
                }
                Ok(())
            }
            InputAction::StackTransfer { spec }
            | InputAction::InventoryTransfer { spec } => {
                let from_player = spec.location.source == 0 || spec.location.source == 2;
                let slot_idx = spec.location.slot_index as usize;
                if from_player {
                    let (selected_entity, transfer) = if let Some(player) = world.players.get_mut(&player_index) {
                        let inv = self.ensure_player_inventory(player, spec.location.inventory_index);
                        let transfer = inv.get(slot_idx).cloned().map(|mut stack| {
                            let move_count = stack.count;
                            stack.count = stack.count.saturating_sub(move_count);
                            if stack.count == 0 {
                                inv.set(slot_idx, None);
                            } else {
                                inv.set(slot_idx, Some(stack.clone()));
                            }
                            (stack.name, move_count)
                        });
                        (player.selected_entity_id, transfer)
                    } else {
                        (None, None)
                    };
                    if let (Some(entity_id), Some((name, count))) = (selected_entity, transfer) {
                        if let Some(surface) = world.nauvis_mut() {
                            if let Some(entity) = surface.get_entity_mut(entity_id) {
                                let ent_inv = self.ensure_entity_inventory(entity, "main", 80);
                                ent_inv.insert(ItemStack::new(name, count));
                            }
                        }
                    }
                } else {
                    let selected_entity = world.players.get(&player_index).and_then(|p| p.selected_entity_id);
                    let transfer = if let Some(entity_id) = selected_entity {
                        if let Some(surface) = world.nauvis_mut() {
                            if let Some(entity) = surface.get_entity_mut(entity_id) {
                                let ent_inv = self.ensure_entity_inventory(entity, "main", 80);
                                if let Some(ent_slot) = ent_inv.get(0).cloned() {
                                    let move_count = ent_slot.count;
                                    let mut remaining = ent_slot.clone();
                                    remaining.count = remaining.count.saturating_sub(move_count);
                                    if remaining.count == 0 {
                                        ent_inv.set(0, None);
                                    } else {
                                        ent_inv.set(0, Some(remaining));
                                    }
                                    Some((ent_slot.name, move_count))
                                } else {
                                    None
                                }
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    } else {
                        None
                    };
                    if let Some((name, count)) = transfer {
                        if let Some(player) = world.players.get_mut(&player_index) {
                            let inv = self.ensure_player_inventory(player, spec.location.inventory_index);
                            inv.insert(ItemStack::new(name, count));
                        }
                    }
                }
                Ok(())
            }
            InputAction::StackSplit { spec }
            | InputAction::InventorySplit { spec } => {
                let from_player = spec.location.source == 0 || spec.location.source == 2;
                let slot_idx = spec.location.slot_index as usize;
                if from_player {
                    let (selected_entity, transfer) = if let Some(player) = world.players.get_mut(&player_index) {
                        let inv = self.ensure_player_inventory(player, spec.location.inventory_index);
                        let transfer = inv.get(slot_idx).cloned().map(|mut stack| {
                            let move_count = (stack.count + 1) / 2;
                            stack.count = stack.count.saturating_sub(move_count);
                            if stack.count == 0 {
                                inv.set(slot_idx, None);
                            } else {
                                inv.set(slot_idx, Some(stack.clone()));
                            }
                            (stack.name, move_count)
                        });
                        (player.selected_entity_id, transfer)
                    } else {
                        (None, None)
                    };
                    if let (Some(entity_id), Some((name, count))) = (selected_entity, transfer) {
                        if let Some(surface) = world.nauvis_mut() {
                            if let Some(entity) = surface.get_entity_mut(entity_id) {
                                let ent_inv = self.ensure_entity_inventory(entity, "main", 80);
                                ent_inv.insert(ItemStack::new(name, count));
                            }
                        }
                    }
                } else {
                    let selected_entity = world.players.get(&player_index).and_then(|p| p.selected_entity_id);
                    let transfer = if let Some(entity_id) = selected_entity {
                        if let Some(surface) = world.nauvis_mut() {
                            if let Some(entity) = surface.get_entity_mut(entity_id) {
                                let ent_inv = self.ensure_entity_inventory(entity, "main", 80);
                                if let Some(ent_slot) = ent_inv.get(0).cloned() {
                                    let move_count = (ent_slot.count + 1) / 2;
                                    let mut remaining = ent_slot.clone();
                                    remaining.count = remaining.count.saturating_sub(move_count);
                                    if remaining.count == 0 {
                                        ent_inv.set(0, None);
                                    } else {
                                        ent_inv.set(0, Some(remaining));
                                    }
                                    Some((ent_slot.name, move_count))
                                } else {
                                    None
                                }
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    } else {
                        None
                    };
                    if let Some((name, count)) = transfer {
                        if let Some(player) = world.players.get_mut(&player_index) {
                            let inv = self.ensure_player_inventory(player, spec.location.inventory_index);
                            inv.insert(ItemStack::new(name, count));
                        }
                    }
                }
                Ok(())
            }
            InputAction::FastEntityTransfer { from_player } => {
                let split = false;
                let selected_entity = world.players.get(&player_index).and_then(|p| p.selected_entity_id);
                if from_player {
                    let transfer = if let Some(player) = world.players.get_mut(&player_index) {
                        let player_inv = self.ensure_player_inventory(player, 1);
                        if let Some(slot_idx) = self.first_filled_slot(player_inv) {
                            let mut stack = player_inv.get(slot_idx).cloned().unwrap();
                            let move_count = if split { (stack.count + 1) / 2 } else { stack.count };
                            stack.count = stack.count.saturating_sub(move_count);
                            if stack.count == 0 {
                                player_inv.set(slot_idx, None);
                            } else {
                                player_inv.set(slot_idx, Some(stack.clone()));
                            }
                            Some((stack.name, move_count))
                        } else {
                            None
                        }
                    } else {
                        None
                    };
                    if let (Some(entity_id), Some((name, count))) = (selected_entity, transfer) {
                        if let Some(surface) = world.nauvis_mut() {
                            if let Some(entity) = surface.get_entity_mut(entity_id) {
                                let ent_inv = self.ensure_entity_inventory(entity, "main", 80);
                                ent_inv.insert(ItemStack::new(name, count));
                            }
                        }
                    }
                } else if let Some(entity_id) = selected_entity {
                    let transfer = if let Some(surface) = world.nauvis_mut() {
                        if let Some(entity) = surface.get_entity_mut(entity_id) {
                            let ent_inv = self.ensure_entity_inventory(entity, "main", 80);
                            if let Some(ent_slot) = ent_inv.get(0).cloned() {
                                let move_count = if split { (ent_slot.count + 1) / 2 } else { ent_slot.count };
                                let mut remaining = ent_slot.clone();
                                remaining.count = remaining.count.saturating_sub(move_count);
                                if remaining.count == 0 {
                                    ent_inv.set(0, None);
                                } else {
                                    ent_inv.set(0, Some(remaining));
                                }
                                Some((ent_slot.name, move_count))
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    } else {
                        None
                    };
                    if let Some((name, count)) = transfer {
                        if let Some(player) = world.players.get_mut(&player_index) {
                            let player_inv = self.ensure_player_inventory(player, 1);
                            player_inv.insert(ItemStack::new(name, count));
                        }
                    }
                }
                Ok(())
            }
            InputAction::FastEntitySplit { from_player } => {
                let split = true;
                let selected_entity = world.players.get(&player_index).and_then(|p| p.selected_entity_id);
                if from_player {
                    let transfer = if let Some(player) = world.players.get_mut(&player_index) {
                        let player_inv = self.ensure_player_inventory(player, 1);
                        if let Some(slot_idx) = self.first_filled_slot(player_inv) {
                            let mut stack = player_inv.get(slot_idx).cloned().unwrap();
                            let move_count = if split { (stack.count + 1) / 2 } else { stack.count };
                            stack.count = stack.count.saturating_sub(move_count);
                            if stack.count == 0 {
                                player_inv.set(slot_idx, None);
                            } else {
                                player_inv.set(slot_idx, Some(stack.clone()));
                            }
                            Some((stack.name, move_count))
                        } else {
                            None
                        }
                    } else {
                        None
                    };
                    if let (Some(entity_id), Some((name, count))) = (selected_entity, transfer) {
                        if let Some(surface) = world.nauvis_mut() {
                            if let Some(entity) = surface.get_entity_mut(entity_id) {
                                let ent_inv = self.ensure_entity_inventory(entity, "main", 80);
                                ent_inv.insert(ItemStack::new(name, count));
                            }
                        }
                    }
                } else if let Some(entity_id) = selected_entity {
                    let transfer = if let Some(surface) = world.nauvis_mut() {
                        if let Some(entity) = surface.get_entity_mut(entity_id) {
                            let ent_inv = self.ensure_entity_inventory(entity, "main", 80);
                            if let Some(ent_slot) = ent_inv.get(0).cloned() {
                                let move_count = if split { (ent_slot.count + 1) / 2 } else { ent_slot.count };
                                let mut remaining = ent_slot.clone();
                                remaining.count = remaining.count.saturating_sub(move_count);
                                if remaining.count == 0 {
                                    ent_inv.set(0, None);
                                } else {
                                    ent_inv.set(0, Some(remaining));
                                }
                                Some((ent_slot.name, move_count))
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    } else {
                        None
                    };
                    if let Some((name, count)) = transfer {
                        if let Some(player) = world.players.get_mut(&player_index) {
                            let player_inv = self.ensure_player_inventory(player, 1);
                            player_inv.insert(ItemStack::new(name, count));
                        }
                    }
                }
                Ok(())
            }
            InputAction::SetGhostCursor { item_id, .. } => {
                let name = self.item_name_from_id(world, item_id);
                if let Some(player) = world.players.get_mut(&player_index) {
                    player.cursor_ghost = Some(name);
                }
                Ok(())
            }

            InputAction::OpenGui { entity_id } => {
                if let Some(player) = world.players.get_mut(&player_index) {
                    player.opened_entity_id = Some(entity_id);
                }
                Ok(())
            }

            InputAction::CloseGui => {
                if let Some(player) = world.players.get_mut(&player_index) {
                    player.opened_entity_id = None;
                    player.opened_gui_type = None;
                }
                Ok(())
            }

            InputAction::SelectedEntityChanged { position } => {
                let entity_id = self.find_entity_at(world, position);
                if let Some(player) = world.players.get_mut(&player_index) {
                    player.selected_tile_position = Some(position);
                    player.selected_entity_id = entity_id;
                }
                Ok(())
            }

            InputAction::SelectedEntityCleared => {
                if let Some(player) = world.players.get_mut(&player_index) {
                    player.selected_entity_id = None;
                }
                Ok(())
            }
            InputAction::ChangePickingState { picking } => {
                if !picking {
                    return Ok(());
                }
                let target_pos = if let Some(player) = world.players.get_mut(&player_index) {
                    if player.main_inventory.is_none() {
                        player.main_inventory = Some(Inventory::new(80));
                    }
                    player.selected_tile_position.unwrap_or(player.position)
                } else {
                    return Ok(());
                };

                let mut closest: Option<(u32, i32)> = None;
                if let Some(surface) = world.nauvis() {
                    for (id, entity) in &surface.entities {
                        if entity.name != "item-entity" {
                            continue;
                        }
                        let dx = entity.position.x.0 - target_pos.x.0;
                        let dy = entity.position.y.0 - target_pos.y.0;
                        let dist = dx * dx + dy * dy;
                        if dist <= 2 * 256 * 2 * 256 {
                            if closest.map(|(_, d)| dist < d).unwrap_or(true) {
                                closest = Some((*id, dist));
                            }
                        }
                    }
                }

                if let Some((entity_id, _)) = closest {
                    let mut picked_stack = None;
                    if let Some(surface) = world.nauvis_mut() {
                        if let Some(entity) = surface.get_entity_mut(entity_id) {
                            picked_stack = entity.item_stack.take();
                        }
                    }
                    let Some(stack) = picked_stack else { return Ok(()); };

                    let mut remainder = Some(stack);
                    if let Some(player) = world.players.get_mut(&player_index) {
                        if let Some(inv) = player.main_inventory.as_mut() {
                            remainder = inv.insert(remainder.take().unwrap());
                        }
                    }

                    if let Some(surface) = world.nauvis_mut() {
                        if remainder.is_none() {
                            surface.remove_entity(entity_id);
                        } else if let Some(entity) = surface.get_entity_mut(entity_id) {
                            entity.item_stack = remainder;
                        }
                    }
                }
                Ok(())
            }
            InputAction::Build { position, direction, .. } => {
                let name = if let Some(player) = world.players.get_mut(&player_index) {
                    let name = player
                        .cursor_stack
                        .as_ref()
                        .map(|s| s.name.clone())
                        .or_else(|| player.cursor_ghost.clone())
                        .unwrap_or_else(|| "unknown-entity".to_string());
                    if let Some(stack) = player.cursor_stack.as_mut() {
                        if stack.count > 0 {
                            stack.count -= 1;
                        }
                        if stack.count == 0 {
                            player.cursor_stack = None;
                        }
                    }
                    name
                } else {
                    "unknown-entity".to_string()
                };
                let id = world.next_entity_id();
                let entity_type = crate::state::entity::entity_type_from_name(&name);
                let mut entity = crate::state::entity::Entity::new(id, name.clone(), position)
                    .with_direction(direction)
                    .with_type(entity_type);
                entity.data = crate::state::entity::default_entity_data_for_type(entity_type);
                if entity_type == crate::state::entity::EntityType::TrainStop {
                    if let crate::state::entity::EntityData::TrainStop(ref mut data) = entity.data {
                        if data.station_name.is_empty() {
                            data.station_name = format!("train-stop-{}", id);
                        }
                    }
                }
                crate::state::entity::init_entity_inventories(&mut entity);
                crate::state::entity::init_belt_metadata(&mut entity);
                if let Some(proto) = Prototypes::global().and_then(|p| p.entity(&entity.name)) {
                    crate::state::entity::apply_entity_prototype(&mut entity, proto);
                }
                if let Some(surface) = world.nauvis_mut() {
                    surface.add_entity(entity);
                }
                Ok(())
            }
            InputAction::BuildTerrain { position, terrain_id } => {
                let tile_name = self.tile_name_from_id(world, terrain_id as u16);
                if let Some(surface) = world.nauvis_mut() {
                    let tile_pos = crate::codec::TilePosition::from(position);
                    let chunk_pos = crate::codec::ChunkPosition::from_tile(tile_pos);
                    let chunk = surface.get_or_create_chunk(chunk_pos);
                    let local_x = tile_pos.x.rem_euclid(32) as u8;
                    let local_y = tile_pos.y.rem_euclid(32) as u8;
                    chunk.set_tile(local_x, local_y, crate::state::surface::Tile::new(tile_name));
                    chunk.generated = true;
                }
                Ok(())
            }
            InputAction::RotateEntity { position, reverse } => {
                let entity_id = self.find_entity_at(world, position);
                if let Some(entity_id) = entity_id {
                    if let Some(surface) = world.nauvis_mut() {
                        if let Some(entity) = surface.get_entity_mut(entity_id) {
                            entity.direction = if reverse {
                                entity.direction.rotate_ccw()
                            } else {
                                entity.direction.rotate_cw()
                            };
                        }
                    }
                }
                Ok(())
            }
            InputAction::SetFilter { location, item_id, .. } => {
                let name = self.item_name_from_id(world, item_id);
                let entity_id = world.players.get(&player_index).and_then(|p| p.selected_entity_id);
                if let Some(entity_id) = entity_id {
                    if let Some(surface) = world.nauvis_mut() {
                        if let Some(entity) = surface.get_entity_mut(entity_id) {
                            if let crate::state::entity::EntityData::Inserter(ref mut data) = entity.data {
                                let slot = location.slot_index as usize;
                                if data.filters.len() <= slot {
                                    data.filters.resize(slot + 1, String::new());
                                }
                                data.filters[slot] = name;
                            }
                        }
                    }
                }
                Ok(())
            }
            InputAction::SetLogisticFilterItem { filter, .. }
            | InputAction::SetLogisticFilterSignal { filter, .. } => {
                if let Some(player) = world.players.get_mut(&player_index) {
                    player.logistic_requests.push(filter);
                }
                Ok(())
            }
            InputAction::SetupAssemblingMachine { recipe_id, .. } => {
                let recipe_name = world
                    .recipe_id_map
                    .get(&recipe_id)
                    .cloned()
                    .unwrap_or_else(|| format!("recipe-{}", recipe_id));
                let entity_id = world.players.get(&player_index).and_then(|p| p.selected_entity_id);
                if let Some(entity_id) = entity_id {
                    if let Some(surface) = world.nauvis_mut() {
                        if let Some(entity) = surface.get_entity_mut(entity_id) {
                            if let crate::state::entity::EntityData::AssemblingMachine(ref mut data) = entity.data {
                                data.recipe = Some(recipe_name);
                            }
                        }
                    }
                }
                Ok(())
            }
            InputAction::StartResearch { technology_id } => {
                let tech = world
                    .tech_id_map
                    .get(&technology_id)
                    .cloned()
                    .unwrap_or_else(|| format!("tech-{}", technology_id));
                world.research.current_research = Some(tech);
                world.research.progress = 0.0;
                Ok(())
            }
            InputAction::CancelResearch => {
                world.research.current_research = None;
                world.research.progress = 0.0;
                Ok(())
            }
            InputAction::DropItem { position } => {
                if let Some(player) = world.players.get_mut(&player_index) {
                    if let Some(stack) = player.cursor_stack.take() {
                        let id = world.next_entity_id();
                        if let Some(surface) = world.nauvis_mut() {
                            let mut entity = crate::state::entity::Entity::new(id, "item-entity".into(), position);
                            entity.item_stack = Some(stack);
                            surface.add_entity(entity);
                        }
                    }
                }
                Ok(())
            }
            InputAction::UseItem { .. } => Ok(()),
            InputAction::PlayerJoinGame { player_index_plus_one, username, .. } => {
                let id = player_index_plus_one;
                let spawn = world.spawn_position;
                let player = world.add_player(id, username);
                player.connected = true;
                player.position = spawn;
                if player.main_inventory.is_none() {
                    init_freeplay_inventory(player);
                }
                Ok(())
            }

            InputAction::PlayerLeaveGame { .. } => {
                if let Some(player) = world.players.get_mut(&player_index) {
                    player.connected = false;
                }
                Ok(())
            }

            // For unhandled actions, just succeed silently
            // Full implementation would handle all 210+ action types
            _ => Ok(()),
        }
    }

    fn execute_start_walking(&mut self, world: &mut GameWorld, player_index: u16, direction_x: f64, direction_y: f64) -> Result<()> {
        // Convert direction vector to Direction enum
        let direction = direction_from_vector(direction_x, direction_y);
        if let Some(player) = world.players.get_mut(&player_index) {
            player.walking = true;
            player.walking_direction = direction;
            player.direction = direction;
        }
        Ok(())
    }

    fn execute_stop_walking(&mut self, world: &mut GameWorld, player_index: u16) -> Result<()> {
        if let Some(player) = world.players.get_mut(&player_index) {
            player.walking = false;
        }
        Ok(())
    }

    fn execute_begin_mining(&mut self, world: &mut GameWorld, player_index: u16, position: MapPosition) -> Result<()> {
        let entity_id = self.find_entity_at(world, position);
        if let Some(player) = world.players.get_mut(&player_index) {
            player.selected_tile_position = Some(position);
        }
        if let Some(entity_id) = entity_id {
            let mined = if let Some(surface) = world.nauvis_mut() {
                let mut mined_name = None;
                let mut remove_entity = false;
                if let Some(entity) = surface.get_entity_mut(entity_id) {
                    if let crate::state::entity::EntityData::Resource(ref mut data) = entity.data {
                        if data.amount > 0 {
                            data.amount = data.amount.saturating_sub(1);
                            mined_name = Some(entity.name.clone());
                            if data.amount == 0 && !data.infinite {
                                remove_entity = true;
                            }
                        }
                    }
                }
                if remove_entity {
                    surface.remove_entity(entity_id);
                }
                mined_name
            } else {
                None
            };
            if let Some(name) = mined {
                if let Some(player) = world.players.get_mut(&player_index) {
                    let inv = self.ensure_player_inventory(player, 1);
                    inv.insert(ItemStack::new(name, 1));
                }
            }
        }
        Ok(())
    }

    fn execute_stop_mining(&mut self, world: &mut GameWorld, player_index: u16) -> Result<()> {
        if let Some(player) = world.players.get_mut(&player_index) {
            player.selected_tile_position = None;
        }
        Ok(())
    }

    fn execute_change_riding_state(
        &mut self,
        world: &mut GameWorld,
        player_index: u16,
        acceleration: crate::codec::RidingAcceleration,
        direction: crate::codec::RidingDirection,
    ) -> Result<()> {
        let vehicle_id = world
            .players
            .get(&player_index)
            .and_then(|p| p.riding_vehicle);
        let Some(vehicle_id) = vehicle_id else {
            return Ok(());
        };
        let Some(surface) = world.nauvis_mut() else {
            return Ok(());
        };
        let Some(vehicle) = surface.get_entity_mut(vehicle_id) else {
            return Ok(());
        };

        let speed = match acceleration {
            crate::codec::RidingAcceleration::Nothing => 0.0,
            crate::codec::RidingAcceleration::Accelerating => 0.15,
            crate::codec::RidingAcceleration::Braking => 0.0,
            crate::codec::RidingAcceleration::Reversing => -0.1,
        };
        let (dx, dy) = match direction {
            crate::codec::RidingDirection::Straight => vehicle.direction.to_vector(),
            crate::codec::RidingDirection::Left => vehicle.direction.rotate_ccw().to_vector(),
            crate::codec::RidingDirection::Right => vehicle.direction.rotate_cw().to_vector(),
        };
        let (vx, vy) = vehicle.position.to_tiles();
        let nx = vx + dx * speed;
        let ny = vy + dy * speed;
        vehicle.position = crate::codec::MapPosition::from_tiles(nx, ny);
        let new_pos = vehicle.position;
        if let Some(player) = world.players.get_mut(&player_index) {
            player.position = new_pos;
        }
        Ok(())
    }

    fn execute_craft(&mut self, world: &mut GameWorld, player_index: u16, recipe_id: u16, count: u32) -> Result<()> {
        // Look up recipe by ID
        let recipe = match world.recipes.get(recipe_id) {
            Some(r) => r.clone(),
            None => {
                // Unknown recipe - just track the queue
                if let Some(player) = world.players.get_mut(&player_index) {
                    player.crafting_queue_size += count;
                }
                return Ok(());
            }
        };

        // Get player's inventory
        let player = match world.players.get_mut(&player_index) {
            Some(p) => p,
            None => return Ok(()),
        };

        // Ensure player has main inventory
        if player.main_inventory.is_none() {
            player.main_inventory = Some(Inventory::new(80));
        }

        let inv = player.main_inventory.as_mut().unwrap();

        // Check if player has required ingredients
        let available = inv.contents();
        if !recipe.can_craft(&available, count) {
            // Not enough ingredients - the server will reject this anyway
            // but we track it for the queue display
            player.crafting_queue_size += count;
            return Ok(());
        }

        // Deduct ingredients
        for ingredient in &recipe.ingredients {
            inv.remove(&ingredient.name, ingredient.amount * count);
        }

        // Add to crafting queue
        player.crafting_queue_size += count;

        // Schedule completion (crafting time in ticks, 60 ticks per second)
        let crafting_ticks = (recipe.crafting_time * 60.0) as u32;
        let finish_tick = world.tick + crafting_ticks;
        self.pending_crafts.push((finish_tick, player_index, recipe.name.clone(), count));

        Ok(())
    }

    fn execute_chat(&mut self, _world: &mut GameWorld, _player_index: u16, _message: String) -> Result<()> {
        // Chat messages don't affect game state directly
        // In a full implementation, could emit events
        Ok(())
    }

    fn ensure_player_inventory<'a>(&self, player: &'a mut Player, index: u8) -> &'a mut Inventory {
        match index {
            1 => player.main_inventory.get_or_insert_with(|| Inventory::new(80)),
            2 => player.quickbar.get_or_insert_with(|| Inventory::new(20)),
            3 => player.guns_inventory.get_or_insert_with(|| Inventory::new(10)),
            4 => player.ammo_inventory.get_or_insert_with(|| Inventory::new(10)),
            5 => player.armor_inventory.get_or_insert_with(|| Inventory::new(1)),
            6 => player.trash_inventory.get_or_insert_with(|| Inventory::new(20)),
            _ => player.main_inventory.get_or_insert_with(|| Inventory::new(80)),
        }
    }

    fn item_name_from_id(&self, world: &GameWorld, item_id: u16) -> String {
        world
            .item_id_map
            .get(&item_id)
            .cloned()
            .unwrap_or_else(|| format!("item-{}", item_id))
    }

    fn tile_name_from_id(&self, world: &GameWorld, tile_id: u16) -> String {
        world
            .tile_id_map
            .get(&tile_id)
            .cloned()
            .unwrap_or_else(|| format!("tile-{}", tile_id))
    }

    fn ensure_entity_inventory<'a>(&self, entity: &'a mut crate::state::entity::Entity, key: &str, size: usize) -> &'a mut Inventory {
        entity
            .inventories
            .entry(key.to_string())
            .or_insert_with(move || Inventory::new(size))
    }

    fn first_filled_slot(&self, inv: &Inventory) -> Option<usize> {
        inv.slots.iter().position(|s| s.is_some())
    }

    fn find_entity_at(&self, world: &GameWorld, position: MapPosition) -> Option<u32> {
        let radius = 1;
        let mut best = None;
        let mut best_dist = i32::MAX;
        for e in world.find_entities_near(1, position, radius) {
            let dx = e.position.x.0 - position.x.0;
            let dy = e.position.y.0 - position.y.0;
            let dist = dx * dx + dy * dy;
            if dist < best_dist {
                best_dist = dist;
                best = Some(e.id);
            }
        }
        best
    }

    fn find_resource_near(
        &self,
        surface: &crate::state::surface::Surface,
        position: MapPosition,
        radius_tiles: f64,
    ) -> Option<(u32, String, u32, bool, f32)> {
        let radius_fixed = (radius_tiles * 256.0) as i32;
        let mut best: Option<(u32, String, u32, bool, f32, i32)> = None;
        for (id, entity) in &surface.entities {
            let crate::state::entity::EntityData::Resource(data) = &entity.data else {
                continue;
            };
            if !data.infinite && data.amount == 0 {
                continue;
            }
            let dx = entity.position.x.0 - position.x.0;
            let dy = entity.position.y.0 - position.y.0;
            let dist = dx * dx + dy * dy;
            if dist > radius_fixed * radius_fixed {
                continue;
            }
            let mining_time = if data.mining_time > 0.0 { data.mining_time } else { 1.0 };
            let entry = (*id, entity.name.clone(), data.amount, data.infinite, mining_time, dist);
            if best.as_ref().map(|b| dist < b.5).unwrap_or(true) {
                best = Some(entry);
            }
        }
        best.map(|(id, name, amount, infinite, mining_time, _)| (id, name, amount, infinite, mining_time))
    }

    fn default_inserter_positions(
        &self,
        entity: &crate::state::entity::Entity,
        px: f64,
        py: f64,
    ) -> (MapPosition, MapPosition) {
        if let Some(proto) = Prototypes::global().and_then(|p| p.entity(&entity.name)) {
            if let (Some(pickup), Some(drop)) = (proto.pickup_position, proto.drop_position) {
                let (pdx, pdy) = self.rotate_offset(pickup.0, pickup.1, entity.direction);
                let (ddx, ddy) = self.rotate_offset(drop.0, drop.1, entity.direction);
                let pickup_pos = crate::codec::MapPosition::from_tiles(px + pdx, py + pdy);
                let drop_pos = crate::codec::MapPosition::from_tiles(px + ddx, py + ddy);
                return (pickup_pos, drop_pos);
            }
        }

        let (dx, dy) = entity.direction.to_vector();
        let default_pickup = crate::codec::MapPosition::from_tiles(px - dx, py - dy);
        let default_drop = crate::codec::MapPosition::from_tiles(px + dx, py + dy);
        (default_pickup, default_drop)
    }

    fn rotate_offset(&self, dx: f64, dy: f64, direction: Direction) -> (f64, f64) {
        match direction {
            Direction::North => (dx, dy),
            Direction::East => (-dy, dx),
            Direction::South => (-dx, -dy),
            Direction::West => (dy, -dx),
            _ => (dx, dy),
        }
    }

    fn inserter_stack_and_speed(
        &self,
        entity: &crate::state::entity::Entity,
        override_stack: Option<u8>,
    ) -> (u32, u32) {
        let mut stack = override_stack.map(|v| v as u32).unwrap_or(1);
        let mut speed_ticks = 15u32;
        if let Some(proto) = Prototypes::global().and_then(|p| p.entity(&entity.name)) {
            if let Some(bonus) = proto.inserter_stack_size {
                stack = stack.saturating_add(bonus as u32).max(1);
            }
            let rotation = proto.inserter_rotation_speed.unwrap_or(0.0);
            let extension = proto.inserter_extension_speed.unwrap_or(0.0);
            let speed = rotation + extension;
            if speed > 0.0 {
                speed_ticks = (1.0 / speed).clamp(5.0, 120.0) as u32;
            }
        }
        (stack, speed_ticks.max(1))
    }

    fn tick_trains(&mut self, world: &mut GameWorld) {
        let train_ids: Vec<u32> = world.trains.keys().copied().collect();
        for train_id in train_ids {
            let (target, schedule_len, manual) = {
                let Some(state) = world.trains.get(&train_id) else { continue; };
                (state.schedule.get(state.current).and_then(|s| s.position), state.schedule.len(), state.manual_mode)
            };
            if manual || schedule_len == 0 {
                if let Some(state) = world.trains.get_mut(&train_id) {
                    state.speed = 0.0;
                }
                continue;
            }
            let Some((tx, ty)) = target else {
                if let Some(state) = world.trains.get_mut(&train_id) {
                    state.speed = 0.0;
                }
                continue;
            };
            let pos = world
                .nauvis()
                .and_then(|s| s.get_entity(train_id))
                .map(|e| e.position.to_tiles());
            let Some((x, y)) = pos else {
                if let Some(state) = world.trains.get_mut(&train_id) {
                    state.speed = 0.0;
                }
                continue;
            };

            let dx = tx - x;
            let dy = ty - y;
            let dist = (dx * dx + dy * dy).sqrt();
            let mut next_pos = None;
            let mut next_speed = 0.0;
            let mut advance = false;
            if dist < 0.2 {
                advance = true;
                next_speed = 0.0;
            } else {
                let speed = 0.1;
                let nx = x + dx / dist * speed;
                let ny = y + dy / dist * speed;
                next_pos = Some((nx, ny));
                next_speed = speed;
            }

            if let Some(state) = world.trains.get_mut(&train_id) {
                state.speed = next_speed;
                if advance {
                    state.current = (state.current + 1) % schedule_len;
                }
            }
            if let Some((nx, ny)) = next_pos {
                if let Some(surface) = world.nauvis_mut() {
                    if let Some(entity) = surface.get_entity_mut(train_id) {
                        entity.position = crate::codec::MapPosition::from_tiles(nx, ny);
                    }
                }
            }
        }
    }

    fn find_entity_at_surface(
        &self,
        surface: &crate::state::surface::Surface,
        position: MapPosition,
        radius_tiles: i32,
    ) -> Option<u32> {
        let radius_fixed = radius_tiles * 256;
        let left = position.x.0 - radius_fixed;
        let right = position.x.0 + radius_fixed;
        let top = position.y.0 - radius_fixed;
        let bottom = position.y.0 + radius_fixed;
        surface
            .entities
            .iter()
            .filter(|(_, e)| {
                e.position.x.0 >= left
                    && e.position.x.0 <= right
                    && e.position.y.0 >= top
                    && e.position.y.0 <= bottom
            })
            .min_by_key(|(_, e)| {
                let dx = e.position.x.0 - position.x.0;
                let dy = e.position.y.0 - position.y.0;
                dx * dx + dy * dy
            })
            .map(|(id, _)| *id)
    }

    fn take_items_from_entity(
        &self,
        entity: &mut crate::state::entity::Entity,
        max_count: u32,
        filters: &[String],
    ) -> Option<ItemStack> {
        let allow_any = filters.is_empty();
        if let crate::state::entity::EntityData::TransportBelt(data) = &mut entity.data {
            for lane in 0..2 {
                if !data.lane_items[lane].is_empty() {
                    let name = data.lane_items[lane][0].clone();
                    if allow_any || filters.contains(&name) {
                        data.lane_items[lane].remove(0);
                        data.line_contents = data
                            .lane_items
                            .iter()
                            .flat_map(|l| l.iter().cloned())
                            .collect();
                        return Some(ItemStack::new(name, 1));
                    }
                }
            }
        }

        if let Some(stack) = entity.item_stack.as_mut() {
            if stack.count > 0 && (allow_any || filters.contains(&stack.name)) {
                let take = stack.count.min(max_count);
                stack.count -= take;
                let name = stack.name.clone();
                if stack.count == 0 {
                    entity.item_stack = None;
                }
                return Some(ItemStack::new(name, take));
            }
        }

        let order = ["output", "result", "main", "source", "input", "fuel"];
        for key in order {
            if let Some(inv) = entity.inventories.get_mut(key) {
                for slot in inv.slots.iter_mut() {
                    if let Some(stack) = slot.as_mut() {
                        if stack.count > 0 && (allow_any || filters.contains(&stack.name)) {
                            let take = stack.count.min(max_count);
                            stack.count -= take;
                            let name = stack.name.clone();
                            if stack.count == 0 {
                                *slot = None;
                            }
                            return Some(ItemStack::new(name, take));
                        }
                    }
                }
            }
        }
        None
    }

    fn insert_stack_into_entity(
        &self,
        entity: &mut crate::state::entity::Entity,
        stack: ItemStack,
    ) -> Option<ItemStack> {
        if let crate::state::entity::EntityData::TransportBelt(data) = &mut entity.data {
            let mut remaining = stack.count;
            while remaining > 0 && data.line_contents.len() < 8 {
                let lane = if data.lane_items[0].len() <= data.lane_items[1].len() { 0 } else { 1 };
                if data.lane_items[lane].len() >= 4 {
                    break;
                }
                data.lane_items[lane].push(stack.name.clone());
                remaining -= 1;
            }
            data.line_contents = data
                .lane_items
                .iter()
                .flat_map(|l| l.iter().cloned())
                .collect();
            if remaining == 0 {
                return None;
            }
            return Some(ItemStack::new(stack.name, remaining));
        }
        let order = ["input", "main", "source", "fuel", "output", "result"];
        let mut remainder = Some(stack);
        for key in order {
            if let Some(inv) = entity.inventories.get_mut(key) {
                if let Some(rem) = remainder.take() {
                    remainder = inv.insert(rem);
                }
                if remainder.is_none() {
                    return None;
                }
            }
        }
        remainder
    }

    fn insert_into_entity(&self, entity: &mut crate::state::entity::Entity, stack: ItemStack) -> bool {
        self.insert_stack_into_entity(entity, stack).is_none()
    }

    fn put_back_item(&self, entity: &mut crate::state::entity::Entity, stack: ItemStack) {
        if self.insert_stack_into_entity(entity, stack.clone()).is_none() {
            return;
        }
        if entity.inventories.is_empty() {
            entity.item_stack = Some(stack);
        }
    }
}

impl Default for ActionExecutor {
    fn default() -> Self {
        Self::new()
    }
}

/// Initialize player inventory with freeplay starting items
pub fn init_freeplay_inventory(player: &mut crate::state::Player) {
    let mut inv = Inventory::new(80);

    // Freeplay scenario starting items
    inv.insert(ItemStack::new("burner-mining-drill", 1));
    inv.insert(ItemStack::new("stone-furnace", 1));
    inv.insert(ItemStack::new("iron-plate", 8));
    inv.insert(ItemStack::new("pistol", 1));
    inv.insert(ItemStack::new("firearm-magazine", 10));

    player.main_inventory = Some(inv);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_start_walking() {
        let mut executor = ActionExecutor::new();
        let mut world = GameWorld::new();
        world.add_player(1, "Test".into());

        // North = (0, -1) in Factorio coordinates
        let action = InputAction::StartWalking { direction_x: 0.0, direction_y: -1.0 };
        executor.execute(&mut world, 1, action).unwrap();

        let player = world.get_player(1).unwrap();
        assert!(player.walking);
        assert_eq!(player.walking_direction, Direction::North);
    }

    #[test]
    fn test_stop_walking() {
        let mut executor = ActionExecutor::new();
        let mut world = GameWorld::new();
        world.add_player(1, "Test".into());

        // Start walking East = (1, 0)
        executor.execute(&mut world, 1, InputAction::StartWalking { direction_x: 1.0, direction_y: 0.0 }).unwrap();

        // Stop walking
        executor.execute(&mut world, 1, InputAction::StopWalking).unwrap();

        let player = world.get_player(1).unwrap();
        assert!(!player.walking);
    }

    #[test]
    fn test_clear_cursor() {
        let mut executor = ActionExecutor::new();
        let mut world = GameWorld::new();
        world.add_player(1, "Test".into());

        executor.execute(&mut world, 1, InputAction::ClearCursor).unwrap();
        // Just verify it doesn't crash
    }
}
