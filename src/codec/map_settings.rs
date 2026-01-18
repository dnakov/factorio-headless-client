use crate::codec::BinaryReader;
use crate::error::Result;

fn skip_opt_bool(reader: &mut BinaryReader) -> Result<()> {
    if reader.read_bool()? {
        let _ = reader.read_bool()?;
    }
    Ok(())
}

fn skip_opt_bytes(reader: &mut BinaryReader, n: usize) -> Result<()> {
    if reader.read_bool()? {
        reader.skip(n)?;
    }
    Ok(())
}

fn skip_opt_vec_u32(reader: &mut BinaryReader) -> Result<()> {
    if reader.read_bool()? {
        let count = reader.read_opt_u32()? as usize;
        for _ in 0..count {
            let _ = reader.read_u32_le()?;
        }
    }
    Ok(())
}

fn skip_opt_vec_f64(reader: &mut BinaryReader) -> Result<()> {
    if reader.read_bool()? {
        let count = reader.read_opt_u32()? as usize;
        for _ in 0..count {
            let _ = reader.read_f64_le()?;
        }
    }
    Ok(())
}

fn skip_pollution_settings(reader: &mut BinaryReader) -> Result<()> {
    skip_opt_bool(reader)?; // enabled
    for _ in 0..11 {
        skip_opt_bytes(reader, 8)?;
    }
    Ok(())
}

fn skip_state_steering_settings(reader: &mut BinaryReader) -> Result<()> {
    skip_opt_bytes(reader, 8)?; // radius
    skip_opt_bytes(reader, 8)?; // separation_factor
    skip_opt_bytes(reader, 8)?; // separation_force
    skip_opt_bool(reader)?; // force_unit_fuzzy_goto_behavior
    Ok(())
}

fn skip_steering_settings(reader: &mut BinaryReader) -> Result<()> {
    skip_state_steering_settings(reader)?; // default
    skip_state_steering_settings(reader)?; // moving
    Ok(())
}

fn skip_enemy_evolution_settings(reader: &mut BinaryReader) -> Result<()> {
    skip_opt_bool(reader)?; // enabled
    skip_opt_bytes(reader, 8)?; // time_factor
    skip_opt_bytes(reader, 8)?; // destroy_factor
    skip_opt_bytes(reader, 8)?; // pollution_factor
    Ok(())
}

fn skip_enemy_expansion_settings(reader: &mut BinaryReader) -> Result<()> {
    skip_opt_bool(reader)?; // enabled
    skip_opt_bytes(reader, 4)?; // max_expansion_distance
    skip_opt_bytes(reader, 4)?; // friendly_base_influence_radius
    skip_opt_bytes(reader, 4)?; // enemy_building_influence_radius
    skip_opt_bytes(reader, 8)?; // building_coefficient
    skip_opt_bytes(reader, 8)?; // other_base_coefficient
    skip_opt_bytes(reader, 8)?; // neighbouring_chunk_coefficient
    skip_opt_bytes(reader, 8)?; // neighbouring_base_chunk_coefficient
    skip_opt_bytes(reader, 8)?; // max_colliding_tiles_coefficient
    skip_opt_bytes(reader, 4)?; // settler_group_min_size
    skip_opt_bytes(reader, 4)?; // settler_group_max_size
    skip_opt_bytes(reader, 4)?; // min_expansion_cooldown
    skip_opt_bytes(reader, 4)?; // max_expansion_cooldown
    Ok(())
}

fn skip_unit_group_settings(reader: &mut BinaryReader) -> Result<()> {
    skip_opt_bytes(reader, 4)?; // min_group_gathering_time
    skip_opt_bytes(reader, 4)?; // max_group_gathering_time
    skip_opt_bytes(reader, 4)?; // max_wait_time_for_late_members
    skip_opt_bytes(reader, 8)?; // max_group_radius
    skip_opt_bytes(reader, 8)?; // min_group_radius
    skip_opt_bytes(reader, 8)?; // max_member_speedup_when_behind
    skip_opt_bytes(reader, 8)?; // max_member_slowdown_when_ahead
    skip_opt_bytes(reader, 8)?; // max_group_slowdown_factor
    skip_opt_bytes(reader, 8)?; // max_group_member_fallback_factor
    skip_opt_bytes(reader, 8)?; // member_disown_distance
    skip_opt_bytes(reader, 4)?; // tick_tolerance_when_member_arrives
    skip_opt_bytes(reader, 4)?; // max_gathering_unit_groups
    skip_opt_bytes(reader, 4)?; // max_unit_group_size
    Ok(())
}

fn skip_path_finder_settings(reader: &mut BinaryReader) -> Result<()> {
    skip_opt_bytes(reader, 4)?; // fwd2bwd_ratio
    skip_opt_bytes(reader, 8)?; // goal_pressure_ratio
    skip_opt_bool(reader)?; // use_path_cache
    skip_opt_bytes(reader, 8)?; // max_steps_worked_per_tick
    skip_opt_bytes(reader, 4)?; // max_work_done_per_tick
    skip_opt_bytes(reader, 4)?; // short_cache_size
    skip_opt_bytes(reader, 4)?; // long_cache_size
    skip_opt_bytes(reader, 8)?; // short_cache_min_cacheable_distance
    skip_opt_bytes(reader, 4)?; // short_cache_min_algo_steps_to_cache
    skip_opt_bytes(reader, 8)?; // long_cache_min_cacheable_distance
    skip_opt_bytes(reader, 4)?; // cache_max_connect_to_cache_steps_multiplier
    skip_opt_bytes(reader, 8)?; // cache_accept_path_start_distance_ratio
    skip_opt_bytes(reader, 8)?; // cache_accept_path_end_distance_ratio
    skip_opt_bytes(reader, 8)?; // negative_cache_accept_path_start_distance_ratio
    skip_opt_bytes(reader, 8)?; // negative_cache_accept_path_end_distance_ratio
    skip_opt_bytes(reader, 8)?; // cache_path_start_distance_rating_multiplier
    skip_opt_bytes(reader, 8)?; // cache_path_end_distance_rating_multiplier
    skip_opt_bytes(reader, 8)?; // stale_enemy_with_same_destination_collision_penalty
    skip_opt_bytes(reader, 8)?; // ignore_moving_enemy_collision_distance
    skip_opt_bytes(reader, 8)?; // enemy_with_different_destination_collision_penalty
    skip_opt_bytes(reader, 8)?; // general_entity_collision_penalty
    skip_opt_bytes(reader, 8)?; // general_entity_subsequent_collision_penalty
    skip_opt_bytes(reader, 8)?; // extended_collision_penalty
    skip_opt_bytes(reader, 4)?; // max_clients_to_accept_any_new_request
    skip_opt_bytes(reader, 4)?; // max_clients_to_accept_short_new_request
    skip_opt_bytes(reader, 4)?; // direct_distance_to_consider_short_request
    skip_opt_bytes(reader, 4)?; // short_request_max_steps
    skip_opt_bytes(reader, 8)?; // short_request_ratio
    skip_opt_bytes(reader, 4)?; // min_steps_to_check_path_find_termination
    skip_opt_bytes(reader, 8)?; // start_to_goal_cost_multiplier_to_terminate_path_find
    skip_opt_vec_u32(reader)?; // overload_levels
    skip_opt_vec_f64(reader)?; // overload_multipliers
    skip_opt_bytes(reader, 4)?; // negative_path_cache_delay_interval
    Ok(())
}

fn skip_difficulty_settings(reader: &mut BinaryReader) -> Result<()> {
    skip_opt_bytes(reader, 8)?; // technology_price_multiplier
    skip_opt_bytes(reader, 8)?; // spoil_time_modifier
    Ok(())
}

fn skip_asteroid_settings(reader: &mut BinaryReader) -> Result<()> {
    skip_opt_bytes(reader, 8)?; // spawning_rate
    skip_opt_bytes(reader, 4)?; // max_ray_portals_expanded_per_tick
    Ok(())
}

pub fn skip_map_settings(reader: &mut BinaryReader) -> Result<()> {
    skip_pollution_settings(reader)?;
    skip_steering_settings(reader)?;
    skip_enemy_evolution_settings(reader)?;
    skip_enemy_expansion_settings(reader)?;
    skip_unit_group_settings(reader)?;
    skip_path_finder_settings(reader)?;
    let _max_failed_behavior_count = reader.read_u32_le()?;
    skip_difficulty_settings(reader)?;
    skip_asteroid_settings(reader)?;
    Ok(())
}
