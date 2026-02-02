use crate::codec::{
    BinaryWriter, ClientItemStackLocation, Direction, InputAction as CodecInputAction,
    ItemStackTransferSpecification, LogisticFilter, MapPosition, RelativeItemStackLocation,
    RidingAcceleration, RidingDirection, ShootingState,
};
use crate::error::{Error, Result};
use super::{Connection, ConnectionState};
use crate::protocol::message::InputAction;

pub struct ConnectionActions<'a> {
    conn: &'a mut Connection,
}

impl<'a> ConnectionActions<'a> {
    pub fn new(conn: &'a mut Connection) -> Self {
        Self { conn }
    }

    /// Send a chat message
    pub async fn send_chat(&mut self, message: &str) -> Result<()> {
        if self.conn.state() != ConnectionState::InGame {
            return Err(Error::InvalidPacket("must be in game to send chat".into()));
        }

        let player_index = self.conn.player_index.unwrap_or(1);
        let mut payload_writer = BinaryWriter::with_capacity(3 + message.len() + 5);
        payload_writer.write_opt_u16(player_index);
        payload_writer.write_string(message);
        let payload = payload_writer.into_vec();

        let mut writer = BinaryWriter::with_capacity(32 + payload.len());
        writer.write_opt_u32(0x01);
        writer.write_opt_u32(0x01);
        writer.write_opt_u16(0x68);
        writer.write_u32_le(0);
        writer.write_opt_u16(player_index);
        writer.write_opt_u32(1);
        writer.write_opt_u32(0);
        writer.write_opt_u32(payload.len() as u32);
        writer.write_bytes(&payload);
        let data = writer.into_vec();

        self.conn.send_action_packet(0x06, &data).await
    }

    /// Execute a server command (e.g. "/c ...")
    pub async fn send_server_command(&mut self, command: &str) -> Result<()> {
        if self.conn.state() != ConnectionState::InGame {
            return Err(Error::InvalidPacket("must be in game".into()));
        }
        let action = CodecInputAction::ServerCommand {
            command: command.to_string(),
        };
        self.conn.send_codec_action(action).await
    }

    /// Toggle picking items on the ground
    pub async fn send_change_picking_state(&mut self, picking: bool) -> Result<()> {
        if self.conn.state() != ConnectionState::InGame {
            return Err(Error::InvalidPacket("must be in game".into()));
        }
        let action = CodecInputAction::ChangePickingState { picking };
        self.conn.send_codec_action(action).await
    }

    /// Continue singleplayer / respawn flow (used by finished-game respawn)
    pub async fn send_continue_singleplayer(&mut self) -> Result<()> {
        if self.conn.state() != ConnectionState::InGame {
            return Err(Error::InvalidPacket("must be in game".into()));
        }
        let action = CodecInputAction::ContinueSinglePlayer;
        self.conn.send_codec_action(action).await
    }

    /// Start walking in a direction (0-7)
    pub async fn send_walk(&mut self, direction: u8) -> Result<()> {
        if self.conn.state() != ConnectionState::InGame {
            if std::env::var("FACTORIO_DEBUG").is_ok() {
                eprintln!("[DEBUG] send_walk: state={:?} (expected InGame)", self.conn.state());
            }
            return Err(Error::InvalidPacket("must be in game".into()));
        }
        let dir = Direction::from_u8(direction)
            .ok_or_else(|| Error::InvalidPacket("invalid walking direction".into()))?;
        let (direction_x, direction_y) = dir.to_vector();
        let action = CodecInputAction::StartWalking { direction_x, direction_y };
        self.conn.send_codec_action(action).await?;
        // Apply locally immediately; server does not echo our own actions.
        self.conn.walk_active = true;
        self.conn.walk_dir = (direction_x, direction_y);
        self.conn.walk_last_tick = self.conn.server_tick;
        Ok(())
    }

    /// Stop walking
    pub async fn send_stop_walk(&mut self) -> Result<()> {
        if self.conn.state() != ConnectionState::InGame {
            return Err(Error::InvalidPacket("must be in game".into()));
        }
        let action = CodecInputAction::StopWalking;
        // Update local position before stopping.
        self.conn.update_position();
        self.conn.send_codec_action(action).await?;
        self.conn.walk_active = false;
        Ok(())
    }

    /// Start mining at current position
    pub async fn send_mine(&mut self, x: f64, y: f64) -> Result<()> {
        if self.conn.state() != ConnectionState::InGame {
            return Err(Error::InvalidPacket("must be in game".into()));
        }
        let position = MapPosition::from_tiles(x, y);
        let action = InputAction::begin_mining_terrain(position);
        self.conn.last_mine_position = Some(position);
        self.conn.send_heartbeat_with_actions(&[action]).await
    }

    /// Begin mining (action type 0x02)
    pub async fn send_begin_mine(&mut self) -> Result<()> {
        if self.conn.state() != ConnectionState::InGame {
            return Err(Error::InvalidPacket("must be in game".into()));
        }
        let action = InputAction::begin_mining();
        self.conn.send_heartbeat_with_actions(&[action]).await
    }

    /// Stop mining
    pub async fn send_stop_mine(&mut self) -> Result<()> {
        if self.conn.state() != ConnectionState::InGame {
            return Err(Error::InvalidPacket("must be in game".into()));
        }
        self.conn.last_mine_position.take();
        let action = InputAction::stop_mining();
        self.conn.send_heartbeat_with_actions(&[action]).await
    }

    /// Build/place an item at a position
    pub async fn send_build(&mut self, x: f64, y: f64, direction: u8) -> Result<()> {
        if self.conn.state() != ConnectionState::InGame {
            return Err(Error::InvalidPacket("must be in game".into()));
        }
        let dir = Direction::from_u8(direction)
            .ok_or_else(|| Error::InvalidPacket("invalid build direction".into()))?;
        let position = MapPosition::from_tiles(x, y);
        let action = CodecInputAction::Build {
            position,
            direction: dir,
            shift_build: false,
            skip_fog_of_war: false,
        };
        self.conn.send_codec_action(action).await
    }

    /// Rotate entity at position
    pub async fn send_rotate(&mut self, x: f64, y: f64, reverse: bool) -> Result<()> {
        if self.conn.state() != ConnectionState::InGame {
            return Err(Error::InvalidPacket("must be in game".into()));
        }
        let position = MapPosition::from_tiles(x, y);
        let action = CodecInputAction::RotateEntity { position, reverse };
        self.conn.send_codec_action(action).await
    }

    /// Craft items
    pub async fn send_craft(&mut self, recipe_id: u16, count: u32) -> Result<()> {
        if self.conn.state() != ConnectionState::InGame {
            return Err(Error::InvalidPacket("must be in game".into()));
        }
        let action = CodecInputAction::Craft { recipe_id, count };
        self.conn.send_codec_action(action).await
    }

    /// Set recipe on an assembling machine (uses currently selected entity)
    pub async fn send_set_recipe(&mut self, recipe_id: u16, quality_id: Option<u8>) -> Result<()> {
        if self.conn.state() != ConnectionState::InGame {
            return Err(Error::InvalidPacket("must be in game".into()));
        }
        let action = CodecInputAction::SetupAssemblingMachine { recipe_id, quality_id };
        self.conn.send_codec_action(action).await
    }

    /// Start researching a technology by id
    pub async fn send_start_research(&mut self, technology_id: u16) -> Result<()> {
        if self.conn.state() != ConnectionState::InGame {
            return Err(Error::InvalidPacket("must be in game".into()));
        }
        let action = CodecInputAction::StartResearch { technology_id };
        self.conn.send_codec_action(action).await
    }

    /// Cancel crafting order by queue index
    pub async fn send_cancel_craft(&mut self, index: u16, count: u32) -> Result<()> {
        if self.conn.state() != ConnectionState::InGame {
            return Err(Error::InvalidPacket("must be in game".into()));
        }
        let action = CodecInputAction::CancelCraft { index, count };
        self.conn.send_codec_action(action).await
    }

    /// Update selected entity based on cursor position
    pub async fn send_selected_entity_changed(&mut self, x: f64, y: f64) -> Result<()> {
        if self.conn.state() != ConnectionState::InGame {
            return Err(Error::InvalidPacket("must be in game".into()));
        }
        let position = MapPosition::from_tiles(x, y);
        let action = CodecInputAction::SelectedEntityChanged { position };
        self.conn.send_codec_action(action).await
    }

    /// Clear selected entity
    pub async fn send_selected_entity_cleared(&mut self) -> Result<()> {
        if self.conn.state() != ConnectionState::InGame {
            return Err(Error::InvalidPacket("must be in game".into()));
        }
        let action = CodecInputAction::SelectedEntityCleared;
        self.conn.send_codec_action(action).await
    }

    /// Clear cursor stack/ghost
    pub async fn send_clear_cursor(&mut self) -> Result<()> {
        if self.conn.state() != ConnectionState::InGame {
            return Err(Error::InvalidPacket("must be in game".into()));
        }
        let action = CodecInputAction::ClearCursor;
        self.conn.send_codec_action(action).await
    }

    /// Drop currently held cursor item at position
    pub async fn send_drop_item(&mut self, x: f64, y: f64) -> Result<()> {
        if self.conn.state() != ConnectionState::InGame {
            return Err(Error::InvalidPacket("must be in game".into()));
        }
        let position = MapPosition::from_tiles(x, y);
        let action = CodecInputAction::DropItem { position };
        self.conn.send_codec_action(action).await
    }

    /// Use currently selected item at position (e.g., place/consume/attack)
    pub async fn send_use_item(&mut self, x: f64, y: f64) -> Result<()> {
        if self.conn.state() != ConnectionState::InGame {
            return Err(Error::InvalidPacket("must be in game".into()));
        }
        let position = MapPosition::from_tiles(x, y);
        let action = CodecInputAction::UseItem { position };
        self.conn.send_codec_action(action).await
    }

    /// Remove cables at position
    pub async fn send_remove_cables(&mut self, x: f64, y: f64) -> Result<()> {
        if self.conn.state() != ConnectionState::InGame {
            return Err(Error::InvalidPacket("must be in game".into()));
        }
        let position = MapPosition::from_tiles(x, y);
        let action = CodecInputAction::RemoveCables { position };
        self.conn.send_codec_action(action).await
    }

    /// Wire dragging at position (connect/disconnect)
    pub async fn send_wire_dragging(&mut self, x: f64, y: f64) -> Result<()> {
        if self.conn.state() != ConnectionState::InGame {
            return Err(Error::InvalidPacket("must be in game".into()));
        }
        let position = MapPosition::from_tiles(x, y);
        let action = CodecInputAction::WireDragging { position };
        self.conn.send_codec_action(action).await
    }

    /// Set ghost cursor to an item id + optional quality
    pub async fn send_set_ghost_cursor(&mut self, item_id: u16, quality_id: Option<u8>) -> Result<()> {
        if self.conn.state() != ConnectionState::InGame {
            return Err(Error::InvalidPacket("must be in game".into()));
        }
        let action = CodecInputAction::SetGhostCursor { item_id, quality_id };
        self.conn.send_codec_action(action).await
    }

    /// Import a blueprint string into the cursor (action type 207)
    pub async fn send_import_blueprint_string(&mut self, blueprint_string: &str, flags: u16, mode: u8) -> Result<()> {
        if self.conn.state() != ConnectionState::InGame {
            return Err(Error::InvalidPacket("must be in game".into()));
        }
        let action = CodecInputAction::ImportBlueprintString {
            blueprint_string: blueprint_string.to_string(),
            flags,
            mode,
        };
        self.conn.send_codec_action(action).await
    }

    /// Set filter for an inventory slot
    pub async fn send_set_filter(
        &mut self,
        location: RelativeItemStackLocation,
        item_id: u16,
        quality_id: u8,
        quality_extra: Option<u8>,
    ) -> Result<()> {
        if self.conn.state() != ConnectionState::InGame {
            return Err(Error::InvalidPacket("must be in game".into()));
        }
        let action = CodecInputAction::SetFilter {
            location,
            item_id,
            quality_id,
            quality_extra,
        };
        self.conn.send_codec_action(action).await
    }

    /// Clear filter for an inventory slot
    pub async fn send_clear_filter(&mut self, location: RelativeItemStackLocation) -> Result<()> {
        self.send_set_filter(location, 0, 0, None).await
    }

    /// Transfer stack to/from cursor
    pub async fn send_cursor_transfer(&mut self, location: ClientItemStackLocation) -> Result<()> {
        if self.conn.state() != ConnectionState::InGame {
            return Err(Error::InvalidPacket("must be in game".into()));
        }
        let action = CodecInputAction::CursorTransfer { location };
        self.conn.send_codec_action(action).await
    }

    /// Split stack to/from cursor
    pub async fn send_cursor_split(&mut self, location: ClientItemStackLocation) -> Result<()> {
        if self.conn.state() != ConnectionState::InGame {
            return Err(Error::InvalidPacket("must be in game".into()));
        }
        let action = CodecInputAction::CursorSplit { location };
        self.conn.send_codec_action(action).await
    }

    /// Transfer stack between inventories
    pub async fn send_stack_transfer(&mut self, spec: ItemStackTransferSpecification) -> Result<()> {
        if self.conn.state() != ConnectionState::InGame {
            return Err(Error::InvalidPacket("must be in game".into()));
        }
        let action = CodecInputAction::StackTransfer { spec };
        self.conn.send_codec_action(action).await
    }

    /// Transfer from inventory using transfer spec
    pub async fn send_inventory_transfer(
        &mut self,
        spec: ItemStackTransferSpecification,
    ) -> Result<()> {
        if self.conn.state() != ConnectionState::InGame {
            return Err(Error::InvalidPacket("must be in game".into()));
        }
        let action = CodecInputAction::InventoryTransfer { spec };
        self.conn.send_codec_action(action).await
    }

    /// Split stack between inventories
    pub async fn send_stack_split(&mut self, spec: ItemStackTransferSpecification) -> Result<()> {
        if self.conn.state() != ConnectionState::InGame {
            return Err(Error::InvalidPacket("must be in game".into()));
        }
        let action = CodecInputAction::StackSplit { spec };
        self.conn.send_codec_action(action).await
    }

    /// Split stack from inventory using transfer spec
    pub async fn send_inventory_split(
        &mut self,
        spec: ItemStackTransferSpecification,
    ) -> Result<()> {
        if self.conn.state() != ConnectionState::InGame {
            return Err(Error::InvalidPacket("must be in game".into()));
        }
        let action = CodecInputAction::InventorySplit { spec };
        self.conn.send_codec_action(action).await
    }

    /// Set logistic request filter (action type 99)
    pub async fn send_set_logistic_filter(
        &mut self,
        filter: LogisticFilter,
        section_type: u8,
        section_index: u8,
        slot_index: u16,
    ) -> Result<()> {
        if self.conn.state() != ConnectionState::InGame {
            return Err(Error::InvalidPacket("must be in game".into()));
        }
        let action = CodecInputAction::SetLogisticFilterItem {
            filter,
            section_type,
            section_index,
            slot_index,
        };
        self.conn.send_codec_action(action).await
    }

    /// Open character inventory GUI
    pub async fn send_open_inventory(&mut self) -> Result<()> {
        if self.conn.state() != ConnectionState::InGame {
            return Err(Error::InvalidPacket("must be in game".into()));
        }
        Err(Error::InvalidPacket("open inventory action encoding not implemented".into()))
    }

    /// Open logistics GUI
    pub async fn send_open_logistics_gui(&mut self) -> Result<()> {
        if self.conn.state() != ConnectionState::InGame {
            return Err(Error::InvalidPacket("must be in game".into()));
        }
        self.conn.send_codec_action(CodecInputAction::OpenLogisticsGui).await
    }

    /// Toggle driving state (enter/exit vehicle)
    pub async fn send_toggle_driving(&mut self) -> Result<()> {
        if self.conn.state() != ConnectionState::InGame {
            return Err(Error::InvalidPacket("must be in game".into()));
        }
        self.conn.send_codec_action(CodecInputAction::ToggleDriving).await
    }

    /// Drive vehicle (acceleration + direction)
    pub async fn send_drive(&mut self, acceleration: u8, direction: u8) -> Result<()> {
        if self.conn.state() != ConnectionState::InGame {
            return Err(Error::InvalidPacket("must be in game".into()));
        }
        let accel = match acceleration {
            0 => RidingAcceleration::Nothing,
            1 => RidingAcceleration::Accelerating,
            2 => RidingAcceleration::Braking,
            3 => RidingAcceleration::Reversing,
            _ => return Err(Error::InvalidPacket("invalid riding acceleration".into())),
        };
        let dir = match direction {
            0 => RidingDirection::Straight,
            1 => RidingDirection::Left,
            2 => RidingDirection::Right,
            _ => return Err(Error::InvalidPacket("invalid riding direction".into())),
        };
        let action = CodecInputAction::ChangeRidingState {
            acceleration: accel,
            direction: dir,
        };
        self.conn.send_codec_action(action).await
    }

    /// Deconstruct entities within a rectangular area
    pub async fn send_deconstruct_area(
        &mut self,
        left_top: (f64, f64),
        right_bottom: (f64, f64),
    ) -> Result<()> {
        if self.conn.state() != ConnectionState::InGame {
            return Err(Error::InvalidPacket("must be in game".into()));
        }
        let area_left_top = MapPosition::from_tiles(left_top.0, left_top.1);
        let area_right_bottom = MapPosition::from_tiles(right_bottom.0, right_bottom.1);
        let action = CodecInputAction::Deconstruct {
            area_left_top,
            area_right_bottom,
        };
        self.conn.send_codec_action(action).await
    }

    /// Fast transfer between player and selected entity (ctrl-click)
    pub async fn send_fast_transfer(&mut self, from_player: bool) -> Result<()> {
        if self.conn.state() != ConnectionState::InGame {
            return Err(Error::InvalidPacket("must be in game".into()));
        }
        let action = CodecInputAction::FastEntityTransfer { from_player };
        self.conn.send_codec_action(action).await
    }

    /// Fast split transfer between player and selected entity (shift+ctrl-click)
    pub async fn send_fast_split(&mut self, from_player: bool) -> Result<()> {
        if self.conn.state() != ConnectionState::InGame {
            return Err(Error::InvalidPacket("must be in game".into()));
        }
        let action = CodecInputAction::FastEntitySplit { from_player };
        self.conn.send_codec_action(action).await
    }

    /// Cancel deconstruction within a rectangular area
    pub async fn send_cancel_deconstruct_area(
        &mut self,
        left_top: (f64, f64),
        right_bottom: (f64, f64),
    ) -> Result<()> {
        if self.conn.state() != ConnectionState::InGame {
            return Err(Error::InvalidPacket("must be in game".into()));
        }
        let area_left_top = MapPosition::from_tiles(left_top.0, left_top.1);
        let area_right_bottom = MapPosition::from_tiles(right_bottom.0, right_bottom.1);
        let action = CodecInputAction::CancelDeconstruct {
            area_left_top,
            area_right_bottom,
        };
        self.conn.send_codec_action(action).await
    }

    /// Shoot at a target position
    pub async fn send_shoot(&mut self, x: f64, y: f64) -> Result<()> {
        if self.conn.state() != ConnectionState::InGame {
            return Err(Error::InvalidPacket("must be in game".into()));
        }
        let position = MapPosition::from_tiles(x, y);
        let action = CodecInputAction::ChangeShootingState {
            state: ShootingState::ShootingSelected,
            position,
        };
        self.conn.send_codec_action(action).await
    }

    /// Stop shooting
    pub async fn send_stop_shoot(&mut self) -> Result<()> {
        if self.conn.state() != ConnectionState::InGame {
            return Err(Error::InvalidPacket("must be in game".into()));
        }
        self.conn.update_position();
        let (x, y) = self.conn.player_position();
        let position = MapPosition::from_tiles(x, y);
        let action = CodecInputAction::ChangeShootingState {
            state: ShootingState::NotShooting,
            position,
        };
        self.conn.send_codec_action(action).await
    }

    /// Copy entity settings from selected entity
    pub async fn send_copy_entity_settings(&mut self) -> Result<()> {
        if self.conn.state() != ConnectionState::InGame {
            return Err(Error::InvalidPacket("must be in game".into()));
        }
        let action = CodecInputAction::CopyEntitySettings;
        self.conn.send_codec_action(action).await
    }

    /// Paste entity settings onto selected entity
    pub async fn send_paste_entity_settings(&mut self) -> Result<()> {
        if self.conn.state() != ConnectionState::InGame {
            return Err(Error::InvalidPacket("must be in game".into()));
        }
        let action = CodecInputAction::PasteEntitySettings;
        self.conn.send_codec_action(action).await
    }

    /// Launch rocket (selected silo)
    pub async fn send_launch_rocket(&mut self) -> Result<()> {
        if self.conn.state() != ConnectionState::InGame {
            return Err(Error::InvalidPacket("must be in game".into()));
        }
        let action = CodecInputAction::LaunchRocket;
        self.conn.send_codec_action(action).await
    }
}
