use crate::codec::{BinaryReader, BinaryWriter, MapPosition, Direction};
use crate::error::{Error, Result};

/// Input action type IDs (from Factorio 2.0 binary reverse engineering)
/// These values were extracted from the Factorio binary using radare2
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum InputActionType {
    Nothing = 0,
    StopWalking = 1,
    BeginMining = 2,
    StopMining = 3,
    ToggleDriving = 4,
    OpenGui = 5,
    OpenCharacterGui = 6,
    OpenCurrentVehicleGui = 7,
    ConnectRollingStock = 8,
    DisconnectRollingStock = 9,
    SelectedEntityCleared = 10,
    ClearCursor = 11,
    ResetAssemblingMachine = 12,
    OpenProductionGui = 13,
    StopRepair = 14,
    CancelNewBlueprint = 15,
    CloseBlueprintRecord = 16,
    CopyEntitySettings = 17,
    PasteEntitySettings = 18,
    DestroyOpenedItem = 19,
    CopyOpenedItem = 20,
    CopyLargeOpenedItem = 21,
    ToggleShowEntityInfo = 22,
    SingleplayerInit = 23,
    MultiplayerInit = 24,
    DisconnectAllPlayers = 25,
    OpenBonusGui = 26,
    OpenAchievementsGui = 27,
    CycleBlueprintBookForwards = 28,
    CycleBlueprintBookBackwards = 29,
    CycleQualityUp = 30,
    CycleQualityDown = 31,
    CycleClipboardForwards = 32,
    CycleClipboardBackwards = 33,
    StopMovementInTheNextTick = 34,
    ToggleEnableVehicleLogisticsWhileMoving = 35,
    ToggleDeconstructionItemEntityFilterMode = 36,
    ToggleDeconstructionItemTileFilterMode = 37,
    SelectNextValidGun = 38,
    ToggleMapEditor = 39,
    DeleteBlueprintLibrary = 40,
    GameCreatedFromScenario = 41,
    ActivatePaste = 42,
    Undo = 43,
    Redo = 44,
    TogglePersonalRoboport = 45,
    ToggleEquipmentMovementBonus = 46,
    TogglePersonalLogisticRequests = 47,
    ToggleEntityLogisticRequests = 48,
    ToggleArtilleryAutoTargeting = 49,
    StopDragBuild = 50,
    FlushOpenedEntityFluid = 51,
    ForceFullCRC = 52,
    AddLogisticSection = 53,
    AcknowledgeTechnology = 54,
    OpenOpenedEntityGrid = 55,
    FinishedButContinuing = 56,
    ContinueSinglePlayer = 57,
    OpenNewPlatformButtonFromRocketSilo = 58,
    ToggleSelectedEntity = 59,
    Cheat = 60,
    CloseGui = 61,
    OpenLogisticsGui = 62,
    OpenBlueprintLibraryGui = 63,
    ChangeBlueprintLibraryTab = 64,
    DropItem = 65,
    Build = 66,
    StartWalking = 67,
    BeginMiningTerrain = 68,
    ChangeRidingState = 69,
    ChangeHeadingRidingState = 70,
    OpenItem = 71,
    OpenParentOfOpenedItem = 72,
    DestroyItem = 73,
    OpenModItem = 74,
    OpenEquipment = 75,
    CursorTransfer = 76,
    CursorSplit = 77,
    StackTransfer = 78,
    SendStackToTrash = 79,
    SendStacksToTrash = 80,
    InventoryTransfer = 81,
    CheckCRCHeuristic = 82,
    Craft = 83,
    WireDragging = 84,
    ChangeShootingState = 85,
    SetupAssemblingMachine = 86,
    SelectedEntityChanged = 87,
    Pipette = 88,
    StackSplit = 89,
    InventorySplit = 90,
    CancelCraft = 91,
    SetFilter = 92,
    SetSpoilPriority = 93,
    CheckCRC = 94,
    SetCircuitCondition = 95,
    SetSignal = 96,
    StartResearch = 97,
    SetCheatModeQuality = 98,
    SetLogisticFilterItem = 99,
    SwapLogisticFilterItems = 100,
    SetCircuitModeOfOperation = 101,
    GuiClick = 102,
    GuiConfirmed = 103,
    WriteToConsole = 104,
    MarketOffer = 105,
    ChangeTrainStopStation = 106,
    ChangeActiveItemGroupForCrafting = 107,
    ChangeActiveItemGroupForFilters = 108,
    ChangeActiveCharacterTab = 109,
    GuiTextChanged = 110,
    GuiCheckedStateChanged = 111,
    GuiSelectionStateChanged = 112,
    GuiSelectedTabChanged = 113,
    GuiValueChanged = 114,
    GuiSwitchStateChanged = 115,
    GuiLocationChanged = 116,
    PlaceEquipment = 117,
    TakeEquipment = 118,
    UseItem = 119,
    SendSpidertron = 120,
    SetInventoryBar = 121,
    SetZoom = 122,
    ZoomAroundPoint = 123,
    MoveOnPan = 124,
    StartRepair = 125,
    Deconstruct = 126,
    Upgrade = 127,
    Copy = 128,
    AlternativeCopy = 129,
    SelectBlueprintEntities = 130,
    AltSelectBlueprintEntities = 131,
    SetupBlueprint = 132,
    SetupSingleBlueprintRecord = 133,
    CopyOpenedBlueprint = 134,
    CopyLargeOpenedBlueprint = 135,
    ReassignBlueprint = 136,
    OpenBlueprintRecord = 137,
    GrabBlueprintRecord = 138,
    DropBlueprintRecord = 139,
    DeleteBlueprintRecord = 140,
    UpgradeOpenedBlueprintByRecord = 141,
    UpgradeOpenedBlueprintByItem = 142,
    SpawnItem = 143,
    SetGhostCursor = 144,
    UpdateBlueprintShelf = 145,
    TransferBlueprint = 146,
    TransferBlueprintImmediately = 147,
    EditBlueprintToolPreview = 148,
    RemoveCables = 149,
    ExportBlueprint = 150,
    ImportBlueprint = 151,
    ImportBlueprintsFiltered = 152,
    PlayerJoinGame = 153,
    PlayerAdminChange = 154,
    CancelDeconstruct = 155,
    CancelUpgrade = 156,
    ChangeArithmeticCombinatorParameters = 157,
    DragDeciderCombinatorCondition = 158,
    AddDeciderCombinatorCondition = 159,
    ModifyDeciderCombinatorCondition = 160,
    RemoveDeciderCombinatorCondition = 161,
    DragDeciderCombinatorOutput = 162,
    AddDeciderCombinatorOutput = 163,
    ModifyDeciderCombinatorOutput = 164,
    RemoveDeciderCombinatorOutput = 165,
    ChangeSelectorCombinatorParameters = 166,
    ChangeProgrammableSpeakerParameters = 167,
    ChangeProgrammableSpeakerAlertParameters = 168,
    ChangeProgrammableSpeakerCircuitParameters = 169,
    SetVehicleAutomaticTargetingParameters = 170,
    BuildTerrain = 171,
    ChangeTrainWaitCondition = 172,
    ChangeTrainWaitConditionData = 173,
    RemoveTrainStation = 174,
    RemoveTrainInterrupt = 175,
    AddTrainStation = 176,
    AddTrainInterrupt = 177,
    ActivateInterrupt = 178,
    EditInterrupt = 179,
    RenameInterrupt = 180,
    GoToTrainStation = 181,
    SetTrainStopped = 182,
    SetScheduleRecordAllowUnloading = 183,
    CustomInput = 184,
    ChangeItemLabel = 185,
    ChangeEntityLabel = 186,
    ChangeTrainName = 187,
    ChangeLogisticPointGroup = 188,
    LaunchRocket = 189,
    DeleteLogisticGroup = 190,
    SetLogisticNetworkName = 191,
    BuildRail = 192,
    CancelResearch = 193,
    MoveResearch = 194,
    SelectArea = 195,
    AltSelectArea = 196,
    ReverseSelectArea = 197,
    AltReverseSelectArea = 198,
    ServerCommand = 199,
    SetInfinityContainerFilterItem = 200,
    SwapInfinityContainerFilterItems = 201,
    SetInfinityPipeFilter = 202,
    ModSettingsChanged = 203,
    SetEntityEnergyProperty = 204,
    EditCustomTag = 205,
    EditPermissionGroup = 206,
    ImportBlueprintString = 207,
    ImportPermissionsString = 208,
    ReloadScript = 209,
    ReloadScriptDataTooLarge = 210,
    GuiElemChanged = 211,
    BlueprintTransferQueueUpdate = 212,
    DragTrainSchedule = 213,
    DragTrainScheduleInterrupt = 214,
    DragTrainWaitCondition = 215,
    SelectItemFilter = 216,
    SwapItemFilters = 217,
    SelectEntitySlot = 218,
    SwapEntitySlots = 219,
    SelectEntityFilterSlot = 220,
    SwapEntityFilterSlots = 221,
    SelectAsteroidChunkSlot = 222,
    SwapAsteroidChunkSlots = 223,
    SelectTileSlot = 224,
    SwapTileSlots = 225,
    SelectMapperSlotFrom = 226,
    SelectMapperSlotTo = 227,
    SwapMappers = 228,
    DisplayResolutionChanged = 229,
    QuickBarSetSlot = 230,
    QuickBarPickSlot = 231,
    QuickBarSetSelectedPage = 232,
    PlayerLeaveGame = 233,
    MapEditorAction = 234,
    PutSpecialItemInMap = 235,
    PutSpecialRecordInMap = 236,
    ChangeMultiplayerConfig = 237,
    AdminAction = 238,
    LuaShortcut = 239,
    TranslateString = 240,
    CreateSpacePlatform = 241,
    DeleteSpacePlatform = 242,
    CancelDeleteSpacePlatform = 243,
    RenameSpacePlatform = 244,
    RemoteViewSurface = 245,
    RemoteViewEntity = 246,
    CloseRemoteView = 247,
    InstantlyCreateSpacePlatform = 248,
    FlushOpenedEntitySpecificFluid = 249,
    ChangePickingState = 250,
    SelectedEntityChangedVeryClose = 251,
    SelectedEntityChangedVeryClosePrecise = 252,
    SelectedEntityChangedRelative = 253,
    SelectedEntityChangedBasedOnUnitNumber = 254,
    SetCombinatorDescription = 255,
    SwitchConstantCombinatorState = 256,
    SwitchPowerSwitchState = 257,
    SwitchInserterFilterModeState = 258,
    SetUseInserterFilters = 259,
    SwitchLoaderFilterMode = 260,
    SwitchMiningDrillFilterModeState = 261,
    SwitchConnectToLogisticNetwork = 262,
    SetBehaviorMode = 263,
    FastEntityTransfer = 264,
    RotateEntity = 265,
    FlipEntity = 266,
    FastEntitySplit = 267,
    RequestMissingConstructionMaterials = 268,
    TrashNotRequestedItems = 269,
    SetAllowCommands = 270,
    SetResearchFinishedStopsGame = 271,
    SetInserterMaxStackSize = 272,
    SetLoaderBeltStackSizeOverride = 273,
    OpenTrainGui = 274,
    OpenTrainsGui = 275,
    SetEntityColor = 276,
    SetCopyColorFromTrainStop = 277,
    SetDeconstructionItemTreesAndRocksOnly = 278,
    SetDeconstructionItemTileSelectionMode = 279,
    DeleteCustomTag = 280,
    DeletePermissionGroup = 281,
    AddPermissionGroup = 282,
    SetInfinityContainerRemoveUnfilteredItems = 283,
    SetCarWeaponsControl = 284,
    SetRequestFromBuffers = 285,
    ChangeActiveQuickBar = 286,
    OpenPermissionsGui = 287,
    DisplayScaleChanged = 288,
    SetSplitterPriority = 289,
    GrabInternalBlueprintFromText = 290,
    SetHeatInterfaceTemperature = 291,
    SetHeatInterfaceMode = 292,
    OpenTrainStationGui = 293,
    RenderModeChanged = 294,
    PlayerInputMethodChanged = 295,
    SetPlayerColor = 296,
    PlayerClickedGpsTag = 297,
    SetTrainsLimit = 298,
    ClearRecipeNotification = 299,
    SetLinkedContainerLinkID = 300,
    SetTurretIgnoreUnlisted = 301,
    SetLampAlwaysOn = 302,
    OpenGlobalElectricNetworkGui = 303,
    SetPumpFluidFilter = 304,
    CustomTestInputAction = 305,
    RemoveLogisticSection = 306,
    EditDisplayPanel = 307,
    EditDisplayPanelAlwaysShow = 308,
    EditDisplayPanelShowInChart = 309,
    EditDisplayPanelIcon = 310,
    EditDisplayPanelParameters = 311,
    EditDisplayPanelSingleEntry = 312,
    ReorderLogisticSection = 313,
    SetLogisticSectionActive = 314,
    AddPin = 315,
    PinSearchResult = 316,
    PinAlertGroup = 317,
    PinCustomAlert = 318,
    EditPin = 319,
    RemovePin = 320,
    MovePin = 321,
    SendTrainToPinTarget = 322,
    GuiHover = 323,
    GuiLeave = 324,
    UpdatePlayerSettings = 325,
    SpectatorChangeSurface = 326,
    AdjustBlueprintSnapping = 327,
    SetTrainStopPriority = 328,
    AchievementGained = 329,
    LandAtPlanet = 330,
    PlayerVisitedPlanet = 331,
    ParametriseBlueprint = 332,
    PlayerLocaleChanged = 333,
    SetRocketSiloSendToOrbitAutomatedMode = 334,
    UdpPacketReceived = 335,
}

impl InputActionType {
    pub fn from_u16(v: u16) -> Option<Self> {
        if v <= 335 {
            Some(unsafe { std::mem::transmute(v) })
        } else {
            None
        }
    }

    pub fn from_u8(v: u8) -> Option<Self> {
        Self::from_u16(v as u16)
    }
}

/// Input action with all possible variants
#[derive(Debug, Clone)]
pub enum InputAction {
    Nothing,
    StopWalking,
    StartWalking { direction_x: f64, direction_y: f64 },
    BeginMining { position: MapPosition, notify_server: bool },
    BeginMiningTerrain { position: MapPosition },
    StopMining,
    ToggleDriving,
    OpenGui { entity_id: u32 },
    CloseGui,
    OpenCharacterGui,
    ClearCursor,

    // Building
    Build {
        position: MapPosition,
        direction: Direction,
        shift_build: bool,
        skip_fog_of_war: bool,
    },
    BuildGhost {
        position: MapPosition,
        direction: Direction,
    },
    BuildTerrain {
        position: MapPosition,
        terrain_id: u8,
    },
    BuildRail {
        position: MapPosition,
        direction: Direction,
        rail_data: Vec<u8>,
    },
    Deconstruct {
        area_left_top: MapPosition,
        area_right_bottom: MapPosition,
    },
    CancelDeconstruct {
        area_left_top: MapPosition,
        area_right_bottom: MapPosition,
    },
    RotateEntity {
        position: MapPosition,
        reverse: bool,
    },
    Upgrade {
        area_left_top: MapPosition,
        area_right_bottom: MapPosition,
    },
    CancelUpgrade {
        area_left_top: MapPosition,
        area_right_bottom: MapPosition,
    },

    // Inventory/Cursor
    CursorTransfer {
        from_player: bool,
        inventory_index: u16,
        slot_index: u16,
    },
    CursorSplit {
        from_player: bool,
        inventory_index: u16,
        slot_index: u16,
    },
    StackTransfer {
        from_player: bool,
        inventory_index: u16,
        slot_index: u16,
    },
    InventoryTransfer {
        inventory_index: u16,
        slot_index: u16,
    },
    FastEntityTransfer {
        from_player: bool,
    },
    FastEntitySplit {
        from_player: bool,
    },
    DropItem {
        position: MapPosition,
    },
    SetFilter {
        inventory_index: u16,
        slot_index: u16,
        item_name: String,
    },
    SetInventoryBar {
        inventory_index: u16,
        bar: u16,
    },

    // Crafting
    Craft {
        recipe_id: u16,
        count: u32,
    },

    // Combat
    ChangeShootingState {
        state: ShootingState,
        position: MapPosition,
    },
    ChangeRidingState {
        acceleration: RidingAcceleration,
        direction: RidingDirection,
    },
    UseItem {
        position: MapPosition,
    },
    UseArtilleryRemote {
        position: MapPosition,
    },
    SendSpidertron {
        position: MapPosition,
    },

    // Research
    StartResearch {
        technology_id: u16,
    },
    CancelResearch,

    // GUI
    GuiClick {
        element_id: u32,
        button: MouseButton,
        is_alt: bool,
        is_ctrl: bool,
        is_shift: bool,
    },
    GuiConfirmed {
        element_id: u32,
    },
    GuiTextChanged {
        element_id: u32,
        text: String,
    },
    GuiCheckedStateChanged {
        element_id: u32,
        state: bool,
    },
    GuiSelectionStateChanged {
        element_id: u32,
        selection: u32,
    },
    GuiSelectedTabChanged {
        element_id: u32,
        tab_index: u32,
    },
    GuiValueChanged {
        element_id: u32,
        value: f64,
    },
    GuiSwitchStateChanged {
        element_id: u32,
        state: SwitchState,
    },
    GuiLocationChanged {
        element_id: u32,
        x: i32,
        y: i32,
    },

    // Console/Chat
    WriteToConsole {
        message: String,
    },
    ServerCommand {
        command: String,
    },

    // Trains
    OpenTrainGui {
        train_id: u32,
    },
    SetTrainStopped {
        train_id: u32,
        stopped: bool,
    },
    ChangeTrainWaitCondition {
        train_id: u32,
        schedule_index: u16,
        condition_index: u16,
        condition_data: Vec<u8>,
    },
    AddTrainStation {
        train_id: u32,
        station_name: String,
    },

    // Blueprints
    SetupBlueprint {
        blueprint_data: Vec<u8>,
    },
    ImportBlueprint {
        blueprint_string: String,
    },
    ExportBlueprint {
        inventory_index: u16,
        slot_index: u16,
    },
    CopyEntitySettings {
        source_entity: u32,
        target_entity: u32,
    },
    PasteEntitySettings {
        source_entity: u32,
        target_entity: u32,
    },

    // Logistics
    SetLogisticFilterItem {
        slot_index: u16,
        item_name: String,
        count: u32,
    },
    SetLogisticFilterSignal {
        slot_index: u16,
        signal_type: u8,
        signal_name: String,
    },

    // Circuit network
    SetCircuitCondition {
        entity_id: u32,
        condition_data: Vec<u8>,
    },
    SetSignal {
        entity_id: u32,
        signal_index: u16,
        signal_type: u8,
        signal_name: String,
        count: i32,
    },
    SwitchConstantCombinatorState {
        entity_id: u32,
    },
    ChangeArithmeticCombinatorParameters {
        entity_id: u32,
        parameters: Vec<u8>,
    },
    ChangeDeciderCombinatorParameters {
        entity_id: u32,
        parameters: Vec<u8>,
    },

    // Equipment
    PlaceEquipment {
        grid_position_x: u32,
        grid_position_y: u32,
        equipment_name: String,
    },
    TakeEquipment {
        grid_position_x: u32,
        grid_position_y: u32,
    },

    // Selection
    SelectArea {
        area_left_top: MapPosition,
        area_right_bottom: MapPosition,
    },
    AltSelectArea {
        area_left_top: MapPosition,
        area_right_bottom: MapPosition,
    },
    SelectedEntityChanged {
        entity_id: Option<u32>,
    },
    SelectedEntityCleared,

    // Quick bar
    QuickBarSetSlot {
        page: u8,
        slot: u8,
        item_name: String,
    },
    QuickBarPickSlot {
        page: u8,
        slot: u8,
    },
    QuickBarSetSelectedPage {
        page: u8,
    },

    // Misc
    SetPlayerColor {
        r: u8,
        g: u8,
        b: u8,
    },
    ToggleShowEntityInfo,
    TogglePersonalRoboport,
    TogglePersonalLogisticRequests,
    LaunchRocket,
    OpenProductionGui,
    OpenLogisticsGui,
    OpenBlueprintLibraryGui,
    OpenTrainsGui,
    OpenAchievementsGui,
    CustomInput {
        custom_input_name: String,
        cursor_position: Option<MapPosition>,
        selected_prototype: Option<String>,
    },
    LuaShortcut {
        shortcut_name: String,
    },

    // Admin
    AdminAction {
        action_type: AdminActionType,
        player_name: String,
    },

    // Multiplayer
    PlayerJoinGame {
        peer_id: u16,
        player_index_plus_one: u16,
        mode: u8,
        username: String,
        flag_a: bool,
        flag_b: bool,
    },
    PlayerLeaveGame {
        peer_id: u16,
        reason: u8,
    },

    // Unknown/raw action
    Raw {
        action_type: u8,
        data: Vec<u8>,
    },
}

impl InputAction {
    pub fn action_type(&self) -> InputActionType {
        match self {
            Self::Nothing => InputActionType::Nothing,
            Self::StopWalking => InputActionType::StopWalking,
            Self::StartWalking { .. } => InputActionType::StartWalking,
            Self::BeginMining { .. } => InputActionType::BeginMining,
            Self::BeginMiningTerrain { .. } => InputActionType::BeginMiningTerrain,
            Self::StopMining => InputActionType::StopMining,
            Self::ToggleDriving => InputActionType::ToggleDriving,
            Self::OpenGui { .. } => InputActionType::OpenGui,
            Self::CloseGui => InputActionType::CloseGui,
            Self::OpenCharacterGui => InputActionType::OpenCharacterGui,
            Self::ClearCursor => InputActionType::ClearCursor,
            Self::Build { .. } => InputActionType::Build,
            Self::BuildGhost { .. } => InputActionType::Build, // BuildGhost merged into Build in 2.0
            Self::BuildTerrain { .. } => InputActionType::BuildTerrain,
            Self::BuildRail { .. } => InputActionType::BuildRail,
            Self::Deconstruct { .. } => InputActionType::Deconstruct,
            Self::CancelDeconstruct { .. } => InputActionType::CancelDeconstruct,
            Self::RotateEntity { .. } => InputActionType::RotateEntity,
            Self::Upgrade { .. } => InputActionType::Upgrade,
            Self::CancelUpgrade { .. } => InputActionType::CancelUpgrade,
            Self::CursorTransfer { .. } => InputActionType::CursorTransfer,
            Self::CursorSplit { .. } => InputActionType::CursorSplit,
            Self::StackTransfer { .. } => InputActionType::StackTransfer,
            Self::InventoryTransfer { .. } => InputActionType::InventoryTransfer,
            Self::FastEntityTransfer { .. } => InputActionType::FastEntityTransfer,
            Self::FastEntitySplit { .. } => InputActionType::FastEntitySplit,
            Self::DropItem { .. } => InputActionType::DropItem,
            Self::SetFilter { .. } => InputActionType::SetFilter,
            Self::SetInventoryBar { .. } => InputActionType::SetInventoryBar,
            Self::Craft { .. } => InputActionType::Craft,
            Self::ChangeShootingState { .. } => InputActionType::ChangeShootingState,
            Self::ChangeRidingState { .. } => InputActionType::ChangeRidingState,
            Self::UseItem { .. } => InputActionType::UseItem,
            Self::UseArtilleryRemote { .. } => InputActionType::UseItem, // UseArtilleryRemote merged into UseItem
            Self::SendSpidertron { .. } => InputActionType::SendSpidertron,
            Self::StartResearch { .. } => InputActionType::StartResearch,
            Self::CancelResearch => InputActionType::CancelResearch,
            Self::GuiClick { .. } => InputActionType::GuiClick,
            Self::GuiConfirmed { .. } => InputActionType::GuiConfirmed,
            Self::GuiTextChanged { .. } => InputActionType::GuiTextChanged,
            Self::GuiCheckedStateChanged { .. } => InputActionType::GuiCheckedStateChanged,
            Self::GuiSelectionStateChanged { .. } => InputActionType::GuiSelectionStateChanged,
            Self::GuiSelectedTabChanged { .. } => InputActionType::GuiSelectedTabChanged,
            Self::GuiValueChanged { .. } => InputActionType::GuiValueChanged,
            Self::GuiSwitchStateChanged { .. } => InputActionType::GuiSwitchStateChanged,
            Self::GuiLocationChanged { .. } => InputActionType::GuiLocationChanged,
            Self::WriteToConsole { .. } => InputActionType::WriteToConsole,
            Self::ServerCommand { .. } => InputActionType::ServerCommand,
            Self::OpenTrainGui { .. } => InputActionType::OpenTrainGui,
            Self::SetTrainStopped { .. } => InputActionType::SetTrainStopped,
            Self::ChangeTrainWaitCondition { .. } => InputActionType::ChangeTrainWaitCondition,
            Self::AddTrainStation { .. } => InputActionType::AddTrainStation,
            Self::SetupBlueprint { .. } => InputActionType::SetupBlueprint,
            Self::ImportBlueprint { .. } => InputActionType::ImportBlueprint,
            Self::ExportBlueprint { .. } => InputActionType::ExportBlueprint,
            Self::CopyEntitySettings { .. } => InputActionType::CopyEntitySettings,
            Self::PasteEntitySettings { .. } => InputActionType::PasteEntitySettings,
            Self::SetLogisticFilterItem { .. } => InputActionType::SetLogisticFilterItem,
            Self::SetLogisticFilterSignal { .. } => InputActionType::SetLogisticFilterItem, // SetLogisticFilterSignal merged
            Self::SetCircuitCondition { .. } => InputActionType::SetCircuitCondition,
            Self::SetSignal { .. } => InputActionType::SetSignal,
            Self::SwitchConstantCombinatorState { .. } => InputActionType::SwitchConstantCombinatorState,
            Self::ChangeArithmeticCombinatorParameters { .. } => InputActionType::ChangeArithmeticCombinatorParameters,
            Self::ChangeDeciderCombinatorParameters { .. } => InputActionType::ModifyDeciderCombinatorCondition, // Changed in 2.0
            Self::PlaceEquipment { .. } => InputActionType::PlaceEquipment,
            Self::TakeEquipment { .. } => InputActionType::TakeEquipment,
            Self::SelectArea { .. } => InputActionType::SelectArea,
            Self::AltSelectArea { .. } => InputActionType::AltSelectArea,
            Self::SelectedEntityChanged { .. } => InputActionType::SelectedEntityChanged,
            Self::SelectedEntityCleared => InputActionType::SelectedEntityCleared,
            Self::QuickBarSetSlot { .. } => InputActionType::QuickBarSetSlot,
            Self::QuickBarPickSlot { .. } => InputActionType::QuickBarPickSlot,
            Self::QuickBarSetSelectedPage { .. } => InputActionType::QuickBarSetSelectedPage,
            Self::SetPlayerColor { .. } => InputActionType::SetPlayerColor,
            Self::ToggleShowEntityInfo => InputActionType::ToggleShowEntityInfo,
            Self::TogglePersonalRoboport => InputActionType::TogglePersonalRoboport,
            Self::TogglePersonalLogisticRequests => InputActionType::TogglePersonalLogisticRequests,
            Self::LaunchRocket => InputActionType::LaunchRocket,
            Self::OpenProductionGui => InputActionType::OpenProductionGui,
            Self::OpenLogisticsGui => InputActionType::OpenLogisticsGui,
            Self::OpenBlueprintLibraryGui => InputActionType::OpenBlueprintLibraryGui,
            Self::OpenTrainsGui => InputActionType::OpenTrainsGui,
            Self::OpenAchievementsGui => InputActionType::OpenAchievementsGui,
            Self::CustomInput { .. } => InputActionType::CustomInput,
            Self::LuaShortcut { .. } => InputActionType::LuaShortcut,
            Self::AdminAction { .. } => InputActionType::AdminAction,
            Self::PlayerJoinGame { .. } => InputActionType::PlayerJoinGame,
            Self::PlayerLeaveGame { .. } => InputActionType::PlayerLeaveGame,
            Self::Raw { action_type, .. } => InputActionType::from_u8(*action_type).unwrap_or(InputActionType::Nothing),
        }
    }

    /// Write the action type as var u16 (1 byte if < 256, else 0xFF + u16)
    pub fn write_type(&self, writer: &mut BinaryWriter) {
        let action_type = self.action_type() as u16;
        if action_type < 256 {
            writer.write_u8(action_type as u8);
        } else {
            writer.write_u8(0xFF);
            writer.write_u16_le(action_type);
        }
    }

    /// Write just the action data (without the type)
    pub fn write_data(&self, writer: &mut BinaryWriter) {
        match self {
            Self::Nothing | Self::StopWalking | Self::StopMining | Self::ToggleDriving |
            Self::CloseGui | Self::OpenCharacterGui | Self::ClearCursor | Self::CancelResearch |
            Self::SelectedEntityCleared | Self::ToggleShowEntityInfo | Self::TogglePersonalRoboport |
            Self::TogglePersonalLogisticRequests | Self::LaunchRocket |
            Self::OpenProductionGui | Self::OpenLogisticsGui | Self::OpenBlueprintLibraryGui |
            Self::OpenTrainsGui | Self::OpenAchievementsGui => {
                // No additional data
            }

            Self::StartWalking { direction_x, direction_y } => {
                writer.write_f64_le(*direction_x);
                writer.write_f64_le(*direction_y);
            }

            Self::BeginMining { position, notify_server } => {
                writer.write_map_position(*position);
                writer.write_bool(*notify_server);
            }

            Self::BeginMiningTerrain { position } => {
                writer.write_map_position(*position);
            }

            Self::OpenGui { entity_id } => {
                writer.write_u32_le(*entity_id);
            }

            Self::Build { position, direction, shift_build, skip_fog_of_war } => {
                writer.write_map_position(*position);
                writer.write_direction(*direction);
                writer.write_bool(*shift_build);
                writer.write_bool(*skip_fog_of_war);
            }

            Self::BuildGhost { position, direction } => {
                writer.write_map_position(*position);
                writer.write_direction(*direction);
            }

            Self::BuildTerrain { position, terrain_id } => {
                writer.write_map_position(*position);
                writer.write_u8(*terrain_id);
            }

            Self::BuildRail { position, direction, rail_data } => {
                writer.write_map_position(*position);
                writer.write_direction(*direction);
                writer.write_opt_u32(rail_data.len() as u32);
                writer.write_bytes(rail_data);
            }

            Self::Deconstruct { area_left_top, area_right_bottom } |
            Self::CancelDeconstruct { area_left_top, area_right_bottom } |
            Self::Upgrade { area_left_top, area_right_bottom } |
            Self::CancelUpgrade { area_left_top, area_right_bottom } |
            Self::SelectArea { area_left_top, area_right_bottom } |
            Self::AltSelectArea { area_left_top, area_right_bottom } => {
                writer.write_map_position(*area_left_top);
                writer.write_map_position(*area_right_bottom);
            }

            Self::RotateEntity { position, reverse } => {
                writer.write_map_position(*position);
                writer.write_bool(*reverse);
            }

            Self::CursorTransfer { from_player, inventory_index, slot_index } |
            Self::CursorSplit { from_player, inventory_index, slot_index } |
            Self::StackTransfer { from_player, inventory_index, slot_index } => {
                writer.write_bool(*from_player);
                writer.write_u16_le(*inventory_index);
                writer.write_u16_le(*slot_index);
            }

            Self::InventoryTransfer { inventory_index, slot_index } => {
                writer.write_u16_le(*inventory_index);
                writer.write_u16_le(*slot_index);
            }

            Self::FastEntityTransfer { from_player } |
            Self::FastEntitySplit { from_player } => {
                writer.write_bool(*from_player);
            }

            Self::DropItem { position } => {
                writer.write_map_position(*position);
            }

            Self::SetFilter { inventory_index, slot_index, item_name } => {
                writer.write_u16_le(*inventory_index);
                writer.write_u16_le(*slot_index);
                writer.write_string(item_name);
            }

            Self::SetInventoryBar { inventory_index, bar } => {
                writer.write_u16_le(*inventory_index);
                writer.write_u16_le(*bar);
            }

            Self::Craft { recipe_id, count } => {
                writer.write_u16_le(*recipe_id);
                writer.write_u32_le(*count);
            }

            Self::ChangeShootingState { state, position } => {
                writer.write_u8(*state as u8);
                writer.write_map_position(*position);
            }

            Self::ChangeRidingState { acceleration, direction } => {
                writer.write_u8(*acceleration as u8);
                writer.write_u8(*direction as u8);
            }

            Self::UseItem { position } |
            Self::UseArtilleryRemote { position } |
            Self::SendSpidertron { position } => {
                writer.write_map_position(*position);
            }

            Self::StartResearch { technology_id } => {
                writer.write_u16_le(*technology_id);
            }

            Self::GuiClick { element_id, button, is_alt, is_ctrl, is_shift } => {
                writer.write_u32_le(*element_id);
                writer.write_u8(*button as u8);
                writer.write_bool(*is_alt);
                writer.write_bool(*is_ctrl);
                writer.write_bool(*is_shift);
            }

            Self::GuiConfirmed { element_id } => {
                writer.write_u32_le(*element_id);
            }

            Self::GuiTextChanged { element_id, text } => {
                writer.write_u32_le(*element_id);
                writer.write_string(text);
            }

            Self::GuiCheckedStateChanged { element_id, state } => {
                writer.write_u32_le(*element_id);
                writer.write_bool(*state);
            }

            Self::GuiSelectionStateChanged { element_id, selection } |
            Self::GuiSelectedTabChanged { element_id, tab_index: selection } => {
                writer.write_u32_le(*element_id);
                writer.write_u32_le(*selection);
            }

            Self::GuiValueChanged { element_id, value } => {
                writer.write_u32_le(*element_id);
                writer.write_f64_le(*value);
            }

            Self::GuiSwitchStateChanged { element_id, state } => {
                writer.write_u32_le(*element_id);
                writer.write_u8(*state as u8);
            }

            Self::GuiLocationChanged { element_id, x, y } => {
                writer.write_u32_le(*element_id);
                writer.write_i32_le(*x);
                writer.write_i32_le(*y);
            }

            Self::WriteToConsole { message } |
            Self::ServerCommand { command: message } => {
                writer.write_string(message);
            }

            Self::OpenTrainGui { train_id } => {
                writer.write_u32_le(*train_id);
            }

            Self::SetTrainStopped { train_id, stopped } => {
                writer.write_u32_le(*train_id);
                writer.write_bool(*stopped);
            }

            Self::ChangeTrainWaitCondition { train_id, schedule_index, condition_index, condition_data } => {
                writer.write_u32_le(*train_id);
                writer.write_u16_le(*schedule_index);
                writer.write_u16_le(*condition_index);
                writer.write_opt_u32(condition_data.len() as u32);
                writer.write_bytes(condition_data);
            }

            Self::AddTrainStation { train_id, station_name } => {
                writer.write_u32_le(*train_id);
                writer.write_string(station_name);
            }

            Self::SetupBlueprint { blueprint_data } => {
                writer.write_opt_u32(blueprint_data.len() as u32);
                writer.write_bytes(blueprint_data);
            }

            Self::ImportBlueprint { blueprint_string } => {
                writer.write_string(blueprint_string);
            }

            Self::ExportBlueprint { inventory_index, slot_index } => {
                writer.write_u16_le(*inventory_index);
                writer.write_u16_le(*slot_index);
            }

            Self::CopyEntitySettings { source_entity, target_entity } |
            Self::PasteEntitySettings { source_entity, target_entity } => {
                writer.write_u32_le(*source_entity);
                writer.write_u32_le(*target_entity);
            }

            Self::SetLogisticFilterItem { slot_index, item_name, count } => {
                writer.write_u16_le(*slot_index);
                writer.write_string(item_name);
                writer.write_u32_le(*count);
            }

            Self::SetLogisticFilterSignal { slot_index, signal_type, signal_name } => {
                writer.write_u16_le(*slot_index);
                writer.write_u8(*signal_type);
                writer.write_string(signal_name);
            }

            Self::SetCircuitCondition { entity_id, condition_data } => {
                writer.write_u32_le(*entity_id);
                writer.write_opt_u32(condition_data.len() as u32);
                writer.write_bytes(condition_data);
            }

            Self::PlaceEquipment { grid_position_x, grid_position_y, equipment_name } => {
                writer.write_u32_le(*grid_position_x);
                writer.write_u32_le(*grid_position_y);
                writer.write_string(equipment_name);
            }

            Self::TakeEquipment { grid_position_x, grid_position_y } => {
                writer.write_u32_le(*grid_position_x);
                writer.write_u32_le(*grid_position_y);
            }

            Self::CustomInput { custom_input_name, cursor_position, selected_prototype } => {
                writer.write_string(custom_input_name);
                if let Some(pos) = cursor_position {
                    writer.write_bool(true);
                    writer.write_map_position(*pos);
                } else {
                    writer.write_bool(false);
                }
                if let Some(proto) = selected_prototype {
                    writer.write_bool(true);
                    writer.write_string(proto);
                } else {
                    writer.write_bool(false);
                }
            }

            Self::LuaShortcut { shortcut_name } => {
                writer.write_string(shortcut_name);
            }

            Self::AdminAction { action_type, player_name } => {
                writer.write_u8(*action_type as u8);
                writer.write_string(player_name);
            }

            Self::PlayerJoinGame {
                peer_id,
                player_index_plus_one,
                mode,
                username,
                flag_a,
                flag_b,
            } => {
                writer.write_opt_u16(*peer_id);
                writer.write_u16_le(*player_index_plus_one);
                writer.write_u8(*mode);
                writer.write_string(username);
                writer.write_bool(*flag_a);
                writer.write_bool(*flag_b);
            }

            Self::PlayerLeaveGame { peer_id, reason } => {
                writer.write_u16_le(*peer_id);
                writer.write_u8(*reason);
            }

            Self::Raw { data, .. } => {
                writer.write_bytes(data);
            }

            // Catch-all for variants with no payload
            _ => {}
        }
    }

    pub fn write(&self, writer: &mut BinaryWriter) {
        self.write_type(writer);
        self.write_data(writer);
    }

    /// Legacy write method - writes player_idx then action (for compatibility)
    /// NOTE: This is WRONG per the protocol doc. Use write_type + write_data separately.
    #[deprecated(note = "Use write_type and write_data separately with proper field order")]
    pub fn write_with_player(&self, writer: &mut BinaryWriter, player_idx: u16) {
        writer.write_opt_u16(player_idx);
        self.write(writer);
    }

    /// Write action in correct protocol order: action_type, player_index_delta, action_data
    pub fn write_protocol_order(&self, writer: &mut BinaryWriter, player_index_delta: u16) {
        self.write_type(writer);
        writer.write_opt_u16(player_index_delta);
        self.write_data(writer);
    }

    pub fn read(reader: &mut BinaryReader) -> Result<Self> {
        Self::read_inner(reader, false)
    }

    pub fn read_known(reader: &mut BinaryReader) -> Result<Self> {
        Self::read_inner(reader, true)
    }

    fn read_inner(reader: &mut BinaryReader, strict: bool) -> Result<Self> {
        let action_type = reader.read_u8()?;

        match InputActionType::from_u8(action_type) {
            Some(InputActionType::Nothing) => Ok(Self::Nothing),
            Some(InputActionType::StopWalking) => Ok(Self::StopWalking),
            Some(InputActionType::StopMining) => Ok(Self::StopMining),
            Some(InputActionType::ToggleDriving) => Ok(Self::ToggleDriving),
            Some(InputActionType::CloseGui) => Ok(Self::CloseGui),
            Some(InputActionType::OpenCharacterGui) => Ok(Self::OpenCharacterGui),
            Some(InputActionType::ClearCursor) => Ok(Self::ClearCursor),
            Some(InputActionType::CancelResearch) => Ok(Self::CancelResearch),
            Some(InputActionType::SelectedEntityCleared) => Ok(Self::SelectedEntityCleared),
            Some(InputActionType::ToggleShowEntityInfo) => Ok(Self::ToggleShowEntityInfo),
            Some(InputActionType::TogglePersonalRoboport) => Ok(Self::TogglePersonalRoboport),
            Some(InputActionType::TogglePersonalLogisticRequests) => Ok(Self::TogglePersonalLogisticRequests),
            Some(InputActionType::LaunchRocket) => Ok(Self::LaunchRocket),
            Some(InputActionType::OpenProductionGui) => Ok(Self::OpenProductionGui),
            Some(InputActionType::OpenLogisticsGui) => Ok(Self::OpenLogisticsGui),
            Some(InputActionType::OpenBlueprintLibraryGui) => Ok(Self::OpenBlueprintLibraryGui),
            Some(InputActionType::OpenTrainsGui) => Ok(Self::OpenTrainsGui),
            Some(InputActionType::OpenAchievementsGui) => Ok(Self::OpenAchievementsGui),

            Some(InputActionType::StartWalking) => {
                let direction_x = reader.read_f64_le()?;
                let direction_y = reader.read_f64_le()?;
                Ok(Self::StartWalking { direction_x, direction_y })
            }

            Some(InputActionType::BeginMining) => {
                Ok(Self::BeginMining {
                    position: reader.read_map_position()?,
                    notify_server: reader.read_bool()?,
                })
            }

            Some(InputActionType::BeginMiningTerrain) => {
                Ok(Self::BeginMiningTerrain {
                    position: reader.read_map_position()?,
                })
            }

            Some(InputActionType::OpenGui) => {
                Ok(Self::OpenGui { entity_id: reader.read_u32_le()? })
            }

            Some(InputActionType::Build) => {
                Ok(Self::Build {
                    position: reader.read_map_position()?,
                    direction: reader.read_direction()?,
                    shift_build: reader.read_bool()?,
                    skip_fog_of_war: reader.read_bool()?,
                })
            }

            Some(InputActionType::Craft) => {
                Ok(Self::Craft {
                    recipe_id: reader.read_u16_le()?,
                    count: reader.read_u32_le()?,
                })
            }

            Some(InputActionType::WriteToConsole) => {
                Ok(Self::WriteToConsole { message: reader.read_string()? })
            }

            Some(InputActionType::PlayerJoinGame) => {
                let peer_id = reader.read_opt_u16()?;
                let player_index_plus_one = reader.read_u16_le()?;
                let mode = reader.read_u8()?;
                let username = reader.read_string()?;
                let flag_a = reader.read_bool()?;
                let flag_b = reader.read_bool()?;
                Ok(Self::PlayerJoinGame {
                    peer_id,
                    player_index_plus_one,
                    mode,
                    username,
                    flag_a,
                    flag_b,
                })
            }

            Some(InputActionType::ChangeShootingState) => {
                let state_byte = reader.read_u8()?;
                let state = match state_byte {
                    0 => ShootingState::NotShooting,
                    1 => ShootingState::ShootingEnemies,
                    2 => ShootingState::ShootingSelected,
                    _ => ShootingState::NotShooting,
                };
                let position = reader.read_map_position()?;
                Ok(Self::ChangeShootingState { state, position })
            }

            Some(InputActionType::SelectedEntityChanged) => {
                let entity_id_raw = reader.read_u32_le()?;
                let entity_id = if entity_id_raw == 0 { None } else { Some(entity_id_raw) };
                Ok(Self::SelectedEntityChanged { entity_id })
            }

            Some(InputActionType::DropItem) => {
                let position = reader.read_map_position()?;
                Ok(Self::DropItem { position })
            }

            Some(InputActionType::PlayerLeaveGame) => {
                let peer_id = reader.read_u16_le()?;
                let reason = reader.read_u8()?;
                Ok(Self::PlayerLeaveGame { peer_id, reason })
            }

            _ => {
                if strict {
                    return Err(Error::InvalidPacket(format!(
                        "unsupported input action type: {action_type}"
                    )));
                }
                // For unimplemented action types, store as raw
                let data = reader.read_remaining().to_vec();
                Ok(Self::Raw { action_type, data })
            }
        }
    }
}

/// Shooting state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ShootingState {
    NotShooting = 0,
    ShootingEnemies = 1,
    ShootingSelected = 2,
}

/// Riding acceleration state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RidingAcceleration {
    Nothing = 0,
    Accelerating = 1,
    Braking = 2,
    Reversing = 3,
}

/// Riding direction state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RidingDirection {
    Straight = 0,
    Left = 1,
    Right = 2,
}

/// Mouse button
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum MouseButton {
    Left = 0,
    Right = 1,
    Middle = 2,
}

/// GUI switch state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SwitchState {
    Left = 0,
    None = 1,
    Right = 2,
}

/// Admin action type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum AdminActionType {
    Kick = 0,
    Ban = 1,
    Unban = 2,
    Promote = 3,
    Demote = 4,
    Mute = 5,
    Unmute = 6,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_action_type_roundtrip() {
        for i in 0..=335u16 {
            let action_type = InputActionType::from_u16(i);
            assert!(action_type.is_some(), "Failed for action type {}", i);
            assert_eq!(action_type.unwrap() as u16, i);
        }
    }

    #[test]
    fn test_start_walking_roundtrip() {
        let action = InputAction::StartWalking { direction_x: 0.0, direction_y: -1.0 };

        let mut writer = BinaryWriter::new();
        action.write(&mut writer);

        let data = writer.into_vec();
        let mut reader = BinaryReader::new(&data);
        let read_action = InputAction::read(&mut reader).unwrap();

        match read_action {
            InputAction::StartWalking { direction_x, direction_y } => {
                assert!((direction_x - 0.0).abs() < f64::EPSILON);
                assert!((direction_y - (-1.0)).abs() < f64::EPSILON);
            }
            _ => panic!("Wrong action type"),
        }
    }

    #[test]
    fn test_craft_roundtrip() {
        let action = InputAction::Craft { recipe_id: 42, count: 10 };

        let mut writer = BinaryWriter::new();
        action.write(&mut writer);

        let data = writer.into_vec();
        let mut reader = BinaryReader::new(&data);
        let read_action = InputAction::read(&mut reader).unwrap();

        match read_action {
            InputAction::Craft { recipe_id, count } => {
                assert_eq!(recipe_id, 42);
                assert_eq!(count, 10);
            }
            _ => panic!("Wrong action type"),
        }
    }

    #[test]
    fn test_write_to_console_roundtrip() {
        let action = InputAction::WriteToConsole { message: "Hello, Factorio!".into() };

        let mut writer = BinaryWriter::new();
        action.write(&mut writer);

        let data = writer.into_vec();
        let mut reader = BinaryReader::new(&data);
        let read_action = InputAction::read(&mut reader).unwrap();

        match read_action {
            InputAction::WriteToConsole { message } => {
                assert_eq!(message, "Hello, Factorio!");
            }
            _ => panic!("Wrong action type"),
        }
    }
}
