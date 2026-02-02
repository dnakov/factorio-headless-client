# Factorio Client - Reverse Engineering Guide

Rust library for connecting to Factorio 2.0 multiplayer servers as a bot/automation client.

## Important Rules

- DO NOT USE pkill

## Project Structure

```
src/
  lib.rs              # Public API exports
  error.rs            # Error types
  codec/              # Binary encoding/decoding
    mod.rs
    reader.rs         # BinaryReader for parsing
    writer.rs         # BinaryWriter for building packets
    types.rs          # Fixed32, MapPosition, etc.
    input_action.rs   # Player input actions (walk, mine, craft, etc.)
    map_transfer.rs   # Map save file parsing
    map_settings.rs   # Map configuration parsing
    map_types.rs      # Map data structures
    heartbeat.rs      # Server heartbeat packets
    synchronizer_action.rs  # Synchronization actions
    tick_closure.rs   # Tick execution closure
  protocol/           # Network protocol
    mod.rs
    packet.rs         # Packet header parsing, message types
    transport.rs      # UDP transport layer
    connection.rs     # Connection state machine
    message.rs        # Message serialization
  state/              # Game state tracking
    mod.rs
    world.rs          # GameWorld container
    surface.rs        # Map surface (entities, tiles)
    entity.rs         # Entity types
    player.rs         # Player state
    inventory.rs      # Inventory management
    recipe.rs         # Crafting recipes
  simulation/         # Deterministic simulation
    mod.rs
    tick.rs           # Tick execution
    checksum.rs       # CRC verification
    action_executor.rs
  client/             # High-level client API
    mod.rs
    session.rs        # Game session management
    commands.rs       # Command interface
    events.rs         # Event types
  daemon/             # Background daemon service
    mod.rs
    protocol.rs       # Daemon protocol
  bot/                # Bot automation
    mod.rs
    controller.rs     # Movement/action controller
    crafting.rs       # Auto-crafting logic
  lua/                # Lua integration
    mod.rs
    noise.rs          # Noise generation bindings
    prototype.rs      # Prototype definitions
    runtime.rs        # Lua runtime management
  bin/                # Executables
    play_game.rs      # Main game client
    factorio_bot.rs   # Bot automation client
    factorio_daemon.rs # Background daemon
    tui.rs            # Terminal UI (requires --features tui)
    tile_decode_test.rs # Tile format testing
    download_map.rs   # Map download tool
    check_map_seed.rs # Map seed verification

factorio-mapgen/      # Workspace member: map generation library
  src/
    lib.rs
    cache.rs          # Caching layer
    compiler.rs       # Map generator compiler
    executor.rs       # Execution engine
    expression.rs     # Expression evaluation
    loader.rs         # Data loader
    operations.rs     # Map generation operations
    program.rs        # Program representation
    terrain.rs        # Terrain generation

lua/                  # Factorio game data (base/, core/, space-age/, etc.)
docs/                 # Protocol analysis and reverse engineering notes
```

## Running

```bash
cargo run --bin play-game
cargo run --bin factorio-bot
cargo run --bin factorio-daemon
cargo run --bin tile-decode-test
cargo run --bin factorio-tui --features tui
```

## Code Quality

Run `kiss check` after writing code and fix any violations. Use `kiss rules` to see current thresholds.

## External Tools

### Radare2 (Binary Analysis)

A radare2 instance is running with the Factorio binary loaded, accessible via HTTP:

```
http://localhost:9090/cmd/<url-encoded-command>
```

Example usage from code or scripts:
```bash
# Disassemble a function
curl 'http://localhost:9090/cmd/pdf%20@%20sym.some_function'

# Search for string references
curl 'http://localhost:9090/cmd/iz~some_string'

# Analyze function at address
curl 'http://localhost:9090/cmd/af%20@%200x12345'

# List functions matching pattern
curl 'http://localhost:9090/cmd/afl~pattern'
```

Use this for disassembly, symbol lookup, string searches, and cross-references when reverse engineering packet formats or game internals.

### RCON (Factorio Server)

A Factorio server is running with RCON enabled:
- Host: `localhost`
- RCON password: `factorio123`

Use RCON to execute console commands on the live server for testing and validation (e.g. teleport player, spawn entities, read game state via Lua commands).

## Protocol Overview

Factorio uses **deterministic lockstep** over UDP:
- Server sends tick confirmations, NOT game state
- All clients run identical simulation from inputs
- Entity destruction, inventory, etc. computed locally
- Packets use custom binary format with zlib compression

### Message Types (packet.rs)

```
0x00 Ping
0x01 PingReply
0x02 ConnectionRequest
0x03 ConnectionRequestReply
0x04 ConnectionRequestReplyConfirm
0x05 ConnectionAcceptOrDeny
0x06 ClientToServerHeartbeat
0x07 ServerToClientHeartbeat
0x0C TransferBlockRequest
0x0D TransferBlock (map data)
0x10 GameInformationRequest
0x11 GameInformationRequestReply
```

### Packet Header Format

```
Byte 0:    Type byte
           Bits 0-4: Message type (0-18)
           Bit 5:    Reliable flag (0x20)
           Bit 6:    Fragmented flag (0x40)
Bytes 1-2: Message ID (u16 LE)
           Bit 15:   Has confirmations flag
[VarShort] Fragment ID (if fragmented)
[VarInt]   Confirmation count (if has confirmations)
[u32 * N]  Confirmation IDs
[...]      Payload
```

### Heartbeat Format (0x07)

```
Byte 0:    Type (0x07 or 0x27 with reliable)
Byte 1:    Flags
           0x06: Single tick mode
           0x02: Multi tick mode
           0x10: Has player state update
Bytes 2-3: Sequence (u16 LE)
Bytes 4-5: 0x1c 0x00 (constant)

Multi mode: Byte 6 = confirmation count
Then: Server tick (u32 LE) + padding

Tick Confirmation:
  0x02 0x52 0x00 (marker)
  CRC/checksum (4 bytes)
  Confirmed tick (u32 LE)
  Padding zeros
```

## Map Save Format

Map data is sent as a ZIP file via TransferBlock packets.

### Archive Contents

```
mapname/
  level.dat0    # Prototype definitions (entities, items, recipes)
  level.dat1-7  # Chunk data (entities, tiles, resources)
  level-init.dat
  script.dat
  control.lua   # (if present)
```

### level.dat0 - Prototype Definitions

Contains all prototype ID mappings. Format:
```
[length: u8] [name: bytes] [id: u16 LE]
```

Extract by scanning for known patterns like "tree-01", "iron-ore", etc.

**Known Entity IDs (Factorio 2.0):**
| ID Range | Entity Type |
|----------|-------------|
| 4-8 | Transport belts |
| 9-12 | Underground belts |
| 135 | coal |
| 136 | stone |
| 137 | iron-ore |
| 138 | copper-ore |
| 139 | uranium-ore |
| 175-182 | Biters |
| 183-186 | Worm turrets |
| 187-188 | Spawners |
| 213-227 | Trees |

### Entity Storage (level.dat1+)

Entities (trees, biters, buildings) stored as:
```
[entity_id: u16 LE] [x: i32 LE] [y: i32 LE]
```
Position is **fixed-point**: divide by 256.0 to get tile coordinates.

Filter criteria for valid entities:
- Position not (0, 0)
- Position within ±500 tiles
- Not aligned to 65536 boundaries (false positive indicator)
- At least one coordinate > 4 tiles (avoid origin cluster)

### Resource Storage

Resources (iron-ore, copper-ore, coal, stone, uranium-ore) are **tile data**, NOT positioned entities. They're stored densely in chunk data.

To count resources: scan for u16 IDs 135-139 in level.dat1+ files. Positions are implicit (chunk coords + tile offset within chunk).

## Reverse Engineering Workflow

### 1. Capture Packets

```bash
cargo run --bin capture-all-packets
cargo run --bin analyze-captured
```

### 2. Save Map for Offline Analysis

```bash
cargo run --bin save-map
cargo run --bin analyze-offline
cargo run --bin test-parsing
```

### 3. Find Unknown Data Patterns

Create a debug binary in `src/bin/`, add `[[bin]]` entry to Cargo.toml.

### 4. Pattern Searching

Look for:
- **Length-prefixed strings**: `[len: u8] [ascii bytes]` followed by u16 ID
- **Clusters of same ID**: entities/resources are spatially grouped
- **Fixed-point positions**: i32 values that make sense when divided by 256
- **Count-prefixed arrays**: `[count: u16/u32] [records...]`

### 5. Validate Findings

Compare against known data:
- Connect to game, note player position
- Save map, search for that position in binary
- Use RCON to query live game state for comparison

## Common Pitfalls

1. **False positives in entity scanning**: Random bytes match `[u16][i32][i32]` pattern. Filter by known entity ID range, position sanity, avoid 65536-aligned positions.
2. **Resources vs entities**: Resources are tile data, not entity records.
3. **Multiple ID occurrences**: Same prototype name may appear multiple times in level.dat0 with different IDs.
4. **Zlib everywhere**: Most data is zlib-compressed. Always try decompression first.
5. **Chunk structure**: Chunks are 32x32 tiles. Entity positions may be relative to chunk origin.

## Input Actions

Player actions sent to server (input_action.rs):

```
0x50 (80)  StartWalking      [direction: u8]
0x01       StopWalking
0x02       BeginMining       [position]
0x03       StopMining
0x5C (92)  Craft             [recipe_id: u16] [count: u32]
0x52 (82)  ChangeShootingState
```

Build packet:
```
[action_type: u8] [tick: u32 LE] [player_id: u16 LE] [action_data...]
```

## Adding New Features

1. **New packet type**: Add to `MessageType` enum in packet.rs, implement parse in relevant module
2. **New entity type**: Add ID range to `entity_patterns` in map_transfer.rs
3. **New action**: Add to `InputActionType` enum, implement serialization
4. **New state tracking**: Add to appropriate state/ module, update GameWorld
