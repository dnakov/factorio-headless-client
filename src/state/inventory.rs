use std::collections::HashMap;

/// An item stack (item name + count)
#[derive(Debug, Clone, PartialEq)]
pub struct ItemStack {
    pub name: String,
    pub count: u32,
    pub health: Option<u32>,  // Health in fixed-point (0-1000 = 0-100%)
    pub durability: Option<u32>,  // Durability in fixed-point
    pub ammo: Option<u32>,
    pub label: Option<String>,
    pub tags: HashMap<String, String>,
}

impl ItemStack {
    pub fn new(name: impl Into<String>, count: u32) -> Self {
        Self {
            name: name.into(),
            count,
            health: None,
            durability: None,
            ammo: None,
            label: None,
            tags: HashMap::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    pub fn can_stack_with(&self, other: &ItemStack) -> bool {
        self.name == other.name &&
        self.health == other.health &&
        self.durability == other.durability &&
        self.label == other.label
    }
}

/// Inventory slot (either empty or contains a stack)
pub type InventorySlot = Option<ItemStack>;

/// An inventory containing item slots
#[derive(Debug, Clone)]
pub struct Inventory {
    pub slots: Vec<InventorySlot>,
    pub bar: Option<u16>,  // Limited bar position
}

impl Inventory {
    pub fn new(size: usize) -> Self {
        Self {
            slots: vec![None; size],
            bar: None,
        }
    }

    pub fn size(&self) -> usize {
        self.slots.len()
    }

    pub fn usable_size(&self) -> usize {
        self.bar.map(|b| b as usize).unwrap_or(self.slots.len())
    }

    pub fn get(&self, slot: usize) -> Option<&ItemStack> {
        self.slots.get(slot).and_then(|s| s.as_ref())
    }

    pub fn get_mut(&mut self, slot: usize) -> Option<&mut ItemStack> {
        self.slots.get_mut(slot).and_then(|s| s.as_mut())
    }

    pub fn set(&mut self, slot: usize, stack: Option<ItemStack>) {
        if slot < self.slots.len() {
            self.slots[slot] = stack;
        }
    }

    pub fn is_empty(&self) -> bool {
        self.slots.iter().all(|s| s.is_none())
    }

    pub fn is_full(&self) -> bool {
        let usable = self.usable_size();
        self.slots.iter().take(usable).all(|s| s.is_some())
    }

    /// Count total items of a specific type
    pub fn count_item(&self, item_name: &str) -> u32 {
        self.slots.iter()
            .filter_map(|s| s.as_ref())
            .filter(|s| s.name == item_name)
            .map(|s| s.count)
            .sum()
    }

    /// Find first slot containing specific item
    pub fn find_item(&self, item_name: &str) -> Option<usize> {
        self.slots.iter()
            .enumerate()
            .find(|(_, s)| s.as_ref().map(|s| s.name == item_name).unwrap_or(false))
            .map(|(i, _)| i)
    }

    /// Find first empty slot
    pub fn find_empty_slot(&self) -> Option<usize> {
        let usable = self.usable_size();
        self.slots.iter()
            .enumerate()
            .take(usable)
            .find(|(_, s)| s.is_none())
            .map(|(i, _)| i)
    }

    /// Get all items as a map of name -> count
    pub fn contents(&self) -> HashMap<String, u32> {
        let mut map = HashMap::new();
        for slot in &self.slots {
            if let Some(stack) = slot {
                *map.entry(stack.name.clone()).or_insert(0) += stack.count;
            }
        }
        map
    }

    /// Insert an item stack, returns remainder if couldn't fit all
    pub fn insert(&mut self, mut stack: ItemStack) -> Option<ItemStack> {
        let usable = self.usable_size();

        // First try to stack with existing items
        for slot in &mut self.slots[..usable] {
            if let Some(existing) = slot {
                if existing.can_stack_with(&stack) {
                    let max_stack = stack_size(&stack.name);
                    let space = max_stack.saturating_sub(existing.count);
                    let transfer = space.min(stack.count);
                    existing.count += transfer;
                    stack.count -= transfer;
                    if stack.count == 0 {
                        return None;
                    }
                }
            }
        }

        // Then try empty slots
        for slot in &mut self.slots[..usable] {
            if slot.is_none() {
                let max_stack = stack_size(&stack.name);
                let transfer = max_stack.min(stack.count);
                *slot = Some(ItemStack::new(stack.name.clone(), transfer));
                stack.count -= transfer;
                if stack.count == 0 {
                    return None;
                }
            }
        }

        if stack.count > 0 {
            Some(stack)
        } else {
            None
        }
    }

    /// Remove items, returns how many were actually removed
    pub fn remove(&mut self, item_name: &str, mut count: u32) -> u32 {
        let mut removed = 0;
        for slot in &mut self.slots {
            if let Some(stack) = slot {
                if stack.name == item_name {
                    let take = stack.count.min(count);
                    stack.count -= take;
                    count -= take;
                    removed += take;
                    if stack.count == 0 {
                        *slot = None;
                    }
                    if count == 0 {
                        break;
                    }
                }
            }
        }
        removed
    }
}

/// Inventory type identifiers
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u16)]
pub enum InventoryType {
    // Player inventories
    PlayerMain = 1,
    PlayerQuickbar = 2,
    PlayerGuns = 3,
    PlayerAmmo = 4,
    PlayerArmor = 5,
    PlayerTools = 6,
    PlayerVehicle = 7,
    PlayerTrash = 8,

    // Entity inventories
    Fuel = 10,
    Burnt = 11,
    ChestOrCargo = 12,
    FurnaceSource = 13,
    FurnaceResult = 14,
    FurnaceModules = 15,
    AssemblerInput = 16,
    AssemblerOutput = 17,
    AssemblerModules = 18,
    LabInput = 19,
    LabModules = 20,
    MiningDrillModules = 21,
    ItemMain = 22,
    RocketSiloRocket = 23,
    RocketSiloResult = 24,
    RobotCargo = 25,
    RobotRepair = 26,
    Turret = 27,
    BeaconModules = 28,
    CharacterCorpse = 29,
    RocketSiloInput = 30,
    CarTrunk = 31,
    CarAmmo = 32,
    CargoWagon = 33,
    ArtilleryWagon = 34,
    ArtilleryTurret = 35,
    SpiderTrunk = 36,
    SpiderAmmo = 37,
    SpiderTrash = 38,
    EditorMain = 39,
    EditorGuns = 40,
    EditorAmmo = 41,
    EditorArmor = 42,
}

/// Get the stack size for an item.
/// Uses Lua prototype data if available, falls back to heuristics.
pub fn stack_size(item_name: &str) -> u32 {
    crate::lua::prototype::stack_size(item_name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_item_stack() {
        let stack = ItemStack::new("iron-plate", 50);
        assert_eq!(stack.name, "iron-plate");
        assert_eq!(stack.count, 50);
        assert!(!stack.is_empty());
    }

    #[test]
    fn test_inventory_operations() {
        let mut inv = Inventory::new(10);
        assert!(inv.is_empty());

        inv.set(0, Some(ItemStack::new("iron-plate", 50)));
        assert!(!inv.is_empty());
        assert_eq!(inv.count_item("iron-plate"), 50);

        inv.insert(ItemStack::new("iron-plate", 30));
        assert_eq!(inv.count_item("iron-plate"), 80);

        let removed = inv.remove("iron-plate", 40);
        assert_eq!(removed, 40);
        assert_eq!(inv.count_item("iron-plate"), 40);
    }

    #[test]
    fn test_inventory_find() {
        let mut inv = Inventory::new(5);
        inv.set(2, Some(ItemStack::new("copper-plate", 25)));

        assert_eq!(inv.find_item("copper-plate"), Some(2));
        assert_eq!(inv.find_item("iron-plate"), None);
        assert_eq!(inv.find_empty_slot(), Some(0));
    }
}
