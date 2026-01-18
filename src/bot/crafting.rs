use std::collections::HashMap;
use crate::codec::InputAction;
use crate::client::ActionBuilder;
use crate::state::Inventory;

/// Recipe definition
#[derive(Debug, Clone)]
pub struct Recipe {
    pub name: String,
    pub id: u16,
    pub ingredients: Vec<(String, u32)>,
    pub products: Vec<(String, u32)>,
    pub crafting_time: f32,
    pub category: String,
}

impl Recipe {
    pub fn new(name: impl Into<String>, id: u16) -> Self {
        Self {
            name: name.into(),
            id,
            ingredients: Vec::new(),
            products: Vec::new(),
            crafting_time: 0.5,
            category: "crafting".into(),
        }
    }

    pub fn with_ingredient(mut self, item: impl Into<String>, count: u32) -> Self {
        self.ingredients.push((item.into(), count));
        self
    }

    pub fn with_product(mut self, item: impl Into<String>, count: u32) -> Self {
        self.products.push((item.into(), count));
        self
    }

    pub fn with_time(mut self, time: f32) -> Self {
        self.crafting_time = time;
        self
    }
}

/// Crafting manager for tracking recipes and calculating what can be crafted
pub struct CraftingManager {
    recipes: HashMap<String, Recipe>,
    recipe_ids: HashMap<u16, String>,
}

impl CraftingManager {
    pub fn new() -> Self {
        let mut manager = Self {
            recipes: HashMap::new(),
            recipe_ids: HashMap::new(),
        };

        // Add basic vanilla recipes
        manager.add_basic_recipes();
        manager
    }

    fn add_basic_recipes(&mut self) {
        // Basic processing
        self.add_recipe(Recipe::new("iron-plate", 1)
            .with_ingredient("iron-ore", 1)
            .with_product("iron-plate", 1)
            .with_time(3.2));

        self.add_recipe(Recipe::new("copper-plate", 2)
            .with_ingredient("copper-ore", 1)
            .with_product("copper-plate", 1)
            .with_time(3.2));

        self.add_recipe(Recipe::new("steel-plate", 3)
            .with_ingredient("iron-plate", 5)
            .with_product("steel-plate", 1)
            .with_time(16.0));

        // Basic intermediates
        self.add_recipe(Recipe::new("iron-gear-wheel", 10)
            .with_ingredient("iron-plate", 2)
            .with_product("iron-gear-wheel", 1)
            .with_time(0.5));

        self.add_recipe(Recipe::new("copper-cable", 11)
            .with_ingredient("copper-plate", 1)
            .with_product("copper-cable", 2)
            .with_time(0.5));

        self.add_recipe(Recipe::new("electronic-circuit", 12)
            .with_ingredient("iron-plate", 1)
            .with_ingredient("copper-cable", 3)
            .with_product("electronic-circuit", 1)
            .with_time(0.5));

        self.add_recipe(Recipe::new("iron-stick", 13)
            .with_ingredient("iron-plate", 1)
            .with_product("iron-stick", 2)
            .with_time(0.5));

        // Basic buildings
        self.add_recipe(Recipe::new("transport-belt", 20)
            .with_ingredient("iron-plate", 1)
            .with_ingredient("iron-gear-wheel", 1)
            .with_product("transport-belt", 2)
            .with_time(0.5));

        self.add_recipe(Recipe::new("inserter", 21)
            .with_ingredient("iron-plate", 1)
            .with_ingredient("iron-gear-wheel", 1)
            .with_ingredient("electronic-circuit", 1)
            .with_product("inserter", 1)
            .with_time(0.5));

        self.add_recipe(Recipe::new("assembling-machine-1", 22)
            .with_ingredient("iron-plate", 9)
            .with_ingredient("iron-gear-wheel", 5)
            .with_ingredient("electronic-circuit", 3)
            .with_product("assembling-machine-1", 1)
            .with_time(0.5));

        self.add_recipe(Recipe::new("electric-mining-drill", 23)
            .with_ingredient("iron-plate", 10)
            .with_ingredient("iron-gear-wheel", 5)
            .with_ingredient("electronic-circuit", 3)
            .with_product("electric-mining-drill", 1)
            .with_time(2.0));

        self.add_recipe(Recipe::new("stone-furnace", 24)
            .with_ingredient("stone", 5)
            .with_product("stone-furnace", 1)
            .with_time(0.5));

        self.add_recipe(Recipe::new("wooden-chest", 25)
            .with_ingredient("wood", 2)
            .with_product("wooden-chest", 1)
            .with_time(0.5));

        self.add_recipe(Recipe::new("iron-chest", 26)
            .with_ingredient("iron-plate", 8)
            .with_product("iron-chest", 1)
            .with_time(0.5));
    }

    pub fn add_recipe(&mut self, recipe: Recipe) {
        self.recipe_ids.insert(recipe.id, recipe.name.clone());
        self.recipes.insert(recipe.name.clone(), recipe);
    }

    pub fn get_recipe(&self, name: &str) -> Option<&Recipe> {
        self.recipes.get(name)
    }

    pub fn get_recipe_by_id(&self, id: u16) -> Option<&Recipe> {
        self.recipe_ids.get(&id)
            .and_then(|name| self.recipes.get(name))
    }

    /// Check if a recipe can be crafted with given inventory
    pub fn can_craft(&self, recipe: &str, inventory: &Inventory, count: u32) -> bool {
        let recipe = match self.get_recipe(recipe) {
            Some(r) => r,
            None => return false,
        };

        for (item, required) in &recipe.ingredients {
            let have = inventory.count_item(item);
            if have < required * count {
                return false;
            }
        }

        true
    }

    /// Calculate maximum craftable count for a recipe
    pub fn max_craftable(&self, recipe: &str, inventory: &Inventory) -> u32 {
        let recipe = match self.get_recipe(recipe) {
            Some(r) => r,
            None => return 0,
        };

        let mut max = u32::MAX;

        for (item, required) in &recipe.ingredients {
            let have = inventory.count_item(item);
            let can_make = have / required;
            max = max.min(can_make);
        }

        max
    }

    /// Generate craft action for a recipe
    pub fn craft(&self, recipe: &str, count: u32) -> Option<InputAction> {
        let recipe = self.get_recipe(recipe)?;
        Some(ActionBuilder::craft(recipe.id, count))
    }

    /// Get missing ingredients for a recipe
    pub fn missing_ingredients(&self, recipe: &str, inventory: &Inventory, count: u32) -> Vec<(String, u32)> {
        let recipe = match self.get_recipe(recipe) {
            Some(r) => r,
            None => return Vec::new(),
        };

        let mut missing = Vec::new();

        for (item, required) in &recipe.ingredients {
            let have = inventory.count_item(item);
            let need = required * count;
            if have < need {
                missing.push((item.clone(), need - have));
            }
        }

        missing
    }
}

impl Default for CraftingManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::ItemStack;

    #[test]
    fn test_crafting_manager() {
        let manager = CraftingManager::new();

        let recipe = manager.get_recipe("iron-gear-wheel").unwrap();
        assert_eq!(recipe.name, "iron-gear-wheel");
        assert_eq!(recipe.ingredients.len(), 1);
    }

    #[test]
    fn test_can_craft() {
        let manager = CraftingManager::new();
        let mut inventory = Inventory::new(10);

        // Can't craft without materials
        assert!(!manager.can_craft("iron-gear-wheel", &inventory, 1));

        // Add materials
        inventory.set(0, Some(ItemStack::new("iron-plate", 10)));

        // Now can craft
        assert!(manager.can_craft("iron-gear-wheel", &inventory, 1));
        assert!(manager.can_craft("iron-gear-wheel", &inventory, 5));
        assert!(!manager.can_craft("iron-gear-wheel", &inventory, 6)); // Need 12 plates
    }

    #[test]
    fn test_max_craftable() {
        let manager = CraftingManager::new();
        let mut inventory = Inventory::new(10);

        inventory.set(0, Some(ItemStack::new("iron-plate", 10)));

        let max = manager.max_craftable("iron-gear-wheel", &inventory);
        assert_eq!(max, 5); // 10 plates / 2 per gear = 5
    }

    #[test]
    fn test_missing_ingredients() {
        let manager = CraftingManager::new();
        let mut inventory = Inventory::new(10);

        inventory.set(0, Some(ItemStack::new("iron-plate", 5)));

        let missing = manager.missing_ingredients("iron-gear-wheel", &inventory, 5);
        assert_eq!(missing.len(), 1);
        assert_eq!(missing[0], ("iron-plate".to_string(), 5)); // Need 10, have 5
    }
}
