use std::collections::HashMap;

/// A recipe ingredient or product
#[derive(Debug, Clone, PartialEq)]
pub struct RecipeItem {
    pub name: String,
    pub amount: u32,
}

impl RecipeItem {
    pub fn new(name: impl Into<String>, amount: u32) -> Self {
        Self { name: name.into(), amount }
    }
}

/// A recipe definition
#[derive(Debug, Clone)]
pub struct Recipe {
    pub name: String,
    pub ingredients: Vec<RecipeItem>,
    pub products: Vec<RecipeItem>,
    pub crafting_time: f64,
    pub category: String,
    pub enabled: bool,
}

impl Recipe {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            ingredients: Vec::new(),
            products: Vec::new(),
            crafting_time: 0.5,
            category: "crafting".into(),
            enabled: true,
        }
    }

    pub fn with_ingredient(mut self, name: impl Into<String>, amount: u32) -> Self {
        self.ingredients.push(RecipeItem::new(name, amount));
        self
    }

    pub fn with_product(mut self, name: impl Into<String>, amount: u32) -> Self {
        self.products.push(RecipeItem::new(name, amount));
        self
    }

    pub fn with_time(mut self, time: f64) -> Self {
        self.crafting_time = time;
        self
    }

    pub fn with_category(mut self, category: impl Into<String>) -> Self {
        self.category = category.into();
        self
    }

    /// Check if a recipe can be crafted with available items
    pub fn can_craft(&self, available: &HashMap<String, u32>, count: u32) -> bool {
        for ing in &self.ingredients {
            let needed = ing.amount * count;
            let have = available.get(&ing.name).copied().unwrap_or(0);
            if have < needed {
                return false;
            }
        }
        true
    }
}

/// Recipe database with ID -> recipe mappings
#[derive(Debug, Clone, Default)]
pub struct RecipeDatabase {
    /// Recipe ID -> Recipe
    recipes_by_id: HashMap<u16, Recipe>,
    /// Recipe name -> ID
    name_to_id: HashMap<String, u16>,
}

impl RecipeDatabase {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a recipe with its ID
    pub fn add(&mut self, id: u16, recipe: Recipe) {
        self.name_to_id.insert(recipe.name.clone(), id);
        self.recipes_by_id.insert(id, recipe);
    }

    /// Get recipe by ID
    pub fn get(&self, id: u16) -> Option<&Recipe> {
        self.recipes_by_id.get(&id)
    }

    /// Get recipe by name
    pub fn get_by_name(&self, name: &str) -> Option<&Recipe> {
        let id = self.name_to_id.get(name)?;
        self.recipes_by_id.get(id)
    }

    /// Get ID by name
    pub fn get_id(&self, name: &str) -> Option<u16> {
        self.name_to_id.get(name).copied()
    }

    /// Get all recipe names
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.name_to_id.keys().map(|s| s.as_str())
    }

    /// Get count of recipes
    pub fn len(&self) -> usize {
        self.recipes_by_id.len()
    }

    pub fn is_empty(&self) -> bool {
        self.recipes_by_id.is_empty()
    }

    /// Extract recipe ID -> name mappings from level.dat0 binary data
    pub fn extract_ids_from_data(&mut self, data: &[u8]) {
        // Recipe names in Factorio follow specific patterns
        // Format in binary: [len: u8] [name: bytes] followed by [id: u16 LE]

        // Recipe keywords that identify recipe prototypes
        let recipe_indicators = [
            "-plate", "-gear", "-cable", "-circuit", "-belt", "-inserter",
            "-furnace", "-drill", "-pole", "-pipe", "-chest", "-turret",
            "-ammo", "-magazine", "-armor", "-module", "-robot", "-science",
            "engine-unit", "electric-engine", "flying-robot-frame",
            "rocket-fuel", "rocket-control", "low-density", "rocket-part",
            "processing-unit", "battery", "sulfur", "plastic-bar", "explosives",
            "automation-science", "logistic-science", "military-science",
            "chemical-science", "production-science", "utility-science",
        ];

        // Exclusion patterns
        let exclude_patterns = [
            "particle", "remnant", "explosion", "projectile", "corpse",
            "sticker", "recycling", "tile", "decorative", "entity-ghost",
        ];

        let mut i = 0;
        while i < data.len().saturating_sub(20) {
            let len = data[i] as usize;

            if len >= 4 && len < 60 && i + 1 + len + 2 <= data.len() {
                let potential = &data[i + 1..i + 1 + len];

                if potential.iter().all(|&b| b >= 32 && b < 127) {
                    if let Ok(name) = std::str::from_utf8(potential) {
                        // Skip excluded patterns
                        let is_excluded = exclude_patterns.iter().any(|p| name.contains(p));
                        if is_excluded {
                            i += 1;
                            continue;
                        }

                        // Check if it looks like a recipe
                        let is_recipe = recipe_indicators.iter().any(|kw| name.contains(kw));

                        if is_recipe && name.contains('-') {
                            // ID is right after the name
                            let id = u16::from_le_bytes([
                                data[i + 1 + len],
                                data[i + 1 + len + 1]
                            ]);

                            if id > 0 && id < 1000 && !self.name_to_id.contains_key(name) {
                                // Create a placeholder recipe with just the name
                                // Full recipe data would need to be parsed from prototype definitions
                                let recipe = Recipe::new(name);
                                self.add(id, recipe);
                            }
                        }
                    }
                }
            }
            i += 1;
        }
    }

}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_recipe_creation() {
        let recipe = Recipe::new("iron-gear-wheel")
            .with_ingredient("iron-plate", 2)
            .with_product("iron-gear-wheel", 1)
            .with_time(0.5);

        assert_eq!(recipe.name, "iron-gear-wheel");
        assert_eq!(recipe.ingredients.len(), 1);
        assert_eq!(recipe.products.len(), 1);
        assert_eq!(recipe.crafting_time, 0.5);
    }

    #[test]
    fn test_recipe_can_craft() {
        let recipe = Recipe::new("iron-gear-wheel")
            .with_ingredient("iron-plate", 2)
            .with_product("iron-gear-wheel", 1);

        let mut available = HashMap::new();
        available.insert("iron-plate".into(), 5);

        assert!(recipe.can_craft(&available, 1));
        assert!(recipe.can_craft(&available, 2));
        assert!(!recipe.can_craft(&available, 3));
    }

    #[test]
    fn test_recipe_database() {
        let mut db = RecipeDatabase::new();

        let recipe = Recipe::new("iron-gear-wheel")
            .with_ingredient("iron-plate", 2)
            .with_product("iron-gear-wheel", 1);

        db.add(42, recipe);

        assert_eq!(db.len(), 1);
        assert!(db.get(42).is_some());
        assert!(db.get_by_name("iron-gear-wheel").is_some());
        assert_eq!(db.get_id("iron-gear-wheel"), Some(42));
    }
}
