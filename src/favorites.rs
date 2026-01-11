// SPDX-License-Identifier: MIT
// gosh-lan-transfer - Favorites persistence

use crate::error::{EngineError, EngineResult};
use crate::types::Favorite;
use chrono::Utc;
use std::sync::RwLock;

/// Trait for persisting favorites
///
/// The engine does not implement persistent storage - consumers provide their own storage.
/// This allows flexibility in how favorites are stored (JSON files, SQLite, etc.).
pub trait FavoritesPersistence: Send + Sync {
    /// List all favorites
    fn list(&self) -> EngineResult<Vec<Favorite>>;

    /// Add a new favorite
    fn add(&self, name: String, address: String) -> EngineResult<Favorite>;

    /// Update an existing favorite
    fn update(
        &self,
        id: &str,
        name: Option<String>,
        address: Option<String>,
    ) -> EngineResult<Favorite>;

    /// Delete a favorite
    fn delete(&self, id: &str) -> EngineResult<()>;

    /// Get a favorite by ID
    fn get(&self, id: &str) -> EngineResult<Option<Favorite>>;

    /// Update the last used timestamp for a favorite
    fn touch(&self, id: &str) -> EngineResult<()> {
        if let Some(fav) = self.get(id)? {
            self.update(id, Some(fav.name), Some(fav.address))?;
        }
        Ok(())
    }
}

/// In-memory favorites store
///
/// Useful for testing or when persistence isn't needed.
/// This implementation does not persist data across restarts.
pub struct InMemoryFavorites {
    favorites: RwLock<Vec<Favorite>>,
}

impl InMemoryFavorites {
    /// Create a new empty in-memory favorites store
    pub fn new() -> Self {
        Self {
            favorites: RwLock::new(Vec::new()),
        }
    }

    /// Create an in-memory store pre-populated with favorites
    pub fn with_favorites(favorites: Vec<Favorite>) -> Self {
        Self {
            favorites: RwLock::new(favorites),
        }
    }
}

impl Default for InMemoryFavorites {
    fn default() -> Self {
        Self::new()
    }
}

impl FavoritesPersistence for InMemoryFavorites {
    fn list(&self) -> EngineResult<Vec<Favorite>> {
        Ok(self
            .favorites
            .read()
            .map_err(|_| EngineError::InvalidConfig("Lock poisoned".to_string()))?
            .clone())
    }

    fn add(&self, name: String, address: String) -> EngineResult<Favorite> {
        let favorite = Favorite::new(name, address);
        self.favorites
            .write()
            .map_err(|_| EngineError::InvalidConfig("Lock poisoned".to_string()))?
            .push(favorite.clone());
        Ok(favorite)
    }

    fn update(
        &self,
        id: &str,
        name: Option<String>,
        address: Option<String>,
    ) -> EngineResult<Favorite> {
        let mut favorites = self
            .favorites
            .write()
            .map_err(|_| EngineError::InvalidConfig("Lock poisoned".to_string()))?;

        let fav = favorites
            .iter_mut()
            .find(|f| f.id == id)
            .ok_or_else(|| EngineError::InvalidConfig(format!("Favorite not found: {}", id)))?;

        if let Some(n) = name {
            fav.name = n;
        }
        if let Some(a) = address {
            fav.address = a;
        }
        fav.last_used = Some(Utc::now());

        Ok(fav.clone())
    }

    fn delete(&self, id: &str) -> EngineResult<()> {
        let mut favorites = self
            .favorites
            .write()
            .map_err(|_| EngineError::InvalidConfig("Lock poisoned".to_string()))?;

        let original_len = favorites.len();
        favorites.retain(|f| f.id != id);

        if favorites.len() == original_len {
            return Err(EngineError::InvalidConfig(format!(
                "Favorite not found: {}",
                id
            )));
        }
        Ok(())
    }

    fn get(&self, id: &str) -> EngineResult<Option<Favorite>> {
        Ok(self
            .favorites
            .read()
            .map_err(|_| EngineError::InvalidConfig("Lock poisoned".to_string()))?
            .iter()
            .find(|f| f.id == id)
            .cloned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_in_memory_favorites() {
        let store = InMemoryFavorites::new();

        // Add a favorite
        let fav = store.add("Test".to_string(), "192.168.1.1".to_string()).unwrap();
        assert_eq!(fav.name, "Test");
        assert_eq!(fav.address, "192.168.1.1");

        // List favorites
        let list = store.list().unwrap();
        assert_eq!(list.len(), 1);

        // Update favorite
        let updated = store
            .update(&fav.id, Some("Updated".to_string()), None)
            .unwrap();
        assert_eq!(updated.name, "Updated");
        assert_eq!(updated.address, "192.168.1.1");

        // Get favorite
        let got = store.get(&fav.id).unwrap().unwrap();
        assert_eq!(got.name, "Updated");

        // Delete favorite
        store.delete(&fav.id).unwrap();
        let list = store.list().unwrap();
        assert!(list.is_empty());
    }
}
