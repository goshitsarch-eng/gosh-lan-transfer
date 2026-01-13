// SPDX-License-Identifier: MIT
// gosh-lan-transfer - Transfer history persistence

use crate::error::{EngineError, EngineResult};
use crate::types::TransferRecord;
use std::sync::RwLock;

/// Trait for persisting transfer history
///
/// The engine does not implement persistent storage - consumers provide their own storage.
/// This allows flexibility in how history is stored (JSON files, SQLite, etc.).
///
/// History records are created automatically by the engine when transfers complete or fail.
pub trait HistoryPersistence: Send + Sync {
    /// List all transfer records, ordered by started_at descending (newest first)
    fn list(&self) -> EngineResult<Vec<TransferRecord>>;

    /// List transfer records with pagination
    fn list_paginated(&self, offset: usize, limit: usize) -> EngineResult<Vec<TransferRecord>> {
        let all = self.list()?;
        Ok(all.into_iter().skip(offset).take(limit).collect())
    }

    /// Get a transfer record by ID
    fn get(&self, transfer_id: &str) -> EngineResult<Option<TransferRecord>>;

    /// Add a transfer record (called internally by the engine)
    fn add(&self, record: TransferRecord) -> EngineResult<()>;

    /// Delete a transfer record
    fn delete(&self, transfer_id: &str) -> EngineResult<()>;

    /// Clear all transfer history
    fn clear(&self) -> EngineResult<()>;

    /// Get the total number of records
    fn count(&self) -> EngineResult<usize> {
        Ok(self.list()?.len())
    }
}

/// In-memory transfer history store
///
/// Useful for testing or when persistence isn't needed.
/// This implementation does not persist data across restarts.
pub struct InMemoryHistory {
    records: RwLock<Vec<TransferRecord>>,
    /// Maximum number of records to keep (0 = unlimited)
    max_records: usize,
}

impl InMemoryHistory {
    /// Create a new empty in-memory history store
    pub fn new() -> Self {
        Self {
            records: RwLock::new(Vec::new()),
            max_records: 0,
        }
    }

    /// Create an in-memory store with a maximum record limit
    ///
    /// When the limit is reached, the oldest records are removed.
    pub fn with_limit(max_records: usize) -> Self {
        Self {
            records: RwLock::new(Vec::new()),
            max_records,
        }
    }

    /// Create an in-memory store pre-populated with records
    pub fn with_records(records: Vec<TransferRecord>) -> Self {
        Self {
            records: RwLock::new(records),
            max_records: 0,
        }
    }
}

impl Default for InMemoryHistory {
    fn default() -> Self {
        Self::new()
    }
}

impl HistoryPersistence for InMemoryHistory {
    fn list(&self) -> EngineResult<Vec<TransferRecord>> {
        let records = self
            .records
            .read()
            .map_err(|_| EngineError::InvalidConfig("Lock poisoned".to_string()))?;

        // Return sorted by started_at descending (newest first)
        let mut sorted = records.clone();
        sorted.sort_by(|a, b| b.started_at.cmp(&a.started_at));
        Ok(sorted)
    }

    fn get(&self, transfer_id: &str) -> EngineResult<Option<TransferRecord>> {
        Ok(self
            .records
            .read()
            .map_err(|_| EngineError::InvalidConfig("Lock poisoned".to_string()))?
            .iter()
            .find(|r| r.id == transfer_id)
            .cloned())
    }

    fn add(&self, record: TransferRecord) -> EngineResult<()> {
        let mut records = self
            .records
            .write()
            .map_err(|_| EngineError::InvalidConfig("Lock poisoned".to_string()))?;

        records.push(record);

        // Enforce max_records limit if set
        if self.max_records > 0 && records.len() > self.max_records {
            // Sort by started_at and keep only the newest
            records.sort_by(|a, b| b.started_at.cmp(&a.started_at));
            records.truncate(self.max_records);
        }

        Ok(())
    }

    fn delete(&self, transfer_id: &str) -> EngineResult<()> {
        let mut records = self
            .records
            .write()
            .map_err(|_| EngineError::InvalidConfig("Lock poisoned".to_string()))?;

        let original_len = records.len();
        records.retain(|r| r.id != transfer_id);

        if records.len() == original_len {
            return Err(EngineError::TransferNotFound(transfer_id.to_string()));
        }
        Ok(())
    }

    fn clear(&self) -> EngineResult<()> {
        let mut records = self
            .records
            .write()
            .map_err(|_| EngineError::InvalidConfig("Lock poisoned".to_string()))?;
        records.clear();
        Ok(())
    }

    fn count(&self) -> EngineResult<usize> {
        Ok(self
            .records
            .read()
            .map_err(|_| EngineError::InvalidConfig("Lock poisoned".to_string()))?
            .len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{TransferDirection, TransferFile, TransferStatus};
    use chrono::Utc;

    fn make_test_record(id: &str) -> TransferRecord {
        TransferRecord {
            id: id.to_string(),
            direction: TransferDirection::Sent,
            status: TransferStatus::Completed,
            peer_address: "192.168.1.1".to_string(),
            files: vec![TransferFile {
                id: "file1".to_string(),
                name: "test.txt".to_string(),
                size: 1024,
                mime_type: Some("text/plain".to_string()),
                relative_path: None,
            }],
            total_size: 1024,
            bytes_transferred: 1024,
            started_at: Utc::now(),
            completed_at: Some(Utc::now()),
            error: None,
        }
    }

    #[test]
    fn test_in_memory_history() {
        let store = InMemoryHistory::new();

        // Add a record
        let record = make_test_record("transfer-1");
        store.add(record.clone()).unwrap();

        // List records
        let list = store.list().unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].id, "transfer-1");

        // Get record
        let got = store.get("transfer-1").unwrap().unwrap();
        assert_eq!(got.id, "transfer-1");

        // Get non-existent
        let missing = store.get("nonexistent").unwrap();
        assert!(missing.is_none());

        // Count
        assert_eq!(store.count().unwrap(), 1);

        // Delete record
        store.delete("transfer-1").unwrap();
        assert_eq!(store.count().unwrap(), 0);

        // Delete non-existent fails
        let result = store.delete("nonexistent");
        assert!(result.is_err());
    }

    #[test]
    fn test_history_with_limit() {
        let store = InMemoryHistory::with_limit(2);

        // Add 3 records
        store.add(make_test_record("transfer-1")).unwrap();
        store.add(make_test_record("transfer-2")).unwrap();
        store.add(make_test_record("transfer-3")).unwrap();

        // Should only keep 2 (most recent)
        assert_eq!(store.count().unwrap(), 2);
    }

    #[test]
    fn test_history_clear() {
        let store = InMemoryHistory::new();

        store.add(make_test_record("transfer-1")).unwrap();
        store.add(make_test_record("transfer-2")).unwrap();
        assert_eq!(store.count().unwrap(), 2);

        store.clear().unwrap();
        assert_eq!(store.count().unwrap(), 0);
    }

    #[test]
    fn test_history_pagination() {
        let store = InMemoryHistory::new();

        for i in 1..=5 {
            store
                .add(make_test_record(&format!("transfer-{}", i)))
                .unwrap();
        }

        let page = store.list_paginated(1, 2).unwrap();
        assert_eq!(page.len(), 2);
    }
}
