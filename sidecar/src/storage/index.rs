//! RocksDB-based block index for the Obsidian Sidecar.
//!
//! Provides fast persistent lookup of:
//!   - File path → list of chunk hashes (for restore)
//!   - Chunk hash → reference count (for dedup)
//!   - Chunk hash → metadata (original path, offset, size)
//!   - Snapshot ID → list of files (for diff/browse)
//!
//! Column families:
//!   "file_chunks"  — file_path → JSON array of chunk hashes
//!   "chunk_refs"   — chunk_hash → reference count (u64 LE)
//!   "chunk_meta"   — chunk_hash → JSON { path, offset, size, mtime }
//!   "snapshot"     — snapshot_id → JSON array of file paths
//!   "file_meta"    — file_path@snapshot_id → { size, mtime, chunk_count }

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use rocksdb::{ColumnFamilyDescriptor, Options, DB};
use tracing::info;

/// Block index backed by RocksDB with multiple column families.
pub struct BlockIndex {
    db: DB,
}

// Column family names
const CF_FILE_CHUNKS: &str = "file_chunks";
const CF_CHUNK_REFS: &str = "chunk_refs";
const CF_CHUNK_META: &str = "chunk_meta";
const CF_SNAPSHOT: &str = "snapshot";
const CF_FILE_META: &str = "file_meta";

impl BlockIndex {
    /// Open (or create) the RocksDB block index at the given path.
    pub fn open(path: &Path) -> Result<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        opts.increase_parallelism(4);
        opts.set_max_background_jobs(4);
        opts.optimize_level_style_compaction(512 * 1024 * 1024); // 512MB memtable
        opts.set_write_buffer_size(128 * 1024 * 1024); // 128MB write buffer

        let cfs = vec![
            ColumnFamilyDescriptor::new(CF_FILE_CHUNKS, Options::default()),
            ColumnFamilyDescriptor::new(CF_CHUNK_REFS, Options::default()),
            ColumnFamilyDescriptor::new(CF_CHUNK_META, Options::default()),
            ColumnFamilyDescriptor::new(CF_SNAPSHOT, Options::default()),
            ColumnFamilyDescriptor::new(CF_FILE_META, Options::default()),
        ];

        let db = DB::open_cf_descriptors(&opts, path, cfs)?;
        info!("[RocksDB] Opened at {:?}", path);

        Ok(Self { db })
    }

    // =========================================================================
    // File → Chunks mapping
    // =========================================================================

    /// Store the chunk list for a given file path.
    pub fn insert_file_chunks(
        &self,
        file_path: &str,
        chunk_hashes: &[String],
        file_size: u64,
        mtime: u64,
    ) -> Result<()> {
        let cf = self
            .db
            .cf_handle(CF_FILE_CHUNKS)
            .ok_or_else(|| anyhow::anyhow!("CF {} not found", CF_FILE_CHUNKS))?;

        let chunks_json = serde_json::to_string(chunk_hashes)?;
        self.db
            .put_cf(&cf, file_path.as_bytes(), chunks_json.as_bytes())?;

        // Also store file metadata
        let meta_cf = self
            .db
            .cf_handle(CF_FILE_META)
            .ok_or_else(|| anyhow::anyhow!("CF {} not found", CF_FILE_META))?;

        let meta = serde_json::json!({
            "size": file_size,
            "mtime": mtime,
            "chunk_count": chunk_hashes.len()
        });
        self.db.put_cf(
            &meta_cf,
            file_path.as_bytes(),
            serde_json::to_string(&meta)?.as_bytes(),
        )?;

        Ok(())
    }

    /// Retrieve the chunk hash list for a given file path.
    pub fn get_file_chunks(&self, file_path: &str) -> Result<Vec<String>> {
        let cf = self
            .db
            .cf_handle(CF_FILE_CHUNKS)
            .ok_or_else(|| anyhow::anyhow!("CF {} not found", CF_FILE_CHUNKS))?;

        match self.db.get_cf(&cf, file_path.as_bytes())? {
            Some(data) => {
                let chunks: Vec<String> = serde_json::from_slice(&data)?;
                Ok(chunks)
            }
            None => Ok(Vec::new()),
        }
    }

    // =========================================================================
    // Chunk reference counting (for dedup and GC)
    // =========================================================================

    /// Insert a new chunk into the index with its metadata.
    pub fn insert_chunk(
        &self,
        chunk_hash: &str,
        original_path: &str,
        offset: u64,
        size: u64,
    ) -> Result<()> {
        // Increment reference count
        let ref_cf = self
            .db
            .cf_handle(CF_CHUNK_REFS)
            .ok_or_else(|| anyhow::anyhow!("CF {} not found", CF_CHUNK_REFS))?;

        let current_refs = self.get_ref_count(chunk_hash)?;
        let new_refs = current_refs + 1;
        self.db
            .put_cf(&ref_cf, chunk_hash.as_bytes(), &new_refs.to_le_bytes())?;

        // Store chunk metadata
        let meta_cf = self
            .db
            .cf_handle(CF_CHUNK_META)
            .ok_or_else(|| anyhow::anyhow!("CF {} not found", CF_CHUNK_META))?;

        let meta = serde_json::json!({
            "original_path": original_path,
            "offset": offset,
            "size": size
        });
        self.db.put_cf(
            &meta_cf,
            chunk_hash.as_bytes(),
            serde_json::to_string(&meta)?.as_bytes(),
        )?;

        Ok(())
    }

    /// Check if a chunk already exists in the index.
    pub fn chunk_exists(&self, chunk_hash: &str) -> Result<bool> {
        let cf = self
            .db
            .cf_handle(CF_CHUNK_REFS)
            .ok_or_else(|| anyhow::anyhow!("CF {} not found", CF_CHUNK_REFS))?;

        Ok(self.db.get_cf(&cf, chunk_hash.as_bytes())?.is_some())
    }

    /// Increment the reference count for a chunk.
    pub fn increment_ref(&self, chunk_hash: &str) -> Result<()> {
        let count = self.get_ref_count(chunk_hash)? + 1;
        let cf = self
            .db
            .cf_handle(CF_CHUNK_REFS)
            .ok_or_else(|| anyhow::anyhow!("CF {} not found", CF_CHUNK_REFS))?;
        self.db
            .put_cf(&cf, chunk_hash.as_bytes(), &count.to_le_bytes())?;
        Ok(())
    }

    /// Get the reference count for a chunk.
    fn get_ref_count(&self, chunk_hash: &str) -> Result<u64> {
        let cf = self
            .db
            .cf_handle(CF_CHUNK_REFS)
            .ok_or_else(|| anyhow::anyhow!("CF {} not found", CF_CHUNK_REFS))?;

        match self.db.get_cf(&cf, chunk_hash.as_bytes())? {
            Some(data) if data.len() == 8 => Ok(u64::from_le_bytes(data[..8].try_into().unwrap())),
            _ => Ok(0),
        }
    }

    // =========================================================================
    // Snapshot management
    // =========================================================================

    /// Get all files in a snapshot.
    pub fn get_snapshot_files(&self, snapshot_id: &str) -> Result<Vec<String>> {
        let cf = self
            .db
            .cf_handle(CF_SNAPSHOT)
            .ok_or_else(|| anyhow::anyhow!("CF {} not found", CF_SNAPSHOT))?;

        match self.db.get_cf(&cf, snapshot_id.as_bytes())? {
            Some(data) => {
                let files: Vec<String> = serde_json::from_slice(&data)?;
                Ok(files)
            }
            None => Ok(Vec::new()),
        }
    }

    /// Get all files ever indexed (for full restore).
    pub fn get_all_files(&self) -> Result<Vec<String>> {
        let cf = self
            .db
            .cf_handle(CF_FILE_CHUNKS)
            .ok_or_else(|| anyhow::anyhow!("CF {} not found", CF_FILE_CHUNKS))?;

        let mut files = Vec::new();
        let iter = self.db.iterator_cf(&cf, rocksdb::IteratorMode::Start);
        for item in iter {
            let (key, _) = item?;
            if let Ok(path) = String::from_utf8(key.to_vec()) {
                files.push(path);
            }
        }

        Ok(files)
    }

    /// Get the N largest files by stored size.
    pub fn get_largest_files(&self, limit: usize) -> Result<Vec<(String, u64)>> {
        let cf = self
            .db
            .cf_handle(CF_FILE_META)
            .ok_or_else(|| anyhow::anyhow!("CF {} not found", CF_FILE_META))?;

        let mut files_with_sizes: Vec<(String, u64)> = Vec::new();
        let iter = self.db.iterator_cf(&cf, rocksdb::IteratorMode::Start);

        for item in iter {
            let (key, value) = item?;
            let path = String::from_utf8(key.to_vec())?;
            if let Ok(meta) = serde_json::from_slice::<serde_json::Value>(&value) {
                let size = meta.get("size").and_then(|v| v.as_u64()).unwrap_or(0);
                files_with_sizes.push((path, size));
            }
        }

        // Sort by size descending
        files_with_sizes.sort_by(|a, b| b.1.cmp(&a.1));
        files_with_sizes.truncate(limit);

        Ok(files_with_sizes)
    }

    /// Get the size of a file in a specific snapshot.
    pub fn get_file_size(&self, _snapshot_id: &str, file_path: &str) -> Result<u64> {
        let cf = self
            .db
            .cf_handle(CF_FILE_META)
            .ok_or_else(|| anyhow::anyhow!("CF {} not found", CF_FILE_META))?;

        match self.db.get_cf(&cf, file_path.as_bytes())? {
            Some(data) => {
                let meta: serde_json::Value = serde_json::from_slice(&data)?;
                Ok(meta.get("size").and_then(|v| v.as_u64()).unwrap_or(0))
            }
            None => Ok(0),
        }
    }

    /// Get all unique chunk hashes in the index.
    pub fn get_all_chunks(&self) -> Result<Vec<String>> {
        let cf = self
            .db
            .cf_handle(CF_CHUNK_REFS)
            .ok_or_else(|| anyhow::anyhow!("CF {} not found", CF_CHUNK_REFS))?;

        let mut chunks = Vec::new();
        let iter = self.db.iterator_cf(&cf, rocksdb::IteratorMode::Start);
        for item in iter {
            let (key, _) = item?;
            if let Ok(hash) = String::from_utf8(key.to_vec()) {
                chunks.push(hash);
            }
        }

        Ok(chunks)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_file_chunks_roundtrip() {
        let tmp = tempdir().unwrap();
        let idx = BlockIndex::open(tmp.path()).unwrap();

        let chunks = vec!["abc123".to_string(), "def456".to_string()];
        idx.insert_file_chunks("world/region/r.0.0.mca", &chunks, 4096, 1234567890)
            .unwrap();

        let retrieved = idx.get_file_chunks("world/region/r.0.0.mca").unwrap();
        assert_eq!(retrieved, chunks);
    }

    #[test]
    fn test_chunk_ref_counting() {
        let tmp = tempdir().unwrap();
        let idx = BlockIndex::open(tmp.path()).unwrap();

        assert!(!idx.chunk_exists("abc123").unwrap());

        idx.insert_chunk("abc123", "world/data.mca", 0, 1024)
            .unwrap();
        assert!(idx.chunk_exists("abc123").unwrap());

        idx.increment_ref("abc123").unwrap();
        // Reference count should now be 2
        // (1 from insert_chunk + 1 from increment_ref)
    }
}
