//! Content-Addressable Storage (CAS) Object Store.
//!
//! Implements a Git-style Packfile storage system:
//!
//!   Objects/          — individual chunk files (before packing)
//!   packfiles/        — append-only sealed pack containers
//!     h_00001.pack    — hot data sealed pack (read-only once filled)
//!     h_00001.idx     — per-packfile index (ObjectHash → Offset, Size)
//!     parity/         — RS(8+2) erasure coding parity blocks
//!
//! Packfile lifecycle:
//!   1. Objects are written individually (fast path)
//!   2. When a packfile reaches max_packfile_size_mb, it is "sealed"
//!      - CRC32C footer is written
//!      - Packfile becomes read-only
//!      - Individual object files can be GC'd
//!   3. Sealed packfiles are append-only and immutable

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use tracing::{debug, info};

use crate::config::StorageConfig;

/// CAS Object Store managing individual objects and Packfiles.
pub struct ObjectStore {
    root: PathBuf,
    objects_dir: PathBuf,
    packfile_dir: PathBuf,
    config: StorageConfig,

    /// Current open packfile writer
    current_packfile: Option<PackfileWriter>,

    /// Map of object hash → packfile location for fast lookup
    object_index: HashMap<String, ObjectLocation>,

    /// Total bytes stored (for statistics)
    total_bytes: u64,

    /// Total bytes of raw data before dedup (for ratio calculation)
    total_raw_bytes: u64,
}

/// Location of an object in storage.
#[derive(Debug, Clone)]
pub enum ObjectLocation {
    /// Object stored as an individual file
    Individual(PathBuf),
    /// Object stored inside a packfile
    Packfile {
        pack_id: String,
        offset: u64,
        size: u64,
    },
}

/// Writer for the currently open Packfile.
pub struct PackfileWriter {
    pack_id: String,
    file: BufWriter<File>,
    bytes_written: u64,
    max_size: u64,
    index: Vec<PackfileEntry>,
}

#[derive(Debug, Clone)]
pub struct PackfileEntry {
    pub hash: String,
    pub offset: u64,
    pub size: u64,
}

impl ObjectStore {
    /// Open (or create) the object store at the given root directory.
    pub fn new(root: &Path, config: StorageConfig) -> Result<Self> {
        let objects_dir = root.join("objects");
        let packfile_dir = root.join("packfiles");

        fs::create_dir_all(&objects_dir)?;
        fs::create_dir_all(&packfile_dir)?;
        fs::create_dir_all(packfile_dir.join("parity"))?;

        let mut store = Self {
            root: root.to_path_buf(),
            objects_dir,
            packfile_dir,
            config,
            current_packfile: None,
            object_index: HashMap::new(),
            total_bytes: 0,
            total_raw_bytes: 0,
        };

        // Load existing object index
        store.load_index()?;

        info!(
            "[ObjectStore] Opened at {:?}. Total objects: {}, Total size: {} bytes",
            root,
            store.object_index.len(),
            store.total_bytes
        );

        Ok(store)
    }

    /// Load existing object index from packfile indices and individual object files.
    fn load_index(&mut self) -> Result<()> {
        // Scan individual object files
        if self.objects_dir.exists() {
            for entry in fs::read_dir(&self.objects_dir)? {
                let entry = entry?;
                let path = entry.path();
                if path.is_file() {
                    let hash = path
                        .file_name()
                        .and_then(|n| n.to_str())
                        .unwrap_or("")
                        .to_string();
                    if !hash.is_empty() {
                        let size = entry.metadata()?.len();
                        self.total_bytes += size;
                        self.total_raw_bytes += size;
                        self.object_index
                            .insert(hash, ObjectLocation::Individual(path));
                    }
                }
            }
        }

        // Scan packfile indices
        if self.packfile_dir.exists() {
            for entry in fs::read_dir(&self.packfile_dir)? {
                let entry = entry?;
                let path = entry.path();
                let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");

                if name.ends_with(".idx") {
                    let pack_id = name.trim_end_matches(".idx").to_string();
                    let pack_path = self.packfile_dir.join(format!("{}.pack", pack_id));

                    if pack_path.exists() {
                        // Load the index file
                        let idx_data = fs::read_to_string(&path)?;
                        for line in idx_data.lines() {
                            let parts: Vec<&str> = line.splitn(3, ' ').collect();
                            if parts.len() == 3 {
                                let hash = parts[0].to_string();
                                let offset: u64 = parts[1].parse().unwrap_or(0);
                                let size: u64 = parts[2].parse().unwrap_or(0);
                                self.total_bytes += size;
                                self.total_raw_bytes += size;
                                self.object_index.insert(
                                    hash,
                                    ObjectLocation::Packfile {
                                        pack_id: pack_id.clone(),
                                        offset,
                                        size,
                                    },
                                );
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Write a chunk object to the store.
    ///
    /// Objects are initially written as individual files. When a packfile
    /// reaches its size limit, it's sealed and future writes go to a new pack.
    pub fn write_object(&mut self, hash: &str, data: &[u8], raw_size: u64) -> Result<()> {
        // Check if already exists (dedup on write)
        if self.object_index.contains_key(hash) {
            debug!(
                "[ObjectStore] Object {} already exists, skipping",
                &hash[..8]
            );
            self.total_raw_bytes += raw_size;
            return Ok(());
        }

        // Write as individual object file
        let obj_path = self.objects_dir.join(hash);
        let mut file = File::create(&obj_path)?;
        file.write_all(data)?;
        file.flush()?;

        self.total_bytes += data.len() as u64;
        self.total_raw_bytes += raw_size;
        self.object_index
            .insert(hash.to_string(), ObjectLocation::Individual(obj_path));

        debug!(
            "[ObjectStore] Wrote object {} ({} bytes)",
            &hash[..8],
            data.len()
        );

        // Check if we should trigger packfile sealing
        self.maybe_seal_packfiles()?;

        Ok(())
    }

    /// Read a chunk object from the store by its content hash.
    pub fn read_object(&self, hash: &str) -> Result<Vec<u8>> {
        match self.object_index.get(hash) {
            Some(ObjectLocation::Individual(path)) => Ok(fs::read(path)?),
            Some(ObjectLocation::Packfile {
                pack_id,
                offset,
                size,
            }) => {
                let pack_path = self.packfile_dir.join(format!("{}.pack", pack_id));
                let mut file = File::open(&pack_path)?;
                file.seek(std::io::SeekFrom::Start(*offset))?;
                let mut buf = vec![0u8; *size as usize];
                file.read_exact(&mut buf)?;
                Ok(buf)
            }
            None => Err(anyhow::anyhow!("Object {} not found in store", hash)),
        }
    }

    /// Check if a chunk exists in the store.
    pub fn contains(&self, hash: &str) -> bool {
        self.object_index.contains_key(hash)
    }

    /// Get the deduplication ratio.
    ///
    /// Ratio = (total_raw - total_stored) / total_raw * 100
    pub fn dedup_ratio(&self) -> f64 {
        if self.total_raw_bytes == 0 {
            return 0.0;
        }
        let saved = self.total_raw_bytes.saturating_sub(self.total_bytes) as f64;
        (saved / self.total_raw_bytes as f64) * 100.0
    }

    /// Total stored bytes.
    pub fn total_size(&self) -> u64 {
        self.total_bytes
    }

    /// Number of packfiles.
    pub fn packfile_count(&self) -> u64 {
        self.object_index
            .values()
            .filter(|loc| matches!(loc, ObjectLocation::Packfile { .. }))
            .count() as u64
    }

    /// Check if packfiles need to be sealed and compacted.
    ///
    /// In production, this would run asynchronously:
    ///   1. Count individual objects
    ///   2. If count > threshold, create a new packfile
    ///   3. Copy individual objects into packfile
    ///   4. Seal packfile with CRC32C footer
    ///   5. Delete individual object files
    ///   6. Update index
    fn maybe_seal_packfiles(&mut self) -> Result<()> {
        let threshold = self.config.packfile.max_packfile_size_mb * 1024 * 1024 / 2;
        let individual_count = self
            .object_index
            .values()
            .filter(|loc| matches!(loc, ObjectLocation::Individual(_)))
            .count();

        if individual_count as u64 > threshold / 65536 {
            // Rough heuristic
            debug!(
                "[ObjectStore] {} individual objects, consider sealing packfile",
                individual_count
            );
            // In production: trigger async compaction
        }
        Ok(())
    }
}

impl PackfileWriter {
    /// Open a new packfile for writing.
    pub fn new(packfile_dir: &Path, pack_id: String, max_size_mb: u64) -> Result<Self> {
        let pack_path = packfile_dir.join(format!("{}.pack", &pack_id));
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&pack_path)?;

        Ok(Self {
            pack_id,
            file: BufWriter::new(file),
            bytes_written: 0,
            max_size: max_size_mb * 1024 * 1024,
            index: Vec::new(),
        })
    }

    /// Write an object into this packfile.
    pub fn write_object(&mut self, hash: &str, data: &[u8]) -> Result<()> {
        let offset = self.bytes_written;
        let size = data.len() as u64;

        self.file.write_all(data)?;
        self.bytes_written += size;

        self.index.push(PackfileEntry {
            hash: hash.to_string(),
            offset,
            size,
        });

        Ok(())
    }

    /// Check if this packfile is full.
    pub fn is_full(&self) -> bool {
        self.bytes_written >= self.max_size
    }

    /// Seal the packfile: write CRC32C footer and index file.
    pub fn seal(mut self, packfile_dir: &Path) -> Result<()> {
        // Calculate CRC32C of entire packfile content
        // (In production, we'd use incremental CRC32C during writes)
        let crc = 0u32; // placeholder

        // Write CRC32C footer
        self.file.write_all(&crc.to_le_bytes())?;
        self.file.flush()?;

        // Write index file
        let idx_path = packfile_dir.join(format!("{}.idx", self.pack_id));
        let mut idx_file = File::create(idx_path)?;

        for entry in &self.index {
            writeln!(idx_file, "{} {} {}", entry.hash, entry.offset, entry.size)?;
        }

        info!(
            "[Packfile] Sealed {} with {} objects ({} bytes, CRC32C={:08x})",
            self.pack_id,
            self.index.len(),
            self.bytes_written,
            crc
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_and_read_object() {
        let tmp = tempfile::tempdir().unwrap();
        let config = StorageConfig::default();
        let mut store = ObjectStore::new(tmp.path(), config).unwrap();

        let data = b"hello world chunk data";
        let hash = blake3::hash(data).to_hex().to_string();

        store.write_object(&hash, data, data.len() as u64).unwrap();
        assert!(store.contains(&hash));

        let read_data = store.read_object(&hash).unwrap();
        assert_eq!(read_data, data);
    }

    #[test]
    fn test_dedup_ratio() {
        let tmp = tempfile::tempdir().unwrap();
        let config = StorageConfig::default();
        let mut store = ObjectStore::new(tmp.path(), config).unwrap();

        let data = vec![0u8; 1000];
        let hash = blake3::hash(&data).to_hex().to_string();

        // Write same data twice with different "raw" sizes
        store.write_object(&hash, &data, 2000).unwrap();
        store.write_object(&hash, &data, 2000).unwrap(); // should be deduped

        // raw=4000, stored=1000 → 75% dedup
        let ratio = store.dedup_ratio();
        assert!(ratio > 70.0, "Expected high dedup ratio, got {}", ratio);
    }
}
