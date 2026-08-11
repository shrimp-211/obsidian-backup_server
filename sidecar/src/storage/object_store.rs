//! Content-Addressable Storage (CAS) Object Store.
//!
//! Implements a Git-style Packfile storage system:
//!
//!   Objects/          — individual chunk files (fast path before packing)
//!   packfiles/        — append-only sealed pack containers
//!     h_00001.pack    — sealed pack (read-only once filled)
//!     h_00001.idx     — per-packfile index (ObjectHash → Offset, Size)
//!     parity/         — per-object RS(8+2) parity shards
//!
//! Object write path:
//!   1. With erasure coding enabled, an object is split into 8 data shards
//!      (`objects/{hash}/d0..d7`) plus 2 Reed-Solomon parity shards stored
//!      in `packfiles/parity/{hash}` (header + shard data). This lets
//!      `verify repair` reconstruct up to 2 corrupted shards in place.
//!   2. Otherwise the object is written as a single individual file.
//!   3. When enough individual objects accumulate, they are packed into a
//!      sealed packfile (CRC32C footer + `.idx`), and the loose files are
//!      removed. Sharded objects are never packed — they stay independently
//!      addressable for shard-level repair.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use tracing::{debug, info};

use crate::config::StorageConfig;
use crate::erasure::{ErasureCodec, DATA_SHARDS, PARITY_SHARDS, TOTAL_SHARDS};

/// Number of header bytes in a parity file:
/// original_size (u64 LE) + shard_size (u64 LE) + TOTAL_SHARDS × 32-byte hash.
const PARITY_HEADER_LEN: usize = 16 + TOTAL_SHARDS * 32;

/// CAS Object Store managing individual objects, sharded objects and Packfiles.
pub struct ObjectStore {
    root: PathBuf,
    objects_dir: PathBuf,
    packfile_dir: PathBuf,
    parity_dir: PathBuf,
    config: StorageConfig,

    /// Map of object hash → packfile location for fast lookup
    object_index: HashMap<String, ObjectLocation>,

    /// Next packfile sequence number (h_00001, h_00002, …)
    next_pack_id: u32,

    /// Total bytes stored (for statistics)
    total_bytes: u64,

    /// Total bytes of raw data before dedup (for ratio calculation)
    total_raw_bytes: u64,
}

/// Where an object lives in the store.
#[derive(Debug, Clone)]
pub enum ObjectLocation {
    /// Object stored as an individual file
    Individual(PathBuf),
    /// Object stored inside a sealed packfile
    Packfile {
        pack_id: String,
        offset: u64,
        size: u64,
    },
    /// Object stored as 8 data shards (+ 2 parity shards in the parity dir)
    Sharded { dir: PathBuf, size: u64 },
}

/// Coarse storage format of an object, used by `verify`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectKind {
    Individual,
    Packfile,
    Sharded,
}

/// Outcome of an integrity check on a single object.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RepairOutcome {
    /// All shards intact.
    Healthy,
    /// Corrupted shards were reconstructed and written back.
    Repaired,
    /// Corruption detected but `repair` was disabled.
    Corrupted,
    /// Too many shards corrupted to recover.
    Unrecoverable,
}

/// Writer for a Packfile being sealed.
pub struct PackfileWriter {
    pack_id: String,
    file: BufWriter<File>,
    bytes_written: u64,
    max_size: u64,
    index: Vec<PackfileEntry>,
    crc: u32,
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
        let parity_dir = packfile_dir.join("parity");

        fs::create_dir_all(&objects_dir)?;
        fs::create_dir_all(&packfile_dir)?;
        fs::create_dir_all(&parity_dir)?;

        let mut store = Self {
            root: root.to_path_buf(),
            objects_dir,
            packfile_dir,
            parity_dir,
            config,
            object_index: HashMap::new(),
            next_pack_id: 1,
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

    /// Load the in-memory index from individual files, sharded objects and
    /// packfile indices on disk.
    fn load_index(&mut self) -> Result<()> {
        // Scan the loose-objects directory: plain files are individual
        // objects, subdirectories are sharded objects.
        if self.objects_dir.exists() {
            for entry in fs::read_dir(&self.objects_dir)? {
                let entry = entry?;
                let path = entry.path();
                let hash = entry
                    .file_name()
                    .to_str()
                    .map(String::from)
                    .unwrap_or_default();
                if hash.is_empty() {
                    continue;
                }

                if path.is_dir() {
                    // Sharded object: recover original size from its parity header.
                    let parity_path = self.parity_dir.join(&hash);
                    match read_parity_header(&parity_path) {
                        Ok((original_size, shard_size)) => {
                            let stored = (DATA_SHARDS + PARITY_SHARDS) as u64 * shard_size as u64
                                + PARITY_HEADER_LEN as u64;
                            self.total_bytes += stored;
                            self.total_raw_bytes += original_size;
                            self.object_index.insert(
                                hash,
                                ObjectLocation::Sharded {
                                    dir: path,
                                    size: original_size,
                                },
                            );
                        }
                        Err(e) => {
                            tracing::warn!(
                                "[ObjectStore] Skipping sharded object {} (bad parity header: {})",
                                hash,
                                e
                            );
                        }
                    }
                } else if path.is_file() {
                    let size = entry.metadata()?.len();
                    self.total_bytes += size;
                    self.total_raw_bytes += size;
                    self.object_index
                        .insert(hash, ObjectLocation::Individual(path));
                }
            }
        }

        // Scan packfile indices.
        if self.packfile_dir.exists() {
            for entry in fs::read_dir(&self.packfile_dir)? {
                let entry = entry?;
                let path = entry.path();
                let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");

                if name.ends_with(".idx") {
                    let pack_id = name.trim_end_matches(".idx").to_string();
                    let pack_path = self.packfile_dir.join(format!("{}.pack", pack_id));

                    if pack_path.exists() {
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

                    // Track the highest pack sequence number.
                    if let Some(seq) = parse_pack_seq(&pack_id) {
                        self.next_pack_id = self.next_pack_id.max(seq + 1);
                    }
                }
            }
        }

        Ok(())
    }

    /// Write a chunk object to the store.
    ///
    /// Objects are written either as sharded objects (erasure coding) or as
    /// individual files. When enough loose objects accumulate, they are
    /// packed into a sealed packfile.
    pub fn write_object(&mut self, hash: &str, data: &[u8], raw_size: u64) -> Result<()> {
        // Dedup on write.
        if self.object_index.contains_key(hash) {
            debug!(
                "[ObjectStore] Object {} already exists, skipping",
                &hash[..8]
            );
            self.total_raw_bytes += raw_size;
            return Ok(());
        }

        if self.config.erasure_coding.enabled {
            self.write_sharded_object(hash, data)?;
        } else {
            self.write_individual_object(hash, data)?;
        }

        self.total_raw_bytes += raw_size;

        // Trigger packfile sealing when the loose-object threshold is met.
        self.maybe_seal_packfiles()?;

        Ok(())
    }

    /// Write an object as a single loose file (fast path).
    fn write_individual_object(&mut self, hash: &str, data: &[u8]) -> Result<()> {
        let obj_path = self.objects_dir.join(hash);
        let mut file = File::create(&obj_path)?;
        file.write_all(data)?;
        file.flush()?;

        self.total_bytes += data.len() as u64;
        self.object_index
            .insert(hash.to_string(), ObjectLocation::Individual(obj_path));

        debug!(
            "[ObjectStore] Wrote object {} ({} bytes)",
            &hash[..8],
            data.len()
        );
        Ok(())
    }

    /// Write an object as 8 data shards plus a parity file.
    fn write_sharded_object(&mut self, hash: &str, data: &[u8]) -> Result<()> {
        let codec = ErasureCodec::new()?;
        let enc = codec.encode(data)?;

        let dir = self.objects_dir.join(hash);
        fs::create_dir_all(&dir)?;
        for (i, shard) in enc.data_shards.iter().enumerate() {
            fs::write(dir.join(format!("d{}", i)), shard)?;
        }

        // Build the parity file: header (sizes + 10 shard hashes) + 2 parity shards.
        let mut body = Vec::with_capacity(
            PARITY_HEADER_LEN + enc.parity_shards.iter().map(Vec::len).sum::<usize>(),
        );
        body.extend_from_slice(&enc.original_size.to_le_bytes());
        body.extend_from_slice(&enc.shard_size.to_le_bytes());
        for shard in enc.data_shards.iter().chain(enc.parity_shards.iter()) {
            body.extend_from_slice(blake3::hash(shard).as_bytes());
        }
        for shard in &enc.parity_shards {
            body.extend_from_slice(shard);
        }
        fs::write(self.parity_dir.join(hash), &body)?;

        let stored = enc.data_shards.iter().map(Vec::len).sum::<usize>()
            + enc.parity_shards.iter().map(Vec::len).sum::<usize>()
            + PARITY_HEADER_LEN;
        self.total_bytes += stored as u64;
        self.object_index.insert(
            hash.to_string(),
            ObjectLocation::Sharded {
                dir,
                size: enc.original_size as u64,
            },
        );

        debug!(
            "[ObjectStore] Wrote sharded object {} ({} bytes → {} data + {} parity)",
            &hash[..8],
            enc.original_size,
            enc.data_shards.len(),
            enc.parity_shards.len()
        );
        Ok(())
    }

    /// Read a chunk object from the store by its content hash.
    pub fn read_object(&self, hash: &str) -> Result<Vec<u8>> {
        match self.object_index.get(hash) {
            Some(ObjectLocation::Individual(path)) => Ok(fs::read(path)?),
            Some(ObjectLocation::Sharded { dir, size }) => {
                let mut data = Vec::with_capacity(*size as usize);
                for i in 0..DATA_SHARDS {
                    let shard = fs::read(dir.join(format!("d{}", i)))
                        .with_context(|| format!("missing shard {} of {}", i, hash))?;
                    data.extend_from_slice(&shard);
                }
                data.truncate(*size as usize);
                Ok(data)
            }
            Some(ObjectLocation::Packfile {
                pack_id,
                offset,
                size,
            }) => {
                let pack_path = self.packfile_dir.join(format!("{}.pack", pack_id));
                let mut file = File::open(&pack_path)?;
                file.seek(SeekFrom::Start(*offset))?;
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

    /// Storage format of an object, or `None` if unknown.
    pub fn object_kind(&self, hash: &str) -> Option<ObjectKind> {
        self.object_index.get(hash).map(|loc| match loc {
            ObjectLocation::Individual(_) => ObjectKind::Individual,
            ObjectLocation::Packfile { .. } => ObjectKind::Packfile,
            ObjectLocation::Sharded { .. } => ObjectKind::Sharded,
        })
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

    /// Number of objects stored inside packfiles.
    pub fn packfile_count(&self) -> u64 {
        self.object_index
            .values()
            .filter(|loc| matches!(loc, ObjectLocation::Packfile { .. }))
            .count() as u64
    }

    /// Verify a single object and repair corrupted shards when possible.
    ///
    /// Only sharded objects can be repaired (they carry RS parity). Loose
    /// and packed objects are reported as `Unrecoverable` — callers should
    /// gate on [`Self::object_kind`] and do a whole-object hash check.
    ///
    /// When `repair` is `false`, corruption is reported as `Corrupted`
    /// without writing anything back.
    pub fn verify_object(&self, hash: &str, repair: bool) -> Result<RepairOutcome> {
        let ObjectLocation::Sharded { dir, .. } = self
            .object_index
            .get(hash)
            .ok_or_else(|| anyhow::anyhow!("Object {} not found in store", hash))?
        else {
            return Ok(RepairOutcome::Unrecoverable);
        };

        let parity_path = self.parity_dir.join(hash);
        let par = fs::read(&parity_path).with_context(|| {
            format!("cannot read parity file for {} (missing header)", hash)
        })?;
        if par.len() < PARITY_HEADER_LEN {
            bail!("truncated parity header for {}", hash);
        }

        let shard_size =
            u64::from_le_bytes(par[8..16].try_into().unwrap()) as usize;
        let mut expected = Vec::with_capacity(TOTAL_SHARDS);
        for i in 0..TOTAL_SHARDS {
            let off = 16 + i * 32;
            expected.push(<[u8; 32]>::try_from(&par[off..off + 32]).unwrap());
        }

        // Read all 10 shards and check each against its expected hash.
        let mut shards: Vec<Option<Vec<u8>>> = Vec::with_capacity(TOTAL_SHARDS);
        let mut corrupted = 0usize;
        for i in 0..DATA_SHARDS {
            let path = dir.join(format!("d{}", i));
            match fs::read(&path) {
                Ok(bytes) if blake3::hash(&bytes).as_bytes() == &expected[i] => {
                    shards.push(Some(bytes))
                }
                _ => {
                    corrupted += 1;
                    shards.push(None);
                }
            }
        }
        for i in 0..PARITY_SHARDS {
            let start = PARITY_HEADER_LEN + i * shard_size;
            let end = start + shard_size;
            if end <= par.len() {
                let bytes = par[start..end].to_vec();
                if blake3::hash(&bytes).as_bytes() == &expected[DATA_SHARDS + i] {
                    shards.push(Some(bytes));
                } else {
                    corrupted += 1;
                    shards.push(None);
                }
            } else {
                corrupted += 1;
                shards.push(None);
            }
        }

        if corrupted == 0 {
            return Ok(RepairOutcome::Healthy);
        }
        if !repair {
            tracing::warn!(
                "[Verify] Object {} has {} corrupted shards — repair disabled",
                hash,
                corrupted
            );
            return Ok(RepairOutcome::Corrupted);
        }
        if corrupted > PARITY_SHARDS {
            tracing::warn!(
                "[Verify] Object {} has {} corrupted shards — unrecoverable (max {})",
                hash,
                corrupted,
                PARITY_SHARDS
            );
            return Ok(RepairOutcome::Unrecoverable);
        }

        // Reconstruct the corrupted shards.
        let data_opt = shards[..DATA_SHARDS].to_vec();
        let parity_opt = shards[DATA_SHARDS..].to_vec();
        let codec = ErasureCodec::new()?;
        let (data, parity) = codec.reconstruct(data_opt, parity_opt, shard_size)?;

        for (i, shard) in data.iter().enumerate() {
            fs::write(dir.join(format!("d{}", i)), shard)?;
        }
        let mut new_par = par[..PARITY_HEADER_LEN].to_vec();
        for shard in &parity {
            new_par.extend_from_slice(shard);
        }
        fs::write(&parity_path, new_par)?;

        info!(
            "[Verify] Repaired {} shard(s) of object {} via RS({}+{})",
            corrupted,
            hash,
            DATA_SHARDS,
            PARITY_SHARDS
        );
        Ok(RepairOutcome::Repaired)
    }

    /// Pack accumulated loose objects into a sealed packfile.
    ///
    /// Runs when the total size of loose objects reaches the configured
    /// threshold. Sharded objects are excluded — they stay as loose shards.
    fn maybe_seal_packfiles(&mut self) -> Result<()> {
        let threshold = self.config.packfile.max_packfile_size_mb * 1024 * 1024;

        let individuals: Vec<(String, PathBuf, u64)> = self
            .object_index
            .iter()
            .filter_map(|(hash, loc)| match loc {
                ObjectLocation::Individual(path) => Some((
                    hash.clone(),
                    path.clone(),
                    path.metadata().ok().map(|m| m.len()).unwrap_or(0),
                )),
                _ => None,
            })
            .collect();

        if individuals.is_empty() {
            return Ok(());
        }
        let total: u64 = individuals.iter().map(|(_, _, s)| *s).sum();
        if total < threshold {
            return Ok(());
        }

        debug!(
            "[ObjectStore] Sealing packfile: {} loose objects ({} bytes)",
            individuals.len(),
            total
        );
        self.seal_packfile(&individuals)
    }

    /// Create and seal one packfile containing the given loose objects.
    fn seal_packfile(&mut self, objects: &[(String, PathBuf, u64)]) -> Result<()> {
        let pack_id = format!("h_{:05}", self.next_pack_id);
        self.next_pack_id += 1;

        let mut writer = PackfileWriter::new(
            &self.packfile_dir,
            pack_id.clone(),
            self.config.packfile.max_packfile_size_mb,
        )?;

        for (hash, path, _size) in objects {
            let data = fs::read(path)
                .with_context(|| format!("cannot read loose object {} while sealing", hash))?;
            writer.write_object(hash, &data)?;
        }

        let entries = writer.seal(&self.packfile_dir)?;
        for entry in &entries {
            self.object_index.insert(
                entry.hash.clone(),
                ObjectLocation::Packfile {
                    pack_id: pack_id.clone(),
                    offset: entry.offset,
                    size: entry.size,
                },
            );
        }

        // Loose files are now fully covered by the sealed packfile.
        for (_, path, _) in objects {
            let _ = fs::remove_file(path);
        }

        info!(
            "[Packfile] Sealed {} with {} objects ({} bytes, CRC32C footer)",
            pack_id,
            entries.len(),
            entries.iter().map(|e| e.size).sum::<u64>()
        );
        Ok(())
    }
}

impl PackfileWriter {
    /// Open a new packfile for writing.
    pub fn new(packfile_dir: &Path, pack_id: String, max_size_mb: u64) -> Result<Self> {
        let pack_path = packfile_dir.join(format!("{}.pack", &pack_id));
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&pack_path)?;

        Ok(Self {
            pack_id,
            file: BufWriter::new(file),
            bytes_written: 0,
            max_size: max_size_mb * 1024 * 1024,
            index: Vec::new(),
            crc: 0,
        })
    }

    /// Write an object into this packfile, incrementally updating the CRC32C.
    pub fn write_object(&mut self, hash: &str, data: &[u8]) -> Result<()> {
        let offset = self.bytes_written;
        let size = data.len() as u64;

        self.file.write_all(data)?;
        self.crc = crc32c::crc32c_append(self.crc, data);
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

    /// Seal the packfile: append the CRC32C footer and write the `.idx`.
    ///
    /// Returns the list of entries so callers can update their object index.
    pub fn seal(mut self, packfile_dir: &Path) -> Result<Vec<PackfileEntry>> {
        // CRC32C footer (8 bytes LE) — appended after the last object.
        self.file.write_all(&self.crc.to_le_bytes())?;
        self.file.flush()?;

        // Write the index file.
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
            self.crc
        );

        Ok(self.index)
    }
}

/// Parse the `original_size` and `shard_size` from a parity file header.
fn read_parity_header(parity_path: &Path) -> Result<(u64, u64)> {
    let par = fs::read(parity_path)?;
    if par.len() < PARITY_HEADER_LEN {
        bail!("parity file too short");
    }
    let original_size = u64::from_le_bytes(par[0..8].try_into().unwrap());
    let shard_size = u64::from_le_bytes(par[8..16].try_into().unwrap());
    Ok((original_size, shard_size))
}

/// Extract the numeric sequence from a pack id like `h_00042`.
fn parse_pack_seq(pack_id: &str) -> Option<u32> {
    pack_id
        .trim_start_matches("h_")
        .parse::<u32>()
        .ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn open_store(tmp: &tempfile::TempDir) -> ObjectStore {
        ObjectStore::new(tmp.path(), StorageConfig::default()).unwrap()
    }

    #[test]
    fn test_write_and_read_object_sharded() {
        let tmp = tempfile::tempdir().unwrap();
        let mut store = open_store(&tmp);

        let data = b"hello world chunk data, padded out to several shards";
        let hash = blake3::hash(data).to_hex().to_string();

        store.write_object(&hash, data, data.len() as u64).unwrap();
        assert!(store.contains(&hash));
        assert_eq!(store.object_kind(&hash), Some(ObjectKind::Sharded));

        let read_data = store.read_object(&hash).unwrap();
        assert_eq!(read_data, data);
    }

    #[test]
    fn test_verify_and_repair_corrupted_shard() {
        let tmp = tempfile::tempdir().unwrap();
        let mut store = open_store(&tmp);

        let data = vec![0x5Au8; 300 * 1024];
        let hash = blake3::hash(&data).to_hex().to_string();
        store.write_object(&hash, &data, data.len() as u64).unwrap();

        // Corrupt one data shard on disk.
        let dir = store.objects_dir.join(&hash);
        let corrupt = vec![0xFFu8; 100];
        fs::write(dir.join("d3"), &corrupt).unwrap();

        let outcome = store.verify_object(&hash, true).unwrap();
        assert_eq!(outcome, RepairOutcome::Repaired);

        // After repair the object reads back intact.
        let repaired = store.read_object(&hash).unwrap();
        assert_eq!(repaired, data);

        // A second pass reports it healthy.
        let outcome = store.verify_object(&hash, true).unwrap();
        assert_eq!(outcome, RepairOutcome::Healthy);
    }

    #[test]
    fn test_verify_unrecoverable_when_too_many_corruptions() {
        let tmp = tempfile::tempdir().unwrap();
        let mut store = open_store(&tmp);

        let data = vec![0x11u8; 100 * 1024];
        let hash = blake3::hash(&data).to_hex().to_string();
        store.write_object(&hash, &data, data.len() as u64).unwrap();

        let dir = store.objects_dir.join(&hash);
        fs::write(dir.join("d0"), vec![0xFFu8; 100]).unwrap();
        fs::write(dir.join("d1"), vec![0xFFu8; 100]).unwrap();
        fs::write(dir.join("d2"), vec![0xFFu8; 100]).unwrap();

        let outcome = store.verify_object(&hash, true).unwrap();
        assert_eq!(outcome, RepairOutcome::Unrecoverable);
    }

    #[test]
    fn test_dedup_ratio() {
        let tmp = tempfile::tempdir().unwrap();
        let mut store = open_store(&tmp);

        let data = vec![0u8; 1000];
        let hash = blake3::hash(&data).to_hex().to_string();

        store.write_object(&hash, &data, 2000).unwrap();
        store.write_object(&hash, &data, 2000).unwrap(); // deduped

        let ratio = store.dedup_ratio();
        assert!(ratio > 0.0, "Expected a positive dedup ratio, got {}", ratio);
    }

    #[test]
    fn test_seal_packfile_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        // Disable erasure coding so objects are stored loose (packable) and
        // set a 1 MB threshold to trigger sealing mid-test.
        let mut config = StorageConfig::default();
        config.erasure_coding.enabled = false;
        config.packfile.max_packfile_size_mb = 1; // 1 MB threshold
        let mut store = ObjectStore::new(tmp.path(), config).unwrap();

        // Write 6 × 256 KB objects (1.5 MB total).
        let mut objects = Vec::new();
        for i in 0..6u8 {
            let data = vec![i; 256 * 1024];
            let hash = blake3::hash(&data).to_hex().to_string();
            store.write_object(&hash, &data, data.len() as u64).unwrap();
            objects.push((hash, data));
        }

        // The first 4 objects (≥ 1 MB accumulated) must have been packed
        // automatically on write.
        assert_eq!(store.object_kind(&objects[0].0), Some(ObjectKind::Packfile));
        assert_eq!(store.object_kind(&objects[3].0), Some(ObjectKind::Packfile));

        // Every object remains readable after packing.
        for (hash, expected) in &objects {
            assert_eq!(&store.read_object(hash).unwrap(), expected);
        }
        assert!(store.packfile_count() >= 1);
    }
}
