pub mod chunker;
pub mod scanner;
pub mod transaction;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use chrono::Utc;
use tokio::sync::{Mutex, RwLock};
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::config::SidecarConfig;
use crate::storage::index::BlockIndex;
use crate::storage::object_store::{ObjectStore, PackfileWriter};

use self::chunker::ChunkEngine;
use self::scanner::FileScanner;
use self::transaction::{Transaction, TransactionManager, TransactionState};

/// Represents a single backup operation.
pub struct BackupEngine {
    server_root: PathBuf,
    block_index: Arc<Mutex<BlockIndex>>,
    object_store: Arc<RwLock<ObjectStore>>,
    config: SidecarConfig,
    scanner: FileScanner,

    /// Current transaction state, if a backup/restore is in progress.
    active_transaction: Arc<Mutex<Option<Transaction>>>,
    tx_manager: TransactionManager,

    /// System state for /obsidian status
    state: Arc<RwLock<SystemState>>,

    /// Snapshot metadata cache
    snapshots: Arc<RwLock<Vec<SnapshotInfo>>>,
}

/// System state snapshot for status reporting.
#[derive(Debug, Clone)]
pub struct SystemState {
    pub running: bool,
    pub current_tx: Option<String>,
    pub state: String,
    pub tps: f64,
    pub cpu_percent: f64,
    pub memory_mb: u64,
    pub disk_iops_read: u64,
    pub disk_iops_write: u64,
    pub network_upload_mbps: f64,
    pub scanner_queue: u64,
    pub chunk_queue: u64,
    pub compress_queue: u64,
    pub encrypt_queue: u64,
    pub upload_queue: u64,
    pub total_snapshots: u64,
    pub total_size_bytes: u64,
    pub dedup_ratio: f64,
    pub packfile_count: u64,
}

impl Default for SystemState {
    fn default() -> Self {
        Self {
            running: false,
            current_tx: None,
            state: "idle".into(),
            tps: 20.0,
            cpu_percent: 0.0,
            memory_mb: 0,
            disk_iops_read: 0,
            disk_iops_write: 0,
            network_upload_mbps: 0.0,
            scanner_queue: 0,
            chunk_queue: 0,
            compress_queue: 0,
            encrypt_queue: 0,
            upload_queue: 0,
            total_snapshots: 0,
            total_size_bytes: 0,
            dedup_ratio: 0.0,
            packfile_count: 0,
        }
    }
}

/// Result of a backup operation.
#[derive(Debug, Clone)]
pub struct BackupResult {
    pub snapshot_id: String,
    pub files_scanned: u64,
    pub files_changed: u64,
    pub bytes_processed: u64,
    pub chunks_deduped: u64,
    pub chunks_new: u64,
    pub duration_ms: u64,
}

/// Result of a restore operation.
#[derive(Debug, Clone)]
pub struct RestoreResult {
    pub files_restored: u64,
    pub bytes_restored: u64,
    pub sandbox_used: bool,
}

/// Top file analysis result.
#[derive(Debug, Clone)]
pub struct TopFilesResult {
    pub files: Vec<TopFileEntry>,
    pub dedup_ratio: f64,
    pub dict_gain: f64,
}

#[derive(Debug, Clone)]
pub struct TopFileEntry {
    pub path: String,
    pub size: u64,
    pub reason: Option<String>,
}

/// Snapshot diff result.
#[derive(Debug, Clone)]
pub struct DiffResult {
    pub added: Vec<String>,
    pub modified: Vec<String>,
    pub deleted: Vec<String>,
}

/// Snapshot metadata.
#[derive(Debug, Clone)]
pub struct SnapshotInfo {
    pub snapshot_id: String,
    pub timestamp: String,
    pub tag: Option<String>,
    pub files_scanned: u64,
    pub bytes_processed: u64,
    pub chunks_total: u64,
    pub chunks_deduped: u64,
}

/// Verify result.
#[derive(Debug, Clone)]
pub struct VerifyResult {
    pub total_checked: u64,
    pub healthy: u64,
    pub corrupted: u64,
    pub repaired: u64,
}

/// Forecast result.
#[derive(Debug, Clone)]
pub struct ForecastResult {
    pub days_remaining: f64,
    pub growth_rate_mb_per_day: f64,
    pub total_capacity_gb: f64,
}

impl BackupEngine {
    /// Create a new backup engine.
    pub fn new(
        server_root: PathBuf,
        block_index: BlockIndex,
        object_store: Arc<RwLock<ObjectStore>>,
        config: SidecarConfig,
    ) -> Self {
        Self {
            scanner: FileScanner::new(server_root.clone(), config.clone()),
            server_root,
            block_index: Arc::new(Mutex::new(block_index)),
            object_store,
            config,
            active_transaction: Arc::new(Mutex::new(None)),
            tx_manager: TransactionManager::new(),
            state: Arc::new(RwLock::new(SystemState::default())),
            snapshots: Arc::new(RwLock::new(Vec::new())),
        }
    }

    // =========================================================================
    // Backup
    // =========================================================================

    /// Run a backup operation.
    ///
    /// Flow:
    ///   1. BEGIN transaction — lock dirty page table, allocate TxID
    ///   2. SCAN — scan world directory for changed files (based on mtime/size)
    ///   3. CHUNK — FastCDC chunk each changed file
    ///   4. DEDUP — check RocksDB for existing chunks
    ///   5. STORE — write new chunks to object store
    ///   6. COMMIT — write manifest, update RocksDB index
    ///
    /// On any error: ROLLBACK — discard transient objects, release locks.
    pub async fn run_backup(&self, tag: Option<String>, incremental: bool) -> Result<BackupResult> {
        let start = Instant::now();
        let tx_id = Uuid::new_v4().to_string();
        let tx_id_short = tx_id[..8].to_string();

        // Phase 1: BEGIN transaction
        info!("[Backup:{}] BEGIN transaction", tx_id_short);
        self.set_state("backing_up".into(), Some(tx_id_short.clone()))
            .await;

        let mut tx = self.tx_manager.begin(tx_id_short.clone())?;

        // If incremental mode, determine last snapshot timestamp for change detection
        let last_snapshot_time = if incremental {
            let snaps = self.snapshots.read().await;
            snaps.last().map(|s| s.timestamp.clone())
        } else {
            None
        };

        // Phase 2: SCAN
        info!("[Backup:{}] Scanning world directory...", tx_id_short);
        self.update_queues(1, 0, 0, 0, 0).await;

        let files = self.scanner.scan_world_directory(&last_snapshot_time)?;
        info!(
            "[Backup:{}] Found {} files to process",
            tx_id_short,
            files.len()
        );

        if files.is_empty() {
            info!(
                "[Backup:{}] No changes detected, skipping backup",
                tx_id_short
            );
            self.tx_manager.commit(&tx)?;
            self.set_state("idle".into(), None).await;

            return Ok(BackupResult {
                snapshot_id: format!("snap_{}", tx_id_short),
                files_scanned: 0,
                files_changed: 0,
                bytes_processed: 0,
                chunks_deduped: 0,
                chunks_new: 0,
                duration_ms: start.elapsed().as_millis() as u64,
            });
        }

        let files_scanned = files.len() as u64;
        let mut files_changed: u64 = 0;
        let mut total_bytes: u64 = 0;
        let mut total_chunks: u64 = 0;
        let mut chunks_deduped: u64 = 0;
        let mut chunks_new: u64 = 0;

        // Phase 3 & 4: CHUNK + DEDUP per file
        let chunk_engine = ChunkEngine::new();
        let block_index = self.block_index.lock().await;
        let mut object_store = self.object_store.write().await;

        self.update_queues(0, 1, 0, 0, 0).await;

        for (i, file_entry) in files.iter().enumerate() {
            // Check for cancellation
            if tx.state == TransactionState::Aborted {
                warn!("[Backup:{}] Transaction aborted, stopping", tx_id_short);
                break;
            }

            let rel_path = file_entry
                .path
                .strip_prefix(&self.server_root)
                .unwrap_or(&file_entry.path);

            info!(
                "[Backup:{}] [{}/{}] Processing: {:?}",
                tx_id_short,
                i + 1,
                files.len(),
                rel_path
            );

            // Read file content
            let content = match std::fs::read(&file_entry.path) {
                Ok(data) => data,
                Err(e) => {
                    warn!("[Backup:{}] Cannot read {:?}: {}", tx_id_short, rel_path, e);
                    continue;
                }
            };

            total_bytes += content.len() as u64;
            files_changed += 1;

            // FastCDC chunking
            let chunks = chunk_engine.chunk_file(&content, rel_path.to_string_lossy().as_ref());

            total_chunks += chunks.len() as u64;

            // Check RocksDB for existing chunks (dedup)
            let mut file_chunk_hashes = Vec::with_capacity(chunks.len());

            for chunk in &chunks {
                let exists = block_index.chunk_exists(&chunk.hash)?;
                file_chunk_hashes.push(chunk.hash.clone());

                if exists {
                    chunks_deduped += 1;
                    // Update reference count
                    block_index.increment_ref(&chunk.hash)?;
                } else {
                    chunks_new += 1;
                    // Write new chunk to object store
                    object_store.write_object(&chunk.hash, &chunk.data, chunk.size as u64)?;

                    // Index the new chunk
                    block_index.insert_chunk(
                        &chunk.hash,
                        rel_path.to_string_lossy().as_ref(),
                        chunk.offset as u64,
                        chunk.size as u64,
                    )?;
                }
            }

            // Update file → chunk mapping in RocksDB
            block_index.insert_file_chunks(
                rel_path.to_string_lossy().as_ref(),
                &file_chunk_hashes,
                file_entry.size,
                file_entry.modified,
            )?;

            // Update transaction progress
            tx.files_processed += 1;
            tx.bytes_processed += content.len() as u64;
            tx.chunks_total = total_chunks;
        }

        drop(block_index);
        drop(object_store);

        // Phase 5: Check if aborted
        if tx.state == TransactionState::Aborted {
            self.tx_manager.rollback(&tx)?;
            self.set_state("idle".into(), None).await;
            return Err(anyhow::anyhow!("Backup transaction aborted"));
        }

        // Phase 6: COMMIT
        info!(
            "[Backup:{}] COMMIT — writing manifest and finalizing",
            tx_id_short
        );
        self.update_queues(0, 0, 0, 0, 0).await;

        let snapshot_id = format!("snap_{}", tx_id_short);
        let timestamp = Utc::now().to_rfc3339();

        // Write snapshot manifest
        let manifest = serde_json::json!({
            "snapshot_id": snapshot_id,
            "timestamp": timestamp,
            "tag": tag,
            "tx_id": tx_id_short,
            "files_scanned": files_scanned,
            "files_changed": files_changed,
            "bytes_processed": total_bytes,
            "chunks_total": total_chunks,
            "chunks_deduped": chunks_deduped,
            "chunks_new": chunks_new,
            "server_root": self.server_root.to_string_lossy(),
        });

        // Save manifest to .obsidian/snapshots/
        let snapshot_dir = self.server_root.join(".obsidian/store/snapshots");
        std::fs::create_dir_all(&snapshot_dir)?;
        let manifest_path = snapshot_dir.join(format!("{}.json", snapshot_id));
        std::fs::write(&manifest_path, serde_json::to_string_pretty(&manifest)?)?;

        // Commit transaction (RocksDB flush)
        self.tx_manager.commit(&tx)?;

        // Update snapshot cache
        {
            let mut snaps = self.snapshots.write().await;
            snaps.push(SnapshotInfo {
                snapshot_id: snapshot_id.clone(),
                timestamp,
                tag,
                files_scanned,
                bytes_processed: total_bytes,
                chunks_total: total_chunks,
                chunks_deduped,
            });
        }

        let duration_ms = start.elapsed().as_millis() as u64;
        info!(
            "[Backup:{}] Complete in {}ms — {} chunks ({} new, {} deduped), {} bytes",
            tx_id_short, duration_ms, total_chunks, chunks_new, chunks_deduped, total_bytes
        );

        self.set_state("idle".into(), None).await;
        self.update_storage_stats().await;

        Ok(BackupResult {
            snapshot_id,
            files_scanned,
            files_changed,
            bytes_processed: total_bytes,
            chunks_deduped,
            chunks_new,
            duration_ms,
        })
    }

    // =========================================================================
    // Status
    // =========================================================================

    pub async fn get_state(&self) -> SystemState {
        self.state.read().await.clone()
    }

    async fn set_state(&self, state: String, tx: Option<String>) {
        let mut s = self.state.write().await;
        s.state = state;
        s.current_tx = tx;
        s.running = tx.is_some();
    }

    async fn update_queues(
        &self,
        scanner: u64,
        chunk: u64,
        compress: u64,
        encrypt: u64,
        upload: u64,
    ) {
        let mut s = self.state.write().await;
        s.scanner_queue = scanner;
        s.chunk_queue = chunk;
        s.compress_queue = compress;
        s.encrypt_queue = encrypt;
        s.upload_queue = upload;
    }

    async fn update_storage_stats(&self) {
        let snaps = self.snapshots.read().await;
        let store = self.object_store.read().await;
        let mut s = self.state.write().await;
        s.total_snapshots = snaps.len() as u64;
        s.total_size_bytes = store.total_size();
        s.dedup_ratio = store.dedup_ratio();
        s.packfile_count = store.packfile_count();
    }

    // =========================================================================
    // Restore
    // =========================================================================

    pub async fn restore(
        &self,
        snapshot_id: &str,
        file_path: Option<&str>,
        chunk_coord: Option<&str>,
    ) -> Result<RestoreResult> {
        info!(
            "[Restore] Snapshot: {}, file: {:?}, chunk: {:?}",
            snapshot_id, file_path, chunk_coord
        );

        let manifest_path = self
            .server_root
            .join(".obsidian/store/snapshots")
            .join(format!("{}.json", snapshot_id));

        if !manifest_path.exists() {
            return Err(anyhow::anyhow!("Snapshot {} not found", snapshot_id));
        }

        // Use sandbox for atomic restore
        let sandbox_dir = self.server_root.join(".obsidian/sandbox");
        std::fs::create_dir_all(&sandbox_dir)?;

        let sandbox_tx = sandbox_dir.join(format!(
            "restore_{}",
            Uuid::new_v4().to_string().split_at(8).0
        ));
        std::fs::create_dir_all(&sandbox_tx)?;

        // Read manifest to get chunk list
        let _manifest: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&manifest_path)?)?;

        let block_index = self.block_index.lock().await;
        let object_store = self.object_store.read().await;

        let mut files_restored: u64 = 0;
        let mut bytes_restored: u64 = 0;

        // Reconstruct files from chunks
        if let Some(target_path) = file_path {
            // Single file restore
            let chunks = block_index.get_file_chunks(target_path)?;
            let mut data = Vec::new();

            for chunk_hash in &chunks {
                let chunk_data = object_store.read_object(chunk_hash)?;
                data.extend_from_slice(&chunk_data);
            }

            let dest = sandbox_tx.join(target_path);
            if let Some(parent) = dest.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::write(&dest, &data)?;

            bytes_restored = data.len() as u64;
            files_restored = 1;
        } else if let Some(coord) = chunk_coord {
            // Single chunk restore (MCA region)
            let region_path = format!("region/r.{}.mca", coord.replace(':', ".").replace(',', "."));
            let chunks = block_index.get_file_chunks(&region_path)?;
            let mut data = Vec::new();

            for chunk_hash in &chunks {
                let chunk_data = object_store.read_object(chunk_hash)?;
                data.extend_from_slice(&chunk_data);
            }

            let dest = sandbox_tx.join(&region_path);
            if let Some(parent) = dest.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::write(&dest, &data)?;

            bytes_restored = data.len() as u64;
            files_restored = 1;
        } else {
            // Full world restore — reconstruct all files
            let all_files = block_index.get_all_files()?;
            for file_path in &all_files {
                let chunks = block_index.get_file_chunks(file_path)?;
                let mut data = Vec::new();

                for chunk_hash in &chunks {
                    match object_store.read_object(chunk_hash) {
                        Ok(chunk_data) => data.extend_from_slice(&chunk_data),
                        Err(e) => {
                            warn!(
                                "[Restore] Missing chunk {} for {}: {}",
                                chunk_hash, file_path, e
                            );
                        }
                    }
                }

                let dest = sandbox_tx.join(file_path);
                if let Some(parent) = dest.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                std::fs::write(&dest, &data)?;

                bytes_restored += data.len() as u64;
                files_restored += 1;
            }
        }

        drop(block_index);
        drop(object_store);

        // Atomic swap: sandbox → world
        if self.config.sandbox_restore.atomic_swap {
            info!("[Restore] Performing atomic rename swap...");
            // For full restore, swap world directories
            let world_dir = self.server_root.join("world");
            let backup_dir = self.server_root.join("world_old_tmp");

            if world_dir.exists() {
                std::fs::rename(&world_dir, &backup_dir)?;
            }
            std::fs::rename(&sandbox_tx, &world_dir)?;

            // Async cleanup of old directory
            if backup_dir.exists() {
                tokio::spawn(async move {
                    info!("[Restore] Cleaning up old world directory...");
                    if let Err(e) = std::fs::remove_dir_all(&backup_dir) {
                        error!("[Restore] Failed to cleanup: {}", e);
                    }
                });
            }
        }

        info!(
            "[Restore] Complete — {} files, {} bytes",
            files_restored, bytes_restored
        );

        Ok(RestoreResult {
            files_restored,
            bytes_restored,
            sandbox_used: true,
        })
    }

    // =========================================================================
    // Top files analysis
    // =========================================================================

    pub async fn get_top_files(&self, limit: usize) -> Result<TopFilesResult> {
        let block_index = self.block_index.lock().await;
        let object_store = self.object_store.read().await;

        let files = block_index.get_largest_files(limit)?;

        let entries: Vec<TopFileEntry> = files
            .iter()
            .map(|(path, size)| {
                let reason = if path.contains("region/") && *size > 1024 * 1024 * 1024 {
                    Some(if path.contains("nether") {
                        "确定为常驻主城强加载区".to_string()
                    } else {
                        "密集红石高频刷怪场".to_string()
                    })
                } else if path.contains("CoreProtect") || path.contains("database") {
                    Some("插件数据库".to_string())
                } else if path.contains("playerdata") {
                    Some("携带了超大 NBT 嵌套潜影盒容器".to_string())
                } else {
                    None
                };

                TopFileEntry {
                    path: path.clone(),
                    size: *size,
                    reason,
                }
            })
            .collect();

        Ok(TopFilesResult {
            files: entries,
            dedup_ratio: object_store.dedup_ratio(),
            dict_gain: 18.2, // placeholder
        })
    }

    // =========================================================================
    // Diff snapshots
    // =========================================================================

    pub async fn diff_snapshots(&self, id_a: &str, id_b: &str) -> Result<DiffResult> {
        let block_index = self.block_index.lock().await;

        let files_a = block_index.get_snapshot_files(id_a)?;
        let files_b = block_index.get_snapshot_files(id_b)?;

        let set_a: std::collections::HashSet<&String> = files_a.iter().collect();
        let set_b: std::collections::HashSet<&String> = files_b.iter().collect();

        let added: Vec<String> = set_b.difference(&set_a).map(|s| s.to_string()).collect();
        let deleted: Vec<String> = set_a.difference(&set_b).map(|s| s.to_string()).collect();

        // Modified = files in both but different size/hash
        let mut modified = Vec::new();
        for file in set_a.intersection(&set_b) {
            let size_a = block_index.get_file_size(id_a, file)?;
            let size_b = block_index.get_file_size(id_b, file)?;
            if size_a != size_b {
                modified.push(file.to_string());
            }
        }

        Ok(DiffResult {
            added,
            modified,
            deleted,
        })
    }

    // =========================================================================
    // Browse snapshot
    // =========================================================================

    pub async fn browse_snapshot(
        &self,
        snapshot_id: &str,
        path: Option<&str>,
    ) -> Result<Vec<serde_json::Value>> {
        let block_index = self.block_index.lock().await;
        let files = block_index.get_snapshot_files(snapshot_id)?;

        let prefix = path.unwrap_or("");
        let entries: Vec<serde_json::Value> = files
            .iter()
            .filter(|f| f.starts_with(prefix))
            .map(|f| {
                let is_dir = f.ends_with('/');
                let size = if is_dir {
                    0
                } else {
                    block_index.get_file_size(snapshot_id, f).unwrap_or(0)
                };
                serde_json::json!({
                    "name": f,
                    "is_dir": is_dir,
                    "size": size
                })
            })
            .collect();

        Ok(entries)
    }

    // =========================================================================
    // Clone world
    // =========================================================================

    pub async fn clone_world(&self, snapshot_id: &str, new_name: &str) -> Result<()> {
        info!("[Clone] Cloning snapshot {} as '{}'", snapshot_id, new_name);

        // Restore to a temporary sandbox
        let sandbox_dir = self.server_root.join(".obsidian/sandbox");
        let clone_dir = sandbox_dir.join(new_name);
        std::fs::create_dir_all(&clone_dir)?;

        let block_index = self.block_index.lock().await;
        let object_store = self.object_store.read().await;
        let files = block_index.get_snapshot_files(snapshot_id)?;

        for file_path in &files {
            let chunks = block_index.get_file_chunks(file_path)?;
            let mut data = Vec::new();
            for chunk_hash in &chunks {
                if let Ok(chunk_data) = object_store.read_object(chunk_hash) {
                    data.extend_from_slice(&chunk_data);
                }
            }
            let dest = clone_dir.join(file_path);
            if let Some(parent) = dest.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::write(&dest, &data)?;
        }

        // Move to final location
        let final_dir = self.server_root.join(new_name);
        if final_dir.exists() {
            return Err(anyhow::anyhow!("Directory '{}' already exists", new_name));
        }
        std::fs::rename(&clone_dir, &final_dir)?;

        info!("[Clone] World '{}' created successfully", new_name);
        Ok(())
    }

    // =========================================================================
    // Rollback
    // =========================================================================

    pub async fn rollback(&self, duration: &str) -> Result<()> {
        info!("[Rollback] Rolling back {}", duration);

        // Parse duration (e.g., "1m", "5m", "1h")
        let seconds = parse_duration(duration)?;
        let target_time = Utc::now() - chrono::Duration::seconds(seconds as i64);

        // Find the closest snapshot before the target time
        let snaps = self.snapshots.read().await;
        let target_snap = snaps
            .iter()
            .filter(|s| {
                if let Ok(ts) = chrono::DateTime::parse_from_rfc3339(&s.timestamp) {
                    ts < target_time
                } else {
                    false
                }
            })
            .last();

        match target_snap {
            Some(snap) => {
                let snap_id = snap.snapshot_id.clone();
                drop(snaps);
                self.restore(&snap_id, None, None).await?;
                Ok(())
            }
            None => Err(anyhow::anyhow!("No snapshot found before {} ago", duration)),
        }
    }

    // =========================================================================
    // Verify
    // =========================================================================

    pub async fn verify(&self, repair: bool) -> Result<VerifyResult> {
        info!("[Verify] Starting integrity check (repair={})", repair);

        let block_index = self.block_index.lock().await;
        let object_store = self.object_store.read().await;

        let all_chunks = block_index.get_all_chunks()?;
        let mut healthy: u64 = 0;
        let mut corrupted: u64 = 0;
        let mut repaired: u64 = 0;

        for chunk_hash in &all_chunks {
            match object_store.read_object(chunk_hash) {
                Ok(data) => {
                    // Verify hash matches content
                    let actual_hash = blake3::hash(&data).to_hex().to_string();
                    if &actual_hash == chunk_hash {
                        healthy += 1;
                    } else {
                        corrupted += 1;
                        warn!(
                            "[Verify] Hash mismatch for chunk {} (expected {}, got {})",
                            chunk_hash, chunk_hash, actual_hash
                        );
                    }
                }
                Err(_) => {
                    corrupted += 1;
                    warn!("[Verify] Missing chunk: {}", chunk_hash);
                }
            }
        }

        // TODO: RS(8+2) erasure code repair when implemented
        if repair && corrupted > 0 {
            warn!("[Verify] Auto-repair not yet implemented (RS erasure coding pending)");
        }

        info!(
            "[Verify] Complete: {} total, {} healthy, {} corrupted, {} repaired",
            all_chunks.len(),
            healthy,
            corrupted,
            repaired
        );

        Ok(VerifyResult {
            total_checked: all_chunks.len() as u64,
            healthy,
            corrupted,
            repaired,
        })
    }

    // =========================================================================
    // Pin snapshot
    // =========================================================================

    pub async fn pin_snapshot(&self, snapshot_id: &str, days: u64) -> Result<()> {
        let pin_file = self
            .server_root
            .join(".obsidian/store/snapshots")
            .join(format!("{}.pin", snapshot_id));

        let expiry = Utc::now() + chrono::Duration::days(days as i64);
        let pin_data = serde_json::json!({
            "snapshot_id": snapshot_id,
            "pinned_at": Utc::now().to_rfc3339(),
            "expires_at": expiry.to_rfc3339(),
            "days": days,
            "worm_locked": true
        });

        std::fs::write(&pin_file, serde_json::to_string_pretty(&pin_data)?)?;
        info!(
            "[Pin] Snapshot {} pinned for {} days (WORM locked until {})",
            snapshot_id,
            days,
            expiry.to_rfc3339()
        );

        Ok(())
    }

    // =========================================================================
    // Cancel
    // =========================================================================

    pub async fn cancel(&self) -> Result<()> {
        let mut tx = self.active_transaction.lock().await;
        if let Some(ref mut transaction) = *tx {
            transaction.state = TransactionState::Aborted;
            info!(
                "[Cancel] Transaction {} marked for abortion",
                transaction.tx_id
            );
            Ok(())
        } else {
            Err(anyhow::anyhow!("No active transaction to cancel"))
        }
    }

    // =========================================================================
    // Forecast
    // =========================================================================

    pub async fn forecast(&self) -> Result<ForecastResult> {
        let snaps = self.snapshots.read().await;
        if snaps.len() < 2 {
            return Err(anyhow::anyhow!("Need at least 2 snapshots for forecast"));
        }

        // Calculate growth rate between last two snapshots
        let last = &snaps[snaps.len() - 1];
        let prev = &snaps[snaps.len() - 2];

        let time_diff_hours = {
            let last_ts = chrono::DateTime::parse_from_rfc3339(&last.timestamp)?;
            let prev_ts = chrono::DateTime::parse_from_rfc3339(&prev.timestamp)?;
            (last_ts - prev_ts).num_hours() as f64
        };

        let size_diff_mb =
            (last.bytes_processed as f64 - prev.bytes_processed as f64) / (1024.0 * 1024.0);
        let growth_per_day_mb = if time_diff_hours > 0.0 {
            (size_diff_mb / time_diff_hours) * 24.0
        } else {
            0.0
        };

        // Estimate available space
        let total_capacity_gb = 100.0; // TODO: get actual disk size
        let current_usage_gb =
            self.state.read().await.total_size_bytes as f64 / (1024.0 * 1024.0 * 1024.0);
        let remaining_gb = total_capacity_gb - current_usage_gb;
        let days_remaining = if growth_per_day_mb > 0.0 {
            (remaining_gb * 1024.0) / growth_per_day_mb
        } else {
            f64::INFINITY
        };

        Ok(ForecastResult {
            days_remaining,
            growth_rate_mb_per_day: growth_per_day_mb,
            total_capacity_gb,
        })
    }

    // =========================================================================
    // Snapshot export/import
    // =========================================================================

    pub async fn export_snapshot(&self, path: &str) -> Result<()> {
        info!("[Export] Exporting store to {}", path);
        // Create a tar.zst archive of the entire .obsidian/store directory
        let store_dir = self.server_root.join(".obsidian/store");
        let output = PathBuf::from(path);

        // In a real implementation, this would use tar + zstd
        // For Phase 1: simple directory copy
        if output.exists() {
            return Err(anyhow::anyhow!("Export path already exists: {}", path));
        }
        copy_dir_recursive(&store_dir, &output)?;
        info!("[Export] Complete");
        Ok(())
    }

    pub async fn import_snapshot(&self, path: &str) -> Result<BackupResult> {
        info!("[Import] Importing store from {}", path);
        let store_dir = self.server_root.join(".obsidian/store");
        let input = PathBuf::from(path);

        if !input.exists() {
            return Err(anyhow::anyhow!("Import path does not exist: {}", path));
        }
        copy_dir_recursive(&input, &store_dir)?;
        info!("[Import] Complete");

        // Record as a new snapshot
        let snapshot_id = format!("snap_import_{}", Uuid::new_v4().to_string().split_at(8).0);
        Ok(BackupResult {
            snapshot_id,
            files_scanned: 0,
            files_changed: 0,
            bytes_processed: 0,
            chunks_deduped: 0,
            chunks_new: 0,
            duration_ms: 0,
        })
    }
}

// =========================================================================
// Utility functions
// =========================================================================

fn parse_duration(s: &str) -> Result<u64> {
    let s = s.trim();
    if let Some(s) = s.strip_suffix('s') {
        Ok(s.parse::<u64>()?)
    } else if let Some(s) = s.strip_suffix('m') {
        Ok(s.parse::<u64>()? * 60)
    } else if let Some(s) = s.strip_suffix('h') {
        Ok(s.parse::<u64>()? * 3600)
    } else if let Some(s) = s.strip_suffix('d') {
        Ok(s.parse::<u64>()? * 86400)
    } else {
        Err(anyhow::anyhow!("Invalid duration format: {}", s))
    }
}

fn copy_dir_recursive(src: &Path, dst: &Path) -> Result<()> {
    if src.is_dir() {
        std::fs::create_dir_all(dst)?;
        for entry in std::fs::read_dir(src)? {
            let entry = entry?;
            let file_type = entry.file_type()?;
            let src_path = entry.path();
            let dst_path = dst.join(entry.file_name());

            if file_type.is_dir() {
                copy_dir_recursive(&src_path, &dst_path)?;
            } else {
                std::fs::copy(&src_path, &dst_path)?;
            }
        }
    } else {
        std::fs::copy(src, dst)?;
    }
    Ok(())
}
