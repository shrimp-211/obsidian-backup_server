//! Obsidian Backup Sidecar Daemon
//!
//! Enterprise-grade CAS (Content-Addressable Storage) incremental backup engine
//! for Minecraft servers. Runs as an independent process communicating with the
//! game server mod/plugin via Unix Domain Socket (UDS) IPC.
//!
//! Architecture:
//!   IPC Server (UDS) → Core Controller → Backup Engine → Storage Layer
//!                                                    → RocksDB Index
//!
//! Key features:
//!   - FastCDC content-defined chunking for deduplication
//!   - RocksDB-based block index with LSM checkpoints
//!   - ACID backup transactions (BEGIN/COMMIT/ROLLBACK)
//!   - CAS object store with Git-style Packfiles
//!   - XChaCha20-Poly1305 encryption (optional)
//!   - Ed25519 manifest signing
//!   - Adaptive worker pool throttling

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use clap::Parser;
use tokio::sync::RwLock;
use tracing::{error, info};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

use crate::backup::BackupEngine;
use crate::config::SidecarConfig;
use crate::ipc::IpcServer;
use crate::storage::index::BlockIndex;
use crate::storage::object_store::ObjectStore;

/// Obsidian Backup Sidecar — independent backup daemon for Minecraft servers.
#[derive(Parser, Debug)]
#[command(name = "obsidian-sidecar")]
#[command(version = "0.1.0")]
#[command(about = "Enterprise CAS incremental backup engine")]
struct Args {
    /// Path to the configuration file
    #[arg(short, long, default_value = ".obsidian/config/obsidian.yml")]
    config: PathBuf,

    /// Path to the UDS socket file
    #[arg(short, long, default_value = ".obsidian/ipc/obsidian.sock")]
    socket: PathBuf,

    /// Server root directory (Minecraft server root)
    #[arg(short, long, default_value = ".")]
    server_root: PathBuf,

    /// Log level
    #[arg(short, long, default_value = "info")]
    log_level: String,

    /// Run a single backup and exit (non-daemon mode)
    #[arg(long)]
    oneshot: bool,

    /// Tag for oneshot backup
    #[arg(long)]
    tag: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing/logging
    tracing_subscriber::registry()
        .with(fmt::layer().with_target(true))
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    let args = Args::parse();

    info!("[Obsidian Sidecar] Starting v{}", env!("CARGO_PKG_VERSION"));
    info!("[Obsidian Sidecar] Config: {:?}", args.config);
    info!("[Obsidian Sidecar] Socket: {:?}", args.socket);
    info!("[Obsidian Sidecar] Server root: {:?}", args.server_root);

    // Load configuration
    let config = SidecarConfig::load(&args.config)?;

    // Initialize storage layer
    let store_root = args.server_root.join(".obsidian/store");
    std::fs::create_dir_all(&store_root)?;

    let rocksdb_root = args.server_root.join(".obsidian/rocksdb");
    std::fs::create_dir_all(&rocksdb_root)?;

    info!("[Storage] Initializing RocksDB index at {:?}", rocksdb_root);
    let block_index = BlockIndex::open(&rocksdb_root)?;

    info!(
        "[Storage] Initializing CAS object store at {:?}",
        store_root
    );
    let object_store = Arc::new(RwLock::new(ObjectStore::new(
        &store_root,
        config.storage.clone(),
    )?));

    // Initialize backup engine
    let engine = Arc::new(BackupEngine::new(
        args.server_root.clone(),
        block_index,
        object_store,
        config.clone(),
    ));

    // Load existing snapshot metadata from disk
    engine.load_snapshots().await?;

    // Oneshot mode: run a single backup and exit
    if args.oneshot {
        info!("[Oneshot] Running single backup...");
        let result = engine
            .run_backup(
                args.tag.clone(),
                true, // incremental
            )
            .await;

        match result {
            Ok(snapshot) => {
                info!(
                    "[Oneshot] Backup complete: snapshot_id={}, files={}, bytes={}",
                    snapshot.snapshot_id, snapshot.files_scanned, snapshot.bytes_processed
                );
            }
            Err(e) => {
                error!("[Oneshot] Backup failed: {}", e);
                return Err(e);
            }
        }
        return Ok(());
    }

    // Daemon mode: start IPC server and listen for commands
    // Clean up any existing socket file
    let socket_path = args.socket.clone();
    if socket_path.exists() {
        info!("[IPC] Removing stale socket file: {:?}", socket_path);
        std::fs::remove_file(&socket_path)?;
    }

    // Ensure parent directory exists
    if let Some(parent) = socket_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    info!("[IPC] Starting UDS server on {:?}", socket_path);
    let ipc_server = IpcServer::new(socket_path, engine, config);

    // Run IPC server (blocks until shutdown)
    ipc_server.run().await?;

    info!("[Obsidian Sidecar] Shutdown complete");
    Ok(())
}
