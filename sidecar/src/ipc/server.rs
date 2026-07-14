//! Unix Domain Socket IPC server.
//!
//! Listens on a UDS socket for JSON-encoded requests from the Minecraft mod
//! and dispatches them to the backup engine. Supports:
//!   - Request/response pattern with transaction IDs
//!   - Progress streaming for long-running operations
//!   - Concurrent connections from mod + CLI client

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tracing::{debug, error, info, warn};

use crate::backup::BackupEngine;
use crate::config::SidecarConfig;

/// IPC message handler dispatching requests to the backup engine.
pub struct IpcServer {
    socket_path: PathBuf,
    engine: Arc<BackupEngine>,
    config: SidecarConfig,
}

impl IpcServer {
    pub fn new(socket_path: PathBuf, engine: Arc<BackupEngine>, config: SidecarConfig) -> Self {
        Self {
            socket_path,
            engine,
            config,
        }
    }

    /// Run the IPC server loop. Blocks until the listener is closed.
    pub async fn run(&self) -> Result<()> {
        let listener = UnixListener::bind(&self.socket_path)?;
        info!("[IPC] Listening on {:?}", self.socket_path);

        loop {
            match listener.accept().await {
                Ok((stream, peer_addr)) => {
                    debug!("[IPC] New connection from {:?}", peer_addr);
                    let engine = self.engine.clone();
                    let config = self.config.clone();
                    tokio::spawn(async move {
                        if let Err(e) = handle_connection(stream, engine, config).await {
                            error!("[IPC] Connection error: {}", e);
                        }
                    });
                }
                Err(e) => {
                    error!("[IPC] Accept error: {}", e);
                    break;
                }
            }
        }

        Ok(())
    }
}

/// Handle a single IPC connection.
async fn handle_connection(
    stream: UnixStream,
    engine: Arc<BackupEngine>,
    _config: SidecarConfig,
) -> Result<()> {
    let (reader, mut writer) = stream.into_split();
    let mut lines = BufReader::new(reader).lines();

    while let Ok(Some(line)) = lines.next_line().await {
        if line.trim().is_empty() {
            continue;
        }

        debug!("[IPC] Received: {}", &line[..line.len().min(200)]);

        let response = match serde_json::from_str::<Value>(&line) {
            Ok(request) => dispatch(&engine, request).await,
            Err(e) => {
                json!({
                    "tx_id": null,
                    "status": "error",
                    "message": format!("Invalid JSON: {}", e)
                })
            }
        };

        let response_str = serde_json::to_string(&response)?;
        writer.write_all(response_str.as_bytes()).await?;
        writer.write_all(b"\n").await?;
        writer.flush().await?;
    }

    debug!("[IPC] Connection closed");
    Ok(())
}

/// Dispatch an IPC request to the appropriate handler based on the "op" field.
async fn dispatch(engine: &Arc<BackupEngine>, request: Value) -> Value {
    let tx_id = request
        .get("tx_id")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");
    let op = match request.get("op").and_then(|v| v.as_str()) {
        Some(op) => op,
        None => {
            return json!({
                "tx_id": tx_id,
                "status": "error",
                "message": "Missing 'op' field in request"
            })
        }
    };

    let params = request.get("params").cloned().unwrap_or(json!({}));

    match op {
        "backup" => handle_backup(engine, tx_id, params).await,
        "status" => handle_status(engine, tx_id).await,
        "restore" => handle_restore(engine, tx_id, params).await,
        "top" => handle_top(engine, tx_id, params).await,
        "diff" => handle_diff(engine, tx_id, params).await,
        "browse" => handle_browse(engine, tx_id, params).await,
        "clone" => handle_clone(engine, tx_id, params).await,
        "rollback" => handle_rollback(engine, tx_id, params).await,
        "verify" => handle_verify(engine, tx_id, params).await,
        "pin" => handle_pin(engine, tx_id, params).await,
        "cancel" => handle_cancel(engine, tx_id).await,
        "forecast" => handle_forecast(engine, tx_id).await,
        _ => json!({
            "tx_id": tx_id,
            "status": "error",
            "message": format!("Unknown operation: {}", op)
        }),
    }
}

async fn handle_backup(engine: &Arc<BackupEngine>, tx_id: &str, params: Value) -> Value {
    let tag = params.get("tag").and_then(|v| v.as_str()).map(String::from);
    let incremental = params
        .get("incremental")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);

    // Check if there's an export/import path
    if let Some(export_path) = params.get("export_path").and_then(|v| v.as_str()) {
        match engine.export_snapshot(export_path).await {
            Ok(_) => json!({
                "tx_id": tx_id,
                "status": "ok",
                "data": { "export_path": export_path }
            }),
            Err(e) => json!({
                "tx_id": tx_id,
                "status": "error",
                "message": format!("Export failed: {}", e)
            }),
        }
    } else if let Some(import_path) = params.get("import_path").and_then(|v| v.as_str()) {
        match engine.import_snapshot(import_path).await {
            Ok(snapshot) => json!({
                "tx_id": tx_id,
                "status": "ok",
                "data": {
                    "snapshot_id": snapshot.snapshot_id,
                    "files_scanned": snapshot.files_scanned,
                    "bytes_processed": snapshot.bytes_processed
                }
            }),
            Err(e) => json!({
                "tx_id": tx_id,
                "status": "error",
                "message": format!("Import failed: {}", e)
            }),
        }
    } else {
        match engine.run_backup(tag, incremental).await {
            Ok(snapshot) => json!({
                "tx_id": tx_id,
                "status": "ok",
                "data": {
                    "snapshot_id": snapshot.snapshot_id,
                    "files_scanned": snapshot.files_scanned,
                    "files_changed": snapshot.files_changed,
                    "bytes_processed": snapshot.bytes_processed,
                    "chunks_deduped": snapshot.chunks_deduped,
                    "chunks_new": snapshot.chunks_new,
                    "duration_ms": snapshot.duration_ms
                }
            }),
            Err(e) => json!({
                "tx_id": tx_id,
                "status": "error",
                "message": format!("Backup failed: {}", e)
            }),
        }
    }
}

async fn handle_status(engine: &Arc<BackupEngine>, tx_id: &str) -> Value {
    let state = engine.get_state().await;
    json!({
        "tx_id": tx_id,
        "status": "ok",
        "data": {
            "running": state.running,
            "current_tx": state.current_tx,
            "state": state.state,
            "tps": state.tps,
            "cpu_percent": state.cpu_percent,
            "memory_mb": state.memory_mb,
            "disk_iops_read": state.disk_iops_read,
            "disk_iops_write": state.disk_iops_write,
            "network_upload_mbps": state.network_upload_mbps,
            "queue_status": {
                "scanner": state.scanner_queue,
                "chunk": state.chunk_queue,
                "compress": state.compress_queue,
                "encrypt": state.encrypt_queue,
                "upload": state.upload_queue
            },
            "storage_stats": {
                "total_snapshots": state.total_snapshots,
                "total_size_bytes": state.total_size_bytes,
                "dedup_ratio": state.dedup_ratio,
                "packfile_count": state.packfile_count
            }
        }
    })
}

async fn handle_restore(engine: &Arc<BackupEngine>, tx_id: &str, params: Value) -> Value {
    let snapshot_id = params
        .get("snapshot_id")
        .and_then(|v| v.as_str())
        .unwrap_or("latest");
    let file_path = params.get("file_path").and_then(|v| v.as_str());
    let chunk_coord = params.get("chunk_coord").and_then(|v| v.as_str());

    match engine.restore(snapshot_id, file_path, chunk_coord).await {
        Ok(result) => json!({
            "tx_id": tx_id,
            "status": "ok",
            "data": {
                "snapshot_id": snapshot_id,
                "files_restored": result.files_restored,
                "bytes_restored": result.bytes_restored,
                "sandbox_used": result.sandbox_used
            }
        }),
        Err(e) => json!({
            "tx_id": tx_id,
            "status": "error",
            "message": format!("Restore failed: {}", e)
        }),
    }
}

async fn handle_top(engine: &Arc<BackupEngine>, tx_id: &str, params: Value) -> Value {
    let limit = params.get("limit").and_then(|v| v.as_u64()).unwrap_or(5) as usize;

    match engine.get_top_files(limit).await {
        Ok(result) => {
            let files: Vec<Value> = result
                .files
                .iter()
                .map(|f| {
                    json!({
                        "path": f.path,
                        "size": f.size,
                        "reason": f.reason
                    })
                })
                .collect();

            json!({
                "tx_id": tx_id,
                "status": "ok",
                "data": {
                    "files": files,
                    "dedup_ratio": result.dedup_ratio,
                    "dict_gain": result.dict_gain
                }
            })
        }
        Err(e) => json!({
            "tx_id": tx_id,
            "status": "error",
            "message": format!("Top analysis failed: {}", e)
        }),
    }
}

async fn handle_diff(engine: &Arc<BackupEngine>, tx_id: &str, params: Value) -> Value {
    let id_a = params.get("id_a").and_then(|v| v.as_str()).unwrap_or("");
    let id_b = params.get("id_b").and_then(|v| v.as_str()).unwrap_or("");

    match engine.diff_snapshots(id_a, id_b).await {
        Ok(diff) => json!({
            "tx_id": tx_id,
            "status": "ok",
            "data": {
                "added": diff.added,
                "modified": diff.modified,
                "deleted": diff.deleted
            }
        }),
        Err(e) => json!({
            "tx_id": tx_id,
            "status": "error",
            "message": format!("Diff failed: {}", e)
        }),
    }
}

async fn handle_browse(engine: &Arc<BackupEngine>, tx_id: &str, params: Value) -> Value {
    let snapshot_id = params
        .get("snapshot_id")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let path = params.get("path").and_then(|v| v.as_str());

    match engine.browse_snapshot(snapshot_id, path).await {
        Ok(entries) => json!({
            "tx_id": tx_id,
            "status": "ok",
            "data": {
                "snapshot_id": snapshot_id,
                "entries": entries
            }
        }),
        Err(e) => json!({
            "tx_id": tx_id,
            "status": "error",
            "message": format!("Browse failed: {}", e)
        }),
    }
}

async fn handle_clone(engine: &Arc<BackupEngine>, tx_id: &str, params: Value) -> Value {
    let snapshot_id = params
        .get("snapshot_id")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let new_name = params
        .get("new_name")
        .and_then(|v| v.as_str())
        .unwrap_or("clone");

    match engine.clone_world(snapshot_id, new_name).await {
        Ok(_) => json!({
            "tx_id": tx_id,
            "status": "ok",
            "message": format!("World cloned as '{}'", new_name)
        }),
        Err(e) => json!({
            "tx_id": tx_id,
            "status": "error",
            "message": format!("Clone failed: {}", e)
        }),
    }
}

async fn handle_rollback(engine: &Arc<BackupEngine>, tx_id: &str, params: Value) -> Value {
    let duration = params
        .get("duration")
        .and_then(|v| v.as_str())
        .unwrap_or("1m");

    match engine.rollback(duration).await {
        Ok(_) => json!({
            "tx_id": tx_id,
            "status": "ok",
            "message": format!("Rolled back {}", duration)
        }),
        Err(e) => json!({
            "tx_id": tx_id,
            "status": "error",
            "message": format!("Rollback failed: {}", e)
        }),
    }
}

async fn handle_verify(engine: &Arc<BackupEngine>, tx_id: &str, params: Value) -> Value {
    let repair = params
        .get("repair")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    match engine.verify(repair).await {
        Ok(result) => json!({
            "tx_id": tx_id,
            "status": "ok",
            "data": {
                "total_checked": result.total_checked,
                "healthy": result.healthy,
                "corrupted": result.corrupted,
                "repaired": result.repaired
            }
        }),
        Err(e) => json!({
            "tx_id": tx_id,
            "status": "error",
            "message": format!("Verify failed: {}", e)
        }),
    }
}

async fn handle_pin(engine: &Arc<BackupEngine>, tx_id: &str, params: Value) -> Value {
    let snapshot_id = params
        .get("snapshot_id")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let days = params.get("days").and_then(|v| v.as_u64()).unwrap_or(30);

    match engine.pin_snapshot(snapshot_id, days).await {
        Ok(_) => json!({
            "tx_id": tx_id,
            "status": "ok",
            "message": format!("Snapshot {} pinned for {} days", snapshot_id, days)
        }),
        Err(e) => json!({
            "tx_id": tx_id,
            "status": "error",
            "message": format!("Pin failed: {}", e)
        }),
    }
}

async fn handle_cancel(engine: &Arc<BackupEngine>, tx_id: &str) -> Value {
    match engine.cancel().await {
        Ok(_) => json!({
            "tx_id": tx_id,
            "status": "ok",
            "message": "Transaction cancelled and rolled back"
        }),
        Err(e) => json!({
            "tx_id": tx_id,
            "status": "error",
            "message": format!("Cancel failed: {}", e)
        }),
    }
}

async fn handle_forecast(engine: &Arc<BackupEngine>, tx_id: &str) -> Value {
    match engine.forecast().await {
        Ok(forecast) => json!({
            "tx_id": tx_id,
            "status": "ok",
            "data": {
                "days_remaining": forecast.days_remaining,
                "growth_rate_mb_per_day": forecast.growth_rate_mb_per_day,
                "total_capacity_gb": forecast.total_capacity_gb
            }
        }),
        Err(e) => json!({
            "tx_id": tx_id,
            "status": "error",
            "message": format!("Forecast failed: {}", e)
        }),
    }
}

// Re-export
pub use server::*;
