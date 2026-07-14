//! Obsidian Backup CLI — Standalone management client.
//!
//! Connects to the Obsidian Sidecar daemon via Unix Domain Socket (UDS)
//! and provides a command-line interface for:
//!   - Initiating backups
//!   - Checking status
//!   - Restoring snapshots
//!   - Storage analysis
//!   - Snapshot management
//!
//! Usage:
//!   obsidian backup [--tag <tag>] [--full]
//!   obsidian status
//!   obsidian restore <snapshot_id> [--file <path>] [--chunk <coord>]
//!   obsidian top [--limit <n>]
//!   obsidian diff <id_a> <id_b>
//!   obsidian browse <snapshot_id> [path]
//!   obsidian verify [--repair]
//!   obsidian pin <snapshot_id> --days <n>
//!   obsidian forecast

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use colored::Colorize;
use indicatif::{ProgressBar, ProgressStyle};
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tracing::{debug, info};
use uuid::Uuid;

/// Obsidian Backup CLI — Enterprise Minecraft backup management.
#[derive(Parser, Debug)]
#[command(name = "obsidian")]
#[command(version = "0.1.0")]
#[command(about = "Obsidian Backup CLI client")]
struct Cli {
    /// Path to the Sidecar UDS socket
    #[arg(short, long, default_value = ".obsidian/ipc/obsidian.sock")]
    socket: PathBuf,

    /// Timeout for requests in seconds
    #[arg(short, long, default_value = "30")]
    timeout: u64,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Run a backup operation
    Backup {
        /// Optional tag for the backup
        #[arg(long)]
        tag: Option<String>,
        /// Perform a full backup (skip incremental)
        #[arg(long)]
        full: bool,
    },

    /// Show Sidecar status and queue health
    Status,

    /// Restore from a snapshot
    Restore {
        /// Snapshot ID or "latest"
        snapshot_id: String,
        /// Restore a single file
        #[arg(long)]
        file: Option<String>,
        /// Restore a single chunk region
        #[arg(long)]
        chunk: Option<String>,
    },

    /// Show top files by storage size
    Top {
        /// Number of files to show
        #[arg(long, default_value = "5")]
        limit: usize,
    },

    /// Diff two snapshots
    Diff {
        /// First snapshot ID
        id_a: String,
        /// Second snapshot ID
        id_b: String,
    },

    /// Browse files in a snapshot
    Browse {
        /// Snapshot ID
        snapshot_id: String,
        /// Optional path within the snapshot
        path: Option<String>,
    },

    /// Clone a world from a snapshot
    Clone {
        /// Source snapshot ID
        snapshot_id: String,
        /// New world name
        new_name: String,
    },

    /// Rollback to a point in time
    Rollback {
        /// Duration to roll back (e.g., "5m", "1h")
        #[arg(long)]
        duration: String,
    },

    /// Verify snapshot integrity
    Verify {
        /// Try to repair corrupted snapshots using erasure coding
        #[arg(long)]
        repair: bool,
    },

    /// Pin a snapshot (WORM lock)
    Pin {
        /// Snapshot ID
        snapshot_id: String,
        /// Number of days to lock
        #[arg(long)]
        days: u64,
    },

    /// Forecast storage capacity
    Forecast,

    /// Cancel the running backup transaction
    Cancel,

    /// Export snapshot archive
    Export {
        /// Output path for .tar.zst archive
        path: String,
    },

    /// Import snapshot archive
    Import {
        /// Path to .tar.zst archive
        path: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env()
            .add_directive("obsidian=info".parse()?))
        .init();

    let cli = Cli::parse();

    // Connect to Sidecar
    let stream = UnixStream::connect(&cli.socket).await
        .with_context(|| format!("Cannot connect to Sidecar at {:?}. Is the daemon running?",
            cli.socket))?;

    let (reader, mut writer) = stream.into_split();
    let mut lines = BufReader::new(reader).lines();

    // Build request
    let (op, params) = build_request(&cli.command)?;
    let tx_id = Uuid::new_v4().to_string()[..8].to_string();

    let request = json!({
        "tx_id": tx_id,
        "op": op,
        "params": params
    });

    // Show spinner for long operations
    let pb = if matches!(cli.command, Commands::Backup { .. } | Commands::Restore { .. } | Commands::Verify { .. }) {
        let pb = ProgressBar::new_spinner();
        pb.set_style(ProgressStyle::with_template("{spinner:.green} {msg}")
            .unwrap()
            .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"));
        pb.set_message(format!("{} in progress...", op));
        pb.enable_steady_tick(Duration::from_millis(80));
        Some(pb)
    } else {
        None
    };

    // Send request
    let request_str = serde_json::to_string(&request)?;
    writer.write_all(request_str.as_bytes()).await?;
    writer.write_all(b"\n").await?;
    writer.flush().await?;

    // Read response with timeout
    let response_line = tokio::time::timeout(
        Duration::from_secs(cli.timeout),
        lines.next_line()
    ).await??;

    if let Some(pb) = &pb {
        pb.finish_and_clear();
    }

    // Parse and display response
    match response_line {
        Some(line) => {
            let response: Value = serde_json::from_str(&line)?;
            display_response(&cli.command, &response);
        }
        None => {
            eprintln!("{} No response from Sidecar", "Error:".red().bold());
        }
    }

    Ok(())
}

/// Build the IPC request from the CLI command.
fn build_request(cmd: &Commands) -> Result<(String, Value)> {
    match cmd {
        Commands::Backup { tag, full } => Ok((
            "backup".into(),
            json!({
                "tag": tag,
                "incremental": !full
            }),
        )),
        Commands::Status => Ok(("status".into(), json!({}))),
        Commands::Restore { snapshot_id, file, chunk } => Ok((
            "restore".into(),
            json!({
                "snapshot_id": snapshot_id,
                "file_path": file,
                "chunk_coord": chunk
            }),
        )),
        Commands::Top { limit } => Ok((
            "top".into(),
            json!({ "limit": limit }),
        )),
        Commands::Diff { id_a, id_b } => Ok((
            "diff".into(),
            json!({ "id_a": id_a, "id_b": id_b }),
        )),
        Commands::Browse { snapshot_id, path } => Ok((
            "browse".into(),
            json!({ "snapshot_id": snapshot_id, "path": path }),
        )),
        Commands::Clone { snapshot_id, new_name } => Ok((
            "clone".into(),
            json!({ "snapshot_id": snapshot_id, "new_name": new_name }),
        )),
        Commands::Rollback { duration } => Ok((
            "rollback".into(),
            json!({ "duration": duration }),
        )),
        Commands::Verify { repair } => Ok((
            "verify".into(),
            json!({ "repair": repair }),
        )),
        Commands::Pin { snapshot_id, days } => Ok((
            "pin".into(),
            json!({ "snapshot_id": snapshot_id, "days": days }),
        )),
        Commands::Forecast => Ok(("forecast".into(), json!({}))),
        Commands::Cancel => Ok(("cancel".into(), json!({}))),
        Commands::Export { path } => Ok(("export".into(), json!({ "path": path }))),
        Commands::Import { path } => Ok(("import".into(), json!({ "path": path }))),
    }
}

/// Display the response from the Sidecar in a human-readable format.
fn display_response(cmd: &Commands, response: &Value) {
    let status = response.get("status").and_then(|v| v.as_str()).unwrap_or("error");
    let message = response.get("message").and_then(|v| v.as_str());

    if status == "error" {
        eprintln!("{} {}", "Error:".red().bold(), message.unwrap_or("Unknown error"));
        return;
    }

    let data = response.get("data");

    match cmd {
        Commands::Status => {
            if let Some(d) = data {
                let running = d.get("running").and_then(|v| v.as_bool()).unwrap_or(false);
                let state = d.get("state").and_then(|v| v.as_str()).unwrap_or("unknown");
                let tps = d.get("tps").and_then(|v| v.as_f64()).unwrap_or(0.0);
                let cpu = d.get("cpu_percent").and_then(|v| v.as_f64()).unwrap_or(0.0);
                let queue = &d["queue_status"];

                println!("{}", "─── Obsidian Sidecar Status ───".yellow().bold());
                println!("  State: {} {}",
                    if running { "●".green() } else { "○".dimmed() },
                    state.bold()
                );
                println!("  TPS: {:.2}  |  CPU: {:.1}%", tps, cpu);
                println!("  Queue: scanner={} chunk={} compress={} encrypt={} upload={}",
                    queue["scanner"], queue["chunk"], queue["compress"],
                    queue["encrypt"], queue["upload"]);

                if let Some(storage) = d.get("storage_stats") {
                    let snaps = storage.get("total_snapshots").and_then(|v| v.as_u64()).unwrap_or(0);
                    let size = storage.get("total_size_bytes").and_then(|v| v.as_u64()).unwrap_or(0);
                    let ratio = storage.get("dedup_ratio").and_then(|v| v.as_f64()).unwrap_or(0.0);
                    println!("  Snapshots: {}  |  Size: {}  |  Dedup: {:.1}%",
                        snaps,
                        humansize::format_size(size, humansize::BINARY),
                        ratio
                    );
                }
            }
        }

        Commands::Backup { .. } => {
            if let Some(d) = data {
                let snap = d.get("snapshot_id").and_then(|v| v.as_str()).unwrap_or("-");
                let files = d.get("files_scanned").and_then(|v| v.as_u64()).unwrap_or(0);
                let bytes = d.get("bytes_processed").and_then(|v| v.as_u64()).unwrap_or(0);
                let dedup = d.get("chunks_deduped").and_then(|v| v.as_u64()).unwrap_or(0);
                let new = d.get("chunks_new").and_then(|v| v.as_u64()).unwrap_or(0);
                let duration = d.get("duration_ms").and_then(|v| v.as_u64()).unwrap_or(0);

                println!("{} Backup complete!", "✓".green().bold());
                println!("  Snapshot: {}", snap.bold());
                println!("  Files: {}  |  Size: {}", files, humansize::format_size(bytes, humansize::BINARY));
                println!("  Chunks: {} new + {} deduped", new, dedup);
                println!("  Duration: {:.1}s", duration as f64 / 1000.0);
            }
        }

        Commands::Restore { .. } => {
            println!("{} Restore complete!", "✓".green().bold());
            if let Some(d) = data {
                let files = d.get("files_restored").and_then(|v| v.as_u64()).unwrap_or(0);
                let bytes = d.get("bytes_restored").and_then(|v| v.as_u64()).unwrap_or(0);
                println!("  Files: {}  |  Size: {}", files, humansize::format_size(bytes, humansize::BINARY));
            }
        }

        Commands::Top { .. } => {
            if let Some(d) = data {
                let ratio = d.get("dedup_ratio").and_then(|v| v.as_f64()).unwrap_or(0.0);
                println!("{} Storage Top Files (Dedup: {:.1}%)", "───".purple().bold(), ratio);
                if let Some(files) = d.get("files").and_then(|v| v.as_array()) {
                    for (i, file) in files.iter().enumerate() {
                        let path = file.get("path").and_then(|v| v.as_str()).unwrap_or("-");
                        let size = file.get("size").and_then(|v| v.as_u64()).unwrap_or(0);
                        let reason = file.get("reason").and_then(|v| v.as_str());
                        println!("  {}. {} [{}] {}",
                            i + 1, path,
                            humansize::format_size(size, humansize::BINARY),
                            reason.map(|r| format!("({})", r)).unwrap_or_default()
                        );
                    }
                }
            }
        }

        Commands::Diff { .. } => {
            if let Some(d) = data {
                let added = d.get("added").and_then(|v| v.as_array()).map(|a| a.len()).unwrap_or(0);
                let modified = d.get("modified").and_then(|v| v.as_array()).map(|a| a.len()).unwrap_or(0);
                let deleted = d.get("deleted").and_then(|v| v.as_array()).map(|a| a.len()).unwrap_or(0);
                println!("{} Snapshot Diff:", "───".yellow().bold());
                println!("  + {} added  |  * {} modified  |  - {} deleted", added, modified, deleted);
            }
        }

        Commands::Verify { .. } => {
            if let Some(d) = data {
                let total = d.get("total_checked").and_then(|v| v.as_u64()).unwrap_or(0);
                let healthy = d.get("healthy").and_then(|v| v.as_u64()).unwrap_or(0);
                let corrupted = d.get("corrupted").and_then(|v| v.as_u64()).unwrap_or(0);
                let repaired = d.get("repaired").and_then(|v| v.as_u64()).unwrap_or(0);

                println!("{} Verify Complete:", "───".purple().bold());
                println!("  Total: {}  |  Healthy: {}  |  Corrupted: {}  |  Repaired: {}",
                    total, healthy, corrupted, repaired);
                if corrupted > 0 {
                    println!("  {} {} corrupted snapshots detected!", "⚠".yellow(), corrupted);
                }
            }
        }

        Commands::Forecast => {
            if let Some(d) = data {
                let days = d.get("days_remaining").and_then(|v| v.as_f64()).unwrap_or(0.0);
                let growth = d.get("growth_rate_mb_per_day").and_then(|v| v.as_f64()).unwrap_or(0.0);
                let capacity = d.get("total_capacity_gb").and_then(|v| v.as_f64()).unwrap_or(0.0);

                println!("{} Storage Forecast:", "───".cyan().bold());
                println!("  Capacity: {:.1} GB  |  Growth: {:.1} MB/day  |  Remaining: {:.1} days",
                    capacity, growth, days);
            }
        }

        _ => {
            if let Some(msg) = message {
                println!("{} {}", "✓".green(), msg);
            } else {
                println!("{}", serde_json::to_string_pretty(response).unwrap_or_default());
            }
        }
    }
}
