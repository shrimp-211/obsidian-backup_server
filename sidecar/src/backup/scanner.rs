//! Lightweight file scanner for the Obsidian Sidecar.
//!
//! Uses WalkDir to traverse the server directory and identify files
//! that have changed since the last backup. Respects exclusion rules
//! to skip lock files, logs, caches, and server libraries.
//!
//! Future: Journal-Driven Scanner with WatchService + Region Dirty Table
//! for instant change detection on large worlds.

use std::path::{Path, PathBuf};
use std::time::SystemTime;

use anyhow::Result;
use tracing::{debug, warn};

use crate::config::SidecarConfig;

/// Metadata about a file to be backed up.
#[derive(Debug, Clone)]
pub struct FileEntry {
    pub path: PathBuf,
    pub size: u64,
    pub modified: u64, // Unix timestamp (seconds)
}

/// Scans the Minecraft server directory for files that need backing up.
pub struct FileScanner {
    server_root: PathBuf,
    config: SidecarConfig,
}

impl FileScanner {
    pub fn new(server_root: PathBuf, config: SidecarConfig) -> Self {
        Self {
            server_root,
            config,
        }
    }

    /// Scan the server's world directories for files.
    ///
    /// If `since` is Some, only return files modified after the given timestamp.
    /// This enables incremental backups by skipping unchanged files.
    pub fn scan_world_directory(&self, since: &Option<String>) -> Result<Vec<FileEntry>> {
        let mut files = Vec::new();

        // Directories to scan for world data
        let world_dirs = self.find_world_directories();

        for world_dir in &world_dirs {
            self.scan_directory(world_dir, since, &mut files)?;
        }

        // Sort by path for deterministic processing
        files.sort_by(|a, b| a.path.cmp(&b.path));

        debug!(
            "[Scanner] Found {} files to process across {} world dirs",
            files.len(),
            world_dirs.len()
        );

        Ok(files)
    }

    /// Identify world directories in the server root.
    ///
    /// Minecraft servers typically have:
    ///   - world/          (overworld)
    ///   - world_nether/   (nether)
    ///   - world_the_end/  (end)
    ///
    /// We detect directories that contain a `region/` or `level.dat` file.
    fn find_world_directories(&self) -> Vec<PathBuf> {
        let mut worlds = Vec::new();

        // Common world directory names
        let candidates = ["world", "world_nether", "world_the_end"];

        for name in &candidates {
            let path = self.server_root.join(name);
            if path.is_dir() && path.join("level.dat").exists() {
                worlds.push(path);
            }
        }

        // Also scan for custom world directories
        if let Ok(entries) = std::fs::read_dir(&self.server_root) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    let dir_name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
                    if (dir_name.starts_with("world") || dir_name.contains("world"))
                        && !candidates.contains(&dir_name)
                        && path.join("level.dat").exists()
                    {
                        worlds.push(path);
                    }
                }
            }
        }

        if worlds.is_empty() {
            warn!("[Scanner] No world directories found! Is this a Minecraft server root?");
        }

        worlds
    }

    /// Recursively scan a directory, respecting exclusion rules.
    fn scan_directory(
        &self,
        dir: &Path,
        since: &Option<String>,
        files: &mut Vec<FileEntry>,
    ) -> Result<()> {
        if !dir.is_dir() {
            return Ok(());
        }

        for entry in walkdir::WalkDir::new(dir)
            .follow_links(false)
            .into_iter()
            .filter_map(|e| e.ok())
        {
            let path = entry.path();

            // Skip directories (we'll recurse into them automatically via WalkDir)
            if entry.file_type().is_dir() {
                continue;
            }

            // Convert to relative path for exclusion check
            let rel_path = path.strip_prefix(&self.server_root).unwrap_or(path);
            let rel_str = rel_path.to_string_lossy().replace('\\', "/");

            // Check exclusion rules
            if self.config.is_excluded(&rel_str) {
                debug!("[Scanner] Excluding: {}", rel_str);
                continue;
            }

            // Get file metadata
            let metadata = match entry.metadata() {
                Ok(m) => m,
                Err(e) => {
                    warn!("[Scanner] Cannot read metadata for {:?}: {}", path, e);
                    continue;
                }
            };

            let size = metadata.len();
            let modified = metadata
                .modified()
                .map(|t| to_unix_timestamp(t))
                .unwrap_or(0);

            // For incremental mode: skip unchanged files
            if let Some(since_str) = since {
                if let Ok(since_ts) = chrono::DateTime::parse_from_rfc3339(since_str) {
                    let since_unix = since_ts.timestamp() as u64;
                    if modified <= since_unix {
                        continue; // File hasn't changed
                    }
                }
            }

            files.push(FileEntry {
                path: path.to_path_buf(),
                size,
                modified,
            });
        }

        Ok(())
    }
}

fn to_unix_timestamp(time: SystemTime) -> u64 {
    time.duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn test_scanner_respects_exclusions() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();

        // Create a fake world structure
        let world = root.join("world");
        std::fs::create_dir_all(world.join("region")).unwrap();
        std::fs::create_dir_all(world.join("playerdata")).unwrap();

        std::fs::write(world.join("level.dat"), b"fake level data").unwrap();
        std::fs::write(world.join("session.lock"), b"lock").unwrap();
        std::fs::write(world.join("region/r.0.0.mca"), vec![0u8; 1024]).unwrap();

        let config = SidecarConfig::default();
        let scanner = FileScanner::new(root.to_path_buf(), config);

        let files = scanner.scan_world_directory(&None).unwrap();

        // session.lock should be excluded
        let has_lock = files.iter().any(|f| f.path.ends_with("session.lock"));
        assert!(!has_lock, "session.lock should be excluded");

        // region file should be included
        let has_region = files.iter().any(|f| f.path.ends_with("r.0.0.mca"));
        assert!(has_region, "region file should be included");
    }
}
