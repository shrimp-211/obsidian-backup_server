//! Configuration system for the Obsidian Sidecar daemon.
//!
//! Loads YAML configuration files with support for:
//!   - Production/development profiles
//!   - Adaptive scheduler thresholds
//!   - Storage structure settings
//!   - Security settings

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::path::Path;

/// Top-level Sidecar configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SidecarConfig {
    #[serde(default)]
    pub profile: String,

    #[serde(default)]
    pub scheduler: SchedulerConfig,

    #[serde(default)]
    pub security: SecurityConfig,

    #[serde(default)]
    pub sandbox_restore: SandboxConfig,

    #[serde(default)]
    pub adaptive_scheduler: AdaptiveSchedulerConfig,

    #[serde(default)]
    pub storage: StorageConfig,

    #[serde(default)]
    pub exclusion_rules: ExclusionRules,
}

/// Multi-scheduler engine concurrency policy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchedulerConfig {
    #[serde(default = "default_true")]
    pub restore_prioritized: bool,

    #[serde(default = "default_true")]
    pub suspend_gc_on_restore: bool,

    #[serde(default)]
    pub backup_windows: Vec<BackupWindow>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupWindow {
    pub window: String,
    #[serde(default)]
    pub bandwidth_limit: String,
    #[serde(default)]
    pub cpu_worker_limit: u32,
    #[serde(default)]
    pub io_iops_max: u32,
}

/// Security configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecurityConfig {
    #[serde(default)]
    pub dual_admin_confirmation: bool,

    #[serde(default)]
    pub snapshot_signing: SnapshotSigningConfig,

    #[serde(default)]
    pub immutable_locks: ImmutableLocksConfig,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SnapshotSigningConfig {
    #[serde(default)]
    pub enabled: bool,

    #[serde(default)]
    pub private_key_secure_path: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ImmutableLocksConfig {
    #[serde(default)]
    pub weekly_retention_locked: bool,
}

/// Sandbox restore settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SandboxConfig {
    #[serde(default = "default_sandbox_temp_dir")]
    pub temp_dir: String,

    #[serde(default = "default_true")]
    pub atomic_swap: bool,

    #[serde(default = "default_true")]
    pub verify_before_swap: bool,

    #[serde(default = "default_true")]
    pub resume_checkpoint: bool,
}

/// Adaptive scheduler for resource throttling.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdaptiveSchedulerConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,

    #[serde(default = "default_polling_interval")]
    pub metrics_polling_interval_ms: u64,

    #[serde(default)]
    pub thresholds: AdaptiveThresholds,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdaptiveThresholds {
    #[serde(default = "default_tps_critical")]
    pub tps_critical: f64,

    #[serde(default = "default_tps_danger")]
    pub tps_danger: f64,

    #[serde(default = "default_memory_cap")]
    pub host_memory_cap_mb: u64,
}

/// Storage structure configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    #[serde(default)]
    pub packfile: PackfileConfig,

    #[serde(default)]
    pub rocksdb_reliability: RocksDbReliability,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PackfileConfig {
    #[serde(default = "default_true")]
    pub adaptive_sizing: bool,

    #[serde(default = "default_max_packfile_size")]
    pub max_packfile_size_mb: u64,

    #[serde(default = "default_true")]
    pub enable_crc32c_footer: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RocksDbReliability {
    #[serde(default = "default_checkpoint_interval")]
    pub checkpoint_interval_minutes: u64,

    #[serde(default = "default_true")]
    pub auto_rebuild_from_pack: bool,
}

/// Hardcoded exclusion rules.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExclusionRules {
    #[serde(default = "default_exclusions")]
    pub hardcoded_ignores: Vec<String>,
}

// --- Default values ---

fn default_true() -> bool {
    true
}
fn default_sandbox_temp_dir() -> String {
    "./.obsidian/sandbox".into()
}
fn default_polling_interval() -> u64 {
    1000
}
fn default_tps_critical() -> f64 {
    15.5
}
fn default_tps_danger() -> f64 {
    16.5
}
fn default_memory_cap() -> u64 {
    2048
}
fn default_max_packfile_size() -> u64 {
    512
}
fn default_checkpoint_interval() -> u64 {
    60
}
fn default_exclusions() -> Vec<String> {
    vec![
        "**/session.lock".into(),
        "**/logs/**".into(),
        "**/cache/**".into(),
        "**/libraries/**".into(),
    ]
}

// --- Default implementations ---

impl Default for SidecarConfig {
    fn default() -> Self {
        Self {
            profile: "production".into(),
            scheduler: SchedulerConfig::default(),
            security: SecurityConfig::default(),
            sandbox_restore: SandboxConfig::default(),
            adaptive_scheduler: AdaptiveSchedulerConfig::default(),
            storage: StorageConfig::default(),
            exclusion_rules: ExclusionRules::default(),
        }
    }
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            restore_prioritized: true,
            suspend_gc_on_restore: true,
            backup_windows: vec![
                BackupWindow {
                    window: "02:00-06:00".into(),
                    bandwidth_limit: "unlimited".into(),
                    cpu_worker_limit: 8,
                    io_iops_max: 5000,
                },
                BackupWindow {
                    window: "06:01-01:59".into(),
                    bandwidth_limit: "20MB/s".into(),
                    cpu_worker_limit: 2,
                    io_iops_max: 800,
                },
            ],
        }
    }
}

impl Default for SecurityConfig {
    fn default() -> Self {
        Self {
            dual_admin_confirmation: true,
            snapshot_signing: SnapshotSigningConfig {
                enabled: true,
                private_key_secure_path: Some("/etc/obsidian/keys/sign.key".into()),
            },
            immutable_locks: ImmutableLocksConfig {
                weekly_retention_locked: true,
            },
        }
    }
}

impl Default for SandboxConfig {
    fn default() -> Self {
        Self {
            temp_dir: default_sandbox_temp_dir(),
            atomic_swap: true,
            verify_before_swap: true,
            resume_checkpoint: true,
        }
    }
}

impl Default for AdaptiveSchedulerConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            metrics_polling_interval_ms: default_polling_interval(),
            thresholds: AdaptiveThresholds {
                tps_critical: default_tps_critical(),
                tps_danger: default_tps_danger(),
                host_memory_cap_mb: default_memory_cap(),
            },
        }
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            packfile: PackfileConfig {
                adaptive_sizing: true,
                max_packfile_size_mb: default_max_packfile_size(),
                enable_crc32c_footer: true,
            },
            rocksdb_reliability: RocksDbReliability {
                checkpoint_interval_minutes: default_checkpoint_interval(),
                auto_rebuild_from_pack: true,
            },
        }
    }
}

impl Default for ExclusionRules {
    fn default() -> Self {
        Self {
            hardcoded_ignores: default_exclusions(),
        }
    }
}

impl SidecarConfig {
    /// Load configuration from a YAML file, falling back to defaults.
    pub fn load(path: &Path) -> Result<Self> {
        if path.exists() {
            let contents = std::fs::read_to_string(path)?;
            let config: SidecarConfig = serde_yaml::from_str(&contents)?;
            tracing::info!("[Config] Loaded from {:?}", path);
            Ok(config)
        } else {
            tracing::warn!("[Config] {:?} not found, using defaults", path);
            Ok(SidecarConfig::default())
        }
    }

    /// Check if a file path matches any exclusion pattern.
    pub fn is_excluded(&self, path: &str) -> bool {
        self.exclusion_rules
            .hardcoded_ignores
            .iter()
            .any(|pattern| glob_match::glob_match(pattern, path))
    }
}

/// Simple glob matching for exclusion rules.
mod glob_match {
    pub fn glob_match(pattern: &str, path: &str) -> bool {
        let pattern = pattern.replace("**", "___DOUBLESTAR___");
        let segments: Vec<&str> = pattern.split('/').collect();
        let path_segments: Vec<&str> = path.replace('\\', "/").split('/').collect();

        match_segments(&segments, &path_segments, 0, 0)
    }

    fn match_segments(pat: &[&str], path: &[&str], pi: usize, si: usize) -> bool {
        if pi >= pat.len() {
            return si >= path.len();
        }

        let p = pat[pi];

        if p == "___DOUBLESTAR___" {
            // ** matches zero or more path segments
            if pi == pat.len() - 1 {
                return true; // ** at end matches everything
            }
            // Try matching ** against 0, 1, 2, ... remaining segments
            for next in si..=path.len() {
                if match_segments(pat, path, pi + 1, next) {
                    return true;
                }
            }
            false
        } else if si < path.len() {
            single_match(p, path[si]) && match_segments(pat, path, pi + 1, si + 1)
        } else {
            false
        }
    }

    fn single_match(pattern: &str, name: &str) -> bool {
        if pattern == "*" {
            return true;
        }

        // Handle patterns like "*.log", "session.*", "foo*bar"
        let mut pi = 0usize;
        let mut ni = 0usize;
        let p_bytes = pattern.as_bytes();
        let n_bytes = name.as_bytes();
        let mut star_idx = None;
        let mut match_idx = 0usize;

        loop {
            if pi < p_bytes.len() && p_bytes[pi] == b'*' {
                star_idx = Some(pi);
                match_idx = ni;
                pi += 1;
            } else if ni < n_bytes.len() && pi < p_bytes.len() && p_bytes[pi] == n_bytes[ni] {
                pi += 1;
                ni += 1;
            } else if let Some(si) = star_idx {
                pi = si + 1;
                ni = match_idx + 1;
                match_idx = ni;
            } else {
                break;
            }
        }

        while pi < p_bytes.len() && p_bytes[pi] == b'*' {
            pi += 1;
        }

        pi == p_bytes.len() && ni == n_bytes.len()
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn test_glob_match() {
            assert!(glob_match("**/session.lock", "world/session.lock"));
            assert!(glob_match("**/session.lock", "session.lock"));
            assert!(glob_match("**/logs/**", "logs/2024/server.log"));
            assert!(glob_match("**/cache/**", ".fabric/cache/foo"));
            assert!(!glob_match("**/session.lock", "world/data.mca"));
            assert!(glob_match("*.log", "server.log"));
        }
    }
}
