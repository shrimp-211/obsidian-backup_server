//! Integration tests for the Obsidian Backup Sidecar.
//!
//! Tests the full backup → restore pipeline using temporary directories.

use std::path::PathBuf;
use std::sync::Arc;

use obsidian_sidecar::backup::BackupEngine;
use obsidian_sidecar::config::SidecarConfig;
use obsidian_sidecar::storage::index::BlockIndex;
use obsidian_sidecar::storage::object_store::ObjectStore;
use tempfile::TempDir;
use tokio::sync::RwLock;

struct TestHarness {
    engine: Arc<BackupEngine>,
    _temp: TempDir,
    world_dir: PathBuf,
}

async fn setup_test_harness() -> TestHarness {
    let temp = TempDir::new().unwrap();
    let root = temp.path().to_path_buf();

    // Create a fake world directory structure
    let world_dir = root.join("world");
    std::fs::create_dir_all(world_dir.join("region")).unwrap();
    std::fs::create_dir_all(world_dir.join("playerdata")).unwrap();
    std::fs::create_dir_all(world_dir.join("data")).unwrap();

    // Create level.dat (marks this as a world)
    std::fs::write(world_dir.join("level.dat"), b"fake level data v1").unwrap();
    std::fs::write(world_dir.join("region/r.0.0.mca"), vec![0x41u8; 8192]).unwrap();
    std::fs::write(
        world_dir.join("playerdata/00000000-0000-0000-0000-000000000000.dat"),
        b"player nbt data",
    )
    .unwrap();

    // Initialize storage
    let store_root = root.join(".obsidian/store");
    std::fs::create_dir_all(&store_root).unwrap();
    let rocksdb_root = root.join(".obsidian/rocksdb");
    std::fs::create_dir_all(&rocksdb_root).unwrap();

    let block_index = BlockIndex::open(&rocksdb_root).unwrap();
    let object_store = Arc::new(RwLock::new(
        ObjectStore::new(&store_root, SidecarConfig::default().storage).unwrap(),
    ));

    let config = SidecarConfig::default();
    let engine = Arc::new(BackupEngine::new(
        root.clone(),
        block_index,
        object_store,
        config,
    ));
    engine.load_snapshots().await.unwrap();

    TestHarness {
        engine,
        _temp: temp,
        world_dir,
    }
}

#[tokio::test]
async fn test_full_backup_and_restore_cycle() {
    let harness = setup_test_harness().await;

    // Run backup
    let result = harness
        .engine
        .run_backup(Some("integration-test".into()), true)
        .await
        .expect("Backup should succeed");

    assert!(
        result.files_changed > 0,
        "Should have processed at least 1 file"
    );
    assert!(
        result.bytes_processed > 0,
        "Should have processed some bytes"
    );
    assert!(result.chunks_new > 0, "Should have created new chunks");
    assert!(
        result.files_skipped == 0,
        "No files should be skipped in a clean backup"
    );

    let snapshot_id = result.snapshot_id.clone();
    assert!(!snapshot_id.is_empty(), "Snapshot ID should not be empty");

    // Verify state is idle after backup
    let state = harness.engine.get_state().await;
    assert_eq!(state.state, "idle", "State should be idle after backup");

    // Restore from the snapshot
    let restore_result = harness
        .engine
        .restore(&snapshot_id, None, None)
        .await
        .expect("Restore should succeed");

    assert!(
        restore_result.files_restored > 0,
        "Should have restored files"
    );
    assert!(
        restore_result.files_missing_chunks.is_empty(),
        "No files should have missing chunks"
    );
    assert!(
        restore_result.sandbox_used,
        "Sandbox should be used for restore"
    );
}

#[tokio::test]
async fn test_status_reports_correctly() {
    let harness = setup_test_harness().await;

    let state = harness.engine.get_state().await;
    assert!(!state.running, "Should be idle initially");
    assert_eq!(state.state, "idle");
}

#[tokio::test]
async fn test_top_files_analysis() {
    let harness = setup_test_harness().await;

    // Run a backup first
    harness
        .engine
        .run_backup(Some("top-test".into()), true)
        .await
        .expect("Backup should succeed");

    let top = harness
        .engine
        .get_top_files(5)
        .await
        .expect("Top files should work");
    assert!(!top.files.is_empty(), "Should have files in top list");
}

#[tokio::test]
async fn test_verify_checks_integrity() {
    let harness = setup_test_harness().await;

    // Run backup
    harness
        .engine
        .run_backup(Some("verify-test".into()), true)
        .await
        .expect("Backup should succeed");

    // Verify
    let verify = harness
        .engine
        .verify(false)
        .await
        .expect("Verify should succeed");

    assert!(verify.total_checked > 0, "Should check some chunks");
    assert_eq!(
        verify.corrupted, 0,
        "No chunks should be corrupted in a clean test"
    );
    assert_eq!(
        verify.healthy, verify.total_checked,
        "All chunks should be healthy"
    );
}

#[tokio::test]
async fn test_path_validation_rejects_traversal() {
    let harness = setup_test_harness().await;

    // Run a backup first
    let result = harness
        .engine
        .run_backup(Some("path-test".into()), true)
        .await
        .expect("Backup should succeed");

    // Try to restore with path traversal
    let evil_path = "../../../etc/passwd";
    let restore = harness
        .engine
        .restore(&result.snapshot_id, Some(evil_path), None)
        .await;

    assert!(
        restore.is_err(),
        "Path traversal restore should be rejected"
    );
    assert!(
        restore.unwrap_err().to_string().contains("Path traversal"),
        "Error should mention path traversal"
    );
}

#[tokio::test]
async fn test_clone_rejects_bad_names() {
    let harness = setup_test_harness().await;

    let result = harness
        .engine
        .run_backup(Some("clone-test".into()), true)
        .await
        .expect("Backup should succeed");

    // Try to clone with path traversal in name
    let bad_name = "../evil_world";
    let clone = harness
        .engine
        .clone_world(&result.snapshot_id, bad_name)
        .await;
    assert!(
        clone.is_err(),
        "Clone with path traversal name should be rejected"
    );

    // Valid clone should work
    let good_name = "my_clone_world";
    let clone = harness
        .engine
        .clone_world(&result.snapshot_id, good_name)
        .await;
    assert!(clone.is_ok(), "Clone with valid name should succeed");

    // Clean up
    let clone_dir = harness.engine.server_root_path().join(good_name);
    if clone_dir.exists() {
        std::fs::remove_dir_all(&clone_dir).unwrap();
    }
}

#[tokio::test]
async fn test_concurrent_backups_do_not_deadlock() {
    let harness = setup_test_harness().await;

    // Running two backups in quick succession should not deadlock
    let r1 = harness
        .engine
        .run_backup(Some("concurrent-1".into()), true)
        .await;
    assert!(r1.is_ok(), "First backup should succeed");

    let r2 = harness
        .engine
        .run_backup(Some("concurrent-2".into()), true)
        .await;
    assert!(r2.is_ok(), "Second backup should succeed");
}

#[tokio::test]
async fn test_forecast_needs_two_snapshots() {
    let harness = setup_test_harness().await;

    // Forecast with 0 snapshots should fail
    let f0 = harness.engine.forecast().await;
    assert!(f0.is_err(), "Forecast with 0 snapshots should fail");

    // Run one backup
    harness
        .engine
        .run_backup(Some("forecast-1".into()), true)
        .await
        .unwrap();
    let f1 = harness.engine.forecast().await;
    assert!(f1.is_err(), "Forecast with 1 snapshot should still fail");

    // Run second backup
    harness
        .engine
        .run_backup(Some("forecast-2".into()), true)
        .await
        .unwrap();
    let f2 = harness.engine.forecast().await;
    assert!(f2.is_ok(), "Forecast with 2 snapshots should succeed");
}
