# 更新日志

本项目的变更记录（[Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/) 风格，遵循 [语义化版本](https://semver.org/lang/zh-CN/)）。

## [Unreleased]

## [0.2.0] - 2026-08-12

### 新增
- **流式分块**：`ChunkEngine::chunk_reader` 有界内存，消除 >16MB 文件 OOM 风险（已知限制 #1）。
- **Packfile 真实密封**：CRC32C footer + `.idx` 索引 + 独立对象自动回收（已知限制 #2）。
- **Ed25519 快照签名**：manifest 签名 / 验证，restore 与 verify 强制防篡改（已知限制 #3）。
- **RS(8+2) 纠删码**：对象分片存储 + parity，`verify repair` 最多自愈 2 个分片（已知限制 #4）。
- **远程同步**：`RemoteSync` 模块 — 双向主动发送（有公网 IP 一方监听）、XChaCha20-Poly1305 加密、共享令牌认证，支持 push / pull / serve 三种模式，接入 IPC（`remote_sync` 操作码）与 CLI（`obsidian remote-sync`）。

### 变更
- 许可证从 MIT 切换为 **MPL-2.0**。
- `reed-solomon-erasure` 切换为纯 Rust 实现（`default-features = false`），提升可移植性。
- `Cargo.toml` 元数据修正为 `https://github.com/shrimp-211/obsidian-backup_server`。

## [0.1.0] - 2026-07-14

### 新增
- Sidecar 守护进程（UDS IPC、15 操作码、令牌认证）。
- FastCDC 分块 + BLAKE3 内容寻址。
- RocksDB 5 列族块索引 + CAS 对象存储。
- ACID 备份事务（BEGIN/COMMIT/ROLLBACK）。
- CLI 管理客户端。
