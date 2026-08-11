# 🗄️ Obsidian Backup Server — 备份引擎

> 企业级 Minecraft **内容寻址（CAS）增量备份**引擎（Rust 独立守护进程）。
>
> [![License: MPL-2.0](https://img.shields.io/badge/license-MPL--2.0-blue.svg)](LICENSE)
> [![Rust](https://img.shields.io/badge/Rust-1.92+-orange.svg)]()

本仓库为 **Obsidian Backup 的备份引擎端**，包含两个 Rust 程序：

- **`obsidian-sidecar`** — 独立备份守护进程，接受 Minecraft 服务端通过 UDS IPC 下发的指令，执行分块、去重、加密、压缩、事务、自愈与远程同步。
- **`obsidian`**（CLI）— 宿主机管理客户端，经同一 UDS 接口与 Sidecar 通信。

游戏端加载器（Fabric / Forge / Bukkit / NeoForge）位于
[obsidian-backup](https://github.com/shrimp-211/obsidian-backup) 仓库。

---

## ✨ 特性

- **零外露端口**：仅本地 Unix Domain Socket，无 TCP/HTTP 监听。
- **FastCDC 流式分块**：有界内存，消除大文件 OOM 风险。
- **CAS 对象存储 + Packfile**：CRC32C footer 密封、`.idx` 索引、独立对象回收。
- **ACID 备份事务**：BEGIN / COMMIT / ROLLBACK，杜绝悬挂对象。
- **Ed25519 快照签名**：manifest 防篡改，restore / verify 强制验证。
- **RS(8+2) 纠删码自愈**：对象分片存储，`verify repair` 最多自愈 2 个分片。
- **远程同步**：与对端互传快照，**两端均可作为主动发送方**（拥有公网 IP 的一方监听），XChaCha20-Poly1305 加密 + 令牌认证。
- **路径穿越防护** + 常数时间令牌比较。

## 🏗️ 架构

```
Minecraft Server (obsidian-backup 仓库)
        │  Unix Domain Socket (UDS) JSON IPC
        ▼
Obsidian Sidecar (本仓库)
├── IPC Server         — UDS 监听 + 令牌认证 + 16 操作码
├── BackupEngine       — 扫描 → 流式分块 → 去重 → 存储 → 事务
├── ChunkEngine        — FastCDC 内容定义分块 + BLAKE3
├── BlockIndex         — RocksDB 5 列族块索引
├── ObjectStore        — CAS 对象存储 + Packfile + RS 分片
├── TransactionManager — ACID 事务 (BEGIN/COMMIT/ROLLBACK)
└── RemoteSync         — 双向主动发送, 加密同步通道
```

## 🚀 快速开始

```bash
# 1. 构建 Sidecar（Linux）
cd sidecar && cargo build --release

# 2. 构建 CLI
cd client-cli && cargo build --release

# 3. 首次启动 Sidecar（作为 Minecraft 服务端的独立进程）
./target/release/obsidian-sidecar --server-root /path/to/minecraft/server

# 4. 游戏内执行 /obsidian backup（由 obsidian-backup 仓库的 mod 触发）
```

### Sidecar 启动参数

```
obsidian-sidecar [--config <path>] [--socket <path>] [--server-root <path>]
                 [--log-level <level>] [--oneshot] [--tag <tag>]
```

## 🔁 远程同步

```bash
# 有公网 IP 的一方（监听）
obsidian remote-sync serve

# 另一方（主动发送 / 拉取）
obsidian remote-sync push <snapshot_id>
obsidian remote-sync pull <snapshot_id>
```

配置示例（`obsidian.yml`）：

```yaml
remote_sync:
  enabled: true
  listen_addr: "0.0.0.0:8890"    # 公网 IP 一侧
  peer_addr: "example.com:8890"  # 另一侧
  token: "shared-secret-token"
```

## 📖 文档

- 技术实现总结（TECHNICAL_SUMMARY.md）随游戏端仓库归档，见 [obsidian-backup](https://github.com/shrimp-211/obsidian-backup)。
- 游戏端加载器：[obsidian-backup](https://github.com/shrimp-211/obsidian-backup)

## 🤝 贡献

欢迎提交 Issue 与 Pull Request，请阅读 [CONTRIBUTING.md](CONTRIBUTING.md)。

## 📄 许可证

[Mozilla Public License 2.0](LICENSE) (MPL-2.0)
