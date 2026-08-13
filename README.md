# 🗄️ Obsidian Backup Server — 备份引擎

> 企业级 Minecraft **内容寻址（CAS）增量备份**引擎（Rust 独立守护进程）。
>
> [![License: MPL-2.0](https://img.shields.io/badge/license-MPL--2.0-blue.svg)](LICENSE)
> [![Rust](https://img.shields.io/badge/Rust-1.92+-orange.svg)]()

本仓库为 **Obsidian Backup 的备份引擎端**，包含两个 Rust 程序：

- **`obsidian-sidecar`** — 独立备份守护进程，接受 Minecraft 服务端通过 IPC 下发的指令，执行分块、去重、加密、压缩、事务、自愈与远程同步。
- **`obsidian`**（CLI）— 宿主机管理客户端，经同一 IPC 接口与 Sidecar 通信。

游戏端加载器（Fabric / Forge / Bukkit / NeoForge / MCDR）位于
[obsidian-backup](https://github.com/shrimp-211/obsidian-backup) 仓库。

---

## 📥 下载（获取二进制）

### 方式一：GitHub Release（推荐）

打开 <https://github.com/shrimp-211/obsidian-backup_server/releases>，下载对应平台的二进制：

| 文件 | 平台 |
|------|------|
| `obsidian-sidecar`（Linux）/ `obsidian-sidecar.exe`（Windows） | 守护进程 |
| `obsidian`（Linux）/ `obsidian.exe`（Windows） | CLI 客户端 |

### 方式二：GitHub Actions Artifacts

打开 <https://github.com/shrimp-211/obsidian-backup_server/actions> → 最新成功 run → **Artifacts** 区。

### 方式三：自行构建

```bash
cd sidecar && cargo build --release       # → target/release/obsidian-sidecar
cd client-cli && cargo build --release    # → target/release/obsidian
```

---

## 🚀 部署

### 第一步：启动 Sidecar 守护进程

```bash
# Linux / macOS
./obsidian-sidecar --server-root /path/to/minecraft/server

# Windows
obsidian-sidecar.exe --server-root D:\minecraft\server
```

Sidecar 启动后会在服务端根目录创建 `.obsidian/` 运行时目录：

```
.obsidian/
├── config/obsidian.yml    # 主配置
├── ipc/obsidian.sock      # IPC socket（Unix）/ 对应 Named Pipe（Windows）
├── rocksdb/               # 块索引（RocksDB）
└── store/                 # CAS 对象存储 + Packfile
```

### 第二步：使用 CLI 管理（可选）

```bash
./obsidian status              # 查看状态
./obsidian backup --tag daily  # 手动备份
./obsidian prune --keep 10     # 清理旧快照（保留最近 10 个）
```

### 第三步：让游戏端连接

游戏内 mod 自动连接 Sidecar，详见 [obsidian-backup](https://github.com/shrimp-211/obsidian-backup) 的部署说明。

---

## ⚙️ 配置

### Sidecar 启动参数

```
obsidian-sidecar [--config <path>] [--socket <path>] [--server-root <path>]
                 [--log-level <level>] [--oneshot] [--tag <tag>]
```

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--config` | `.obsidian/config/obsidian.yml` | 配置文件路径 |
| `--socket` | `.obsidian/ipc/obsidian.sock` | IPC 地址 |
| `--server-root` | `.` | Minecraft 服务端根目录 |
| `--log-level` | `info` | 日志级别 |
| `--oneshot` | — | 单次备份后退出（非守护模式） |
| `--tag` | — | 单次备份的标签 |

### obsidian.yml 配置

```yaml
profile: production

# 存储结构
storage:
  packfile:
    max_packfile_size_mb: 512   # Packfile 满 512MB 密封
    enable_crc32c_footer: true
  erasure_coding:
    enabled: true               # RS(8+2) 纠删码（约 +25% 存储开销）

# 安全
security:
  shared_token: "change-me"     # ⚠️ 生产环境务必修改，与游戏端一致
  snapshot_signing:
    enabled: true               # Ed25519 快照签名

# 沙箱恢复
sandbox_restore:
  temp_dir: "./.obsidian/sandbox"
  atomic_swap: true             # 原子切换（杜绝原地覆盖）
  verify_before_swap: true

# 排除规则
exclusion_rules:
  hardcoded_ignores:
    - "**/session.lock"         # 严禁备份锁文件
    - "**/logs/**"
    - "**/cache/**"
    - "**/libraries/**"
```

> ⚠️ `shared_token` 必须与游戏端 mod 的 `obsidian.token` 一致。

---

## 🖥️ CLI 完整指令

```bash
obsidian status                            # 流水线状态
obsidian backup [--tag <t>] [--full]      # 手动备份
obsidian restore <snapshot_id> [--file <path>] [--chunk <coord>]  # 恢复
obsidian verify [--repair]                 # 巡检 + RS 自愈
obsidian top [--limit <n>]                 # 存储热力图
obsidian diff <id_a> <id_b>                # 快照差异
obsidian browse <snapshot_id> [path]       # 浏览快照
obsidian clone <snapshot_id> <new_name>    # 世界克隆
obsidian rollback --duration <d>           # 近线闪回
obsidian pin <snapshot_id> --days <n>      # WORM 锁定
obsidian forecast                          # 容量预测
obsidian cancel                            # 取消备份事务
obsidian export <path>                     # 导出归档
obsidian import <path>                     # 导入归档
obsidian prune --keep <n>                  # 清理旧快照
obsidian remote-sync push|pull <snapshot_id>  # 远程同步
```

CLI 通用参数：

```bash
obsidian --socket <path> --timeout <sec> <command>
# 默认 socket: .obsidian/ipc/obsidian.sock
```

---

## 🔁 远程同步

实现 **Sidecar ↔ Sidecar** 之间的快照传输，两端均可作为主动发送方。

### 配置（obsidian.yml）

```yaml
remote_sync:
  enabled: true
  listen_addr: "0.0.0.0:8890"    # 公网 IP 一侧：监听地址
  peer_addr: "backup.example.com:8890"  # 另一侧：对端地址
  token: "shared-secret-token"   # 同步认证令牌
```

### 使用

```bash
# 拥有公网 IP 的一方（备份节点）启动监听
obsidian remote-sync serve

# 另一方（MC 服务端）主动推送 / 拉取
obsidian remote-sync push <snapshot_id>
obsidian remote-sync pull <snapshot_id>
```

> 传输全程 XChaCha20-Poly1305 加密 + 共享令牌认证。

---

## 🖥️ Windows 说明

| 平台 | IPC | 编译工具链 |
|------|-----|-----------|
| Linux / macOS | Unix Domain Socket | GCC / Clang |
| Windows | Named Pipe | MSVC |

Windows 下 `--socket` 路径会自动转换为 Named Pipe 名称，无需额外配置。若用 MinGW 本地编译，参考 `sidecar/.cargo/config.toml`（含 rocksdb gcc16 的 CXXFLAGS 修复）。

---

## 🔒 安全

- **零外露端口**：仅本地 IPC，无 TCP/HTTP 监听（远程同步除外，需显式开启）。
- **IPC 认证**：共享令牌 + 常数时间比较。
- **快照防篡改**：Ed25519 签名，恢复/巡检前强制验证。
- **数据自愈**：RS(8+2) 纠删码。
- **路径穿越防护**。

---

## 📖 文档

- 游戏端加载器：[obsidian-backup](https://github.com/shrimp-211/obsidian-backup)
- 技术总结：见游戏端仓库 [TECHNICAL_SUMMARY.md](https://github.com/shrimp-211/obsidian-backup)

## 🤝 贡献

欢迎提交 Issue 与 Pull Request，请阅读 [CONTRIBUTING.md](CONTRIBUTING.md)。

## 📄 许可证

[Mozilla Public License 2.0](LICENSE) (MPL-2.0)
