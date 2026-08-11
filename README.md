# 💎 Obsidian Backup — Minecraft 服务端灾备

> 企业级 Minecraft **内容寻址（CAS）增量备份**游戏端桥接层。
>
> [![License: MPL-2.0](https://img.shields.io/badge/license-MPL--2.0-blue.svg)](LICENSE)
> [![MC 1.21.1 / 1.20.1](https://img.shields.io/badge/Minecraft-1.21.1%20%7C%201.20.1-green)]()

本仓库为 **Minecraft 服务端一侧**的加载器实现（Fabric / Forge / Bukkit / NeoForge），
通过本地 **Unix Domain Socket (UDS) IPC** 与独立的 [Obsidian Sidecar 备份守护进程](https://github.com/shrimp-211/obsidian-backup_server)
通信，实现零侵入、零外露端口的备份 / 恢复 / 分析能力。

备份引擎（Rust）位于独立的 [obsidian-backup_server](https://github.com/shrimp-211/obsidian-backup_server) 仓库，
游戏主进程仅保留轻量逻辑桥，彻底杜绝备份引发的 JVM GC 停顿与 TPS 掉帧。

---

## ✨ 特性

- **零外露端口**：全面移除 WebUI / REST API / 宿主机 CLI，所有控制通过游戏内 Brigadier 指令树 + 本地 UDS IPC。
- **多加载器支持**：NeoForge 1.21.1 (Kotlin，功能最全) · Fabric · Forge · Paper/Bukkit（1.21.1 / 1.20.1）。
- **两阶段生命周期**：实时备份保持 100% 原始字节流式分块；快照转入冷备归档时才做 NBT 结构化压缩。
- **恢复即原始**：沙箱隔离 + 原子切换（Atomic Rename Swap），杜绝"原地在线覆盖恢复"。
- **远程同步**：MC 服务端与备份服务端之间互传快照，两端均可作为主动发送方（拥有公网 IP 的一方监听）。
- **BossBar / 富文本**：四色流式进度指示器 + 高密度流水线状态拓扑渲染。

## 🏗️ 架构

```
Minecraft Server (本仓库, Java/Kotlin)
├── NeoForge / Fabric / Forge / Bukkit 加载器
├── common/ 共享 Java 库（零 MC 依赖）
└── Brigadier 指令树 (/obsidian …)
        │  Unix Domain Socket (UDS) JSON IPC
        ▼
Obsidian Sidecar (Rust, 独立仓库 obsidian-backup_server)
├── BackupEngine — 扫描→分块→去重→存储→事务
├── ChunkEngine — FastCDC 内容定义分块 + BLAKE3
├── RocksDB 5 列族块索引 + CAS 对象存储 + Packfile
├── ACID 事务 + Ed25519 签名 + RS(8+2) 纠删码
└── RemoteSync — 双向主动发送, XChaCha20-Poly1305 加密
```

## 📦 支持矩阵

| 加载器 | MC 版本 | 指令集 |
|--------|---------|--------|
| NeoForge (Kotlin) | 1.21.1 | 完整 12 指令 + remote-sync |
| Fabric | 1.21.1 / 1.20.1 | 核心子集 |
| Forge | 1.21.1 / 1.20.1 | 核心子集 |
| Paper/Bukkit | 1.21.1 / 1.20.1 | 核心子集 |

## 🚀 快速开始

```bash
# 1. 构建 MC 服务端 mod/plugin（默认 1.21.1）
./gradlew :fabric:build :forge:build :bukkit:build

# 2. 或 1.20.1
./gradlew :fabric:build :bukkit:build -Pmc=1.20.1

# 3. 将构建产物放入 mods/plugins 目录
# 4. 启动独立备份守护进程（见 obsidian-backup_server）
# 5. 启动游戏服务端，mod 自动连接 UDS
```

## 🎮 游戏内指令

```
/obsidian status            # 流水线实时状态诊断
/obsidian top [limit]       # 存储热力图 TOP
/obsidian forecast          # 存储容量预测
/obsidian backup [--tag] [--full|--cancel]
/obsidian restore <id> [--file <path>|--chunk <coord>]
/obsidian diff <a> <b>      # 快照差异对比
/obsidian browse <id> [path]
/obsidian clone <id> <name>
/obsidian rollback --duration <1m>
/obsidian verify [repair]   # 巡检 + RS(8+2) 纠删码自愈
/obsidian pin <id> --days <n>
/obsidian snapshot export|import <path>
/obsidian remote-sync push|pull <id> | serve   # 远程同步
```

## 🔒 安全

- 零外露端口：仅本地 UDS socket，无 TCP/HTTP。
- IPC 认证：共享令牌 + 常数时间比较（mod 连接即认证握手）。
- 快照 Ed25519 签名 + RS(8+2) 纠删码自愈。
- 路径穿越防护、快照 ID 白名单校验。

## 📖 文档

- [TECHNICAL_SUMMARY.md](TECHNICAL_SUMMARY.md) — 技术实现总结
- [mainidea.md](mainidea.md) — 原始需求与设计文档
- [obsidian-backup_server](https://github.com/shrimp-211/obsidian-backup_server) — Rust 备份引擎仓库

## 🤝 贡献

欢迎提交 Issue 与 Pull Request，请阅读 [CONTRIBUTING.md](CONTRIBUTING.md)。

## 📄 许可证

[Mozilla Public License 2.0](LICENSE) (MPL-2.0)
