# 贡献指南

感谢您对 Obsidian Backup 的关注！本项目是面向 Minecraft 服务器的企业级备份系统，
包含两个仓库：

- **[obsidian-backup](https://github.com/shrimp-211/obsidian-backup)**（本仓库）— MC 服务端加载器（Java/Kotlin）
- **[obsidian-backup_server](https://github.com/shrimp-211/obsidian-backup_server)** — Rust 备份引擎（Sidecar + CLI）

## 工作流

1. Fork 本仓库并创建功能分支：`git checkout -b feat/your-feature`
2. 遵循 [Conventional Commits](https://www.conventionalcommits.org/) 规范提交
   （`feat:` / `fix:` / `docs:` / `refactor:` / `test:` / `chore:`）。
3. 提交信息建议中英双语：标题英文、正文中文。
4. 运行 `./gradlew build` 确认构建通过后，发起 Pull Request。
5. 所有 PR 需通过 GitHub Actions CI（多 MC 版本 × 多加载器矩阵）。

## 环境要求

- JDK 21（构建 1.21.1）/ JDK 17（构建 1.20.1）
- Gradle 8.x（建议使用仓库内 `./gradlew`）

## 代码风格

- Java：遵循项目现有风格（4 空格缩进、无尾随空格）。
- Kotlin：遵循 Kotlin 官方风格指南。
- 新增指令 / IPC 操作码时，必须同步更新：
  - `common/.../IpcProtocol.java`（Java 端操作码）
  - `mod-neoforge/.../ipc/IpcProtocol.kt`（Kotlin 端操作码）
  - Rust 侧 `ipc/server.rs` 的 dispatch 表（在 obsidian-backup_server 仓库）

## 测试

- `./gradlew :common:test` — common 共享库
- 各加载器构建：`./gradlew :fabric:build :forge:build :bukkit:build`

## 分支模型

- `master` — 主开发分支，保持可构建状态。
- `feat/*` / `fix/*` — 功能与修复分支。
