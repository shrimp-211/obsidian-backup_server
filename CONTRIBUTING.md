# 贡献指南

感谢您对 **Obsidian Backup Server**（Rust 备份引擎）的关注！

本项目包含两个仓库：

- **[obsidian-backup_server](https://github.com/shrimp-211/obsidian-backup_server)**（本仓库）— Rust 备份引擎（Sidecar + CLI）
- **[obsidian-backup](https://github.com/shrimp-211/obsidian-backup)** — MC 服务端加载器（Java/Kotlin）

## 工作流

1. Fork 本仓库并创建功能分支：`git checkout -b feat/your-feature`
2. 遵循 [Conventional Commits](https://www.conventionalcommits.org/) 规范提交。
3. 运行 `cargo fmt`、`cargo clippy -- -D warnings`、`cargo test` 确认通过。
4. 发起 Pull Request，需通过 GitHub Actions（build + test + clippy）。

## 环境要求

- Rust 1.92+（`rustup update stable`）
- Linux 优先（RocksDB 依赖），Windows/macOS 亦可构建

## 代码风格

- 使用 `cargo fmt` 格式化。
- 新操作码（IPC op）必须同步更新：
  - `sidecar/src/ipc/server.rs` 的 dispatch 表
  - 游戏端 `common` / `mod-neoforge` 的协议定义（obsidian-backup 仓库）
- 新增模块需附带单元测试（`#[cfg(test)]`），关键路径补集成测试（`sidecar/tests/`）。

## 测试

```bash
cargo test                    # 单元测试
cd sidecar && cargo test      # 集成测试
cargo clippy -- -D warnings   # 静态检查
```

## 分支模型

- 主开发分支：`master`（合入需 review）
- 功能分支：`feat/*` / `fix/*`
