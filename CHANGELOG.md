# 更新日志

本项目的变更记录（[Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/) 风格，遵循 [语义化版本](https://semver.org/lang/zh-CN/)）。

## [Unreleased]

## [0.2.0] - 2026-08-12

### 新增
- 加载器端一致性与功能补齐：
  - Kotlin IPC 协议补全 `AUTH` / `EXPORT` / `IMPORT` / `REMOTE_SYNC` 操作码。
  - Kotlin `IpcClient.connect()` 补上认证握手（与 Java common 端一致）。
  - 修复 `snapshot export|import` 误用 `BACKUP` 操作码的问题。
  - 新增 `/obsidian remote-sync push|pull|serve` 指令（远程同步）。
  - common `IpcClient` 新增同步请求方法 `sendRequestSync`。

### 变更
- 许可证从 MIT 切换为 **MPL-2.0**。
- NeoForge 元数据统一为 `mod_version=0.2.0`。

## [0.1.0] - 2026-07-14

### 新增
- 多加载器架构：NeoForge (Kotlin) / Fabric / Forge / Bukkit + common 共享库。
- Brigadier 指令树（status / top / forecast / backup / restore / diff / browse / clone / rollback / verify / pin / snapshot）。
- BossBar 四色进度指示器、富文本状态渲染。
- 多 MC 版本构建矩阵（1.21.1 / 1.20.1）。
