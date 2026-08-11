# 安全策略

## 支持的版本

| 版本 | 支持状态 |
|------|----------|
| v0.2.x | ✅ 积极维护 |
| v0.1.x | ⚠️ 仅安全修复 |

## 漏洞报告

请**不要**在 GitHub Issues 中公开安全漏洞。请通过以下方式私下报告：

- 在 GitHub 仓库创建 **Security Advisory**（推荐）
- 或发送邮件至仓库维护者

我们承诺：

- 48 小时内确认收到报告；
- 90 天内修复并发布补丁；
- 修复完成后，在 CHANGELOG 中披露（CVE 编号如有）。

## 安全设计要点

本项目遵循"零外露"原则：

- **零外露端口**：所有 IPC 走本地 Unix Domain Socket，无 TCP/HTTP 监听。
- **IPC 认证**：连接即认证握手，共享令牌 + 常数时间比较，防时序攻击。
- **路径穿越防护**：restore / clone / export 路径经 `validate_safe_path` 双重校验。
- **快照防篡改**：manifest Ed25519 签名，恢复 / 巡检前强制验证。
- **数据自愈**：RS(8+2) 纠删码分片存储，最多容忍 2 个分片损坏。
- **远程同步加密**：XChaCha20-Poly1305 端到端加密 + 共享令牌认证。

> 注意：Sidecar 的备份数据（`.obsidian/store`、`.obsidian/rocksdb`）包含服务端世界
> 敏感信息，请勿提交到 git（已在 `.gitignore` 排除）。
