# Codex Quota Ledger

一个本地、隐私优先的 Codex 额度账本。它不只显示当前百分比，还记录重置边界、
历史容量，以及官方观测与本地工作量之间的缺口。

[English](README.md)

## 让 Codex 安装

把本仓库地址发给 Codex，然后说：

> 在这台 Windows 电脑上安装 Codex Quota Ledger，启动记录器，并在首次本地扫描完成后告诉我。

Codex 会运行仓库里的 `install.ps1`。安装器会下载已发布的 Windows 程序、校验
SHA-256、设置登录后自动运行，并安装一个很轻的个人 Codex Skill。用户不需要安装 Rust。

安装后新建一个 Codex 任务，直接询问额度、用量、重置窗口、对账或记录器健康状态。
打开本地网页：

```powershell
codex-quota-ledger open
```

网页只监听 `127.0.0.1`，不加载任何远程资源。

## 记录什么

- 官方额度观测和官方每日用量，始终作为两类独立证据保存。
- 活动及归档 Codex 会话中的隐私安全 Token 元数据。
- 重置周期、采集健康、断线恢复、定价版本和跨来源对账。

它不会上传 Prompt 或聊天内容，不会复制凭据，不含遥测，也不会调用模型采集额度。
网页、Cloud、其他设备或已删除会话造成的缺口会明确保持为未知。API 等价金额只是估算，
不是 OpenAI 账单，也不是官方披露的订阅额度公式。

## 卸载

让 Codex 在本仓库运行 `uninstall.ps1`。默认移除记录器、程序、PATH 项和 Skill，
但保留本地账本。只有明确希望删除账本数据时才使用 `-RemoveData`。

## 从源码构建

当前版本面向 Windows，使用仓库固定的 Rust 工具链：

```powershell
cargo test --all --locked
cargo build --release --locked
```

MIT License。本项目是独立社区项目，与 OpenAI 不存在隶属或官方认可关系。
