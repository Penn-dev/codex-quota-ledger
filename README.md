# Codex Quota Ledger

A local, private ledger for understanding Codex subscription usage—not just the
current percentage, but reset boundaries, historical capacity, and gaps between
official observations and local work.

[简体中文](README.zh-CN.md)

## Install with Codex

Give Codex this repository URL and say:

> Install Codex Quota Ledger on this Windows computer, start its recorder, and
> confirm when the first local scan is complete.

Codex will run the repository's `install.ps1`. The installer downloads the
published Windows binary, verifies its SHA-256 checksum, installs a logon task,
and adds a small personal Codex skill. Rust is not required.

After installation, start a new Codex task and ask about your quota, usage,
reset window, reconciliation, or recorder health. To open the local dashboard:

```powershell
codex-quota-ledger open
```

The dashboard listens only on `127.0.0.1` and loads no remote assets.

## What it records

- Official quota observations and official daily usage as separate evidence.
- Privacy-safe token metadata from active and archived local Codex sessions.
- Reset epochs, collection health, recovery, pricing version, and reconciliation.

It does not upload prompts or chats, copy credentials, add telemetry, or invoke
a model to collect usage. Missing web, cloud, other-device, or deleted-session
activity remains explicitly unknown. API-equivalent cost is an estimate, not an
OpenAI bill or a documented subscription accounting formula.

## Uninstall

Ask Codex to run `uninstall.ps1` from this repository. It removes the recorder,
binary, PATH entry, and skill while preserving the local ledger by default.
Use `-RemoveData` only when you intentionally want to delete the ledger too.

## Build from source

The current release targets Windows and uses the pinned Rust toolchain:

```powershell
cargo test --all --locked
cargo build --release --locked
```

Licensed under MIT. This independent community project is not affiliated with
or endorsed by OpenAI.
