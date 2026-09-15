# Codex Quota Ledger user repository

This is the public, use-oriented source repository.

- When the user asks to install on Windows, run `install.ps1` from this checkout.
- Do not require Rust when installing the published version; the installer uses
  the release binary and verifies its SHA-256 checksum.
- Do not read, copy, or print Codex credentials, prompts, chats, or raw JSONL.
- After installation, use the installed `codex-quota-ledger` commands or its
  Codex skill. Use `codex-quota-ledger open` for the local dashboard.
- Never describe API-equivalent estimates as an OpenAI bill or documented
  subscription quota formula.
