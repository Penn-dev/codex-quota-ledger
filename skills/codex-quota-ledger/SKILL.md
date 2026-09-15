---
name: codex-quota-ledger
description: Query an installed Codex Quota Ledger or open its local dashboard when the user asks about Codex quota, usage, reset windows, API-equivalent cost, reconciliation, or recorder health.
---

# Codex Quota Ledger

Use the installed ledger executable. Prefer `codex-quota-ledger` from `PATH`;
on Windows, fall back to
`%LOCALAPPDATA%\CodexQuotaLedger\codex-quota-ledger.exe`.

- Run only the smallest relevant read command: `status`, `estimate`, `capacity`,
  `reconcile`, `history`, `diagnose`, or `paths`.
- Use `open` when the user wants the local dashboard. It starts or reuses the
  loopback-only page and opens the browser.
- Do not inspect SQLite or Codex JSONL directly when the versioned commands can
  answer the request.
- Keep official quota, official daily usage, web observations, and local token
  events distinct. Treat missing data as unknown, never zero.
- Call API-equivalent cost an estimate, not an OpenAI bill or a documented
  subscription accounting formula.
- Never send prompts, credentials, raw sessions, or ledger data to a model or
  network service for collection.

If the executable is missing, tell the user Codex Quota Ledger is not installed
and direct them to the repository installer. Summarize command output in the
user's language and retain its confidence and missing-data qualifications.
