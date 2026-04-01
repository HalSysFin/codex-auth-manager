# Changelog

## 1.12.6 - 2026-04-01

- added bidirectional auth reconciliation on lease check-in
- detects fresher local `~/.codex/auth.json` and uploads it to Auth Manager
- rewrites the local auth file automatically when the manager has newer tokens
