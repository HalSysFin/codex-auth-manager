# Changelog

## 1.2.5 - 2026-04-01

- added bidirectional lease-auth reconciliation so managed clients can push fresher local `auth.json` back to Auth Manager and pull newer manager auth on the next check-in
- added `POST /api/leases/{lease_id}/reconcile-auth` and wired it into the shared lease runtime contract
- kept lease-backed auth materialization aligned across the VS Code, Cursor, Antigravity, desktop, and headless clients
- documented the release split for the monorepo and per-component packages
