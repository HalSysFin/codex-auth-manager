# Broker Lifecycle Testing

This repo keeps the broker lifecycle aligned across the backend and all client surfaces with a practical, scriptable test stack.

## Covered Scenarios

The current suite exercises the canonical lease lifecycle paths:

- acquire plus materialize
- startup with an existing healthy lease
- missing or stale lease lookup leading to reacquire
- revoked and expired lease handling
- `replacement_required` rotation behavior
- near-expiry renew behavior
- no eligible credentials denial handling
- backend unavailable degraded-state handling
- exhaustion-driven lease invalidation on the backend
- weekly reset confirmation behavior
- telemetry round-trip and latest-summary updates
- OpenClaw usage normalization and telemetry flushing
- auth-file rewrite after reacquire or rotate in the headless client

## Coverage By Surface

- `tests/test_lease_broker_store.py`
  Backend broker/store integration against a temporary SQLite test DB. This covers acquire, materialize, renew, rotate, revoke/exhaust, `replacement_required`, release, ownership validation, telemetry summary updates, and weekly reset confirmation behavior.
- `tests/test_cached_accounts_api.py`
  Backend API-level integration coverage for lease materialization and telemetry responses. This verifies that the FastAPI lease endpoints return `credential_material.auth_json` and expose updated lease summary fields after telemetry ingestion.
- `packages/lease-runtime/src/test/*.test.ts`
  Shared lifecycle parity tests for acquire/reacquire/rotate/renew/noop decisions, no-eligible/backend-unavailable error handling, auth materialization contract handling, and truthful telemetry shaping.
- `vscode-extension/src/test/*.test.ts`
  VS Code runtime/helper tests that verify the shared startup and health parity matrix, auth payload validation, auth-file writes, and persisted lease metadata behavior.
- `desktop-app/src/test/*.test.ts`
  Desktop runtime tests for the shared startup and health parity matrix, auth payload handling, and persisted state.
- `headless-client/src/test/*.test.ts`
  Headless runtime tests for status rendering, shared startup parity, auth file helpers, and end-to-end temp-file flows for acquire/materialize, revoked->reacquire rewrite, denied acquire, and backend-unavailable behavior.
- `openclaw-plugin/src/test/*.test.ts`
  OpenClaw usage normalization, telemetry posting, request-threshold flushing, stop-time flush behavior, and lease-context update behavior.

## What Remains Mocked

- GUI event loops and full VS Code/Tauri host integration are still covered through runtime/helper tests instead of full UI harnesses.
- Real OpenClaw runtime hook-up is still outside this repo. The plugin tests here prove the in-repo entry/service/flush behavior, but they do not pretend to execute the external OpenClaw runtime lifecycle.
- Backend API round-trips for TypeScript clients are covered with mocked fetch implementations rather than a live HTTP server.

## Running The Suite

From the repo root:

```bash
./scripts/run-broker-lifecycle-tests.sh
```

The runner tries the backend broker test in the local Python environment first. If local backend Python dependencies are missing, it falls back to the running `auth_manager-auth-manager-1` container when available.

Or run the pieces directly:

```bash
python3 -m unittest tests.test_lease_broker_store
npm --prefix packages/lease-runtime test
npm --prefix vscode-extension test
npm --prefix desktop-app test
npm --prefix headless-client test
npm --prefix openclaw-plugin test
```

## Notes

- The backend test uses the repo’s temporary SQLite broker store setup, so it does not require a running Postgres instance.
- The client suites expect their package dependencies to be installed first with `npm install` in each package directory.
- The root runner bootstraps missing package-local Node test dependencies automatically with `npm install --no-package-lock` before running the TypeScript suites.
