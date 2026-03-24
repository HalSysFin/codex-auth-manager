from __future__ import annotations

import json
import secrets
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .account_usage_store import (
    _as_utc,
    _connect,
    _ensure_schema,
    _parse_iso,
    _to_iso,
    get_runtime_settings,
)
from .config import settings


ACTIVE_LEASE_STATES = {"active", "rotation_required"}
UNASSIGNABLE_CREDENTIAL_STATES = {
    "leased",
    "exhausted",
    "cooldown",
    "revoked",
    "expired",
    "unavailable_for_assignment",
}
ROTATION_REASONS = {
    "approaching_utilization_threshold",
    "low_quota_remaining",
    "unhealthy_credential",
    "expiry_approaching",
    "admin_requested_rotation",
}


def _runtime_value(key: str, default: Any) -> Any:
    runtime = get_runtime_settings()
    return runtime.get(key, default)


def _runtime_bool(key: str, default: bool) -> bool:
    return bool(_runtime_value(key, default))


def _runtime_int(key: str, default: int) -> int:
    return int(_runtime_value(key, default))


def _runtime_float(key: str, default: float) -> float:
    return float(_runtime_value(key, default))




def _effective_rotation_policy(machine_id: str, agent_id: str) -> str:
    runtime = get_runtime_settings()
    machine_overrides = runtime.get("rotation_policy_by_machine") or {}
    if isinstance(machine_overrides, dict):
        machine_policy = str(machine_overrides.get(machine_id) or "").strip()
        if machine_policy in {"replacement_required_only", "recommended_or_required"}:
            return machine_policy
    agent_overrides = runtime.get("rotation_policy_by_agent") or {}
    if isinstance(agent_overrides, dict):
        agent_policy = str(agent_overrides.get(agent_id) or "").strip()
        if agent_policy in {"replacement_required_only", "recommended_or_required"}:
            return agent_policy
    default_policy = str(runtime.get("rotation_policy_default") or "").strip()
    if default_policy in {"replacement_required_only", "recommended_or_required"}:
        return default_policy
    return "replacement_required_only"

def initialize_lease_broker_store(db_path: Path | None = None) -> None:
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        _ensure_lease_broker_schema(conn)


def _ensure_lease_broker_schema(conn: Any) -> None:
    if getattr(conn, "_kind", "sqlite") == "postgres":
        _ensure_lease_broker_schema_postgres(conn)
        return
    _ensure_lease_broker_schema_sqlite(conn)


def _ensure_lease_broker_schema_postgres(conn: Any) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS broker_credentials (
            id TEXT PRIMARY KEY,
            label TEXT NOT NULL,
            state TEXT NOT NULL,
            utilization_pct DOUBLE PRECISION NULL,
            quota_remaining BIGINT NULL,
            health_score DOUBLE PRECISION NULL,
            weekly_reset_at TEXT NULL,
            last_assigned_at TEXT NULL,
            exhausted_at TEXT NULL,
            revoked_at TEXT NULL,
            cooldown_until TEXT NULL,
            metadata JSONB NULL,
            reset_confirmation_due_after TEXT NULL,
            reset_confirmed_at TEXT NULL,
            last_telemetry_at TEXT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS broker_leases (
            id TEXT PRIMARY KEY,
            credential_id TEXT NOT NULL,
            machine_id TEXT NOT NULL,
            agent_id TEXT NOT NULL,
            state TEXT NOT NULL,
            issued_at TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            renewed_at TEXT NULL,
            revoked_at TEXT NULL,
            released_at TEXT NULL,
            rotation_reason TEXT NULL,
            replacement_lease_id TEXT NULL,
            last_seen_at TEXT NULL,
            last_telemetry_at TEXT NULL,
            latest_utilization_pct DOUBLE PRECISION NULL,
            latest_quota_remaining BIGINT NULL,
            last_success_at TEXT NULL,
            last_error_at TEXT NULL,
            reason TEXT NULL,
            metadata JSONB NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS broker_lease_telemetry (
            id BIGSERIAL PRIMARY KEY,
            lease_id TEXT NOT NULL,
            credential_id TEXT NOT NULL,
            machine_id TEXT NOT NULL,
            agent_id TEXT NOT NULL,
            captured_at TEXT NOT NULL,
            requests_count BIGINT NULL,
            tokens_in BIGINT NULL,
            tokens_out BIGINT NULL,
            utilization_pct DOUBLE PRECISION NULL,
            quota_remaining BIGINT NULL,
            rate_limit_remaining BIGINT NULL,
            status TEXT NULL,
            last_success_at TEXT NULL,
            last_error_at TEXT NULL,
            error_rate_1h DOUBLE PRECISION NULL,
            metadata JSONB NULL,
            created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_broker_credentials_state ON broker_credentials(state)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_broker_credentials_last_assigned ON broker_credentials(last_assigned_at)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_broker_leases_credential_state ON broker_leases(credential_id, state)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_broker_leases_owner_state ON broker_leases(machine_id, agent_id, state)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_broker_lease_telemetry_lease_captured ON broker_lease_telemetry(lease_id, captured_at)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_broker_lease_telemetry_credential_captured ON broker_lease_telemetry(credential_id, captured_at)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_broker_lease_telemetry_machine_agent_captured ON broker_lease_telemetry(machine_id, agent_id, captured_at)"
    )
    conn.execute(
        "ALTER TABLE broker_credentials ADD COLUMN IF NOT EXISTS metadata JSONB NULL"
    )
    conn.execute(
        "ALTER TABLE broker_credentials ADD COLUMN IF NOT EXISTS reset_confirmation_due_after TEXT NULL"
    )
    conn.execute(
        "ALTER TABLE broker_credentials ADD COLUMN IF NOT EXISTS reset_confirmed_at TEXT NULL"
    )
    conn.execute(
        "ALTER TABLE broker_credentials ADD COLUMN IF NOT EXISTS last_telemetry_at TEXT NULL"
    )
    conn.execute(
        "ALTER TABLE broker_leases ADD COLUMN IF NOT EXISTS metadata JSONB NULL"
    )
    conn.execute(
        "ALTER TABLE broker_leases ADD COLUMN IF NOT EXISTS last_seen_at TEXT NULL"
    )


def _ensure_lease_broker_schema_sqlite(conn: Any) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS broker_credentials (
            id TEXT PRIMARY KEY,
            label TEXT NOT NULL,
            state TEXT NOT NULL,
            utilization_pct REAL NULL,
            quota_remaining INTEGER NULL,
            health_score REAL NULL,
            weekly_reset_at TEXT NULL,
            last_assigned_at TEXT NULL,
            exhausted_at TEXT NULL,
            revoked_at TEXT NULL,
            cooldown_until TEXT NULL,
            metadata TEXT NULL,
            reset_confirmation_due_after TEXT NULL,
            reset_confirmed_at TEXT NULL,
            last_telemetry_at TEXT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS broker_leases (
            id TEXT PRIMARY KEY,
            credential_id TEXT NOT NULL,
            machine_id TEXT NOT NULL,
            agent_id TEXT NOT NULL,
            state TEXT NOT NULL,
            issued_at TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            renewed_at TEXT NULL,
            revoked_at TEXT NULL,
            released_at TEXT NULL,
            rotation_reason TEXT NULL,
            replacement_lease_id TEXT NULL,
            last_seen_at TEXT NULL,
            last_telemetry_at TEXT NULL,
            latest_utilization_pct REAL NULL,
            latest_quota_remaining INTEGER NULL,
            last_success_at TEXT NULL,
            last_error_at TEXT NULL,
            reason TEXT NULL,
            metadata TEXT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS broker_lease_telemetry (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            lease_id TEXT NOT NULL,
            credential_id TEXT NOT NULL,
            machine_id TEXT NOT NULL,
            agent_id TEXT NOT NULL,
            captured_at TEXT NOT NULL,
            requests_count INTEGER NULL,
            tokens_in INTEGER NULL,
            tokens_out INTEGER NULL,
            utilization_pct REAL NULL,
            quota_remaining INTEGER NULL,
            rate_limit_remaining INTEGER NULL,
            status TEXT NULL,
            last_success_at TEXT NULL,
            last_error_at TEXT NULL,
            error_rate_1h REAL NULL,
            metadata TEXT NULL,
            created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_broker_credentials_state ON broker_credentials(state)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_broker_credentials_last_assigned ON broker_credentials(last_assigned_at)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_broker_leases_credential_state ON broker_leases(credential_id, state)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_broker_leases_owner_state ON broker_leases(machine_id, agent_id, state)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_broker_lease_telemetry_lease_captured ON broker_lease_telemetry(lease_id, captured_at)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_broker_lease_telemetry_credential_captured ON broker_lease_telemetry(credential_id, captured_at)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_broker_lease_telemetry_machine_agent_captured ON broker_lease_telemetry(machine_id, agent_id, captured_at)"
    )
    try:
        conn.execute("ALTER TABLE broker_leases ADD COLUMN last_seen_at TEXT NULL")
    except Exception:
        pass


def sync_broker_credential(
    *,
    credential_id: str,
    label: str,
    utilization_pct: float | None,
    quota_remaining: int | None,
    health_score: float | None,
    weekly_reset_at: str | None,
    last_telemetry_at: str | None,
    metadata: dict[str, Any] | None = None,
    now: datetime | None = None,
    db_path: Path | None = None,
) -> dict[str, Any]:
    now_dt = _as_utc(now)
    now_iso = _to_iso(now_dt)
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        _ensure_lease_broker_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            "SELECT * FROM broker_credentials WHERE id = ?",
            (credential_id,),
        ).fetchone()
        if row is None:
            conn.execute(
                """
                INSERT INTO broker_credentials (
                    id, label, state, utilization_pct, quota_remaining, health_score,
                    weekly_reset_at, last_assigned_at, exhausted_at, revoked_at,
                    cooldown_until, metadata, reset_confirmation_due_after,
                    reset_confirmed_at, last_telemetry_at, created_at, updated_at
                ) VALUES (?, ?, 'available', ?, ?, ?, ?, NULL, NULL, NULL, NULL, ?, NULL, NULL, ?, ?, ?)
                """,
                (
                    credential_id,
                    label,
                    _nullable_float(utilization_pct),
                    _nullable_int(quota_remaining),
                    _nullable_float(health_score),
                    weekly_reset_at,
                    _json_dump(metadata),
                    last_telemetry_at,
                    now_iso,
                    now_iso,
                ),
            )
        else:
            conn.execute(
                """
                UPDATE broker_credentials
                SET label = ?,
                    utilization_pct = ?,
                    quota_remaining = ?,
                    health_score = ?,
                    weekly_reset_at = ?,
                    metadata = ?,
                    last_telemetry_at = COALESCE(?, last_telemetry_at),
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    label,
                    _nullable_float(utilization_pct),
                    _nullable_int(quota_remaining),
                    _nullable_float(health_score),
                    weekly_reset_at,
                    _json_dump(metadata),
                    last_telemetry_at,
                    now_iso,
                    credential_id,
                ),
            )
        credential = dict(
            conn.execute("SELECT * FROM broker_credentials WHERE id = ?", (credential_id,)).fetchone()
        )
        lease_row = conn.execute(
            f"SELECT 1 FROM broker_leases WHERE credential_id = ? AND state IN ({','.join('?' for _ in ACTIVE_LEASE_STATES)}) LIMIT 1",
            (credential_id, *ACTIVE_LEASE_STATES),
        ).fetchone()
        credential = _reconcile_credential_row(
            conn,
            credential,
            has_active_lease=lease_row is not None,
            now_dt=now_dt,
        )
        return credential


def list_broker_credentials(db_path: Path | None = None) -> list[dict[str, Any]]:
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        _ensure_lease_broker_schema(conn)
        _reconcile_expired_active_leases(conn)
        rows = conn.execute(
            "SELECT * FROM broker_credentials ORDER BY label ASC, id ASC"
        ).fetchall()
        return [_decode_credential_row(dict(row)) for row in rows]


def list_active_broker_leases_by_credential(
    db_path: Path | None = None,
) -> dict[str, dict[str, Any]]:
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        _ensure_lease_broker_schema(conn)
        _reconcile_expired_active_leases(conn)
        rows = conn.execute(
            f"""
            SELECT *
            FROM broker_leases
            WHERE state IN ({','.join('?' for _ in ACTIVE_LEASE_STATES)})
            ORDER BY created_at DESC, id DESC
            """,
            tuple(ACTIVE_LEASE_STATES),
        ).fetchall()
        out: dict[str, dict[str, Any]] = {}
        for row in rows:
            lease = _decode_lease_row(dict(row))
            credential_id = str(lease.get("credential_id") or "").strip()
            if credential_id and credential_id not in out:
                out[credential_id] = lease
        return out


def list_broker_leases(
    *,
    active_only: bool = False,
    limit: int | None = None,
    db_path: Path | None = None,
) -> list[dict[str, Any]]:
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        _ensure_lease_broker_schema(conn)
        _reconcile_expired_active_leases(conn)
        where_sql = ""
        params: list[Any] = []
        if active_only:
            where_sql = f"WHERE state IN ({','.join('?' for _ in ACTIVE_LEASE_STATES)})"
            params.extend(sorted(ACTIVE_LEASE_STATES))
        limit_sql = ""
        if isinstance(limit, int) and limit > 0:
            limit_sql = " LIMIT ?"
            params.append(int(limit))
        rows = conn.execute(
            f"""
            SELECT *
            FROM broker_leases
            {where_sql}
            ORDER BY updated_at DESC, issued_at DESC, id DESC
            {limit_sql}
            """,
            tuple(params),
        ).fetchall()
        return [_decode_lease_row(dict(row)) for row in rows]


def get_broker_credential(
    credential_id: str,
    *,
    db_path: Path | None = None,
) -> dict[str, Any] | None:
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        _ensure_lease_broker_schema(conn)
        row = conn.execute(
            "SELECT * FROM broker_credentials WHERE id = ?",
            (credential_id,),
        ).fetchone()
        return _decode_credential_row(dict(row)) if row else None


def _reconcile_expired_active_leases(conn: Any, *, now: datetime | None = None) -> None:
    now_dt = _as_utc(now)
    rows = conn.execute(
        f"""
        SELECT *
        FROM broker_leases
        WHERE state IN ({','.join('?' for _ in ACTIVE_LEASE_STATES)})
        ORDER BY updated_at DESC, issued_at DESC, id DESC
        """,
        tuple(sorted(ACTIVE_LEASE_STATES)),
    ).fetchall()
    for row in rows:
        _expire_lease_if_needed(conn, dict(row), now_dt)


def acquire_broker_lease(
    *,
    machine_id: str,
    agent_id: str,
    requested_ttl_seconds: int | None = None,
    reason: str | None = None,
    now: datetime | None = None,
    db_path: Path | None = None,
) -> dict[str, Any]:
    now_dt = _as_utc(now)
    now_iso = _to_iso(now_dt)
    ttl_seconds = _lease_ttl_seconds(requested_ttl_seconds)
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        _ensure_lease_broker_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        _reconcile_expired_active_leases(conn, now=now_dt)

        existing_machine_lease = _select_existing_machine_lease(
            conn,
            machine_id=machine_id,
            now_dt=now_dt,
        )
        if existing_machine_lease is not None:
            conn.execute(
                "UPDATE broker_leases SET last_seen_at = ?, updated_at = ? WHERE id = ?",
                (now_iso, now_iso, existing_machine_lease["id"]),
            )
            existing_machine_lease = dict(
                conn.execute("SELECT * FROM broker_leases WHERE id = ?", (existing_machine_lease["id"],)).fetchone()
            )
            lease = _decode_lease_row(existing_machine_lease)
            return {"status": "ok", "reason": "existing_machine_lease_reused", "lease": lease}

        credential = _select_best_eligible_credential(conn, now_dt=now_dt)
        if credential is None:
            return {
                "status": "denied",
                "reason": "no_eligible_credentials_available",
                "lease": None,
            }
        lease_id = f"lease_{secrets.token_urlsafe(12)}"
        expires_at = _to_iso(now_dt + timedelta(seconds=ttl_seconds))
        conn.execute(
            """
            INSERT INTO broker_leases (
                id, credential_id, machine_id, agent_id, state, issued_at, expires_at,
                renewed_at, revoked_at, released_at, rotation_reason, replacement_lease_id, last_seen_at,
                last_telemetry_at, latest_utilization_pct, latest_quota_remaining,
                last_success_at, last_error_at, reason, metadata, created_at, updated_at
            ) VALUES (?, ?, ?, ?, 'active', ?, ?, NULL, NULL, NULL, NULL, NULL, ?, NULL, ?, ?, NULL, NULL, ?, NULL, ?, ?)
            """,
            (
                lease_id,
                credential["id"],
                machine_id,
                agent_id,
                now_iso,
                expires_at,
                now_iso,
                credential.get("utilization_pct"),
                credential.get("quota_remaining"),
                reason,
                now_iso,
                now_iso,
            ),
        )
        conn.execute(
            """
            UPDATE broker_credentials
            SET state = 'leased',
                last_assigned_at = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (now_iso, now_iso, credential["id"]),
        )
        lease = _decode_lease_row(
            dict(conn.execute("SELECT * FROM broker_leases WHERE id = ?", (lease_id,)).fetchone())
        )
        return {"status": "ok", "reason": None, "lease": lease}


def _select_existing_machine_lease(
    conn: Any,
    *,
    machine_id: str,
    now_dt: datetime,
) -> dict[str, Any] | None:
    rows = conn.execute(
        f"""
        SELECT * FROM broker_leases
        WHERE machine_id = ?
          AND state IN ({','.join('?' for _ in ACTIVE_LEASE_STATES)})
        ORDER BY updated_at DESC, issued_at DESC, id DESC
        """,
        (machine_id, *ACTIVE_LEASE_STATES),
    ).fetchall()
    for row in rows:
        lease = dict(row)
        _expire_lease_if_needed(conn, lease, now_dt)
        refreshed = conn.execute(
            "SELECT * FROM broker_leases WHERE id = ?",
            (lease["id"],),
        ).fetchone()
        if refreshed is None:
            continue
        lease = dict(refreshed)
        if lease.get("state") not in ACTIVE_LEASE_STATES:
            continue
        credential = _credential_for_lease(conn, lease)
        if _credential_is_usable_for_existing_lease(credential, now_dt=now_dt):
            return lease
    return None


def renew_broker_lease(
    *,
    lease_id: str,
    machine_id: str,
    agent_id: str,
    now: datetime | None = None,
    db_path: Path | None = None,
) -> dict[str, Any]:
    now_dt = _as_utc(now)
    now_iso = _to_iso(now_dt)
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        _ensure_lease_broker_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        lease = _owned_lease_or_error(conn, lease_id, machine_id, agent_id)
        if lease is None:
            return {"status": "denied", "reason": "lease_not_found_or_not_owned", "lease": None}
        credential = _credential_for_lease(conn, lease)
        _expire_lease_if_needed(conn, lease, now_dt)
        lease = dict(conn.execute("SELECT * FROM broker_leases WHERE id = ?", (lease_id,)).fetchone())
        if lease["state"] != "active":
            return {"status": "denied", "reason": f"lease_not_renewable:{lease['state']}", "lease": _decode_lease_row(lease)}
        if not _credential_is_usable_for_existing_lease(credential, now_dt=now_dt):
            return {"status": "denied", "reason": "credential_not_usable", "lease": _decode_lease_row(lease)}
        renewed_expires_at = _to_iso(now_dt + timedelta(seconds=_lease_ttl_seconds(None)))
        conn.execute(
            """
            UPDATE broker_leases
            SET renewed_at = ?, expires_at = ?, last_seen_at = ?, updated_at = ?
            WHERE id = ?
            """,
            (now_iso, renewed_expires_at, now_iso, now_iso, lease_id),
        )
        row = dict(conn.execute("SELECT * FROM broker_leases WHERE id = ?", (lease_id,)).fetchone())
        return {"status": "ok", "reason": None, "lease": _decode_lease_row(row)}


def release_broker_lease(
    *,
    lease_id: str,
    machine_id: str,
    agent_id: str,
    reason: str | None = None,
    now: datetime | None = None,
    db_path: Path | None = None,
) -> dict[str, Any]:
    now_dt = _as_utc(now)
    now_iso = _to_iso(now_dt)
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        _ensure_lease_broker_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        lease = _owned_lease_or_error(conn, lease_id, machine_id, agent_id)
        if lease is None:
            return {"status": "denied", "reason": "lease_not_found_or_not_owned", "lease": None}
        if lease["state"] not in ACTIVE_LEASE_STATES:
            return {"status": "denied", "reason": f"lease_not_releasable:{lease['state']}", "lease": _decode_lease_row(lease)}
        conn.execute(
            """
            UPDATE broker_leases
            SET state = 'released',
                released_at = ?,
                reason = COALESCE(?, reason),
                updated_at = ?
            WHERE id = ?
            """,
            (now_iso, reason, now_iso, lease_id),
        )
        credential = dict(conn.execute("SELECT * FROM broker_credentials WHERE id = ?", (lease["credential_id"],)).fetchone())
        updated = _reconcile_credential_row(conn, credential, has_active_lease=False, now_dt=now_dt)
        row = dict(conn.execute("SELECT * FROM broker_leases WHERE id = ?", (lease_id,)).fetchone())
        return {"status": "ok", "reason": None, "lease": _decode_lease_row(row), "credential": updated}


def record_broker_lease_telemetry(
    *,
    lease_id: str,
    machine_id: str,
    agent_id: str,
    captured_at: str,
    requests_count: int | None,
    tokens_in: int | None,
    tokens_out: int | None,
    utilization_pct: float | None,
    quota_remaining: int | None,
    rate_limit_remaining: int | None,
    status: str | None,
    last_success_at: str | None,
    last_error_at: str | None,
    error_rate_1h: float | None,
    metadata: dict[str, Any] | None = None,
    db_path: Path | None = None,
) -> dict[str, Any]:
    captured_dt = _parse_iso(captured_at)
    captured_iso = _to_iso(captured_dt)
    created_iso = _to_iso(datetime.now(timezone.utc))
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        _ensure_lease_broker_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        lease = _owned_lease_or_error(conn, lease_id, machine_id, agent_id)
        if lease is None:
            return {"status": "denied", "reason": "lease_not_found_or_not_owned", "lease": None}
        credential = dict(
            conn.execute("SELECT * FROM broker_credentials WHERE id = ?", (lease["credential_id"],)).fetchone()
        )
        conn.execute(
            """
            INSERT INTO broker_lease_telemetry (
                lease_id, credential_id, machine_id, agent_id, captured_at,
                requests_count, tokens_in, tokens_out, utilization_pct, quota_remaining,
                rate_limit_remaining, status, last_success_at, last_error_at, error_rate_1h,
                metadata, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                lease_id,
                lease["credential_id"],
                machine_id,
                agent_id,
                captured_iso,
                _nullable_int(requests_count),
                _nullable_int(tokens_in),
                _nullable_int(tokens_out),
                _nullable_float(utilization_pct),
                _nullable_int(quota_remaining),
                _nullable_int(rate_limit_remaining),
                status,
                last_success_at,
                last_error_at,
                _nullable_float(error_rate_1h),
                _json_dump(metadata),
                created_iso,
            ),
        )
        health_score = _derive_health_score(
            utilization_pct=utilization_pct,
            quota_remaining=quota_remaining,
            error_rate_1h=error_rate_1h,
            status=status,
        )
        conn.execute(
            """
            UPDATE broker_leases
            SET last_seen_at = ?,
                last_telemetry_at = ?,
                latest_utilization_pct = ?,
                latest_quota_remaining = ?,
                last_success_at = COALESCE(?, last_success_at),
                last_error_at = COALESCE(?, last_error_at),
                updated_at = ?
            WHERE id = ?
            """,
            (
                captured_iso,
                captured_iso,
                _nullable_float(utilization_pct),
                _nullable_int(quota_remaining),
                last_success_at,
                last_error_at,
                created_iso,
                lease_id,
            ),
        )
        credential_updates = {
            "utilization_pct": utilization_pct,
            "quota_remaining": quota_remaining,
            "health_score": health_score,
            "last_telemetry_at": captured_iso,
        }
        credential_row = _apply_telemetry_to_credential(
            conn,
            credential,
            lease_id=lease_id,
            captured_iso=captured_iso,
            utilization_pct=utilization_pct,
            quota_remaining=quota_remaining,
            status=status,
            last_success_at=last_success_at,
            last_error_at=last_error_at,
            updates=credential_updates,
            now_iso=created_iso,
        )
        lease_row = dict(conn.execute("SELECT * FROM broker_leases WHERE id = ?", (lease_id,)).fetchone())
        return {
            "status": "ok",
            "reason": None,
            "lease": _decode_lease_row(lease_row),
            "credential": credential_row,
        }


def rotate_broker_lease(
    *,
    lease_id: str,
    machine_id: str,
    agent_id: str,
    reason: str,
    now: datetime | None = None,
    db_path: Path | None = None,
) -> dict[str, Any]:
    if reason not in ROTATION_REASONS:
        return {"status": "denied", "reason": "invalid_rotation_reason", "lease": None}
    if not _runtime_bool("allow_client_initiated_rotation", settings.allow_client_initiated_rotation):
        return {"status": "denied", "reason": "client_rotation_disabled", "lease": None}
    now_dt = _as_utc(now)
    now_iso = _to_iso(now_dt)
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        _ensure_lease_broker_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        lease = _owned_lease_or_error(conn, lease_id, machine_id, agent_id)
        if lease is None:
            return {"status": "denied", "reason": "lease_not_found_or_not_owned", "lease": None}
        if lease["state"] not in ACTIVE_LEASE_STATES:
            return {"status": "denied", "reason": f"lease_not_rotatable:{lease['state']}", "lease": _decode_lease_row(lease)}
        replacement = _select_best_eligible_credential(
            conn,
            now_dt=now_dt,
            exclude_ids={lease["credential_id"]},
        )
        if replacement is None:
            return {"status": "denied", "reason": "no_eligible_credentials_available", "lease": _decode_lease_row(lease)}
        new_lease_id = f"lease_{secrets.token_urlsafe(12)}"
        expires_at = _to_iso(now_dt + timedelta(seconds=_lease_ttl_seconds(None)))
        conn.execute(
            """
            INSERT INTO broker_leases (
                id, credential_id, machine_id, agent_id, state, issued_at, expires_at,
                renewed_at, revoked_at, released_at, rotation_reason, replacement_lease_id, last_seen_at,
                last_telemetry_at, latest_utilization_pct, latest_quota_remaining,
                last_success_at, last_error_at, reason, metadata, created_at, updated_at
            ) VALUES (?, ?, ?, ?, 'active', ?, ?, NULL, NULL, NULL, ?, NULL, ?, NULL, ?, ?, NULL, NULL, ?, NULL, ?, ?)
            """,
            (
                new_lease_id,
                replacement["id"],
                machine_id,
                agent_id,
                now_iso,
                expires_at,
                reason,
                now_iso,
                replacement.get("utilization_pct"),
                replacement.get("quota_remaining"),
                reason,
                now_iso,
                now_iso,
            ),
        )
        conn.execute(
            """
            UPDATE broker_leases
            SET state = 'released',
                released_at = ?,
                rotation_reason = ?,
                replacement_lease_id = ?,
                reason = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (now_iso, reason, new_lease_id, reason, now_iso, lease_id),
        )
        conn.execute(
            "UPDATE broker_credentials SET state = 'leased', last_assigned_at = ?, updated_at = ? WHERE id = ?",
            (now_iso, now_iso, replacement["id"]),
        )
        old_credential = dict(conn.execute("SELECT * FROM broker_credentials WHERE id = ?", (lease["credential_id"],)).fetchone())
        _reconcile_credential_row(conn, old_credential, has_active_lease=False, now_dt=now_dt)
        new_lease = dict(conn.execute("SELECT * FROM broker_leases WHERE id = ?", (new_lease_id,)).fetchone())
        return {"status": "ok", "reason": None, "lease": _decode_lease_row(new_lease)}


def materialize_broker_lease(
    *,
    lease_id: str,
    machine_id: str,
    agent_id: str,
    now: datetime | None = None,
    db_path: Path | None = None,
) -> dict[str, Any]:
    now_dt = _as_utc(now)
    now_iso = _to_iso(now_dt)
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        _ensure_lease_broker_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        lease = _owned_lease_or_error(conn, lease_id, machine_id, agent_id)
        if lease is None:
            return {"status": "denied", "reason": "lease_not_found_or_not_owned", "lease": None}
        _expire_lease_if_needed(conn, lease, now_dt)
        lease = dict(conn.execute("SELECT * FROM broker_leases WHERE id = ?", (lease_id,)).fetchone())
        if lease["state"] not in ACTIVE_LEASE_STATES:
            return {"status": "denied", "reason": f"lease_not_materializable:{lease['state']}", "lease": _decode_lease_row(lease)}
        credential = _credential_for_lease(conn, lease)
        if not _credential_is_usable_for_existing_lease(credential, now_dt=now_dt):
            return {"status": "denied", "reason": "credential_not_usable", "lease": _decode_lease_row(lease)}
        metadata = lease.get("metadata")
        if not isinstance(metadata, dict):
            metadata = {}
        delivery_count = int(metadata.get("delivery_count") or 0) + 1
        metadata["delivery_count"] = delivery_count
        metadata["last_materialized_at"] = now_iso
        if not metadata.get("first_materialized_at"):
            metadata["first_materialized_at"] = now_iso
        conn.execute(
            """
            UPDATE broker_leases
            SET metadata = ?,
                last_seen_at = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (_json_dump(metadata), now_iso, now_iso, lease_id),
        )
        updated = dict(conn.execute("SELECT * FROM broker_leases WHERE id = ?", (lease_id,)).fetchone())
        return {"status": "ok", "reason": None, "lease": _decode_lease_row(updated)}


def mark_broker_credential_exhausted(
    credential_id: str,
    *,
    reason: str = "admin_marked_exhausted",
    now: datetime | None = None,
    db_path: Path | None = None,
) -> dict[str, Any] | None:
    now_dt = _as_utc(now)
    now_iso = _to_iso(now_dt)
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        _ensure_lease_broker_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute("SELECT * FROM broker_credentials WHERE id = ?", (credential_id,)).fetchone()
        if row is None:
            return None
        credential = dict(row)
        due_after = credential.get("weekly_reset_at")
        conn.execute(
            """
            UPDATE broker_credentials
            SET state = 'exhausted',
                exhausted_at = COALESCE(exhausted_at, ?),
                reset_confirmation_due_after = COALESCE(reset_confirmation_due_after, ?),
                updated_at = ?
            WHERE id = ?
            """,
            (now_iso, due_after, now_iso, credential_id),
        )
        _revoke_active_leases_for_credential(
            conn,
            credential_id=credential_id,
            reason=reason,
            revoked_at=now_iso,
        )
        updated = dict(conn.execute("SELECT * FROM broker_credentials WHERE id = ?", (credential_id,)).fetchone())
        return _decode_credential_row(updated)


def get_broker_lease(
    lease_id: str,
    *,
    db_path: Path | None = None,
) -> dict[str, Any] | None:
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        _ensure_lease_broker_schema(conn)
        row = conn.execute("SELECT * FROM broker_leases WHERE id = ?", (lease_id,)).fetchone()
        if row is None:
            return None
        return _decode_lease_row(dict(row))


def get_broker_lease_status(
    lease_id: str,
    *,
    now: datetime | None = None,
    db_path: Path | None = None,
) -> dict[str, Any] | None:
    now_dt = _as_utc(now)
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        _ensure_lease_broker_schema(conn)
        row = conn.execute("SELECT * FROM broker_leases WHERE id = ?", (lease_id,)).fetchone()
        if row is None:
            return None
        lease = dict(row)
        _expire_lease_if_needed(conn, lease, now_dt)
        lease = dict(conn.execute("SELECT * FROM broker_leases WHERE id = ?", (lease_id,)).fetchone())
        if lease["state"] in ACTIVE_LEASE_STATES:
            now_iso = _to_iso(now_dt)
            conn.execute(
                "UPDATE broker_leases SET last_seen_at = ?, updated_at = ? WHERE id = ?",
                (now_iso, now_iso, lease_id),
            )
            lease = dict(conn.execute("SELECT * FROM broker_leases WHERE id = ?", (lease_id,)).fetchone())
        credential = dict(conn.execute("SELECT * FROM broker_credentials WHERE id = ?", (lease["credential_id"],)).fetchone())
        credential = _decode_credential_row(credential)
        expires_at = _parse_iso(str(lease["expires_at"]))
        seconds_remaining = max(0, int((expires_at - now_dt).total_seconds()))
        rotation_recommended = bool(
            lease["state"] == "rotation_required"
            or (
                lease.get("latest_utilization_pct") is not None
                and float(lease["latest_utilization_pct"]) >= _runtime_float("rotation_request_threshold_percent", settings.rotation_request_threshold_percent)
            )
            or seconds_remaining <= _runtime_int("lease_renewal_min_remaining_seconds", settings.lease_renewal_min_remaining_seconds)
        )
        replacement_required = bool(
            credential["state"] in {"exhausted", "revoked", "expired", "unavailable_for_assignment"}
            or lease["state"] in {"rotation_required", "revoked", "expired"}
        )
        seconds_since_seen = _lease_seconds_since_seen(lease, now_dt)
        effective_rotation_policy = _effective_rotation_policy(str(lease["machine_id"]), str(lease["agent_id"]))
        return {
            "lease_id": lease["id"],
            "credential_id": lease["credential_id"],
            "state": lease["state"],
            "issued_at": lease["issued_at"],
            "expires_at": lease["expires_at"],
            "renewed_at": lease["renewed_at"],
            "machine_id": lease["machine_id"],
            "agent_id": lease["agent_id"],
            "latest_telemetry_at": lease["last_telemetry_at"],
            "latest_utilization_pct": lease["latest_utilization_pct"],
            "latest_quota_remaining": lease["latest_quota_remaining"],
            "last_success_at": lease["last_success_at"],
            "last_error_at": lease["last_error_at"],
            "rotation_recommended": rotation_recommended,
            "replacement_required": replacement_required,
            "reason": lease["reason"],
            "credential_state": credential["state"],
            "effective_rotation_policy": effective_rotation_policy,
            "last_seen_at": lease.get("last_seen_at"),
            "seconds_since_seen": seconds_since_seen,
            "is_stale": seconds_since_seen is not None and seconds_since_seen >= _runtime_int("lease_stale_after_seconds", settings.lease_stale_after_seconds),
        }


def reconcile_broker_leases(
    *,
    now: datetime | None = None,
    db_path: Path | None = None,
) -> int:
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        _ensure_lease_broker_schema(conn)
        before = conn.execute(
            f"SELECT COUNT(*) AS count FROM broker_leases WHERE state IN ({','.join('?' for _ in ACTIVE_LEASE_STATES)})",
            tuple(sorted(ACTIVE_LEASE_STATES)),
        ).fetchone()
        _reconcile_expired_active_leases(conn, now=now)
        after = conn.execute(
            f"SELECT COUNT(*) AS count FROM broker_leases WHERE state IN ({','.join('?' for _ in ACTIVE_LEASE_STATES)})",
            tuple(sorted(ACTIVE_LEASE_STATES)),
        ).fetchone()
        return max(int(before["count"]) - int(after["count"]), 0)


def list_broker_lease_telemetry(
    lease_id: str,
    *,
    db_path: Path | None = None,
) -> list[dict[str, Any]]:
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        _ensure_lease_broker_schema(conn)
        rows = conn.execute(
            "SELECT * FROM broker_lease_telemetry WHERE lease_id = ? ORDER BY captured_at ASC, id ASC",
            (lease_id,),
        ).fetchall()
        return [_decode_json_fields(dict(row), {"metadata"}) for row in rows]


def is_credential_assignable(
    credential: dict[str, Any],
    *,
    now: datetime | None = None,
) -> tuple[bool, str | None]:
    now_dt = _as_utc(now)
    state = str(credential.get("state") or "")
    if state in UNASSIGNABLE_CREDENTIAL_STATES:
        return False, f"state:{state}"
    if state == "degraded":
        return False, "state:degraded"
    utilization_pct = _nullable_float(credential.get("utilization_pct"))
    if utilization_pct is not None and utilization_pct >= _runtime_float("exhausted_utilization_percent", settings.exhausted_utilization_percent):
        return False, "utilization_exhausted"
    if utilization_pct is not None and utilization_pct >= _runtime_float("max_assignable_utilization_percent", settings.max_assignable_utilization_percent):
        return False, "utilization_above_assignment_threshold"
    quota_remaining = _nullable_int(credential.get("quota_remaining"))
    if quota_remaining is not None and quota_remaining <= _runtime_int("min_quota_remaining", settings.min_quota_remaining):
        return False, "quota_below_minimum"
    cooldown_until = credential.get("cooldown_until")
    if cooldown_until:
        if _parse_iso(str(cooldown_until)) > now_dt:
            return False, "cooldown_active"
    due_after = credential.get("reset_confirmation_due_after")
    if _runtime_bool("weekly_reset_confirmation_required", settings.weekly_reset_confirmation_required) and due_after:
        confirmed_at = credential.get("reset_confirmed_at")
        if not confirmed_at or _parse_iso(str(confirmed_at)) < _parse_iso(str(due_after)):
            return False, "reset_confirmation_pending"
    return True, None


def rank_credential_for_assignment(
    credential: dict[str, Any],
    *,
    now_dt: datetime,
) -> tuple[float, float, float, float, str]:
    weekly_reset_at = credential.get("weekly_reset_at")
    seconds_until_reset = float("inf")
    if weekly_reset_at:
        reset_dt = _parse_iso(str(weekly_reset_at))
        if reset_dt > now_dt:
            seconds_until_reset = (reset_dt - now_dt).total_seconds()
    quota = float(credential.get("quota_remaining") or float("inf"))
    utilization = float(credential.get("utilization_pct") or 0.0)
    health = float(credential.get("health_score") or 0.0)
    last_assigned = str(credential.get("last_assigned_at") or "")
    return (seconds_until_reset, quota, -utilization, -health, last_assigned)


def _select_best_eligible_credential(
    conn: Any,
    *,
    now_dt: datetime,
    exclude_ids: set[str] | None = None,
) -> dict[str, Any] | None:
    exclude = exclude_ids or set()
    rows = conn.execute("SELECT * FROM broker_credentials ORDER BY label ASC, id ASC").fetchall()
    eligible: list[dict[str, Any]] = []
    for row in rows:
        credential = _reconcile_credential_row(
            conn,
            dict(row),
            has_active_lease=_credential_has_active_lease(conn, str(row["id"])),
            now_dt=now_dt,
        )
        if credential["id"] in exclude:
            continue
        ok, _ = is_credential_assignable(credential, now=now_dt)
        if ok:
            eligible.append(credential)
    if not eligible:
        return None
    eligible.sort(key=lambda credential: rank_credential_for_assignment(credential, now_dt=now_dt))
    return eligible[0]


def _credential_has_active_lease(conn: Any, credential_id: str) -> bool:
    row = conn.execute(
        f"SELECT 1 FROM broker_leases WHERE credential_id = ? AND state IN ({','.join('?' for _ in ACTIVE_LEASE_STATES)}) LIMIT 1",
        (credential_id, *ACTIVE_LEASE_STATES),
    ).fetchone()
    return row is not None


def _credential_for_lease(conn: Any, lease: dict[str, Any]) -> dict[str, Any]:
    row = conn.execute(
        "SELECT * FROM broker_credentials WHERE id = ?",
        (lease["credential_id"],),
    ).fetchone()
    if row is None:
        raise KeyError(f"Credential not found for lease {lease['id']}")
    return _decode_credential_row(dict(row))


def _owned_lease_or_error(
    conn: Any,
    lease_id: str,
    machine_id: str,
    agent_id: str,
) -> dict[str, Any] | None:
    row = conn.execute("SELECT * FROM broker_leases WHERE id = ?", (lease_id,)).fetchone()
    if row is None:
        return None
    lease = dict(row)
    if lease["machine_id"] != machine_id or lease["agent_id"] != agent_id:
        return None
    return lease


def _credential_is_usable_for_existing_lease(
    credential: dict[str, Any],
    *,
    now_dt: datetime,
) -> bool:
    state = str(credential.get("state") or "")
    if state in {"exhausted", "revoked", "expired", "cooldown", "unavailable_for_assignment"}:
        return False
    due_after = credential.get("reset_confirmation_due_after")
    if _runtime_bool("weekly_reset_confirmation_required", settings.weekly_reset_confirmation_required) and due_after:
        confirmed_at = credential.get("reset_confirmed_at")
        if not confirmed_at or _parse_iso(str(confirmed_at)) < _parse_iso(str(due_after)):
            return False
    return True


def _expire_lease_if_needed(conn: Any, lease: dict[str, Any], now_dt: datetime) -> None:
    if lease["state"] not in ACTIVE_LEASE_STATES:
        return
    reason = None
    if _parse_iso(str(lease["expires_at"])) <= now_dt:
        reason = "lease_expired"
    else:
        last_seen_dt = _lease_last_seen_dt(lease)
        if last_seen_dt is not None:
            seconds_since_seen = int((now_dt - last_seen_dt).total_seconds())
            if seconds_since_seen >= _runtime_int("lease_reclaim_after_seconds", settings.lease_reclaim_after_seconds):
                reason = "lease_heartbeat_timeout"
    if reason is None:
        return
    now_iso = _to_iso(now_dt)
    conn.execute(
        """
        UPDATE broker_leases
        SET state = 'expired',
            reason = COALESCE(reason, ?),
            updated_at = ?
        WHERE id = ?
        """,
        (reason, now_iso, lease["id"]),
    )
    credential = dict(conn.execute("SELECT * FROM broker_credentials WHERE id = ?", (lease["credential_id"],)).fetchone())
    _reconcile_credential_row(conn, credential, has_active_lease=False, now_dt=now_dt)


def _apply_telemetry_to_credential(
    conn: Any,
    credential: dict[str, Any],
    *,
    lease_id: str,
    captured_iso: str,
    utilization_pct: float | None,
    quota_remaining: int | None,
    status: str | None,
    last_success_at: str | None,
    last_error_at: str | None,
    updates: dict[str, Any],
    now_iso: str,
) -> dict[str, Any]:
    weekly_reset_at = credential.get("weekly_reset_at")
    next_state = str(credential.get("state") or "available")
    due_after = credential.get("reset_confirmation_due_after")
    exhausted_at = credential.get("exhausted_at")
    reset_confirmed_at = credential.get("reset_confirmed_at")
    utilization = _nullable_float(utilization_pct)
    quota = _nullable_int(quota_remaining)
    if utilization is not None and utilization >= _runtime_float("exhausted_utilization_percent", settings.exhausted_utilization_percent):
        next_state = "exhausted"
        exhausted_at = exhausted_at or now_iso
        due_after = due_after or weekly_reset_at
        _revoke_active_leases_for_credential(
            conn,
            credential_id=str(credential["id"]),
            reason="credential_exhausted",
            revoked_at=now_iso,
        )
    elif (
        utilization is not None
        and utilization >= _runtime_float("rotation_request_threshold_percent", settings.rotation_request_threshold_percent)
    ) or (
        quota is not None and quota <= _runtime_int("min_quota_remaining", settings.min_quota_remaining)
    ):
        conn.execute(
            """
            UPDATE broker_leases
            SET state = CASE WHEN state = 'active' THEN 'rotation_required' ELSE state END,
                reason = COALESCE(reason, ?),
                updated_at = ?
            WHERE id = ?
            """,
            (
                "approaching_utilization_threshold"
                if utilization is not None and utilization >= _runtime_float("rotation_request_threshold_percent", settings.rotation_request_threshold_percent)
                else "low_quota_remaining",
                now_iso,
                lease_id,
            ),
        )
        if utilization is not None and utilization >= _runtime_float("max_assignable_utilization_percent", settings.max_assignable_utilization_percent):
            next_state = "unavailable_for_assignment"
            due_after = due_after or weekly_reset_at
    elif due_after and weekly_reset_at:
        due_dt = _parse_iso(str(due_after))
        captured_dt = _parse_iso(captured_iso)
        if (
            captured_dt >= due_dt
            and (utilization is None or utilization < _runtime_float("max_assignable_utilization_percent", settings.max_assignable_utilization_percent))
            and (quota is None or quota > _runtime_int("min_quota_remaining", settings.min_quota_remaining))
        ):
            next_state = "available"
            exhausted_at = None
            reset_confirmed_at = captured_iso
            due_after = None
    if status and status.lower() in {"error", "failed", "unhealthy"}:
        if next_state == "available":
            next_state = "degraded"
    elif next_state == "degraded":
        next_state = "available"
    if next_state == "available":
        next_state = "leased"
    conn.execute(
        """
        UPDATE broker_credentials
        SET utilization_pct = ?,
            quota_remaining = ?,
            health_score = ?,
            last_telemetry_at = ?,
            exhausted_at = ?,
            reset_confirmation_due_after = ?,
            reset_confirmed_at = ?,
            state = ?,
            updated_at = ?
        WHERE id = ?
        """,
        (
            _nullable_float(updates.get("utilization_pct")),
            _nullable_int(updates.get("quota_remaining")),
            _nullable_float(updates.get("health_score")),
            updates.get("last_telemetry_at"),
            exhausted_at,
            due_after,
            reset_confirmed_at,
            next_state,
            now_iso,
            credential["id"],
        ),
    )
    row = dict(conn.execute("SELECT * FROM broker_credentials WHERE id = ?", (credential["id"],)).fetchone())
    return _decode_credential_row(row)


def _revoke_active_leases_for_credential(
    conn: Any,
    *,
    credential_id: str,
    reason: str,
    revoked_at: str,
) -> None:
    conn.execute(
        f"""
        UPDATE broker_leases
        SET state = 'revoked',
            revoked_at = ?,
            reason = ?,
            updated_at = ?
        WHERE credential_id = ? AND state IN ({','.join('?' for _ in ACTIVE_LEASE_STATES)})
        """,
        (revoked_at, reason, revoked_at, credential_id, *ACTIVE_LEASE_STATES),
    )


def _reconcile_credential_row(
    conn: Any,
    credential: dict[str, Any],
    *,
    has_active_lease: bool,
    now_dt: datetime,
) -> dict[str, Any]:
    now_iso = _to_iso(now_dt)
    state = str(credential.get("state") or "available")
    utilization = _nullable_float(credential.get("utilization_pct"))
    quota = _nullable_int(credential.get("quota_remaining"))
    revoked_at = credential.get("revoked_at")
    cooldown_until = credential.get("cooldown_until")
    due_after = credential.get("reset_confirmation_due_after")
    reset_confirmed_at = credential.get("reset_confirmed_at")
    weekly_reset_at = credential.get("weekly_reset_at")
    if revoked_at:
        state = "revoked"
    elif cooldown_until and _parse_iso(str(cooldown_until)) > now_dt:
        state = "cooldown"
    elif due_after and _runtime_bool("weekly_reset_confirmation_required", settings.weekly_reset_confirmation_required):
        due_dt = _parse_iso(str(due_after))
        last_telemetry_at = credential.get("last_telemetry_at")
        if (
            not reset_confirmed_at
            and last_telemetry_at
            and _parse_iso(str(last_telemetry_at)) >= due_dt
            and (utilization is None or utilization < _runtime_float("max_assignable_utilization_percent", settings.max_assignable_utilization_percent))
            and (quota is None or quota > _runtime_int("min_quota_remaining", settings.min_quota_remaining))
        ):
            reset_confirmed_at = last_telemetry_at
            conn.execute(
                "UPDATE broker_credentials SET reset_confirmed_at = ?, updated_at = ? WHERE id = ?",
                (reset_confirmed_at, now_iso, credential["id"]),
            )
        confirmed_ok = bool(reset_confirmed_at and _parse_iso(str(reset_confirmed_at)) >= due_dt)
        if confirmed_ok and (utilization is None or utilization < _runtime_float("max_assignable_utilization_percent", settings.max_assignable_utilization_percent)) and (
            quota is None or quota > _runtime_int("min_quota_remaining", settings.min_quota_remaining)
        ):
            state = "available"
            due_after = None
        elif utilization is not None and utilization >= _runtime_float("exhausted_utilization_percent", settings.exhausted_utilization_percent):
            state = "exhausted"
        else:
            state = "unavailable_for_assignment"
    elif utilization is not None and utilization >= _runtime_float("exhausted_utilization_percent", settings.exhausted_utilization_percent):
        state = "exhausted"
        if weekly_reset_at:
            due_after = due_after or weekly_reset_at
    elif (
        utilization is not None and utilization >= _runtime_float("max_assignable_utilization_percent", settings.max_assignable_utilization_percent)
    ) or (
        quota is not None and quota <= _runtime_int("min_quota_remaining", settings.min_quota_remaining)
    ):
        state = "unavailable_for_assignment"
        if weekly_reset_at:
            due_after = due_after or weekly_reset_at
    elif state == "degraded":
        state = "degraded"
    else:
        state = "available"
    if has_active_lease and state == "available":
        state = "leased"
    conn.execute(
        """
        UPDATE broker_credentials
        SET state = ?,
            reset_confirmation_due_after = ?,
            updated_at = ?
        WHERE id = ?
        """,
        (state, due_after, now_iso, credential["id"]),
    )
    updated = dict(conn.execute("SELECT * FROM broker_credentials WHERE id = ?", (credential["id"],)).fetchone())
    return _decode_credential_row(updated)


def _derive_health_score(
    *,
    utilization_pct: float | None,
    quota_remaining: int | None,
    error_rate_1h: float | None,
    status: str | None,
) -> float:
    base = 100.0
    if utilization_pct is not None:
        base -= min(max(float(utilization_pct), 0.0), 100.0) * 0.6
    if quota_remaining is not None and quota_remaining > 0:
        base += min(float(quota_remaining) / 100000.0, 10.0)
    if error_rate_1h is not None:
        base -= min(max(float(error_rate_1h), 0.0), 1.0) * 40.0
    if status and status.lower() in {"error", "failed", "unhealthy"}:
        base -= 25.0
    return round(max(0.0, min(base, 100.0)), 2)


def _lease_ttl_seconds(requested_ttl_seconds: int | None) -> int:
    ttl = requested_ttl_seconds if requested_ttl_seconds is not None else _runtime_int("lease_default_ttl_seconds", settings.lease_default_ttl_seconds)
    return max(60, int(ttl))


def _decode_credential_row(row: dict[str, Any]) -> dict[str, Any]:
    return _decode_json_fields(row, {"metadata"})


def _decode_lease_row(row: dict[str, Any]) -> dict[str, Any]:
    out = _decode_json_fields(row, {"metadata"})
    now_dt = datetime.now(timezone.utc)
    seconds_since_seen = _lease_seconds_since_seen(out, now_dt)
    out["seconds_since_seen"] = seconds_since_seen
    out["is_stale"] = bool(
        seconds_since_seen is not None
        and seconds_since_seen >= _runtime_int("lease_stale_after_seconds", settings.lease_stale_after_seconds)
        and str(out.get("state") or "") in ACTIVE_LEASE_STATES
    )
    return out


def _lease_last_seen_dt(lease: dict[str, Any]) -> datetime | None:
    for field in ("last_seen_at", "last_telemetry_at", "renewed_at", "issued_at"):
        raw = lease.get(field)
        if raw:
            try:
                return _parse_iso(str(raw))
            except Exception:
                continue
    return None


def _lease_seconds_since_seen(lease: dict[str, Any], now_dt: datetime) -> int | None:
    last_seen_dt = _lease_last_seen_dt(lease)
    if last_seen_dt is None:
        return None
    return max(0, int((now_dt - last_seen_dt).total_seconds()))


def _decode_json_fields(row: dict[str, Any], fields: set[str]) -> dict[str, Any]:
    out = dict(row)
    for field in fields:
        raw = out.get(field)
        if raw is None or isinstance(raw, dict):
            continue
        try:
            out[field] = json.loads(str(raw))
        except Exception:
            out[field] = None
    return out


def _json_dump(value: dict[str, Any] | None) -> str | None:
    if value is None:
        return None
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


def _nullable_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return round(float(value), 2)
    except (TypeError, ValueError):
        return None


def _nullable_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
