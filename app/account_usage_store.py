from __future__ import annotations

import calendar
import hashlib
import json
import re
import sqlite3
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .config import settings

try:  # Optional in tests that don't require Postgres.
    import psycopg
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover - fallback when psycopg is unavailable
    psycopg = None
    dict_row = None


DEFAULT_WINDOW_TYPE = "daily"


@dataclass
class AccountUsageState:
    id: str
    provider_account_id: str | None
    name: str | None
    rate_limit_window_type: str
    usage_limit: int
    usage_in_window: int
    rate_limit_refresh_at: str
    rate_limit_last_refreshed_at: str | None
    primary_used_percent: float | None
    primary_resets_at: str | None
    secondary_used_percent: float | None
    secondary_resets_at: str | None
    last_usage_sync_at: str | None
    lifetime_used: int
    created_at: str
    updated_at: str


def _db_url() -> str | None:
    value = getattr(settings, "database_url", None)
    if not value:
        return None
    text = str(value).strip()
    return text or None


def _is_postgres_configured() -> bool:
    url = _db_url()
    if not url:
        return False
    return url.startswith("postgres://") or url.startswith("postgresql://")


class _CompatConnection:
    def __init__(self, conn: Any, *, kind: str) -> None:
        self._conn = conn
        self._kind = kind

    def __enter__(self) -> "_CompatConnection":
        self._conn.__enter__()
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self._conn.__exit__(exc_type, exc, tb)

    def execute(self, sql: str, params: tuple[Any, ...] | list[Any] = ()) -> Any:
        query = sql
        values: tuple[Any, ...] | list[Any] = params
        if self._kind == "postgres":
            query = _translate_sql_to_postgres(sql)
        return self._conn.execute(query, values)


def _translate_sql_to_postgres(sql: str) -> str:
    query = sql
    upper = query.strip().upper()
    if upper.startswith("BEGIN IMMEDIATE"):
        return "BEGIN"
    if "PRAGMA " in upper:
        return "SELECT 1"
    if "INSERT OR IGNORE INTO" in upper:
        table_match = re.search(r"INSERT OR IGNORE INTO\s+([A-Za-z0-9_]+)", query, re.IGNORECASE)
        table = table_match.group(1).lower() if table_match else ""
        query = re.sub(r"INSERT OR IGNORE INTO", "INSERT INTO", query, flags=re.IGNORECASE)
        if table == "usage_rollovers":
            query = (
                query.rstrip()
                + " ON CONFLICT (account_id, window_started_at, window_ended_at) DO NOTHING"
            )
        elif table == "app_meta":
            query = query.rstrip() + " ON CONFLICT (key) DO NOTHING"
        else:
            query = query.rstrip() + " ON CONFLICT DO NOTHING"
    query = query.replace("?", "%s")
    return query


def initialize_usage_store(db_path: Path | None = None) -> None:
    with _connect(db_path) as conn:
        _ensure_schema(conn)


def ensure_account(
    account_id: str,
    now: datetime | None = None,
    *,
    provider_account_id: str | None = None,
    name: str | None = None,
    rate_limit_window_type: str | None = None,
    usage_limit: int | None = None,
    rate_limit_refresh_at: str | None = None,
    db_path: Path | None = None,
) -> AccountUsageState:
    now_dt = _as_utc(now)
    now_iso = _to_iso(now_dt)
    window_type = _normalize_window_type(rate_limit_window_type)
    refresh_iso = rate_limit_refresh_at or _to_iso(_next_boundary(now_dt, window_type))
    safe_usage_limit = max(int(usage_limit), 0) if usage_limit is not None else 0

    with _connect(db_path) as conn:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        row = _get_account_row(conn, account_id)
        if row is None:
            conn.execute(
                """
                INSERT INTO accounts (
                    id,
                    provider_account_id,
                    name,
                    rate_limit_window_type,
                    usage_limit,
                    usage_in_window,
                    rate_limit_refresh_at,
                    rate_limit_last_refreshed_at,
                    last_usage_sync_at,
                    lifetime_used,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, NULL, 0, ?, ?)
                """,
                (
                    account_id,
                    provider_account_id,
                    name,
                    window_type,
                    safe_usage_limit,
                    0,
                    refresh_iso,
                    now_iso,
                    now_iso,
                ),
            )
        else:
            updates: list[str] = []
            params: list[Any] = []
            if provider_account_id is not None and provider_account_id != row["provider_account_id"]:
                updates.append("provider_account_id = ?")
                params.append(provider_account_id)
            if name is not None and name != row["name"]:
                updates.append("name = ?")
                params.append(name)
            if (
                rate_limit_window_type is not None
                and window_type != row["rate_limit_window_type"]
            ):
                updates.append("rate_limit_window_type = ?")
                params.append(window_type)
            if usage_limit is not None and safe_usage_limit != int(row["usage_limit"]):
                updates.append("usage_limit = ?")
                params.append(safe_usage_limit)
            if rate_limit_refresh_at is not None and refresh_iso != row["rate_limit_refresh_at"]:
                updates.append("rate_limit_refresh_at = ?")
                params.append(refresh_iso)
            if updates:
                updates.append("updated_at = ?")
                params.append(now_iso)
                params.append(account_id)
                conn.execute(
                    f"UPDATE accounts SET {', '.join(updates)} WHERE id = ?",
                    tuple(params),
                )

        refreshed = _refresh_account_window_if_needed_locked(conn, account_id, now_dt)
        return _row_to_state(refreshed)


def get_account(account_id: str, db_path: Path | None = None) -> AccountUsageState | None:
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        row = _get_account_row(conn, account_id)
        if row is None:
            return None
        return _row_to_state(row)


def get_accounts_by_ids(
    account_ids: list[str], db_path: Path | None = None
) -> dict[str, AccountUsageState]:
    if not account_ids:
        return {}

    placeholders = ",".join("?" for _ in account_ids)
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        rows = conn.execute(
            f"SELECT * FROM accounts WHERE id IN ({placeholders})",
            tuple(account_ids),
        ).fetchall()
        return {str(row["id"]): _row_to_state(row) for row in rows}


def refresh_account_window_if_needed(
    account_id: str, now: datetime | None = None, db_path: Path | None = None
) -> AccountUsageState:
    now_dt = _as_utc(now)
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        row = _get_account_row(conn, account_id)
        if row is None:
            raise KeyError(f"Account not found: {account_id}")
        updated = _refresh_account_window_if_needed_locked(conn, account_id, now_dt)
        return _row_to_state(updated)


def record_account_usage(
    account_id: str,
    usage_delta: int,
    now: datetime | None = None,
    db_path: Path | None = None,
) -> AccountUsageState:
    now_dt = _as_utc(now)
    now_iso = _to_iso(now_dt)
    delta = int(usage_delta)
    if delta < 0:
        raise ValueError("usage_delta must be >= 0")

    with _connect(db_path) as conn:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        row = _get_account_row(conn, account_id)
        if row is None:
            raise KeyError(f"Account not found: {account_id}")

        refreshed = _refresh_account_window_if_needed_locked(conn, account_id, now_dt)
        next_window = int(refreshed["usage_in_window"]) + delta
        next_lifetime = int(refreshed["lifetime_used"]) + delta

        conn.execute(
            """
            UPDATE accounts
            SET usage_in_window = ?,
                lifetime_used = ?,
                last_usage_sync_at = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (next_window, next_lifetime, now_iso, now_iso, account_id),
        )
        final_row = _get_account_row(conn, account_id)
        if final_row is None:
            raise KeyError(f"Account not found after update: {account_id}")
        return _row_to_state(final_row)


def sync_account_usage_snapshot(
    account_id: str,
    *,
    usage_limit: int | None,
    usage_used: int | None,
    rate_limit_window_type: str | None = None,
    rate_limit_refresh_at: str | None = None,
    provider_account_id: str | None = None,
    name: str | None = None,
    now: datetime | None = None,
    db_path: Path | None = None,
) -> AccountUsageState:
    now_dt = _as_utc(now)
    now_iso = _to_iso(now_dt)

    with _connect(db_path) as conn:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        row = _get_account_row(conn, account_id)
        if row is None:
            seed_window_type = rate_limit_window_type or DEFAULT_WINDOW_TYPE
            seed_refresh = rate_limit_refresh_at or _to_iso(
                _next_boundary(now_dt, _normalize_window_type(seed_window_type))
            )
            conn.execute(
                """
                INSERT INTO accounts (
                    id,
                    provider_account_id,
                    name,
                    rate_limit_window_type,
                    usage_limit,
                    usage_in_window,
                    rate_limit_refresh_at,
                    rate_limit_last_refreshed_at,
                    last_usage_sync_at,
                    lifetime_used,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, 0, 0, ?, NULL, NULL, 0, ?, ?)
                """,
                (account_id, provider_account_id, name, _normalize_window_type(seed_window_type), seed_refresh, now_iso, now_iso),
            )

        refreshed = _refresh_account_window_if_needed_locked(conn, account_id, now_dt)
        current_in_window = int(refreshed["usage_in_window"])
        current_lifetime = int(refreshed["lifetime_used"])
        next_in_window = current_in_window
        next_lifetime = current_lifetime

        updates: list[str] = []
        params: list[Any] = []

        if provider_account_id is not None and provider_account_id != refreshed["provider_account_id"]:
            updates.append("provider_account_id = ?")
            params.append(provider_account_id)
        if name is not None and name != refreshed["name"]:
            updates.append("name = ?")
            params.append(name)
        if rate_limit_window_type is not None:
            window_type = _normalize_window_type(rate_limit_window_type)
            if window_type != refreshed["rate_limit_window_type"]:
                updates.append("rate_limit_window_type = ?")
                params.append(window_type)
        if rate_limit_refresh_at is not None and rate_limit_refresh_at != refreshed["rate_limit_refresh_at"]:
            updates.append("rate_limit_refresh_at = ?")
            params.append(rate_limit_refresh_at)
        if usage_limit is not None:
            safe_limit = max(int(usage_limit), 0)
            if safe_limit != int(refreshed["usage_limit"]):
                updates.append("usage_limit = ?")
                params.append(safe_limit)
        if usage_used is not None:
            safe_used = max(int(usage_used), 0)
            if safe_used >= current_in_window:
                delta = safe_used - current_in_window
                next_in_window = safe_used
                next_lifetime = current_lifetime + delta
            else:
                # Snapshot likely reflects a provider-side reset already applied.
                next_in_window = safe_used
            updates.append("usage_in_window = ?")
            params.append(next_in_window)
            updates.append("lifetime_used = ?")
            params.append(next_lifetime)

        updates.append("last_usage_sync_at = ?")
        params.append(now_iso)
        updates.append("updated_at = ?")
        params.append(now_iso)
        params.append(account_id)
        conn.execute(
            f"UPDATE accounts SET {', '.join(updates)} WHERE id = ?",
            tuple(params),
        )
        final_row = _get_account_row(conn, account_id)
        if final_row is None:
            raise KeyError(f"Account not found after sync: {account_id}")
        conn.execute(
            """
            INSERT INTO usage_absolute_snapshots (
                account_id,
                captured_at,
                usage_in_window,
                usage_limit,
                lifetime_used,
                rate_limit_refresh_at,
                primary_used_percent,
                secondary_used_percent
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                account_id,
                now_iso,
                int(final_row["usage_in_window"]),
                int(final_row["usage_limit"]),
                int(final_row["lifetime_used"]),
                str(final_row["rate_limit_refresh_at"]) if final_row["rate_limit_refresh_at"] is not None else None,
                float(final_row["primary_used_percent"]) if final_row["primary_used_percent"] is not None else None,
                float(final_row["secondary_used_percent"]) if final_row["secondary_used_percent"] is not None else None,
            ),
        )
        return _row_to_state(final_row)


def sync_account_rate_limit_percentages(
    account_id: str,
    *,
    primary_used_percent: float | None,
    primary_resets_at: str | None = None,
    secondary_used_percent: float | None = None,
    secondary_resets_at: str | None = None,
    provider_account_id: str | None = None,
    name: str | None = None,
    now: datetime | None = None,
    db_path: Path | None = None,
) -> AccountUsageState:
    now_dt = _as_utc(now)
    now_iso = _to_iso(now_dt)

    def _clamp_percent(value: float | None) -> float | None:
        if value is None:
            return None
        try:
            val = float(value)
        except (TypeError, ValueError):
            return None
        return max(0.0, min(100.0, val))

    p1 = _clamp_percent(primary_used_percent)
    p2 = _clamp_percent(secondary_used_percent)

    with _connect(db_path) as conn:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        row = _get_account_row(conn, account_id)
        if row is None:
            conn.execute(
                """
                INSERT INTO accounts (
                    id,
                    provider_account_id,
                    name,
                    rate_limit_window_type,
                    usage_limit,
                    usage_in_window,
                    rate_limit_refresh_at,
                    rate_limit_last_refreshed_at,
                    primary_used_percent,
                    primary_resets_at,
                    secondary_used_percent,
                    secondary_resets_at,
                    last_usage_sync_at,
                    lifetime_used,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, 0, 0, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?)
                """,
                (
                    account_id,
                    provider_account_id,
                    name,
                    DEFAULT_WINDOW_TYPE,
                    _to_iso(_next_boundary(now_dt, DEFAULT_WINDOW_TYPE)),
                    now_iso,
                    p1,
                    primary_resets_at,
                    p2,
                    secondary_resets_at,
                    now_iso,
                    now_iso,
                    now_iso,
                ),
            )
        else:
            updates: list[str] = []
            params: list[Any] = []
            if provider_account_id is not None and provider_account_id != row["provider_account_id"]:
                updates.append("provider_account_id = ?")
                params.append(provider_account_id)
            if name is not None and name != row["name"]:
                updates.append("name = ?")
                params.append(name)
            updates.append("primary_used_percent = ?")
            params.append(p1)
            if primary_resets_at is not None:
                updates.append("primary_resets_at = ?")
                params.append(primary_resets_at)
            updates.append("secondary_used_percent = ?")
            params.append(p2)
            if secondary_resets_at is not None:
                updates.append("secondary_resets_at = ?")
                params.append(secondary_resets_at)
            updates.append("rate_limit_last_refreshed_at = ?")
            params.append(now_iso)
            updates.append("last_usage_sync_at = ?")
            params.append(now_iso)
            updates.append("updated_at = ?")
            params.append(now_iso)
            params.append(account_id)
            conn.execute(
                f"UPDATE accounts SET {', '.join(updates)} WHERE id = ?",
                tuple(params),
            )

        final_row = _get_account_row(conn, account_id)
        if final_row is None:
            raise KeyError(f"Account not found after sync: {account_id}")
        conn.execute(
            """
            INSERT INTO usage_absolute_snapshots (
                account_id,
                captured_at,
                usage_in_window,
                usage_limit,
                lifetime_used,
                rate_limit_refresh_at,
                primary_used_percent,
                secondary_used_percent
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                account_id,
                now_iso,
                int(final_row["usage_in_window"]),
                int(final_row["usage_limit"]),
                int(final_row["lifetime_used"]),
                str(final_row["rate_limit_refresh_at"]) if final_row["rate_limit_refresh_at"] is not None else None,
                float(final_row["primary_used_percent"]) if final_row["primary_used_percent"] is not None else None,
                float(final_row["secondary_used_percent"]) if final_row["secondary_used_percent"] is not None else None,
            ),
        )
        return _row_to_state(final_row)


def reconcile_due_accounts(
    now: datetime | None = None, db_path: Path | None = None
) -> int:
    now_dt = _as_utc(now)
    now_iso = _to_iso(now_dt)
    refreshed = 0

    with _connect(db_path) as conn:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        due_rows = conn.execute(
            "SELECT id FROM accounts WHERE rate_limit_refresh_at <= ?",
            (now_iso,),
        ).fetchall()
        for row in due_rows:
            _refresh_account_window_if_needed_locked(conn, str(row["id"]), now_dt)
            refreshed += 1
    return refreshed


def list_usage_rollovers(
    account_id: str, db_path: Path | None = None
) -> list[dict[str, Any]]:
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        rows = conn.execute(
            """
            SELECT id, account_id, window_started_at, window_ended_at, usage_limit, usage_used, usage_wasted,
                   primary_percent_at_reset, secondary_percent_at_reset, rolled_over_at, window_type
            FROM usage_rollovers
            WHERE account_id = ?
            ORDER BY window_ended_at ASC, id ASC
            """,
            (account_id,),
        ).fetchall()
        return [dict(row) for row in rows]


def record_percentage_snapshot(
    account_id: str,
    primary_used_percent: float | None,
    secondary_used_percent: float | None,
    now: datetime | None = None,
    db_path: Path | None = None,
) -> None:
    """Record a periodic snapshot of utilization percentages for time-series graphing."""
    now_dt = _as_utc(now)
    now_iso = _to_iso(now_dt)
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        conn.execute(
            """
            INSERT INTO usage_snapshots (account_id, primary_used_percent, secondary_used_percent, captured_at)
            VALUES (?, ?, ?, ?)
            """,
            (account_id, primary_used_percent, secondary_used_percent, now_iso),
        )


def list_usage_snapshots(
    account_id: str | None = None,
    hours: int = 168,
    db_path: Path | None = None,
) -> list[dict[str, Any]]:
    """Return percentage snapshots for an account (or all accounts) over the last N hours."""
    cutoff = _to_iso(_as_utc(None) - timedelta(hours=hours))
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        if account_id:
            rows = conn.execute(
                "SELECT * FROM usage_snapshots WHERE account_id = ? AND captured_at >= ? ORDER BY captured_at ASC",
                (account_id, cutoff),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM usage_snapshots WHERE captured_at >= ? ORDER BY captured_at ASC",
                (cutoff,),
            ).fetchall()
        return [dict(row) for row in rows]


def record_absolute_usage_snapshot(
    account_id: str,
    *,
    usage_in_window: int | None,
    usage_limit: int | None,
    lifetime_used: int | None,
    rate_limit_refresh_at: str | None,
    primary_used_percent: float | None = None,
    secondary_used_percent: float | None = None,
    now: datetime | None = None,
    db_path: Path | None = None,
) -> None:
    now_iso = _to_iso(_as_utc(now))
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        conn.execute(
            """
            INSERT INTO usage_absolute_snapshots (
                account_id,
                captured_at,
                usage_in_window,
                usage_limit,
                lifetime_used,
                rate_limit_refresh_at,
                primary_used_percent,
                secondary_used_percent
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                account_id,
                now_iso,
                int(usage_in_window) if usage_in_window is not None else None,
                int(usage_limit) if usage_limit is not None else None,
                int(lifetime_used) if lifetime_used is not None else None,
                rate_limit_refresh_at,
                float(primary_used_percent) if primary_used_percent is not None else None,
                float(secondary_used_percent) if secondary_used_percent is not None else None,
            ),
        )


def list_absolute_usage_snapshots(
    *,
    account_id: str | None = None,
    since_iso: str | None = None,
    db_path: Path | None = None,
) -> list[dict[str, Any]]:
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        clauses: list[str] = []
        params: list[Any] = []
        if account_id:
            clauses.append("account_id = ?")
            params.append(account_id)
        if since_iso:
            clauses.append("captured_at >= ?")
            params.append(since_iso)

        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = conn.execute(
            f"""
            SELECT id, account_id, captured_at, usage_in_window, usage_limit, lifetime_used,
                   rate_limit_refresh_at, primary_used_percent, secondary_used_percent
            FROM usage_absolute_snapshots
            {where_sql}
            ORDER BY captured_at ASC, id ASC
            """,
            tuple(params),
        ).fetchall()
        return [dict(row) for row in rows]


def delete_account_data(account_id: str, db_path: Path | None = None) -> None:
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("DELETE FROM usage_rollovers WHERE account_id = ?", (account_id,))
        conn.execute("DELETE FROM accounts WHERE id = ?", (account_id,))


def rename_account_data(
    old_account_id: str,
    new_account_id: str,
    db_path: Path | None = None,
) -> bool:
    old_id = (old_account_id or "").strip()
    new_id = (new_account_id or "").strip()
    if not old_id or not new_id or old_id == new_id:
        return False

    with _connect(db_path) as conn:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        exists_old = conn.execute(
            "SELECT 1 FROM accounts WHERE id = ?",
            (old_id,),
        ).fetchone()
        if exists_old is None:
            return False
        exists_new = conn.execute(
            "SELECT 1 FROM accounts WHERE id = ?",
            (new_id,),
        ).fetchone()
        if exists_new is not None:
            raise ValueError(f"Account data for '{new_id}' already exists")

        conn.execute(
            "UPDATE usage_rollovers SET account_id = ? WHERE account_id = ?",
            (new_id, old_id),
        )
        conn.execute(
            "UPDATE accounts SET id = ?, updated_at = ? WHERE id = ?",
            (new_id, _to_iso(datetime.now(timezone.utc)), old_id),
        )
    return True


def merge_account_data(
    from_account_id: str,
    into_account_id: str,
    db_path: Path | None = None,
) -> bool:
    source_id = (from_account_id or "").strip()
    target_id = (into_account_id or "").strip()
    if not source_id or not target_id or source_id == target_id:
        return False

    with _connect(db_path) as conn:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")

        source = _get_account_row(conn, source_id)
        if source is None:
            return False
        target = _get_account_row(conn, target_id)
        if target is None:
            conn.execute("UPDATE usage_rollovers SET account_id = ? WHERE account_id = ?", (target_id, source_id))
            conn.execute("UPDATE accounts SET id = ?, updated_at = ? WHERE id = ?", (target_id, _to_iso(datetime.now(timezone.utc)), source_id))
            return True

        now_iso = _to_iso(datetime.now(timezone.utc))
        merged_limit = max(int(source["usage_limit"]), int(target["usage_limit"]))
        merged_window = max(int(source["usage_in_window"]), int(target["usage_in_window"]))
        merged_lifetime = max(int(source["lifetime_used"]), int(target["lifetime_used"]))
        merged_refresh = str(target["rate_limit_refresh_at"]) if str(target["rate_limit_refresh_at"]) >= str(source["rate_limit_refresh_at"]) else str(source["rate_limit_refresh_at"])
        merged_window_type = str(target["rate_limit_window_type"]) or str(source["rate_limit_window_type"])
        merged_provider = target["provider_account_id"] or source["provider_account_id"]
        merged_name = target["name"] or source["name"]
        merged_last_refreshed = target["rate_limit_last_refreshed_at"] or source["rate_limit_last_refreshed_at"]
        merged_last_sync = target["last_usage_sync_at"] or source["last_usage_sync_at"]
        merged_created = target["created_at"] if str(target["created_at"]) <= str(source["created_at"]) else source["created_at"]

        conn.execute(
            """
            UPDATE accounts
            SET provider_account_id = ?,
                name = ?,
                rate_limit_window_type = ?,
                usage_limit = ?,
                usage_in_window = ?,
                rate_limit_refresh_at = ?,
                rate_limit_last_refreshed_at = ?,
                last_usage_sync_at = ?,
                lifetime_used = ?,
                created_at = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (
                merged_provider,
                merged_name,
                merged_window_type,
                merged_limit,
                merged_window,
                merged_refresh,
                merged_last_refreshed,
                merged_last_sync,
                merged_lifetime,
                merged_created,
                now_iso,
                target_id,
            ),
        )
        conn.execute(
            """
            INSERT OR IGNORE INTO usage_rollovers (
                account_id,
                window_started_at,
                window_ended_at,
                usage_limit,
                usage_used,
                usage_wasted,
                rolled_over_at
            )
            SELECT ?, window_started_at, window_ended_at, usage_limit, usage_used, usage_wasted, rolled_over_at
            FROM usage_rollovers
            WHERE account_id = ?
            """,
            (target_id, source_id),
        )
        conn.execute("DELETE FROM usage_rollovers WHERE account_id = ?", (source_id,))
        conn.execute("DELETE FROM accounts WHERE id = ?", (source_id,))
    return True


def migrate_account_ids(id_map: dict[str, str], db_path: Path | None = None) -> int:
    changed = 0
    for old_id, new_id in id_map.items():
        old = (old_id or "").strip()
        new = (new_id or "").strip()
        if not old or not new or old == new:
            continue
        if merge_account_data(old, new, db_path=db_path):
            changed += 1
    return changed


def reconcile_legacy_account_aliases(db_path: Path | None = None) -> int:
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        profiles = conn.execute(
            "SELECT label, account_key, email FROM saved_profiles ORDER BY label ASC"
        ).fetchall()
        existing_accounts = {
            str(row["id"])
            for row in conn.execute("SELECT id FROM accounts").fetchall()
        }

    id_map: dict[str, str] = {}
    for row in profiles:
        label = str(row["label"] or "").strip()
        account_key = str(row["account_key"] or "").strip()
        email = str(row["email"] or "").strip()
        if not label or not account_key:
            continue
        label_alias = f"acct:{label}"
        if label_alias in existing_accounts and label_alias != account_key:
            id_map[label_alias] = account_key
        if email:
            email_alias = f"email:{email}"
            if email_alias in existing_accounts and email_alias != account_key:
                id_map[email_alias] = account_key

    if not id_map:
        return 0
    return migrate_account_ids(id_map, db_path=db_path)


def upsert_saved_profile(
    *,
    label: str,
    account_key: str,
    auth_json: dict[str, Any],
    email: str | None = None,
    name: str | None = None,
    subject: str | None = None,
    user_id: str | None = None,
    provider_account_id: str | None = None,
    access_token_expires_at: str | None = None,
    id_token_expires_at: str | None = None,
    refresh_token_expires_at: str | None = None,
    last_refresh_at: str | None = None,
    refresh_token_present: bool | None = None,
    reauth_required: bool | None = None,
    reauth_reason: str | None = None,
    now: datetime | None = None,
    db_path: Path | None = None,
) -> None:
    clean_label = (label or "").strip()
    if not clean_label:
        raise ValueError("label is required")
    clean_account_key = (account_key or "").strip() or "unknown"
    now_iso = _to_iso(_as_utc(now))
    auth_payload = json.dumps(auth_json, sort_keys=True)
    if (
        access_token_expires_at is None
        and id_token_expires_at is None
        and refresh_token_expires_at is None
        and last_refresh_at is None
        and refresh_token_present is None
    ):
        from .oauth_flow import extract_auth_db_metadata

        metadata = extract_auth_db_metadata(auth_json)
        access_token_expires_at = metadata.get("access_token_expires_at")
        id_token_expires_at = metadata.get("id_token_expires_at")
        refresh_token_expires_at = metadata.get("refresh_token_expires_at")
        last_refresh_at = metadata.get("last_refresh_at")
        refresh_token_present = metadata.get("refresh_token_present")
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        # Keep a single canonical row per concrete account identity.
        # This avoids stale duplicate labels representing the same user.
        if clean_account_key and clean_account_key != "unknown":
            conn.execute(
                "DELETE FROM saved_profiles WHERE account_key = ? AND label <> ?",
                (clean_account_key, clean_label),
            )
        existing = conn.execute("SELECT auth_json FROM saved_profiles WHERE label = ?", (clean_label,)).fetchone()
        auth_updated_at = now_iso
        if existing:
            prev_raw = existing["auth_json"] if isinstance(existing, dict) else existing[0]
            prev_payload = None
            if isinstance(prev_raw, (dict, list)):
                prev_payload = json.dumps(prev_raw, sort_keys=True)
            else:
                prev_payload = str(prev_raw or "")
            if prev_payload == auth_payload:
                prev_ts_row = conn.execute("SELECT auth_updated_at FROM saved_profiles WHERE label = ?", (clean_label,)).fetchone()
                prev_ts = prev_ts_row["auth_updated_at"] if isinstance(prev_ts_row, dict) else (prev_ts_row[0] if prev_ts_row else None)
                auth_updated_at = str(prev_ts) if prev_ts else now_iso
        is_pg = getattr(conn, "_kind", "sqlite") == "postgres"
        if is_pg:
            conn.execute(
                """
                INSERT INTO saved_profiles (
                    label, account_key, email, name, subject, user_id, provider_account_id,
                    auth_json, created_at, updated_at, auth_updated_at,
                    access_token_expires_at, id_token_expires_at, refresh_token_expires_at, last_refresh_at, refresh_token_present,
                    reauth_required, reauth_reason
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (label) DO UPDATE
                SET account_key = EXCLUDED.account_key,
                    email = EXCLUDED.email,
                    name = EXCLUDED.name,
                    subject = EXCLUDED.subject,
                    user_id = EXCLUDED.user_id,
                    provider_account_id = EXCLUDED.provider_account_id,
                    auth_json = EXCLUDED.auth_json,
                    updated_at = EXCLUDED.updated_at,
                    auth_updated_at = EXCLUDED.auth_updated_at,
                    access_token_expires_at = EXCLUDED.access_token_expires_at,
                    id_token_expires_at = EXCLUDED.id_token_expires_at,
                    refresh_token_expires_at = EXCLUDED.refresh_token_expires_at,
                    last_refresh_at = EXCLUDED.last_refresh_at,
                    refresh_token_present = EXCLUDED.refresh_token_present,
                    reauth_required = EXCLUDED.reauth_required,
                    reauth_reason = EXCLUDED.reauth_reason
                """,
                (
                    clean_label,
                    clean_account_key,
                    email,
                    name,
                    subject,
                    user_id,
                    provider_account_id,
                    auth_payload,
                    now_iso,
                    now_iso,
                    auth_updated_at,
                    access_token_expires_at,
                    id_token_expires_at,
                    refresh_token_expires_at,
                    last_refresh_at,
                    bool(refresh_token_present) if refresh_token_present is not None else None,
                    bool(reauth_required) if reauth_required is not None else None,
                    reauth_reason,
                ),
            )
        else:
            conn.execute(
                """
                INSERT INTO saved_profiles (
                    label, account_key, email, name, subject, user_id, provider_account_id,
                    auth_json, created_at, updated_at, auth_updated_at,
                    access_token_expires_at, id_token_expires_at, refresh_token_expires_at, last_refresh_at, refresh_token_present,
                    reauth_required, reauth_reason
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(label) DO UPDATE SET
                    account_key = excluded.account_key,
                    email = excluded.email,
                    name = excluded.name,
                    subject = excluded.subject,
                    user_id = excluded.user_id,
                    provider_account_id = excluded.provider_account_id,
                    auth_json = excluded.auth_json,
                    updated_at = excluded.updated_at,
                    auth_updated_at = excluded.auth_updated_at,
                    access_token_expires_at = excluded.access_token_expires_at,
                    id_token_expires_at = excluded.id_token_expires_at,
                    refresh_token_expires_at = excluded.refresh_token_expires_at,
                    last_refresh_at = excluded.last_refresh_at,
                    refresh_token_present = excluded.refresh_token_present,
                    reauth_required = excluded.reauth_required,
                    reauth_reason = excluded.reauth_reason
                """,
                (
                    clean_label,
                    clean_account_key,
                    email,
                    name,
                    subject,
                    user_id,
                    provider_account_id,
                    auth_payload,
                    now_iso,
                    now_iso,
                    auth_updated_at,
                    access_token_expires_at,
                    id_token_expires_at,
                    refresh_token_expires_at,
                    last_refresh_at,
                    bool(refresh_token_present) if refresh_token_present is not None else None,
                    bool(reauth_required) if reauth_required is not None else None,
                    reauth_reason,
                ),
            )


def update_saved_profile_reauth_status(
    label: str,
    *,
    reauth_required: bool,
    reauth_reason: str | None = None,
    now: datetime | None = None,
    db_path: Path | None = None,
) -> None:
    clean = (label or "").strip()
    if not clean:
        return
    now_iso = _to_iso(_as_utc(now))
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        conn.execute(
            """
            UPDATE saved_profiles
            SET reauth_required = ?,
                reauth_reason = ?,
                updated_at = ?
            WHERE label = ?
            """,
            (bool(reauth_required), reauth_reason, now_iso, clean),
        )


def get_saved_profile(label: str, db_path: Path | None = None) -> dict[str, Any] | None:
    clean = (label or "").strip()
    if not clean:
        return None
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        row = conn.execute("SELECT * FROM saved_profiles WHERE label = ?", (clean,)).fetchone()
        if not row:
            return None
        return _row_to_saved_profile(row)


def list_saved_profiles(db_path: Path | None = None) -> list[dict[str, Any]]:
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        rows = conn.execute("SELECT * FROM saved_profiles ORDER BY label ASC").fetchall()
        return [_row_to_saved_profile(row) for row in rows]


def delete_saved_profile(label: str, db_path: Path | None = None) -> bool:
    clean = (label or "").strip()
    if not clean:
        return False
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        res = conn.execute("DELETE FROM saved_profiles WHERE label = ?", (clean,))
        return bool(getattr(res, "rowcount", 0))


def rename_saved_profile(old_label: str, new_label: str, db_path: Path | None = None) -> bool:
    old = (old_label or "").strip()
    new = (new_label or "").strip()
    if not old or not new or old == new:
        return False
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        exists = conn.execute("SELECT 1 FROM saved_profiles WHERE label = ?", (new,)).fetchone()
        if exists:
            raise ValueError(f"profile '{new}' already exists")
        res = conn.execute("UPDATE saved_profiles SET label = ?, updated_at = ? WHERE label = ?", (new, _to_iso(datetime.now(timezone.utc)), old))
        return bool(getattr(res, "rowcount", 0))


def set_active_profile_label(label: str | None, db_path: Path | None = None) -> None:
    now_iso = _to_iso(datetime.now(timezone.utc))
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        conn.execute(
            """
            INSERT INTO app_meta (key, value, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
            """,
            ("active_profile_label", (label or "").strip() or None, now_iso),
        )
        if label:
            conn.execute(
                "UPDATE saved_profiles SET switched_at = ?, last_used_at = ? WHERE label = ?",
                (now_iso, now_iso, label),
            )


def get_active_profile_label(db_path: Path | None = None) -> str | None:
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        row = conn.execute("SELECT value FROM app_meta WHERE key = ?", ("active_profile_label",)).fetchone()
        if not row:
            return None
        value = row["value"] if isinstance(row, dict) else row[0]
        text = str(value).strip() if value is not None else ""
        return text or None


def touch_profile_last_used(label: str, db_path: Path | None = None) -> None:
    clean = (label or "").strip()
    if not clean:
        return
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        conn.execute(
            "UPDATE saved_profiles SET last_used_at = ?, updated_at = ? WHERE label = ?",
            (_to_iso(datetime.now(timezone.utc)), _to_iso(datetime.now(timezone.utc)), clean),
        )


def get_meta_value(key: str, db_path: Path | None = None) -> str | None:
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        row = conn.execute("SELECT value FROM app_meta WHERE key = ?", (key,)).fetchone()
        if not row:
            return None
        value = row["value"] if isinstance(row, dict) else row[0]
        return str(value) if value is not None else None


def set_meta_value(key: str, value: str | None, db_path: Path | None = None) -> None:
    now_iso = _to_iso(datetime.now(timezone.utc))
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        conn.execute(
            """
            INSERT INTO app_meta (key, value, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
            """,
            (key, value, now_iso),
        )


RUNTIME_SETTINGS_KEY = "runtime_settings"
ACTIVE_AUTH_JSON_KEY = "active_auth_json"
ACTIVE_AUTH_UPDATED_AT_KEY = "active_auth_updated_at"


def get_active_auth_json(db_path: Path | None = None) -> dict[str, Any] | None:
    raw = get_meta_value(ACTIVE_AUTH_JSON_KEY, db_path=db_path)
    if not raw:
        return None
    try:
        parsed = json.loads(raw)
    except Exception:
        return None
    return parsed if isinstance(parsed, dict) else None


def set_active_auth_json(payload: dict[str, Any] | None, db_path: Path | None = None) -> None:
    now_iso = _to_iso(datetime.now(timezone.utc))
    encoded = json.dumps(payload, sort_keys=True) if isinstance(payload, dict) else None
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        conn.execute(
            """
            INSERT INTO app_meta (key, value, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
            """,
            (ACTIVE_AUTH_JSON_KEY, encoded, now_iso),
        )
        conn.execute(
            """
            INSERT INTO app_meta (key, value, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
            """,
            (ACTIVE_AUTH_UPDATED_AT_KEY, now_iso if payload is not None else None, now_iso),
        )


def get_active_auth_updated_at(db_path: Path | None = None) -> str | None:
    return get_meta_value(ACTIVE_AUTH_UPDATED_AT_KEY, db_path=db_path)


def runtime_settings_defaults() -> dict[str, Any]:
    return {
        "analytics_snapshot_interval_seconds": max(int(settings.analytics_snapshot_interval_seconds or 600), 60),
        "allow_client_initiated_rotation": bool(settings.allow_client_initiated_rotation),
        "lease_default_ttl_seconds": max(int(settings.lease_default_ttl_seconds or 3600), 60),
        "lease_renewal_min_remaining_seconds": max(int(settings.lease_renewal_min_remaining_seconds or 300), 15),
        "lease_stale_after_seconds": max(int(settings.lease_stale_after_seconds or 60), 15),
        "lease_reclaim_after_seconds": max(int(settings.lease_reclaim_after_seconds or 180), 30),
        "rotation_request_threshold_percent": float(settings.rotation_request_threshold_percent or 90.0),
        "max_assignable_utilization_percent": float(settings.max_assignable_utilization_percent or 99.0),
        "exhausted_utilization_percent": float(settings.exhausted_utilization_percent or 100.0),
        "min_quota_remaining": max(int(settings.min_quota_remaining or 0), 0),
        "weekly_reset_confirmation_required": bool(settings.weekly_reset_confirmation_required),
        "rotation_policy_default": "replacement_required_only",
        "rotation_policy_by_agent": {},
        "rotation_policy_by_machine": {},
    }


def _normalize_runtime_settings(values: dict[str, Any] | None) -> dict[str, Any]:
    merged = runtime_settings_defaults()
    if not isinstance(values, dict):
        return merged

    def _int(key: str, minimum: int) -> None:
        raw = values.get(key)
        if raw is None or raw == "":
            return
        try:
            merged[key] = max(int(raw), minimum)
        except (TypeError, ValueError):
            return

    def _float(key: str, minimum: float, maximum: float) -> None:
        raw = values.get(key)
        if raw is None or raw == "":
            return
        try:
            merged[key] = min(max(float(raw), minimum), maximum)
        except (TypeError, ValueError):
            return

    def _bool(key: str) -> None:
        raw = values.get(key)
        if raw is None:
            return
        if isinstance(raw, bool):
            merged[key] = raw
            return
        text = str(raw).strip().lower()
        if text in {"1", "true", "yes", "on"}:
            merged[key] = True
        elif text in {"0", "false", "no", "off"}:
            merged[key] = False

    def _rotation_policy(key: str) -> None:
        raw = values.get(key)
        if raw is None or raw == "":
            return
        text = str(raw).strip()
        if text in {"replacement_required_only", "recommended_or_required"}:
            merged[key] = text

    def _rotation_policy_map(key: str) -> None:
        raw = values.get(key)
        if not isinstance(raw, dict):
            return
        normalized: dict[str, str] = {}
        for map_key, map_value in raw.items():
            map_key_text = str(map_key).strip()
            map_value_text = str(map_value).strip()
            if not map_key_text:
                continue
            if map_value_text in {"replacement_required_only", "recommended_or_required"}:
                normalized[map_key_text] = map_value_text
        merged[key] = normalized

    _int("analytics_snapshot_interval_seconds", 60)
    _int("lease_default_ttl_seconds", 60)
    _int("lease_renewal_min_remaining_seconds", 15)
    _int("lease_stale_after_seconds", 15)
    _int("lease_reclaim_after_seconds", 30)
    _int("min_quota_remaining", 0)
    _float("rotation_request_threshold_percent", 0.0, 100.0)
    _float("max_assignable_utilization_percent", 0.0, 100.0)
    _float("exhausted_utilization_percent", 0.0, 100.0)
    _bool("allow_client_initiated_rotation")
    _bool("weekly_reset_confirmation_required")
    _rotation_policy("rotation_policy_default")
    _rotation_policy_map("rotation_policy_by_agent")
    _rotation_policy_map("rotation_policy_by_machine")

    if merged["lease_reclaim_after_seconds"] <= merged["lease_stale_after_seconds"]:
        merged["lease_reclaim_after_seconds"] = merged["lease_stale_after_seconds"] + 60
    if merged["rotation_request_threshold_percent"] > merged["max_assignable_utilization_percent"]:
        merged["rotation_request_threshold_percent"] = merged["max_assignable_utilization_percent"]
    if merged["max_assignable_utilization_percent"] > merged["exhausted_utilization_percent"]:
        merged["max_assignable_utilization_percent"] = merged["exhausted_utilization_percent"]

    return merged


def get_runtime_settings(db_path: Path | None = None) -> dict[str, Any]:
    raw = get_meta_value(RUNTIME_SETTINGS_KEY, db_path=db_path)
    if not raw:
        return runtime_settings_defaults()
    try:
        payload = json.loads(raw)
    except Exception:
        return runtime_settings_defaults()
    return _normalize_runtime_settings(payload if isinstance(payload, dict) else None)


def update_runtime_settings(values: dict[str, Any], db_path: Path | None = None) -> dict[str, Any]:
    current = get_runtime_settings(db_path=db_path)
    merged = dict(current)
    if isinstance(values, dict):
        merged.update(values)
    normalized = _normalize_runtime_settings(merged)
    set_meta_value(
        RUNTIME_SETTINGS_KEY,
        json.dumps(normalized, separators=(",", ":"), sort_keys=True),
        db_path=db_path,
    )
    return normalized


def import_openclaw_usage_export(
    *,
    export_data: dict[str, Any],
    machine_id: str | None = None,
    agent_id: str | None = None,
    lease_id: str | None = None,
    credential_id: str | None = None,
    source_name: str | None = None,
    imported_at: datetime | None = None,
    db_path: Path | None = None,
) -> dict[str, Any]:
    if not isinstance(export_data, dict):
        raise ValueError("export_data must be an object")

    sessions = export_data.get("sessions")
    daily = export_data.get("daily")
    totals = export_data.get("totals")
    if sessions is None:
        sessions = []
    if not isinstance(sessions, list) or not isinstance(daily, list) or not isinstance(totals, dict):
        raise ValueError("export_data must include daily[] and totals; sessions[] is optional")

    resolved_machine_id = (machine_id or "").strip() or "openclaw"
    resolved_agent_id = (agent_id or "").strip()
    if not resolved_agent_id:
        for row in sessions:
            if isinstance(row, dict):
                candidate = str(row.get("agentId") or "").strip()
                if candidate:
                    resolved_agent_id = candidate
                    break
    resolved_agent_id = resolved_agent_id or "openclaw"
    resolved_lease_id = (lease_id or "").strip() or None
    resolved_credential_id = (credential_id or "").strip() or None

    created_iso = _to_iso(_as_utc(imported_at))
    export_payload = json.dumps(export_data, sort_keys=True, separators=(",", ":"))
    import_key = hashlib.sha256(
        (
            f"{resolved_machine_id}\n{resolved_agent_id}\n{resolved_lease_id or ''}\n"
            f"{resolved_credential_id or ''}\n{source_name or ''}\n{export_payload}"
        ).encode("utf-8")
    ).hexdigest()

    def _int(value: Any) -> int:
        if value is None or value == "":
            return 0
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, (int, float)):
            return int(value)
        if isinstance(value, str):
            with suppress(Exception):
                return int(float(value.strip()))
        return 0

    def _float(value: Any) -> float | None:
        if value is None or value == "":
            return None
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            with suppress(Exception):
                return float(value.strip())
        return None

    def _text(value: Any) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    daily_rows = [row for row in daily if isinstance(row, dict)]
    session_rows = [row for row in sessions if isinstance(row, dict)]

    with _connect(db_path) as conn:
        _ensure_schema(conn)
        if resolved_lease_id and not resolved_credential_id:
            with suppress(Exception):
                lease_row = conn.execute(
                    "SELECT credential_id FROM broker_leases WHERE id = ?",
                    (resolved_lease_id,),
                ).fetchone()
                if lease_row:
                    resolved_credential_id = str(lease_row.get("credential_id") or "").strip() or None
        existing = conn.execute(
            "SELECT import_key FROM openclaw_usage_imports WHERE import_key = ?",
            (import_key,),
        ).fetchone()
        if existing:
            return {
                "status": "ok",
                "imported": False,
                "import_key": import_key,
                "machine_id": resolved_machine_id,
                "agent_id": resolved_agent_id,
                "lease_id": resolved_lease_id,
                "credential_id": resolved_credential_id,
                "daily_rows": 0,
                "session_rows": 0,
            }

        is_pg = getattr(conn, "_kind", "sqlite") == "postgres"
        if is_pg:
            conn.execute(
                """
                INSERT INTO openclaw_usage_imports (
                    import_key, source_name, machine_id, agent_id, lease_id, credential_id, totals_json, created_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s)
                """,
                (
                    import_key,
                    source_name,
                    resolved_machine_id,
                    resolved_agent_id,
                    resolved_lease_id,
                    resolved_credential_id,
                    json.dumps(totals),
                    created_iso,
                ),
            )
        else:
            conn.execute(
                """
                INSERT INTO openclaw_usage_imports (
                    import_key, source_name, machine_id, agent_id, lease_id, credential_id, totals_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    import_key,
                    source_name,
                    resolved_machine_id,
                    resolved_agent_id,
                    resolved_lease_id,
                    resolved_credential_id,
                    json.dumps(totals),
                    created_iso,
                ),
            )

        daily_upserts = 0
        for row in daily_rows:
            usage_date = _text(row.get("date"))
            if not usage_date:
                continue
            raw_json = json.dumps(row, sort_keys=True)
            params = (
                resolved_machine_id,
                resolved_agent_id,
                resolved_lease_id,
                resolved_credential_id,
                usage_date,
                _int(row.get("input") or row.get("inputTokens")),
                _int(row.get("output") or row.get("outputTokens")),
                _int(row.get("cacheRead") or row.get("cacheReadTokens")),
                _int(row.get("cacheWrite") or row.get("cacheWriteTokens")),
                _int(row.get("totalTokens")),
                _float(row.get("inputCost")),
                _float(row.get("outputCost")),
                _float(row.get("cacheReadCost")),
                _float(row.get("cacheWriteCost")),
                _float(row.get("totalCost")),
                _int(row.get("missingCostEntries")),
                raw_json,
                created_iso,
            )
            if is_pg:
                conn.execute(
                    """
                    INSERT INTO openclaw_daily_usage (
                        machine_id, agent_id, lease_id, credential_id, usage_date,
                        input_tokens, output_tokens, cache_read_tokens, cache_write_tokens, total_tokens,
                        input_cost, output_cost, cache_read_cost, cache_write_cost, total_cost, missing_cost_entries,
                        raw_json, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
                    ON CONFLICT (machine_id, agent_id, usage_date) DO UPDATE
                    SET lease_id = EXCLUDED.lease_id,
                        credential_id = EXCLUDED.credential_id,
                        input_tokens = EXCLUDED.input_tokens,
                        output_tokens = EXCLUDED.output_tokens,
                        cache_read_tokens = EXCLUDED.cache_read_tokens,
                        cache_write_tokens = EXCLUDED.cache_write_tokens,
                        total_tokens = EXCLUDED.total_tokens,
                        input_cost = EXCLUDED.input_cost,
                        output_cost = EXCLUDED.output_cost,
                        cache_read_cost = EXCLUDED.cache_read_cost,
                        cache_write_cost = EXCLUDED.cache_write_cost,
                        total_cost = EXCLUDED.total_cost,
                        missing_cost_entries = EXCLUDED.missing_cost_entries,
                        raw_json = EXCLUDED.raw_json,
                        updated_at = EXCLUDED.updated_at
                    """,
                    params,
                )
            else:
                conn.execute(
                    """
                    INSERT INTO openclaw_daily_usage (
                        machine_id, agent_id, lease_id, credential_id, usage_date,
                        input_tokens, output_tokens, cache_read_tokens, cache_write_tokens, total_tokens,
                        input_cost, output_cost, cache_read_cost, cache_write_cost, total_cost, missing_cost_entries,
                        raw_json, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(machine_id, agent_id, usage_date) DO UPDATE SET
                        lease_id = excluded.lease_id,
                        credential_id = excluded.credential_id,
                        input_tokens = excluded.input_tokens,
                        output_tokens = excluded.output_tokens,
                        cache_read_tokens = excluded.cache_read_tokens,
                        cache_write_tokens = excluded.cache_write_tokens,
                        total_tokens = excluded.total_tokens,
                        input_cost = excluded.input_cost,
                        output_cost = excluded.output_cost,
                        cache_read_cost = excluded.cache_read_cost,
                        cache_write_cost = excluded.cache_write_cost,
                        total_cost = excluded.total_cost,
                        missing_cost_entries = excluded.missing_cost_entries,
                        raw_json = excluded.raw_json,
                        updated_at = excluded.updated_at
                    """,
                    params,
                )
            daily_upserts += 1

        session_upserts = 0
        for row in session_rows:
            session_key = _text(row.get("key"))
            updated_at_value = row.get("updatedAt")
            if not session_key or updated_at_value in (None, ""):
                continue
            updated_dt = None
            if isinstance(updated_at_value, (int, float)):
                updated_dt = datetime.fromtimestamp(float(updated_at_value) / 1000.0, tz=timezone.utc)
            elif isinstance(updated_at_value, str):
                with suppress(Exception):
                    updated_dt = datetime.fromtimestamp(float(updated_at_value.strip()) / 1000.0, tz=timezone.utc)
            if updated_dt is None:
                continue
            session_agent_id = _text(row.get("agentId")) or resolved_agent_id
            usage = row.get("usage") if isinstance(row.get("usage"), dict) else {}
            message_counts = usage.get("messageCounts") if isinstance(usage.get("messageCounts"), dict) else {}
            tool_usage = usage.get("toolUsage") if isinstance(usage.get("toolUsage"), dict) else {}
            raw_json = json.dumps(row, sort_keys=True)
            params = (
                resolved_machine_id,
                session_agent_id,
                resolved_lease_id,
                resolved_credential_id,
                session_key,
                _text(row.get("sessionId")),
                _text(row.get("label")),
                _text(row.get("channel")),
                _text(row.get("chatType")),
                _text(row.get("modelProvider")),
                _text(row.get("model")),
                _to_iso(updated_dt),
                _int((usage or {}).get("durationMs") or row.get("durationMs")),
                _int((message_counts or {}).get("total") or row.get("messages")),
                _int((message_counts or {}).get("errors") or row.get("errors")),
                _int((tool_usage or {}).get("totalCalls") or row.get("toolCalls")),
                _int((usage or {}).get("inputTokens") or row.get("inputTokens")),
                _int((usage or {}).get("outputTokens") or row.get("outputTokens")),
                _int((usage or {}).get("cacheReadTokens") or row.get("cacheReadTokens")),
                _int((usage or {}).get("cacheWriteTokens") or row.get("cacheWriteTokens")),
                _int((usage or {}).get("totalTokens") or row.get("totalTokens")),
                _float((usage or {}).get("totalCost") or row.get("totalCost")),
                raw_json,
            )
            if is_pg:
                conn.execute(
                    """
                    INSERT INTO openclaw_session_usage (
                        machine_id, agent_id, lease_id, credential_id, session_key, session_id, label, channel, chat_type,
                        model_provider, model, updated_at, duration_ms, messages, errors, tool_calls,
                        input_tokens, output_tokens, cache_read_tokens, cache_write_tokens, total_tokens,
                        total_cost, raw_json
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                    ON CONFLICT (machine_id, agent_id, session_key) DO UPDATE
                    SET lease_id = EXCLUDED.lease_id,
                        credential_id = EXCLUDED.credential_id,
                        session_id = EXCLUDED.session_id,
                        label = EXCLUDED.label,
                        channel = EXCLUDED.channel,
                        chat_type = EXCLUDED.chat_type,
                        model_provider = EXCLUDED.model_provider,
                        model = EXCLUDED.model,
                        updated_at = EXCLUDED.updated_at,
                        duration_ms = EXCLUDED.duration_ms,
                        messages = EXCLUDED.messages,
                        errors = EXCLUDED.errors,
                        tool_calls = EXCLUDED.tool_calls,
                        input_tokens = EXCLUDED.input_tokens,
                        output_tokens = EXCLUDED.output_tokens,
                        cache_read_tokens = EXCLUDED.cache_read_tokens,
                        cache_write_tokens = EXCLUDED.cache_write_tokens,
                        total_tokens = EXCLUDED.total_tokens,
                        total_cost = EXCLUDED.total_cost,
                        raw_json = EXCLUDED.raw_json
                    """,
                    params,
                )
            else:
                conn.execute(
                    """
                    INSERT INTO openclaw_session_usage (
                        machine_id, agent_id, lease_id, credential_id, session_key, session_id, label, channel, chat_type,
                        model_provider, model, updated_at, duration_ms, messages, errors, tool_calls,
                        input_tokens, output_tokens, cache_read_tokens, cache_write_tokens, total_tokens,
                        total_cost, raw_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(machine_id, agent_id, session_key) DO UPDATE SET
                        lease_id = excluded.lease_id,
                        credential_id = excluded.credential_id,
                        session_id = excluded.session_id,
                        label = excluded.label,
                        channel = excluded.channel,
                        chat_type = excluded.chat_type,
                        model_provider = excluded.model_provider,
                        model = excluded.model,
                        updated_at = excluded.updated_at,
                        duration_ms = excluded.duration_ms,
                        messages = excluded.messages,
                        errors = excluded.errors,
                        tool_calls = excluded.tool_calls,
                        input_tokens = excluded.input_tokens,
                        output_tokens = excluded.output_tokens,
                        cache_read_tokens = excluded.cache_read_tokens,
                        cache_write_tokens = excluded.cache_write_tokens,
                        total_tokens = excluded.total_tokens,
                        total_cost = excluded.total_cost,
                        raw_json = excluded.raw_json
                    """,
                    params,
                )
            session_upserts += 1

    return {
        "status": "ok",
        "imported": True,
        "import_key": import_key,
        "machine_id": resolved_machine_id,
        "agent_id": resolved_agent_id,
        "lease_id": resolved_lease_id,
        "credential_id": resolved_credential_id,
        "daily_rows": daily_upserts,
        "session_rows": session_upserts,
        "totals": {
            "input": _int(totals.get("input")),
            "output": _int(totals.get("output")),
            "cacheRead": _int(totals.get("cacheRead")),
            "cacheWrite": _int(totals.get("cacheWrite")),
            "totalTokens": _int(totals.get("totalTokens")),
            "totalCost": _float(totals.get("totalCost")),
        },
    }


def list_openclaw_usage_by_credential(
    *,
    since_date: str | None = None,
    db_path: Path | None = None,
) -> list[dict[str, Any]]:
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        is_pg = getattr(conn, "_kind", "sqlite") == "postgres"
        conditions = ["credential_id IS NOT NULL", "TRIM(COALESCE(credential_id, '')) <> ''"]
        params: list[Any] = []
        if since_date:
            conditions.append("usage_date >= " + ("%s" if is_pg else "?"))
            params.append(since_date)
        where_clause = " AND ".join(conditions)
        query = f"""
            SELECT
                credential_id,
                MAX(lease_id) AS lease_id,
                SUM(input_tokens) AS input_tokens,
                SUM(output_tokens) AS output_tokens,
                SUM(cache_read_tokens) AS cache_read_tokens,
                SUM(cache_write_tokens) AS cache_write_tokens,
                SUM(total_tokens) AS total_tokens,
                SUM(COALESCE(total_cost, 0)) AS total_cost,
                COUNT(*) AS day_count,
                COUNT(DISTINCT machine_id) AS machine_count,
                COUNT(DISTINCT agent_id) AS agent_count,
                MAX(updated_at) AS last_updated_at
            FROM openclaw_daily_usage
            WHERE {where_clause}
            GROUP BY credential_id
            ORDER BY SUM(total_tokens) DESC, credential_id ASC
        """
        rows = conn.execute(query, params).fetchall()
    return [dict(row) for row in rows]


def migrate_legacy_local_state(
    *,
    sqlite_usage_path: Path | None = None,
    profiles_dir: Path | None = None,
    db_path: Path | None = None,
) -> dict[str, int]:
    result = {
        "profiles_migrated": 0,
        "accounts_migrated": 0,
        "rollovers_migrated": 0,
        "snapshots_migrated": 0,
        "absolute_snapshots_migrated": 0,
    }
    if not _is_postgres_configured():
        return result
    if get_meta_value("legacy_migration_v1_complete", db_path=db_path) == "1":
        return result

    src_profiles = profiles_dir or settings.profiles_dir()
    if src_profiles.exists():
        for path in sorted(src_profiles.glob("*.json")):
            try:
                payload = json.loads(path.read_text())
            except Exception:
                continue
            if not isinstance(payload, dict):
                continue
            auth_json = payload.get("authJson") if isinstance(payload.get("authJson"), dict) else payload
            if not isinstance(auth_json, dict):
                continue
            from .account_identity import extract_account_identity
            identity = extract_account_identity(auth_json)
            upsert_saved_profile(
                label=path.stem,
                account_key=identity.account_key or path.stem,
                auth_json=auth_json,
                email=identity.email,
                name=identity.name,
                subject=identity.subject,
                user_id=identity.user_id,
                provider_account_id=identity.account_id,
                db_path=db_path,
            )
            result["profiles_migrated"] += 1

    src_sqlite = sqlite_usage_path or settings.usage_db_file()
    if src_sqlite.exists():
        source = sqlite3.connect(src_sqlite)
        source.row_factory = sqlite3.Row
        try:
            with _connect(db_path) as dest:
                _ensure_schema(dest)
                src_tables = {str(r["name"]) for r in source.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
                if "accounts" in src_tables:
                    rows = source.execute("SELECT * FROM accounts").fetchall()
                    for row in rows:
                        dest.execute(
                            """
                            INSERT INTO accounts (
                                id, provider_account_id, name, rate_limit_window_type, usage_limit, usage_in_window,
                                rate_limit_refresh_at, rate_limit_last_refreshed_at, primary_used_percent, primary_resets_at,
                                secondary_used_percent, secondary_resets_at, last_usage_sync_at, lifetime_used, created_at, updated_at
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            ON CONFLICT(id) DO UPDATE SET
                                provider_account_id = excluded.provider_account_id,
                                name = excluded.name,
                                rate_limit_window_type = excluded.rate_limit_window_type,
                                usage_limit = excluded.usage_limit,
                                usage_in_window = excluded.usage_in_window,
                                rate_limit_refresh_at = excluded.rate_limit_refresh_at,
                                rate_limit_last_refreshed_at = excluded.rate_limit_last_refreshed_at,
                                primary_used_percent = excluded.primary_used_percent,
                                primary_resets_at = excluded.primary_resets_at,
                                secondary_used_percent = excluded.secondary_used_percent,
                                secondary_resets_at = excluded.secondary_resets_at,
                                last_usage_sync_at = excluded.last_usage_sync_at,
                                lifetime_used = excluded.lifetime_used,
                                updated_at = excluded.updated_at
                            """,
                            (
                                row["id"],
                                row["provider_account_id"] if "provider_account_id" in row.keys() else None,
                                row["name"] if "name" in row.keys() else None,
                                row["rate_limit_window_type"] if "rate_limit_window_type" in row.keys() else DEFAULT_WINDOW_TYPE,
                                row["usage_limit"] if "usage_limit" in row.keys() else 0,
                                row["usage_in_window"] if "usage_in_window" in row.keys() else 0,
                                row["rate_limit_refresh_at"] if "rate_limit_refresh_at" in row.keys() else _to_iso(datetime.now(timezone.utc)),
                                row["rate_limit_last_refreshed_at"] if "rate_limit_last_refreshed_at" in row.keys() else None,
                                row["primary_used_percent"] if "primary_used_percent" in row.keys() else None,
                                row["primary_resets_at"] if "primary_resets_at" in row.keys() else None,
                                row["secondary_used_percent"] if "secondary_used_percent" in row.keys() else None,
                                row["secondary_resets_at"] if "secondary_resets_at" in row.keys() else None,
                                row["last_usage_sync_at"] if "last_usage_sync_at" in row.keys() else None,
                                row["lifetime_used"] if "lifetime_used" in row.keys() else 0,
                                row["created_at"] if "created_at" in row.keys() else _to_iso(datetime.now(timezone.utc)),
                                row["updated_at"] if "updated_at" in row.keys() else _to_iso(datetime.now(timezone.utc)),
                            ),
                        )
                        result["accounts_migrated"] += 1
                if "usage_rollovers" in src_tables:
                    rows = source.execute("SELECT * FROM usage_rollovers").fetchall()
                    for row in rows:
                        dest.execute(
                            """
                            INSERT INTO usage_rollovers (
                                account_id, window_started_at, window_ended_at, usage_limit, usage_used, usage_wasted,
                                primary_percent_at_reset, secondary_percent_at_reset, rolled_over_at
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                            ON CONFLICT (account_id, window_started_at, window_ended_at) DO NOTHING
                            """,
                            (
                                row["account_id"],
                                row["window_started_at"],
                                row["window_ended_at"],
                                row["usage_limit"],
                                row["usage_used"],
                                row["usage_wasted"],
                                row["primary_percent_at_reset"] if "primary_percent_at_reset" in row.keys() else None,
                                row["secondary_percent_at_reset"] if "secondary_percent_at_reset" in row.keys() else None,
                                row["rolled_over_at"],
                            ),
                        )
                        result["rollovers_migrated"] += 1
                if "usage_snapshots" in src_tables:
                    rows = source.execute("SELECT account_id, primary_used_percent, secondary_used_percent, captured_at FROM usage_snapshots").fetchall()
                    for row in rows:
                        dest.execute(
                            "INSERT INTO usage_snapshots (account_id, primary_used_percent, secondary_used_percent, captured_at) VALUES (?, ?, ?, ?)",
                            (row["account_id"], row["primary_used_percent"], row["secondary_used_percent"], row["captured_at"]),
                        )
                        result["snapshots_migrated"] += 1
                if "usage_absolute_snapshots" in src_tables:
                    rows = source.execute(
                        """
                        SELECT account_id, captured_at, usage_in_window, usage_limit, lifetime_used,
                               rate_limit_refresh_at, primary_used_percent, secondary_used_percent
                        FROM usage_absolute_snapshots
                        """
                    ).fetchall()
                    for row in rows:
                        dest.execute(
                            """
                            INSERT INTO usage_absolute_snapshots (
                                account_id, captured_at, usage_in_window, usage_limit, lifetime_used,
                                rate_limit_refresh_at, primary_used_percent, secondary_used_percent
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                row["account_id"],
                                row["captured_at"],
                                row["usage_in_window"],
                                row["usage_limit"],
                                row["lifetime_used"],
                                row["rate_limit_refresh_at"],
                                row["primary_used_percent"],
                                row["secondary_used_percent"],
                            ),
                        )
                        result["absolute_snapshots_migrated"] += 1
        finally:
            source.close()

    set_meta_value("legacy_migration_v1_complete", "1", db_path=db_path)
    return result


def _row_to_saved_profile(row: Any) -> dict[str, Any]:
    raw_auth = row["auth_json"] if isinstance(row, dict) else row[7]
    if isinstance(raw_auth, (dict, list)):
        auth_obj = raw_auth
    else:
        text = str(raw_auth or "")
        try:
            auth_obj = json.loads(text) if text else {}
        except Exception:
            auth_obj = {}
    return {
        "label": str(row["label"]) if isinstance(row, dict) else str(row[0]),
        "account_key": str(row["account_key"]) if isinstance(row, dict) else str(row[1]),
        "email": row["email"] if isinstance(row, dict) else row[2],
        "name": row["name"] if isinstance(row, dict) else row[3],
        "subject": row["subject"] if isinstance(row, dict) else row[4],
        "user_id": row["user_id"] if isinstance(row, dict) else row[5],
        "provider_account_id": row["provider_account_id"] if isinstance(row, dict) else row[6],
        "auth_json": auth_obj if isinstance(auth_obj, dict) else {},
        "created_at": row["created_at"] if isinstance(row, dict) else row[8],
        "updated_at": row["updated_at"] if isinstance(row, dict) else row[9],
        "auth_updated_at": row.get("auth_updated_at") if isinstance(row, dict) else (row[10] if len(row) > 10 else None),
        "access_token_expires_at": row.get("access_token_expires_at") if isinstance(row, dict) else (row[11] if len(row) > 11 else None),
        "id_token_expires_at": row.get("id_token_expires_at") if isinstance(row, dict) else (row[12] if len(row) > 12 else None),
        "refresh_token_expires_at": row.get("refresh_token_expires_at") if isinstance(row, dict) else (row[13] if len(row) > 13 else None),
        "last_refresh_at": row.get("last_refresh_at") if isinstance(row, dict) else (row[14] if len(row) > 14 else None),
        "refresh_token_present": row.get("refresh_token_present") if isinstance(row, dict) else (row[15] if len(row) > 15 else None),
        "last_used_at": row.get("last_used_at") if isinstance(row, dict) else (row[16] if len(row) > 16 else None),
        "switched_at": row.get("switched_at") if isinstance(row, dict) else (row[17] if len(row) > 17 else None),
        "reauth_required": row.get("reauth_required") if isinstance(row, dict) else (row[18] if len(row) > 18 else None),
        "reauth_reason": row.get("reauth_reason") if isinstance(row, dict) else (row[19] if len(row) > 19 else None),
    }


def _refresh_account_window_if_needed_locked(
    conn: sqlite3.Connection, account_id: str, now_dt: datetime
) -> sqlite3.Row:
    row = _get_account_row(conn, account_id)
    if row is None:
        raise KeyError(f"Account not found: {account_id}")

    refresh_at = _parse_iso(str(row["rate_limit_refresh_at"]))
    now_iso = _to_iso(now_dt)
    window_type = _normalize_window_type(str(row["rate_limit_window_type"]))
    usage_limit = max(int(row["usage_limit"]), 0)
    usage_in_window = max(int(row["usage_in_window"]), 0)
    primary_pct = row["primary_used_percent"] if row["primary_used_percent"] is not None else None
    secondary_pct = row["secondary_used_percent"] if row["secondary_used_percent"] is not None else None
    last_refreshed_at = (
        _parse_iso(str(row["rate_limit_last_refreshed_at"]))
        if row["rate_limit_last_refreshed_at"]
        else None
    )

    # Primary window (e.g. 5-hour) reconciliation
    changed = False
    while now_dt >= refresh_at:
        window_started = last_refreshed_at or _previous_boundary(refresh_at, window_type)
        window_started_iso = _to_iso(window_started)
        window_ended_iso = _to_iso(refresh_at)
        wasted = max(usage_limit - usage_in_window, 0)

        conn.execute(
            """
            INSERT OR IGNORE INTO usage_rollovers (
                account_id, window_started_at, window_ended_at, usage_limit, usage_used, usage_wasted,
                primary_percent_at_reset, secondary_percent_at_reset, rolled_over_at, window_type
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                account_id, window_started_iso, window_ended_iso, usage_limit, usage_in_window, wasted,
                primary_pct, secondary_pct, now_iso, 'short',
            ),
        )

        last_refreshed_at = refresh_at
        refresh_at = _next_boundary(refresh_at, window_type)
        usage_in_window = 0
        changed = True

    # Secondary (Weekly) window wastage recording
    # This is where we capture the "leftover" at the end of the 168-hour week.
    sec_resets_at_str = row["secondary_resets_at"]
    if sec_resets_at_str:
        sec_resets_at = _parse_iso(str(sec_resets_at_str))
        if now_dt >= sec_resets_at:
            # Weekly rollover semantics are percentage-based:
            # usage_used = weekly utilization %, usage_wasted = unused weekly %.
            if secondary_pct is not None:
                sec_used_pct = max(0.0, min(100.0, float(secondary_pct)))
            elif usage_limit > 0:
                sec_used_pct = max(0.0, min(100.0, (usage_in_window / usage_limit) * 100.0))
            else:
                sec_used_pct = 0.0
            sec_wasted_pct = max(0.0, 100.0 - sec_used_pct)
            weekly_window_start_iso = _to_iso(sec_resets_at - timedelta(days=7))
            weekly_window_end_iso = _to_iso(sec_resets_at)

            conn.execute(
                """
                INSERT OR IGNORE INTO usage_rollovers (
                    account_id, window_started_at, window_ended_at, usage_limit, usage_used, usage_wasted,
                    primary_percent_at_reset, secondary_percent_at_reset, rolled_over_at, window_type
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    account_id, 
                    weekly_window_start_iso,
                    weekly_window_end_iso,
                    100,
                    int(round(sec_used_pct)),
                    int(round(sec_wasted_pct)),
                    primary_pct, 
                    sec_used_pct,
                    now_iso, 
                    'weekly'
                ),
            )

    if changed:
        conn.execute(
            """
            UPDATE accounts
            SET usage_in_window = ?,
                rate_limit_refresh_at = ?,
                rate_limit_last_refreshed_at = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (usage_in_window, _to_iso(refresh_at), _to_iso(last_refreshed_at), now_iso, account_id),
        )

    latest = _get_account_row(conn, account_id)
    if latest is None:
        raise KeyError(f"Account not found after refresh: {account_id}")
    return latest


def _connect(db_path: Path | None) -> _CompatConnection:
    if db_path is None and _is_postgres_configured():
        if psycopg is None:
            raise RuntimeError("DATABASE_URL is configured for Postgres but psycopg is not installed")
        conn = psycopg.connect(_db_url(), autocommit=True, row_factory=dict_row)
        return _CompatConnection(conn, kind="postgres")

    path = db_path or settings.usage_db_file()
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return _CompatConnection(conn, kind="sqlite")


def _ensure_schema(conn: Any) -> None:
    conn_kind = getattr(conn, "_kind", None)
    if conn_kind == "postgres":
        _ensure_schema_postgres(conn)
        return
    _ensure_schema_sqlite(conn)


def _ensure_sqlite_column(conn: Any, table_name: str, column_name: str, definition: str) -> None:
    existing = {
        str(row["name"] if isinstance(row, sqlite3.Row) else row[1])
        for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    }
    if column_name in existing:
        return
    conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {definition}")


def _ensure_schema_postgres(conn: Any) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS accounts (
            id TEXT PRIMARY KEY,
            provider_account_id TEXT NULL,
            name TEXT NULL,
            rate_limit_window_type TEXT NOT NULL,
            usage_limit BIGINT NOT NULL DEFAULT 0,
            usage_in_window BIGINT NOT NULL DEFAULT 0,
            rate_limit_refresh_at TEXT NOT NULL,
            rate_limit_last_refreshed_at TEXT NULL,
            primary_used_percent DOUBLE PRECISION NULL,
            primary_resets_at TEXT NULL,
            secondary_used_percent DOUBLE PRECISION NULL,
            secondary_resets_at TEXT NULL,
            last_usage_sync_at TEXT NULL,
            lifetime_used BIGINT NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS saved_profiles (
            label TEXT PRIMARY KEY,
            account_key TEXT NOT NULL,
            email TEXT NULL,
            name TEXT NULL,
            subject TEXT NULL,
            user_id TEXT NULL,
            provider_account_id TEXT NULL,
            auth_json JSONB NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            auth_updated_at TEXT NULL,
            access_token_expires_at TEXT NULL,
            id_token_expires_at TEXT NULL,
            refresh_token_expires_at TEXT NULL,
            last_refresh_at TEXT NULL,
            refresh_token_present BOOLEAN NULL,
            reauth_required BOOLEAN NULL,
            reauth_reason TEXT NULL,
            last_used_at TEXT NULL,
            switched_at TEXT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS app_meta (
            key TEXT PRIMARY KEY,
            value TEXT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS usage_rollovers (
            id BIGSERIAL PRIMARY KEY,
            account_id TEXT NOT NULL,
            window_started_at TEXT NOT NULL,
            window_ended_at TEXT NOT NULL,
            usage_limit BIGINT NOT NULL,
            usage_used BIGINT NOT NULL,
            usage_wasted BIGINT NOT NULL,
            rolled_over_at TEXT NOT NULL,
            window_type TEXT NOT NULL DEFAULT 'short',
            primary_percent_at_reset DOUBLE PRECISION NULL,
            secondary_percent_at_reset DOUBLE PRECISION NULL
        )
        """
    )
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_usage_rollovers_window_unique ON usage_rollovers(account_id, window_started_at, window_ended_at)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_accounts_rate_limit_refresh_at ON accounts(rate_limit_refresh_at)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_usage_rollovers_account_id ON usage_rollovers(account_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_usage_rollovers_window_ended_at ON usage_rollovers(window_ended_at)"
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS usage_snapshots (
            id BIGSERIAL PRIMARY KEY,
            account_id TEXT NOT NULL,
            primary_used_percent DOUBLE PRECISION NULL,
            secondary_used_percent DOUBLE PRECISION NULL,
            captured_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_usage_snapshots_account_captured ON usage_snapshots(account_id, captured_at)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_usage_snapshots_captured ON usage_snapshots(captured_at)"
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS usage_absolute_snapshots (
            id BIGSERIAL PRIMARY KEY,
            account_id TEXT NOT NULL,
            captured_at TEXT NOT NULL,
            usage_in_window BIGINT NULL,
            usage_limit BIGINT NULL,
            lifetime_used BIGINT NULL,
            rate_limit_refresh_at TEXT NULL,
            primary_used_percent DOUBLE PRECISION NULL,
            secondary_used_percent DOUBLE PRECISION NULL
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_usage_absolute_snapshots_account_captured ON usage_absolute_snapshots(account_id, captured_at)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_usage_absolute_snapshots_captured ON usage_absolute_snapshots(captured_at)"
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS openclaw_usage_imports (
            import_key TEXT PRIMARY KEY,
            source_name TEXT NULL,
            machine_id TEXT NOT NULL,
            agent_id TEXT NOT NULL,
            lease_id TEXT NULL,
            credential_id TEXT NULL,
            totals_json JSONB NOT NULL,
            created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS openclaw_daily_usage (
            machine_id TEXT NOT NULL,
            agent_id TEXT NOT NULL,
            lease_id TEXT NULL,
            credential_id TEXT NULL,
            usage_date TEXT NOT NULL,
            input_tokens BIGINT NOT NULL DEFAULT 0,
            output_tokens BIGINT NOT NULL DEFAULT 0,
            cache_read_tokens BIGINT NOT NULL DEFAULT 0,
            cache_write_tokens BIGINT NOT NULL DEFAULT 0,
            total_tokens BIGINT NOT NULL DEFAULT 0,
            input_cost DOUBLE PRECISION NULL,
            output_cost DOUBLE PRECISION NULL,
            cache_read_cost DOUBLE PRECISION NULL,
            cache_write_cost DOUBLE PRECISION NULL,
            total_cost DOUBLE PRECISION NULL,
            missing_cost_entries BIGINT NULL,
            raw_json JSONB NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (machine_id, agent_id, usage_date)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS openclaw_session_usage (
            machine_id TEXT NOT NULL,
            agent_id TEXT NOT NULL,
            lease_id TEXT NULL,
            credential_id TEXT NULL,
            session_key TEXT NOT NULL,
            session_id TEXT NULL,
            label TEXT NULL,
            channel TEXT NULL,
            chat_type TEXT NULL,
            model_provider TEXT NULL,
            model TEXT NULL,
            updated_at TEXT NOT NULL,
            duration_ms BIGINT NULL,
            messages BIGINT NULL,
            errors BIGINT NULL,
            tool_calls BIGINT NULL,
            input_tokens BIGINT NOT NULL DEFAULT 0,
            output_tokens BIGINT NOT NULL DEFAULT 0,
            cache_read_tokens BIGINT NOT NULL DEFAULT 0,
            cache_write_tokens BIGINT NOT NULL DEFAULT 0,
            total_tokens BIGINT NOT NULL DEFAULT 0,
            total_cost DOUBLE PRECISION NULL,
            raw_json JSONB NOT NULL,
            PRIMARY KEY (machine_id, agent_id, session_key)
        )
        """
    )
    conn.execute("ALTER TABLE accounts ADD COLUMN IF NOT EXISTS provider_account_id TEXT NULL")
    conn.execute("ALTER TABLE accounts ADD COLUMN IF NOT EXISTS name TEXT NULL")
    conn.execute("ALTER TABLE accounts ADD COLUMN IF NOT EXISTS primary_used_percent DOUBLE PRECISION NULL")
    conn.execute("ALTER TABLE accounts ADD COLUMN IF NOT EXISTS primary_resets_at TEXT NULL")
    conn.execute("ALTER TABLE accounts ADD COLUMN IF NOT EXISTS secondary_used_percent DOUBLE PRECISION NULL")
    conn.execute("ALTER TABLE accounts ADD COLUMN IF NOT EXISTS secondary_resets_at TEXT NULL")
    conn.execute("ALTER TABLE accounts ADD COLUMN IF NOT EXISTS last_usage_sync_at TEXT NULL")
    conn.execute("ALTER TABLE accounts ADD COLUMN IF NOT EXISTS lifetime_used BIGINT NOT NULL DEFAULT 0")
    conn.execute("ALTER TABLE accounts ADD COLUMN IF NOT EXISTS created_at TEXT NOT NULL DEFAULT '1970-01-01T00:00:00+00:00'")
    conn.execute("ALTER TABLE accounts ADD COLUMN IF NOT EXISTS updated_at TEXT NOT NULL DEFAULT '1970-01-01T00:00:00+00:00'")
    conn.execute("ALTER TABLE saved_profiles ADD COLUMN IF NOT EXISTS auth_updated_at TEXT NULL")
    conn.execute("ALTER TABLE saved_profiles ADD COLUMN IF NOT EXISTS access_token_expires_at TEXT NULL")
    conn.execute("ALTER TABLE saved_profiles ADD COLUMN IF NOT EXISTS id_token_expires_at TEXT NULL")
    conn.execute("ALTER TABLE saved_profiles ADD COLUMN IF NOT EXISTS refresh_token_expires_at TEXT NULL")
    conn.execute("ALTER TABLE saved_profiles ADD COLUMN IF NOT EXISTS last_refresh_at TEXT NULL")
    conn.execute("ALTER TABLE saved_profiles ADD COLUMN IF NOT EXISTS refresh_token_present BOOLEAN NULL")
    conn.execute("ALTER TABLE saved_profiles ADD COLUMN IF NOT EXISTS reauth_required BOOLEAN NULL")
    conn.execute("ALTER TABLE saved_profiles ADD COLUMN IF NOT EXISTS reauth_reason TEXT NULL")
    conn.execute("ALTER TABLE usage_rollovers ADD COLUMN IF NOT EXISTS primary_percent_at_reset DOUBLE PRECISION NULL")
    conn.execute("ALTER TABLE usage_rollovers ADD COLUMN IF NOT EXISTS secondary_percent_at_reset DOUBLE PRECISION NULL")
    conn.execute("ALTER TABLE usage_rollovers ADD COLUMN IF NOT EXISTS window_type TEXT NOT NULL DEFAULT 'short'")
    conn.execute("ALTER TABLE openclaw_usage_imports ADD COLUMN IF NOT EXISTS lease_id TEXT NULL")
    conn.execute("ALTER TABLE openclaw_usage_imports ADD COLUMN IF NOT EXISTS credential_id TEXT NULL")
    conn.execute("ALTER TABLE openclaw_daily_usage ADD COLUMN IF NOT EXISTS lease_id TEXT NULL")
    conn.execute("ALTER TABLE openclaw_daily_usage ADD COLUMN IF NOT EXISTS credential_id TEXT NULL")
    conn.execute("ALTER TABLE openclaw_session_usage ADD COLUMN IF NOT EXISTS lease_id TEXT NULL")
    conn.execute("ALTER TABLE openclaw_session_usage ADD COLUMN IF NOT EXISTS credential_id TEXT NULL")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_openclaw_daily_usage_date ON openclaw_daily_usage(usage_date)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_openclaw_session_usage_updated_at ON openclaw_session_usage(updated_at)"
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_openclaw_usage_imports_lease_id ON openclaw_usage_imports(lease_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_openclaw_usage_imports_credential_id ON openclaw_usage_imports(credential_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_openclaw_daily_usage_lease_id ON openclaw_daily_usage(lease_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_openclaw_daily_usage_credential_id ON openclaw_daily_usage(credential_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_openclaw_session_usage_lease_id ON openclaw_session_usage(lease_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_openclaw_session_usage_credential_id ON openclaw_session_usage(credential_id)")

    now_iso = _to_iso(datetime.now(timezone.utc))
    conn.execute(
        """
        UPDATE accounts
        SET rate_limit_window_type = COALESCE(NULLIF(rate_limit_window_type, ''), %s),
            usage_limit = COALESCE(usage_limit, 0),
            usage_in_window = COALESCE(usage_in_window, 0),
            lifetime_used = COALESCE(lifetime_used, 0),
            rate_limit_refresh_at = COALESCE(NULLIF(rate_limit_refresh_at, ''), %s),
            created_at = COALESCE(NULLIF(created_at, ''), %s),
            updated_at = COALESCE(NULLIF(updated_at, ''), %s)
        """,
        (DEFAULT_WINDOW_TYPE, now_iso, now_iso, now_iso),
    )
    conn.execute(
        """
        UPDATE usage_rollovers
        SET window_type = 'weekly'
        WHERE usage_limit = 100
          AND COALESCE(window_type, 'short') = 'short'
          AND window_started_at = window_ended_at
        """
    )


def _ensure_schema_sqlite(conn: Any) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS accounts (
            id TEXT PRIMARY KEY,
            provider_account_id TEXT NULL,
            name TEXT NULL,
            rate_limit_window_type TEXT NOT NULL,
            usage_limit INTEGER NOT NULL DEFAULT 0,
            usage_in_window INTEGER NOT NULL DEFAULT 0,
            rate_limit_refresh_at TEXT NOT NULL,
            rate_limit_last_refreshed_at TEXT NULL,
            primary_used_percent REAL NULL,
            primary_resets_at TEXT NULL,
            secondary_used_percent REAL NULL,
            secondary_resets_at TEXT NULL,
            last_usage_sync_at TEXT NULL,
            lifetime_used INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )

    existing = {str(row["name"]) for row in conn.execute("PRAGMA table_info(accounts)").fetchall()}
    additions = [
        ("provider_account_id", "TEXT NULL"),
        ("name", "TEXT NULL"),
        ("rate_limit_window_type", "TEXT NOT NULL DEFAULT 'daily'"),
        ("usage_limit", "INTEGER NOT NULL DEFAULT 0"),
        ("usage_in_window", "INTEGER NOT NULL DEFAULT 0"),
        ("rate_limit_refresh_at", "TEXT NOT NULL DEFAULT '1970-01-01T00:00:00+00:00'"),
        ("rate_limit_last_refreshed_at", "TEXT NULL"),
        ("primary_used_percent", "REAL NULL"),
        ("primary_resets_at", "TEXT NULL"),
        ("secondary_used_percent", "REAL NULL"),
        ("secondary_resets_at", "TEXT NULL"),
        ("last_usage_sync_at", "TEXT NULL"),
        ("lifetime_used", "INTEGER NOT NULL DEFAULT 0"),
        ("created_at", "TEXT NOT NULL DEFAULT '1970-01-01T00:00:00+00:00'"),
        ("updated_at", "TEXT NOT NULL DEFAULT '1970-01-01T00:00:00+00:00'"),
    ]
    for column, ddl in additions:
        if column not in existing:
            conn.execute(f"ALTER TABLE accounts ADD COLUMN {column} {ddl}")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS usage_rollovers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account_id TEXT NOT NULL,
            window_started_at TEXT NOT NULL,
            window_ended_at TEXT NOT NULL,
            usage_limit INTEGER NOT NULL,
            usage_used INTEGER NOT NULL,
            usage_wasted INTEGER NOT NULL,
            rolled_over_at TEXT NOT NULL,
            FOREIGN KEY(account_id) REFERENCES accounts(id)
        )
        """
    )
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_usage_rollovers_window_unique ON usage_rollovers(account_id, window_started_at, window_ended_at)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_accounts_rate_limit_refresh_at ON accounts(rate_limit_refresh_at)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_usage_rollovers_account_id ON usage_rollovers(account_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_usage_rollovers_window_ended_at ON usage_rollovers(window_ended_at)"
    )

    # Percentage snapshots time-series table
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS usage_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account_id TEXT NOT NULL,
            primary_used_percent REAL NULL,
            secondary_used_percent REAL NULL,
            captured_at TEXT NOT NULL,
            FOREIGN KEY(account_id) REFERENCES accounts(id)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_usage_snapshots_account_captured ON usage_snapshots(account_id, captured_at)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_usage_snapshots_captured ON usage_snapshots(captured_at)"
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS usage_absolute_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account_id TEXT NOT NULL,
            captured_at TEXT NOT NULL,
            usage_in_window INTEGER NULL,
            usage_limit INTEGER NULL,
            lifetime_used INTEGER NULL,
            rate_limit_refresh_at TEXT NULL,
            primary_used_percent REAL NULL,
            secondary_used_percent REAL NULL,
            FOREIGN KEY(account_id) REFERENCES accounts(id)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_usage_absolute_snapshots_account_captured ON usage_absolute_snapshots(account_id, captured_at)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_usage_absolute_snapshots_captured ON usage_absolute_snapshots(captured_at)"
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS openclaw_usage_imports (
            import_key TEXT PRIMARY KEY,
            source_name TEXT NULL,
            machine_id TEXT NOT NULL,
            agent_id TEXT NOT NULL,
            lease_id TEXT NULL,
            credential_id TEXT NULL,
            totals_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS openclaw_daily_usage (
            machine_id TEXT NOT NULL,
            agent_id TEXT NOT NULL,
            lease_id TEXT NULL,
            credential_id TEXT NULL,
            usage_date TEXT NOT NULL,
            input_tokens INTEGER NOT NULL DEFAULT 0,
            output_tokens INTEGER NOT NULL DEFAULT 0,
            cache_read_tokens INTEGER NOT NULL DEFAULT 0,
            cache_write_tokens INTEGER NOT NULL DEFAULT 0,
            total_tokens INTEGER NOT NULL DEFAULT 0,
            input_cost REAL NULL,
            output_cost REAL NULL,
            cache_read_cost REAL NULL,
            cache_write_cost REAL NULL,
            total_cost REAL NULL,
            missing_cost_entries INTEGER NULL,
            raw_json TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (machine_id, agent_id, usage_date)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS openclaw_session_usage (
            machine_id TEXT NOT NULL,
            agent_id TEXT NOT NULL,
            lease_id TEXT NULL,
            credential_id TEXT NULL,
            session_key TEXT NOT NULL,
            session_id TEXT NULL,
            label TEXT NULL,
            channel TEXT NULL,
            chat_type TEXT NULL,
            model_provider TEXT NULL,
            model TEXT NULL,
            updated_at TEXT NOT NULL,
            duration_ms INTEGER NULL,
            messages INTEGER NULL,
            errors INTEGER NULL,
            tool_calls INTEGER NULL,
            input_tokens INTEGER NOT NULL DEFAULT 0,
            output_tokens INTEGER NOT NULL DEFAULT 0,
            cache_read_tokens INTEGER NOT NULL DEFAULT 0,
            cache_write_tokens INTEGER NOT NULL DEFAULT 0,
            total_tokens INTEGER NOT NULL DEFAULT 0,
            total_cost REAL NULL,
            raw_json TEXT NOT NULL,
            PRIMARY KEY (machine_id, agent_id, session_key)
        )
        """
    )
    _ensure_sqlite_column(conn, "openclaw_usage_imports", "lease_id", "TEXT NULL")
    _ensure_sqlite_column(conn, "openclaw_usage_imports", "credential_id", "TEXT NULL")
    _ensure_sqlite_column(conn, "openclaw_daily_usage", "lease_id", "TEXT NULL")
    _ensure_sqlite_column(conn, "openclaw_daily_usage", "credential_id", "TEXT NULL")
    _ensure_sqlite_column(conn, "openclaw_session_usage", "lease_id", "TEXT NULL")
    _ensure_sqlite_column(conn, "openclaw_session_usage", "credential_id", "TEXT NULL")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_openclaw_daily_usage_date ON openclaw_daily_usage(usage_date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_openclaw_session_usage_updated_at ON openclaw_session_usage(updated_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_openclaw_usage_imports_lease_id ON openclaw_usage_imports(lease_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_openclaw_usage_imports_credential_id ON openclaw_usage_imports(credential_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_openclaw_daily_usage_lease_id ON openclaw_daily_usage(lease_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_openclaw_daily_usage_credential_id ON openclaw_daily_usage(credential_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_openclaw_session_usage_lease_id ON openclaw_session_usage(lease_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_openclaw_session_usage_credential_id ON openclaw_session_usage(credential_id)")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS saved_profiles (
            label TEXT PRIMARY KEY,
            account_key TEXT NOT NULL,
            email TEXT NULL,
            name TEXT NULL,
            subject TEXT NULL,
            user_id TEXT NULL,
            provider_account_id TEXT NULL,
            auth_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            auth_updated_at TEXT NULL,
            access_token_expires_at TEXT NULL,
            id_token_expires_at TEXT NULL,
            refresh_token_expires_at TEXT NULL,
            last_refresh_at TEXT NULL,
            refresh_token_present INTEGER NULL,
            reauth_required INTEGER NULL,
            reauth_reason TEXT NULL,
            last_used_at TEXT NULL,
            switched_at TEXT NULL
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_saved_profiles_account_key ON saved_profiles(account_key)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_saved_profiles_email ON saved_profiles(email)")
    cols_saved = {str(row["name"]) for row in conn.execute("PRAGMA table_info(saved_profiles)").fetchall()}
    if "auth_updated_at" not in cols_saved:
        conn.execute("ALTER TABLE saved_profiles ADD COLUMN auth_updated_at TEXT NULL")
    if "access_token_expires_at" not in cols_saved:
        conn.execute("ALTER TABLE saved_profiles ADD COLUMN access_token_expires_at TEXT NULL")
    if "id_token_expires_at" not in cols_saved:
        conn.execute("ALTER TABLE saved_profiles ADD COLUMN id_token_expires_at TEXT NULL")
    if "refresh_token_expires_at" not in cols_saved:
        conn.execute("ALTER TABLE saved_profiles ADD COLUMN refresh_token_expires_at TEXT NULL")
    if "last_refresh_at" not in cols_saved:
        conn.execute("ALTER TABLE saved_profiles ADD COLUMN last_refresh_at TEXT NULL")
    if "refresh_token_present" not in cols_saved:
        conn.execute("ALTER TABLE saved_profiles ADD COLUMN refresh_token_present INTEGER NULL")
    if "reauth_required" not in cols_saved:
        conn.execute("ALTER TABLE saved_profiles ADD COLUMN reauth_required INTEGER NULL")
    if "reauth_reason" not in cols_saved:
        conn.execute("ALTER TABLE saved_profiles ADD COLUMN reauth_reason TEXT NULL")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS app_meta (
            key TEXT PRIMARY KEY,
            value TEXT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )

    # Add percentage columns to rollovers if missing
    rollover_cols = {str(row["name"]) for row in conn.execute("PRAGMA table_info(usage_rollovers)").fetchall()}
    if "primary_percent_at_reset" not in rollover_cols:
        conn.execute("ALTER TABLE usage_rollovers ADD COLUMN primary_percent_at_reset REAL NULL")
    if "secondary_percent_at_reset" not in rollover_cols:
        conn.execute("ALTER TABLE usage_rollovers ADD COLUMN secondary_percent_at_reset REAL NULL")
    if "window_type" not in rollover_cols:
        conn.execute("ALTER TABLE usage_rollovers ADD COLUMN window_type TEXT NOT NULL DEFAULT 'short'")

    # Backfill any legacy/partial rows with consistent UTC defaults.
    now_iso = _to_iso(datetime.now(timezone.utc))
    conn.execute(
        """
        UPDATE accounts
        SET rate_limit_window_type = COALESCE(NULLIF(rate_limit_window_type, ''), ?),
            usage_limit = COALESCE(usage_limit, 0),
            usage_in_window = COALESCE(usage_in_window, 0),
            lifetime_used = COALESCE(lifetime_used, 0),
            rate_limit_refresh_at = COALESCE(NULLIF(rate_limit_refresh_at, ''), ?),
            created_at = COALESCE(NULLIF(created_at, ''), ?),
            updated_at = COALESCE(NULLIF(updated_at, ''), ?)
        WHERE rate_limit_window_type IS NULL
           OR rate_limit_window_type = ''
           OR usage_limit IS NULL
           OR usage_in_window IS NULL
           OR lifetime_used IS NULL
           OR rate_limit_refresh_at IS NULL
           OR rate_limit_refresh_at = ''
           OR created_at IS NULL
           OR created_at = ''
           OR updated_at IS NULL
           OR updated_at = ''
        """,
        (DEFAULT_WINDOW_TYPE, now_iso, now_iso, now_iso),
    )


def _get_account_row(conn: sqlite3.Connection, account_id: str) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM accounts WHERE id = ?", (account_id,)).fetchone()


def _row_to_state(row: sqlite3.Row) -> AccountUsageState:
    return AccountUsageState(
        id=str(row["id"]),
        provider_account_id=row["provider_account_id"],
        name=row["name"],
        rate_limit_window_type=str(row["rate_limit_window_type"]),
        usage_limit=int(row["usage_limit"]),
        usage_in_window=int(row["usage_in_window"]),
        rate_limit_refresh_at=str(row["rate_limit_refresh_at"]),
        rate_limit_last_refreshed_at=(
            str(row["rate_limit_last_refreshed_at"])
            if row["rate_limit_last_refreshed_at"] is not None
            else None
        ),
        primary_used_percent=(
            float(row["primary_used_percent"])
            if row["primary_used_percent"] is not None
            else None
        ),
        primary_resets_at=(
            str(row["primary_resets_at"]) if row["primary_resets_at"] is not None else None
        ),
        secondary_used_percent=(
            float(row["secondary_used_percent"])
            if row["secondary_used_percent"] is not None
            else None
        ),
        secondary_resets_at=(
            str(row["secondary_resets_at"]) if row["secondary_resets_at"] is not None else None
        ),
        last_usage_sync_at=(
            str(row["last_usage_sync_at"]) if row["last_usage_sync_at"] is not None else None
        ),
        lifetime_used=int(row["lifetime_used"]),
        created_at=str(row["created_at"]),
        updated_at=str(row["updated_at"]),
    )


def _as_utc(value: datetime | None) -> datetime:
    dt = value or datetime.now(timezone.utc)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _to_iso(value: datetime) -> str:
    return _as_utc(value).isoformat()


def _parse_iso(value: str) -> datetime:
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _normalize_window_type(value: str | None) -> str:
    text = (value or DEFAULT_WINDOW_TYPE).strip().lower()
    if text in {"daily", "monthly"}:
        return text
    return DEFAULT_WINDOW_TYPE


def _next_boundary(value: datetime, window_type: str) -> datetime:
    dt = _as_utc(value)
    if window_type == "monthly":
        first_of_month = dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        return _add_months(first_of_month, 1)
    return (dt.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1))


def _previous_boundary(value: datetime, window_type: str) -> datetime:
    dt = _as_utc(value)
    if window_type == "monthly":
        return _add_months(dt, -1)
    return dt - timedelta(days=1)


def _add_months(value: datetime, months: int) -> datetime:
    dt = _as_utc(value)
    year = dt.year + (dt.month - 1 + months) // 12
    month = ((dt.month - 1 + months) % 12) + 1
    day = min(dt.day, calendar.monthrange(year, month)[1])
    return dt.replace(year=year, month=month, day=day)
