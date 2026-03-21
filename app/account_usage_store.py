from __future__ import annotations

import calendar
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .config import settings


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
    last_usage_sync_at: str | None
    lifetime_used: int
    created_at: str
    updated_at: str


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
            SELECT id, account_id, window_started_at, window_ended_at, usage_limit, usage_used, usage_wasted, rolled_over_at
            FROM usage_rollovers
            WHERE account_id = ?
            ORDER BY window_ended_at ASC, id ASC
            """,
            (account_id,),
        ).fetchall()
        return [dict(row) for row in rows]


def delete_account_data(account_id: str, db_path: Path | None = None) -> None:
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("DELETE FROM usage_rollovers WHERE account_id = ?", (account_id,))
        conn.execute("DELETE FROM accounts WHERE id = ?", (account_id,))


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
    last_refreshed_at = (
        _parse_iso(str(row["rate_limit_last_refreshed_at"]))
        if row["rate_limit_last_refreshed_at"]
        else None
    )

    changed = False
    while now_dt >= refresh_at:
        window_started = last_refreshed_at or _previous_boundary(refresh_at, window_type)
        window_started_iso = _to_iso(window_started)
        window_ended_iso = _to_iso(refresh_at)
        wasted = max(usage_limit - usage_in_window, 0)

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
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                account_id,
                window_started_iso,
                window_ended_iso,
                usage_limit,
                usage_in_window,
                wasted,
                now_iso,
            ),
        )

        last_refreshed_at = refresh_at
        refresh_at = _next_boundary(refresh_at, window_type)
        usage_in_window = 0
        changed = True

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


def _connect(db_path: Path | None) -> sqlite3.Connection:
    path = db_path or settings.usage_db_file()
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def _ensure_schema(conn: sqlite3.Connection) -> None:
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
