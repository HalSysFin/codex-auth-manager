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
    primary_used_percent: float | None
    primary_resets_at: str | None
    secondary_used_percent: float | None
    secondary_resets_at: str | None
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
                   primary_percent_at_reset, secondary_percent_at_reset, rolled_over_at
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
                primary_percent_at_reset,
                secondary_percent_at_reset,
                rolled_over_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                account_id,
                window_started_iso,
                window_ended_iso,
                usage_limit,
                usage_in_window,
                wasted,
                primary_pct,
                secondary_pct,
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

    # Add percentage columns to rollovers if missing
    rollover_cols = {str(row["name"]) for row in conn.execute("PRAGMA table_info(usage_rollovers)").fetchall()}
    if "primary_percent_at_reset" not in rollover_cols:
        conn.execute("ALTER TABLE usage_rollovers ADD COLUMN primary_percent_at_reset REAL NULL")
    if "secondary_percent_at_reset" not in rollover_cols:
        conn.execute("ALTER TABLE usage_rollovers ADD COLUMN secondary_percent_at_reset REAL NULL")

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
