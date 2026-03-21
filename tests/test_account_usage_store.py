from __future__ import annotations

import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from app.account_usage_store import (
    ensure_account,
    get_account,
    initialize_usage_store,
    list_usage_rollovers,
    record_account_usage,
    refresh_account_window_if_needed,
)


class AccountUsageStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        self.db_path = Path(temp_dir.name) / "usage.sqlite3"
        initialize_usage_store(self.db_path)

    def test_new_account_initialization_with_refresh_metadata(self) -> None:
        now = datetime(2026, 3, 21, 10, 30, tzinfo=timezone.utc)
        state = ensure_account(
            "acct-new",
            now=now,
            provider_account_id="provider-1",
            name="acct@example.com",
            rate_limit_window_type="daily",
            usage_limit=1000,
            db_path=self.db_path,
        )
        self.assertEqual(state.id, "acct-new")
        self.assertEqual(state.provider_account_id, "provider-1")
        self.assertEqual(state.name, "acct@example.com")
        self.assertEqual(state.rate_limit_window_type, "daily")
        self.assertEqual(state.usage_limit, 1000)
        self.assertEqual(state.usage_in_window, 0)
        self.assertIsNone(state.rate_limit_last_refreshed_at)
        self.assertTrue(state.rate_limit_refresh_at.endswith("+00:00"))

    def test_usage_increments_within_same_window(self) -> None:
        now = datetime(2026, 3, 21, 8, 0, tzinfo=timezone.utc)
        ensure_account(
            "acct-inc",
            now=now,
            usage_limit=100,
            rate_limit_refresh_at="2026-03-22T00:00:00+00:00",
            db_path=self.db_path,
        )
        state = record_account_usage("acct-inc", 15, now=now, db_path=self.db_path)
        self.assertEqual(state.usage_in_window, 15)
        self.assertEqual(state.lifetime_used, 15)

        state = record_account_usage("acct-inc", 10, now=now, db_path=self.db_path)
        self.assertEqual(state.usage_in_window, 25)
        self.assertEqual(state.lifetime_used, 25)

    def test_single_rollover_creates_wastage_record(self) -> None:
        start = datetime(2026, 3, 20, 1, 0, tzinfo=timezone.utc)
        ensure_account(
            "acct-rollover",
            now=start,
            usage_limit=100,
            rate_limit_refresh_at="2026-03-21T00:00:00+00:00",
            db_path=self.db_path,
        )
        record_account_usage("acct-rollover", 40, now=start, db_path=self.db_path)

        after = datetime(2026, 3, 21, 1, 0, tzinfo=timezone.utc)
        refreshed = refresh_account_window_if_needed(
            "acct-rollover", now=after, db_path=self.db_path
        )
        self.assertEqual(refreshed.usage_in_window, 0)
        rows = list_usage_rollovers("acct-rollover", db_path=self.db_path)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["usage_used"], 40)
        self.assertEqual(rows[0]["usage_wasted"], 60)

    def test_rollover_exact_limit_has_zero_wastage(self) -> None:
        start = datetime(2026, 3, 20, 1, 0, tzinfo=timezone.utc)
        ensure_account(
            "acct-zero-waste",
            now=start,
            usage_limit=50,
            rate_limit_refresh_at="2026-03-21T00:00:00+00:00",
            db_path=self.db_path,
        )
        record_account_usage("acct-zero-waste", 50, now=start, db_path=self.db_path)
        refresh_account_window_if_needed(
            "acct-zero-waste",
            now=datetime(2026, 3, 21, 0, 0, tzinfo=timezone.utc),
            db_path=self.db_path,
        )
        rows = list_usage_rollovers("acct-zero-waste", db_path=self.db_path)
        self.assertEqual(rows[0]["usage_wasted"], 0)

    def test_multiple_missed_rollovers_roll_forward(self) -> None:
        start = datetime(2026, 3, 20, 1, 0, tzinfo=timezone.utc)
        ensure_account(
            "acct-multi",
            now=start,
            usage_limit=10,
            rate_limit_refresh_at="2026-03-21T00:00:00+00:00",
            db_path=self.db_path,
        )
        record_account_usage("acct-multi", 4, now=start, db_path=self.db_path)
        final = refresh_account_window_if_needed(
            "acct-multi",
            now=datetime(2026, 3, 24, 12, 0, tzinfo=timezone.utc),
            db_path=self.db_path,
        )
        self.assertEqual(final.rate_limit_refresh_at, "2026-03-25T00:00:00+00:00")
        rows = list_usage_rollovers("acct-multi", db_path=self.db_path)
        self.assertEqual(len(rows), 4)
        self.assertEqual(rows[0]["usage_used"], 4)
        self.assertEqual(rows[1]["usage_used"], 0)
        self.assertEqual(rows[2]["usage_used"], 0)
        self.assertEqual(rows[3]["usage_used"], 0)

    def test_all_timestamps_are_utc_iso_strings(self) -> None:
        now = datetime(2026, 3, 21, 9, 30, tzinfo=timezone.utc)
        state = ensure_account(
            "acct-utc",
            now=now,
            usage_limit=20,
            rate_limit_refresh_at="2026-03-22T00:00:00+00:00",
            db_path=self.db_path,
        )
        record_account_usage("acct-utc", 5, now=now, db_path=self.db_path)
        refresh_account_window_if_needed(
            "acct-utc",
            now=datetime(2026, 3, 22, 0, 1, tzinfo=timezone.utc),
            db_path=self.db_path,
        )
        refreshed = get_account("acct-utc", db_path=self.db_path)
        assert refreshed is not None

        for value in [
            state.created_at,
            state.updated_at,
            refreshed.rate_limit_refresh_at,
            refreshed.updated_at,
        ]:
            parsed = datetime.fromisoformat(value)
            self.assertEqual(parsed.tzinfo, timezone.utc)

        row = list_usage_rollovers("acct-utc", db_path=self.db_path)[0]
        self.assertEqual(
            datetime.fromisoformat(row["rolled_over_at"]).tzinfo, timezone.utc
        )

    def test_sqlite_persistence_roundtrip(self) -> None:
        now = datetime(2026, 3, 21, 12, 0, tzinfo=timezone.utc)
        ensure_account(
            "acct-persist",
            now=now,
            usage_limit=200,
            rate_limit_refresh_at="2026-03-22T00:00:00+00:00",
            db_path=self.db_path,
        )
        record_account_usage("acct-persist", 7, now=now, db_path=self.db_path)

        # Read directly via sqlite to prove persistence.
        conn = sqlite3.connect(self.db_path)
        row = conn.execute(
            "SELECT usage_in_window, lifetime_used FROM accounts WHERE id = ?",
            ("acct-persist",),
        ).fetchone()
        conn.close()
        assert row is not None
        self.assertEqual(row[0], 7)
        self.assertEqual(row[1], 7)

    def test_repeated_refresh_does_not_duplicate_rollovers(self) -> None:
        start = datetime(2026, 3, 20, 1, 0, tzinfo=timezone.utc)
        ensure_account(
            "acct-idempotent",
            now=start,
            usage_limit=30,
            rate_limit_refresh_at="2026-03-21T00:00:00+00:00",
            db_path=self.db_path,
        )
        record_account_usage("acct-idempotent", 10, now=start, db_path=self.db_path)
        checkpoint = datetime(2026, 3, 21, 2, 0, tzinfo=timezone.utc)
        refresh_account_window_if_needed("acct-idempotent", now=checkpoint, db_path=self.db_path)
        refresh_account_window_if_needed("acct-idempotent", now=checkpoint, db_path=self.db_path)
        self.assertEqual(
            len(list_usage_rollovers("acct-idempotent", db_path=self.db_path)), 1
        )

    def test_usage_delta_zero_updates_sync_without_changing_totals(self) -> None:
        now = datetime(2026, 3, 21, 8, 0, tzinfo=timezone.utc)
        ensure_account(
            "acct-zero-delta",
            now=now,
            usage_limit=60,
            rate_limit_refresh_at="2026-03-22T00:00:00+00:00",
            db_path=self.db_path,
        )
        first = record_account_usage("acct-zero-delta", 11, now=now, db_path=self.db_path)
        second = record_account_usage(
            "acct-zero-delta",
            0,
            now=datetime(2026, 3, 21, 8, 30, tzinfo=timezone.utc),
            db_path=self.db_path,
        )
        self.assertEqual(second.usage_in_window, first.usage_in_window)
        self.assertEqual(second.lifetime_used, first.lifetime_used)
        self.assertIsNotNone(second.last_usage_sync_at)

    def test_optional_provider_fields_can_be_null(self) -> None:
        now = datetime(2026, 3, 21, 11, 0, tzinfo=timezone.utc)
        state = ensure_account(
            "acct-nullable",
            now=now,
            provider_account_id=None,
            name=None,
            usage_limit=5,
            db_path=self.db_path,
        )
        self.assertIsNone(state.provider_account_id)
        self.assertIsNone(state.name)


if __name__ == "__main__":
    unittest.main()
