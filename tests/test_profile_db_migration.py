from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from app.account_usage_store import (
    _CompatConnection,
    _ensure_schema_sqlite,
    ensure_account,
    get_active_profile_label,
    get_account,
    get_saved_profile,
    initialize_usage_store,
    list_saved_profiles,
    migrate_legacy_local_state,
    record_account_usage,
    refresh_account_window_if_needed,
    upsert_saved_profile,
)
from app.codex_switch import switch_label
from app.config import settings
from app.main import _persist_active_auth_db_copy


class ProfileDbMigrationTests(unittest.TestCase):
    def test_saved_profile_crud_in_db(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "usage.sqlite3"
            initialize_usage_store(db_path)

            upsert_saved_profile(
                label="james",
                account_key="acct:james",
                auth_json={"tokens": {"access_token": "abc"}},
                email="james@example.com",
                name="James",
                db_path=db_path,
            )
            one = get_saved_profile("james", db_path=db_path)
            self.assertIsNotNone(one)
            assert one is not None
            self.assertEqual(one["account_key"], "acct:james")
            self.assertEqual(one["email"], "james@example.com")

            all_profiles = list_saved_profiles(db_path=db_path)
            self.assertEqual(len(all_profiles), 1)
            self.assertEqual(all_profiles[0]["label"], "james")

    def test_switch_materializes_active_auth_from_db(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "usage.sqlite3"
            auth_path = Path(tmp) / "auth.json"
            initialize_usage_store(db_path)
            upsert_saved_profile(
                label="max",
                account_key="acct:max",
                auth_json={"tokens": {"access_token": "tok-max"}, "email": "max@example.com"},
                email="max@example.com",
                db_path=db_path,
            )

            with (
                patch.object(settings, "usage_db_path", str(db_path)),
                patch.object(settings, "codex_auth_path", str(auth_path)),
                patch("app.account_usage_store._is_postgres_configured", return_value=False),
            ):
                result = switch_label("max")

            self.assertEqual(result.returncode, 0)
            payload = json.loads(auth_path.read_text())
            self.assertEqual(payload["tokens"]["access_token"], "tok-max")
            self.assertEqual(get_active_profile_label(db_path=db_path), "max")

    def test_migrate_legacy_sqlite_and_json_profiles(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            src_db = Path(tmp) / "legacy.sqlite3"
            dest_db = Path(tmp) / "dest.sqlite3"
            profiles_dir = Path(tmp) / "profiles"
            profiles_dir.mkdir(parents=True, exist_ok=True)
            (profiles_dir / "james.json").write_text(json.dumps({"email": "james@example.com", "tokens": {"access_token": "legacy"}}))

            initialize_usage_store(src_db)
            now = datetime(2026, 3, 22, 0, 0, tzinfo=timezone.utc)
            ensure_account(
                "acct:james",
                now=now,
                usage_limit=100,
                rate_limit_refresh_at="2026-03-23T00:00:00+00:00",
                db_path=src_db,
            )
            record_account_usage("acct:james", 30, now=now, db_path=src_db)
            refresh_account_window_if_needed(
                "acct:james",
                now=datetime(2026, 3, 23, 1, 0, tzinfo=timezone.utc),
                db_path=src_db,
            )

            def _sqlite_connect_override(db_path: Path | None):
                path = db_path or dest_db
                conn = sqlite3.connect(path, isolation_level=None)
                conn.row_factory = sqlite3.Row
                return _CompatConnection(conn, kind="sqlite")

            with (
                patch("app.account_usage_store._is_postgres_configured", return_value=True),
                patch("app.account_usage_store._connect", side_effect=_sqlite_connect_override),
                patch("app.account_usage_store._ensure_schema", side_effect=_ensure_schema_sqlite),
            ):
                migrated = migrate_legacy_local_state(
                    sqlite_usage_path=src_db,
                    profiles_dir=profiles_dir,
                    db_path=dest_db,
                )

            self.assertGreaterEqual(migrated["profiles_migrated"], 1)
            self.assertGreaterEqual(migrated["accounts_migrated"], 1)

            profile = get_saved_profile("james", db_path=dest_db)
            self.assertIsNotNone(profile)
            account = get_account("acct:james", db_path=dest_db)
            self.assertIsNotNone(account)

    def test_active_auth_change_is_persisted_back_to_db(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "usage.sqlite3"
            auth_path = Path(tmp) / "auth.json"
            initialize_usage_store(db_path)
            upsert_saved_profile(
                label="james",
                account_key="acct:james",
                auth_json={"tokens": {"access_token": "old-token"}, "email": "james@example.com"},
                email="james@example.com",
                db_path=db_path,
            )
            auth_path.write_text(json.dumps({"tokens": {"access_token": "new-token"}, "email": "james@example.com"}))

            with (
                patch.object(settings, "usage_db_path", str(db_path)),
                patch.object(settings, "codex_auth_path", str(auth_path)),
                patch("app.account_usage_store._is_postgres_configured", return_value=False),
            ):
                changed = _persist_active_auth_db_copy("james")

            self.assertTrue(changed)
            saved = get_saved_profile("james", db_path=db_path)
            self.assertIsNotNone(saved)
            assert saved is not None
            self.assertEqual(saved["auth_json"]["tokens"]["access_token"], "new-token")
            self.assertIsNotNone(saved.get("auth_updated_at"))


if __name__ == "__main__":
    unittest.main()
