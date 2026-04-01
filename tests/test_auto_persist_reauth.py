from __future__ import annotations

import asyncio
import base64
import json
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from app.accounts import AccountProfile
from app.auth_store import AuthStoreSwitchResult
from app.codex_cli import LoginStatusResult
from app.login_sessions import LoginSession
from app.main import _persist_current_auth_to_profile, auth_login_status


def _json_response_payload(response) -> dict:
    return json.loads(response.body.decode("utf-8"))


def _jwt_with_claims(claims: dict) -> str:
    header = {"alg": "none", "typ": "JWT"}

    def _enc(obj: dict) -> str:
        raw = json.dumps(obj, separators=(",", ":")).encode("utf-8")
        return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")

    return f"{_enc(header)}.{_enc(claims)}."


class AutoPersistReauthTests(unittest.TestCase):
    def test_expired_token_reauth_auto_persists_matching_profile(self) -> None:
        profile = AccountProfile(
            label="other",
            path=Path("/tmp/other.json"),
            auth={"email": "james@systemsfinance.co.uk", "tokens": {"access_token": "old"}},
            access_token="old",
            email="james@systemsfinance.co.uk",
        )
        status_result = LoginStatusResult(
            status="complete",
            auth_exists=True,
            auth_updated=True,
            auth_path="/tmp/auth.json",
            started_at="2026-03-21T00:00:00+00:00",
            completed_at="2026-03-21T00:01:00+00:00",
            browser_url=None,
            pid=123,
            error=None,
        )
        switch_result = AuthStoreSwitchResult(
            command=["internal-save", "--label", "other"],
            returncode=0,
            stdout="ok",
            stderr="",
        )

        with (
            patch("app.main.get_latest_session", return_value=None),
            patch("app.main.get_login_status", return_value=status_result),
            patch("app.main.session_state", return_value=("complete", None)),
            patch(
                "app.main.read_current_auth",
                return_value={"email": "james@systemsfinance.co.uk", "tokens": {"access_token": "new"}},
            ),
            patch("app.main.list_profiles", return_value=[profile]),
            patch("app.main.list_auth_labels", return_value=["other"]),
            patch("app.main.persist_current_auth"),
            patch("app.main._touch_account_usage"),
            patch("app.main.save_current_auth_under_label", return_value=switch_result) as save_mock,
        ):
            response = asyncio.run(auth_login_status())

        payload = _json_response_payload(response)
        self.assertEqual(payload["status"], "complete")
        self.assertEqual(payload["auto_persist"]["status"], "persisted")
        self.assertEqual(payload["auto_persist"]["label"], "other")
        save_mock.assert_called_once_with("other")

    def test_callback_received_but_not_finalized_does_not_persist(self) -> None:
        now = datetime.now(timezone.utc)
        session = LoginSession(
            session_id="sess-1",
            relay_token="rtok",
            auth_url="https://auth.openai.com/oauth/authorize?state=abc",
            created_at=now,
            expires_at=now,
            callback_payload={"code": "abc"},
            callback_received_at=now,
            relay_used=True,
        )
        status_result = LoginStatusResult(
            status="pending",
            auth_exists=True,
            auth_updated=False,
            auth_path="/tmp/auth.json",
            started_at="2026-03-21T00:00:00+00:00",
            completed_at=None,
            browser_url=None,
            pid=456,
            error=None,
        )

        with (
            patch("app.main.get_latest_session", return_value=session),
            patch("app.main.get_login_status", return_value=status_result),
            patch("app.main.session_state", return_value=("callback_received", None)),
            patch("app.main.save_current_auth_under_label") as save_mock,
        ):
            response = asyncio.run(auth_login_status())

        payload = _json_response_payload(response)
        self.assertEqual(payload["status"], "callback_received")
        self.assertEqual(payload["auto_persist"]["attempted"], False)
        save_mock.assert_not_called()

    def test_matching_by_email_reuses_same_label(self) -> None:
        profile = AccountProfile(
            label="other",
            path=Path("/tmp/other.json"),
            auth={"email": "james@systemsfinance.co.uk", "tokens": {"access_token": "old"}},
            access_token="old",
            email="james@systemsfinance.co.uk",
        )
        switch_result = AuthStoreSwitchResult(
            command=["internal-save", "--label", "other"],
            returncode=0,
            stdout="ok",
            stderr="",
        )

        with (
            patch(
                "app.main.read_current_auth",
                return_value={"email": "james@systemsfinance.co.uk", "tokens": {"access_token": "new"}},
            ),
            patch("app.main.list_profiles", return_value=[profile]),
            patch("app.main.list_auth_labels", return_value=["other"]),
            patch("app.main.persist_current_auth"),
            patch("app.main._touch_account_usage"),
            patch("app.main.save_current_auth_under_label", return_value=switch_result) as save_mock,
        ):
            result = _persist_current_auth_to_profile(
                desired_label=None,
                create_if_missing=False,
            )

        self.assertTrue(result.persisted)
        self.assertEqual(result.label, "other")
        self.assertTrue(result.matched_existing_profile)
        save_mock.assert_called_once_with("other")

    def test_repeated_reauth_is_idempotent_no_duplicate_save(self) -> None:
        old_profile = AccountProfile(
            label="other",
            path=Path("/tmp/other.json"),
            auth={"email": "james@systemsfinance.co.uk", "tokens": {"access_token": "old"}},
            access_token="old",
            email="james@systemsfinance.co.uk",
        )
        new_auth = {"email": "james@systemsfinance.co.uk", "tokens": {"access_token": "new"}}
        updated_profile = AccountProfile(
            label="other",
            path=Path("/tmp/other.json"),
            auth=new_auth,
            access_token="new",
            email="james@systemsfinance.co.uk",
        )
        switch_result = AuthStoreSwitchResult(
            command=["internal-save", "--label", "other"],
            returncode=0,
            stdout="ok",
            stderr="",
        )

        with (
            patch("app.main.read_current_auth", return_value=new_auth),
            patch("app.main.list_profiles", side_effect=[[old_profile], [updated_profile]]),
            patch("app.main.list_auth_labels", return_value=["other"]),
            patch("app.main.persist_current_auth"),
            patch("app.main._touch_account_usage"),
            patch("app.main.save_current_auth_under_label", return_value=switch_result) as save_mock,
        ):
            first = _persist_current_auth_to_profile(desired_label=None, create_if_missing=False)
            second = _persist_current_auth_to_profile(desired_label=None, create_if_missing=False)

        self.assertTrue(first.persisted)
        self.assertTrue(second.skipped)
        self.assertTrue(second.up_to_date)
        self.assertEqual(save_mock.call_count, 1)

    def test_brand_new_account_flow_still_creates_profile_on_import(self) -> None:
        switch_result = AuthStoreSwitchResult(
            command=["internal-save", "--label", "newuser"],
            returncode=0,
            stdout="ok",
            stderr="",
        )
        with (
            patch("app.main.read_current_auth", return_value={"email": "newuser@example.com"}),
            patch("app.main.list_profiles", return_value=[]),
            patch("app.main.list_auth_labels", return_value=[]),
            patch("app.main.persist_current_auth"),
            patch("app.main._touch_account_usage"),
            patch("app.main.save_current_auth_under_label", return_value=switch_result) as save_mock,
        ):
            result = _persist_current_auth_to_profile(
                desired_label=None,
                create_if_missing=True,
            )

        self.assertTrue(result.persisted)
        self.assertTrue(result.created_new_profile)
        self.assertEqual(result.label, "newuser")
        save_mock.assert_called_once_with("newuser")

    def test_login_status_auto_persist_creates_new_profile_when_unmatched(self) -> None:
        status_result = LoginStatusResult(
            status="complete",
            auth_exists=True,
            auth_updated=True,
            auth_path="/tmp/auth.json",
            started_at="2026-03-21T00:00:00+00:00",
            completed_at="2026-03-21T00:01:00+00:00",
            browser_url=None,
            pid=123,
            error=None,
        )
        switch_result = AuthStoreSwitchResult(
            command=["internal-save", "--label", "fresh"],
            returncode=0,
            stdout="ok",
            stderr="",
        )

        with (
            patch("app.main.get_latest_session", return_value=None),
            patch("app.main.get_login_status", return_value=status_result),
            patch("app.main.session_state", return_value=("complete", None)),
            patch("app.main.read_current_auth", return_value={"email": "fresh@example.com"}),
            patch("app.main.list_profiles", return_value=[]),
            patch("app.main.list_auth_labels", return_value=[]),
            patch("app.main.persist_current_auth"),
            patch("app.main._touch_account_usage"),
            patch("app.main.save_current_auth_under_label", return_value=switch_result) as save_mock,
        ):
            response = asyncio.run(auth_login_status())

        payload = _json_response_payload(response)
        self.assertEqual(payload["status"], "complete")
        self.assertEqual(payload["auto_persist"]["status"], "persisted")
        self.assertEqual(payload["auto_persist"]["label"], "fresh")
        self.assertTrue(payload["auto_persist"]["created_new_profile"])
        save_mock.assert_called_once_with("fresh")

    def test_callback_auth_with_expired_token_is_not_persisted(self) -> None:
        now = datetime.now(timezone.utc)
        session = LoginSession(
            session_id="sess-expired",
            relay_token="rtok",
            auth_url="https://auth.openai.com/oauth/authorize?state=abc",
            created_at=now,
            expires_at=now,
            callback_payload={"code": "abc"},
            callback_received_at=now,
            relay_used=True,
        )
        status_result = LoginStatusResult(
            status="complete",
            auth_exists=True,
            auth_updated=True,
            auth_path="/tmp/auth.json",
            started_at="2026-03-21T19:20:54+00:00",
            completed_at="2026-03-21T19:24:34+00:00",
            browser_url=None,
            pid=123,
            error=None,
        )
        expired_access = _jwt_with_claims(
            {
                "exp": int(datetime(2026, 3, 21, 17, 10, 24, tzinfo=timezone.utc).timestamp()),
                "iat": int(datetime(2026, 3, 11, 17, 10, 23, tzinfo=timezone.utc).timestamp()),
                "sub": "auth0|xKsUogc5K6oFmHnFY171YbaT",
            }
        )

        with (
            patch("app.main.get_latest_session", return_value=session),
            patch("app.main.get_login_status", return_value=status_result),
            patch("app.main.session_state", return_value=("complete", None)),
            patch("app.main.read_current_auth", return_value={"tokens": {"access_token": expired_access}}),
            patch("app.main.save_current_auth_under_label") as save_mock,
        ):
            response = asyncio.run(auth_login_status())

        payload = _json_response_payload(response)
        self.assertEqual(payload["status"], "complete")
        self.assertEqual(payload["auto_persist"]["status"], "error")
        self.assertEqual(payload["auto_persist"]["reason"], "auth_not_fresh")
        self.assertEqual(
            payload["auto_persist"]["auth_validation"]["reason"],
            "access_token_expired",
        )
        save_mock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
