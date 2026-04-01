from __future__ import annotations

import asyncio
import base64
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from app.account_identity import extract_account_identity
from app.accounts import AccountProfile, list_profiles
from app.auth_store import AuthStoreSwitchResult
from app.main import _infer_account_type, _persist_current_auth_to_profile, auth_switch


class _UsageState:
    def __init__(self) -> None:
        self.rate_limit_window_type = "daily"
        self.usage_limit = 100
        self.usage_in_window = 10
        self.rate_limit_refresh_at = "2026-03-22T00:00:00+00:00"
        self.rate_limit_last_refreshed_at = None
        self.last_usage_sync_at = None
        self.lifetime_used = 10
        self.created_at = "2026-03-21T00:00:00+00:00"
        self.updated_at = "2026-03-21T00:00:00+00:00"


def _jwt(claims: dict) -> str:
    header = {"alg": "none", "typ": "JWT"}

    def _enc(obj: dict) -> str:
        raw = json.dumps(obj, separators=(",", ":")).encode("utf-8")
        return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")

    return f"{_enc(header)}.{_enc(claims)}."


class AccountIdentityTrackingTests(unittest.TestCase):
    def test_jwt_plan_type_plus_maps_to_chatgpt_plus(self) -> None:
        auth = {
            "tokens": {
                "id_token": _jwt(
                    {
                        "sub": "auth0|plus-user",
                        "https://api.openai.com/auth": {
                            "chatgpt_plan_type": "plus",
                        },
                    }
                )
            }
        }
        identity = extract_account_identity(auth)
        self.assertEqual(identity.plan_type, "plus")

        profile = AccountProfile(
            label="plus-user",
            path=Path("/tmp/plus-user.json"),
            auth=auth,
            plan_type=identity.plan_type,
        )
        self.assertEqual(_infer_account_type(profile), "ChatGPT Plus")

    def test_jwt_plan_type_team_maps_to_chatgpt_business(self) -> None:
        auth = {
            "tokens": {
                "id_token": _jwt(
                    {
                        "sub": "auth0|team-user",
                        "https://api.openai.com/auth": {
                            "chatgpt_plan_type": "team",
                        },
                    }
                )
            }
        }
        identity = extract_account_identity(auth)
        self.assertEqual(identity.plan_type, "team")

        profile = AccountProfile(
            label="team-user",
            path=Path("/tmp/team-user.json"),
            auth=auth,
            plan_type=identity.plan_type,
        )
        self.assertEqual(_infer_account_type(profile), "ChatGPT Business")

    def test_missing_plan_type_falls_back_to_heuristic(self) -> None:
        profile = AccountProfile(
            label="legacy-team",
            path=Path("/tmp/legacy-team.json"),
            auth={},
            name="Systems Finance Team",
            plan_type=None,
        )
        self.assertEqual(_infer_account_type(profile), "ChatGPT Business")

    def test_reauth_matches_profile_by_stable_subject(self) -> None:
        existing_profile = AccountProfile(
            label="james",
            path=Path("/tmp/james.json"),
            auth={"email": "old@example.com", "tokens": {"access_token": "old"}},
            account_key="sub:auth0|same-user",
            access_token="old",
            email="old@example.com",
        )
        incoming_auth = {
            "tokens": {
                "id_token": _jwt({"sub": "auth0|same-user", "email": "new@example.com"}),
                "access_token": _jwt({"sub": "auth0|same-user"}),
            }
        }

        with (
            patch("app.main.read_current_auth", return_value=incoming_auth),
            patch("app.main.list_profiles", return_value=[existing_profile]),
            patch("app.main.list_auth_labels", return_value=["james"]),
            patch("app.main.persist_current_auth"),
            patch("app.main.save_current_auth_under_label"),
            patch("app.main._touch_account_usage"),
        ):
            result = _persist_current_auth_to_profile(desired_label=None, create_if_missing=True)

        self.assertEqual(result.label, "james")
        self.assertTrue(result.matched_existing_profile)
        self.assertFalse(result.created_new_profile)
        self.assertEqual(result.account_key, "sub:auth0|same-user")

    def test_switch_touches_usage_by_resolved_profile_identity(self) -> None:
        profile = AccountProfile(
            label="max",
            path=Path("/tmp/max.json"),
            auth={},
            account_key="sub:auth0|max-user",
            email="max@example.com",
        )

        with (
            patch("app.main._require_internal_auth", return_value=None),
            patch("app.main._profile_for_label", return_value=profile),
            patch(
                "app.main.switch_active_auth_to_label",
                return_value=AuthStoreSwitchResult(
                    command=["internal-switch", "--label", "max"],
                    returncode=0,
                    stdout="ok",
                    stderr="",
                ),
            ),
            patch("app.main.read_current_auth", return_value={}),
            patch("app.main.list_profiles", return_value=[profile]),
            patch("app.main._resolve_current_label", return_value="max"),
            patch("app.main._touch_account_usage") as touch_mock,
        ):
            response = asyncio.run(auth_switch(request=None, payload={"label": "max"}))

        self.assertEqual(response.status_code, 200)
        touch_mock.assert_called_once_with(profile=profile)

    def test_list_profiles_links_usage_by_account_key(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            profile_path = Path(tmp) / "max.json"
            profile_path.write_text(
                json.dumps(
                    {
                        "tokens": {
                            "id_token": _jwt({"sub": "auth0|max-sub", "email": "max@example.com"}),
                            "access_token": _jwt({"sub": "auth0|max-sub"}),
                        }
                    }
                )
            )

            with (
                patch("app.accounts.settings.codex_profiles_dir", tmp),
                patch("app.accounts.get_accounts_by_ids", return_value={"sub:auth0|max-sub": _UsageState()}),
            ):
                profiles = list_profiles()

        self.assertEqual(len(profiles), 1)
        self.assertEqual(profiles[0].account_key, "sub:auth0|max-sub")
        self.assertEqual(profiles[0].usage_in_window, 10)


if __name__ == "__main__":
    unittest.main()
