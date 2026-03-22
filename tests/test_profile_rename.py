from __future__ import annotations

import asyncio
import unittest
from unittest.mock import patch

from fastapi import HTTPException

from app.main import auth_rename


class ProfileRenameTests(unittest.TestCase):
    def test_rename_profile_db_record_and_usage_id(self) -> None:
        with (
            patch("app.main._require_internal_auth", return_value=None),
            patch("app.main.get_saved_profile", side_effect=[{"label": "old", "auth_json": {}}, None]),
            patch("app.main.rename_saved_profile", return_value=True) as rename_profile_mock,
            patch("app.main.rename_account_data") as rename_usage_mock,
            patch("app.main.current_label", return_value=None),
        ):
            response = asyncio.run(
                auth_rename(
                    request=None,  # ignored by patched auth guard
                    payload={"old_label": "old", "new_label": "new"},
                )
            )

        rename_profile_mock.assert_called_once_with("old", "new")
        rename_usage_mock.assert_called_once_with("old", "new")
        self.assertEqual(response.status_code, 200)

    def test_rename_rejects_invalid_label(self) -> None:
        with patch("app.main._require_internal_auth", return_value=None):
            with self.assertRaises(HTTPException) as exc:
                asyncio.run(
                    auth_rename(
                        request=None,
                        payload={"old_label": "old", "new_label": "../bad"},
                    )
                )
        self.assertEqual(exc.exception.status_code, 400)


if __name__ == "__main__":
    unittest.main()
