from __future__ import annotations

import asyncio
import json
import unittest
from pathlib import Path
from unittest.mock import patch

from app.accounts import AccountProfile
from app.main import api_accounts, api_accounts_stream


def _json_body(response) -> dict:
    return json.loads(response.body.decode('utf-8'))


class CachedAccountsApiTests(unittest.TestCase):
    def test_api_accounts_returns_cached_without_live_probe(self) -> None:
        profile = AccountProfile(
            label='acct',
            path=Path('/tmp/acct.json'),
            auth={},
            account_key='sub:auth0|acct',
            email='acct@example.com',
        )
        with (
            patch('app.main._require_internal_auth', return_value=None),
            patch('app.main._dedupe_profiles', return_value=[profile]),
            patch('app.main.list_profiles', return_value=[profile]),
            patch('app.main.read_current_auth', return_value={}),
            patch('app.main._resolve_current_label', return_value='acct'),
            patch('app.main._usage_tracking_payload', return_value={
                'usage_limit': 100,
                'usage_in_window': 20,
                'lifetime_used': 120,
                'rate_limit_refresh_at': '2026-03-22T00:00:00+00:00',
            }),
            patch('app.main._fetch_rate_limits') as probe_mock,
            patch('app.main._fetch_session_limits_for_profiles') as session_mock,
            patch('app.main.list_usage_rollovers', return_value=[]),
        ):
            response = asyncio.run(api_accounts(request=None))

        payload = _json_body(response)
        self.assertIn('accounts', payload)
        self.assertEqual(payload['accounts'][0]['label'], 'acct')
        probe_mock.assert_not_called()
        session_mock.assert_not_called()

    def test_stream_emits_snapshot_then_complete_for_empty_profiles(self) -> None:
        with (
            patch('app.main._require_internal_auth_or_query', return_value=None),
            patch('app.main.list_profiles', return_value=[]),
            patch('app.main.read_current_auth', return_value={}),
        ):
            response = asyncio.run(api_accounts_stream(request=None))

        async def collect() -> str:
            chunks = []
            async for part in response.body_iterator:
                chunks.append(part.decode('utf-8') if isinstance(part, (bytes, bytearray)) else str(part))
            return ''.join(chunks)

        body = asyncio.run(collect())
        self.assertIn('event: snapshot', body)
        self.assertIn('event: complete', body)


if __name__ == '__main__':
    unittest.main()
