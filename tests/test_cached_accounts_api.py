from __future__ import annotations

import asyncio
import json
import unittest
from pathlib import Path
from unittest.mock import patch

from app.accounts import AccountProfile
from app.main import (
    api_account_history,
    api_accounts,
    api_accounts_stream,
    api_lease_materialize,
    api_lease_telemetry,
    api_usage_history,
)


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

    def test_usage_history_returns_cumulative_and_daily_series(self) -> None:
        profile = AccountProfile(
            label='max',
            path=Path('/tmp/max.json'),
            auth={},
            account_key='acct:max',
            email='max@example.com',
        )
        snapshots = [
            {'account_id': 'acct:max', 'captured_at': '2026-03-20T10:00:00+00:00', 'lifetime_used': 100},
            {'account_id': 'acct:max', 'captured_at': '2026-03-21T10:00:00+00:00', 'lifetime_used': 130},
            {'account_id': 'acct:max', 'captured_at': '2026-03-22T10:00:00+00:00', 'lifetime_used': 170},
        ]
        with (
            patch('app.main._require_internal_auth', return_value=None),
            patch('app.main._dedupe_profiles', return_value=[profile]),
            patch('app.main.list_profiles', return_value=[profile]),
            patch('app.main._touch_profiles_usage', return_value=None),
            patch('app.main._build_cached_accounts_snapshot', return_value={
                'accounts': [{'account_key': 'acct:max', 'label': 'max', 'display_label': 'max', 'email': 'max@example.com', 'refresh_status': {}}],
                'current_label': 'max',
                'aggregate': {
                    'total_current_window_used': 10,
                    'total_current_window_limit': 100,
                    'total_remaining': 90,
                    'stale_accounts': 0,
                    'failed_accounts': 0,
                    'last_refresh_time': '2026-03-22T10:00:00+00:00',
                },
            }),
            patch('app.main.list_absolute_usage_snapshots', return_value=snapshots),
            patch('app.main.list_usage_rollovers', return_value=[]),
        ):
            response = asyncio.run(api_usage_history(request=None, range='30d'))
        payload = _json_body(response)
        self.assertEqual(payload['range'], '30d')
        self.assertTrue(len(payload['series']['daily_usage']) >= 1)
        self.assertTrue(len(payload['series']['cumulative_usage']) >= 1)

    def test_account_history_returns_current_state_and_completed_windows(self) -> None:
        profile = AccountProfile(
            label='james',
            path=Path('/tmp/james.json'),
            auth={},
            account_key='acct:james',
            email='james@example.com',
        )
        with (
            patch('app.main._require_internal_auth', return_value=None),
            patch('app.main._profile_for_label', return_value=profile),
            patch('app.main._touch_account_usage', return_value=None),
            patch('app.main._usage_tracking_payload', return_value={
                'usage_in_window': 20,
                'usage_limit': 100,
                'lifetime_used': 220,
                'rate_limit_refresh_at': '2026-03-23T00:00:00+00:00',
                'last_usage_sync_at': '2026-03-22T10:00:00+00:00',
            }),
            patch('app.main.list_usage_rollovers', return_value=[
                {
                    'window_started_at': '2026-03-21T00:00:00+00:00',
                    'window_ended_at': '2026-03-22T00:00:00+00:00',
                    'usage_used': 80,
                    'usage_limit': 100,
                    'usage_wasted': 20,
                    'rolled_over_at': '2026-03-22T00:00:05+00:00',
                }
            ]),
            patch('app.main.list_absolute_usage_snapshots', return_value=[
                {'account_id': 'acct:james', 'captured_at': '2026-03-21T10:00:00+00:00', 'lifetime_used': 200},
                {'account_id': 'acct:james', 'captured_at': '2026-03-22T10:00:00+00:00', 'lifetime_used': 220},
            ]),
        ):
            response = asyncio.run(api_account_history(request=None, label='james', range='30d'))
        payload = _json_body(response)
        self.assertEqual(payload['label'], 'james')
        self.assertIn('current_state', payload)
        self.assertIn('consumption_trend', payload)
        self.assertIn('completed_windows', payload)
        self.assertEqual(payload['current_state']['usage_in_window'], 20)
        self.assertEqual(len(payload['completed_windows']), 1)

    def test_lease_materialize_returns_credential_material_auth_json(self) -> None:
        with (
            patch('app.main._require_internal_auth', return_value=None),
            patch('app.main.materialize_broker_lease', return_value={
                'status': 'ok',
                'reason': None,
                'lease': {
                    'id': 'lease-1',
                    'credential_id': 'acct:max',
                    'metadata': {'label': 'max'},
                },
            }),
            patch('app.main._leased_profile_payload_for_credential', return_value={
                'label': 'max',
                'account_key': 'acct:max',
                'email': 'max@example.com',
                'name': 'Max',
                'provider_account_id': 'provider-max',
                'auth_json': {
                    'auth_mode': 'chatgpt',
                    'OPENAI_API_KEY': None,
                    'tokens': {
                        'id_token': 'id-token',
                        'access_token': 'access-token',
                        'refresh_token': 'refresh-token',
                        'account_id': 'acct-max',
                    },
                },
            }),
            patch('app.main._audit_broker_event', return_value=None),
        ):
            response = asyncio.run(api_lease_materialize(
                request=None,
                lease_id='lease-1',
                payload={'machine_id': 'machine-a', 'agent_id': 'agent-a'},
            ))

        payload = _json_body(response)
        self.assertEqual(payload['status'], 'ok')
        self.assertEqual(payload['credential_material']['label'], 'max')
        self.assertEqual(payload['credential_material']['auth_json']['tokens']['account_id'], 'acct-max')

    def test_lease_telemetry_returns_updated_latest_summary(self) -> None:
        with (
            patch('app.main._require_internal_auth', return_value=None),
            patch('app.main.record_broker_lease_telemetry', return_value={
                'status': 'ok',
                'reason': None,
                'lease': {
                    'id': 'lease-1',
                    'credential_id': 'acct:max',
                    'state': 'active',
                    'latest_utilization_pct': 84.0,
                    'latest_quota_remaining': 16000,
                    'reason': None,
                },
                'credential': {
                    'id': 'acct:max',
                    'state': 'leased',
                },
            }),
            patch('app.main._audit_broker_event', return_value=None),
        ):
            response = asyncio.run(api_lease_telemetry(
                request=None,
                lease_id='lease-1',
                payload={
                    'machine_id': 'machine-a',
                    'agent_id': 'agent-a',
                    'captured_at': '2026-03-23T10:00:00+00:00',
                    'requests_count': 2,
                    'tokens_in': 100,
                    'tokens_out': 50,
                    'status': 'ok',
                    'utilization_pct': 84.0,
                    'quota_remaining': 16000,
                },
            ))

        payload = _json_body(response)
        self.assertEqual(payload['status'], 'ok')
        self.assertEqual(payload['lease']['latest_utilization_pct'], 84.0)
        self.assertEqual(payload['lease']['latest_quota_remaining'], 16000)


if __name__ == '__main__':
    unittest.main()
