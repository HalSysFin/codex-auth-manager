from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from app.account_usage_store import initialize_usage_store
from app.lease_broker_store import (
    acquire_broker_lease,
    get_broker_credential,
    get_broker_lease,
    get_broker_lease_status,
    initialize_lease_broker_store,
    materialize_broker_lease,
    record_broker_lease_telemetry,
    release_broker_lease,
    renew_broker_lease,
    rotate_broker_lease,
    sync_broker_credential,
)


class LeaseBrokerStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        self.db_path = Path(temp_dir.name) / "usage.sqlite3"
        initialize_usage_store(self.db_path)
        initialize_lease_broker_store(self.db_path)

    def _sync_credential(
        self,
        credential_id: str,
        *,
        label: str | None = None,
        utilization_pct: float | None = 10.0,
        quota_remaining: int | None = 50000,
        health_score: float | None = 90.0,
        weekly_reset_at: str | None = None,
        last_telemetry_at: str | None = None,
    ) -> dict:
        return sync_broker_credential(
            credential_id=credential_id,
            label=label or credential_id,
            utilization_pct=utilization_pct,
            quota_remaining=quota_remaining,
            health_score=health_score,
            weekly_reset_at=weekly_reset_at or "2026-03-29T00:00:00+00:00",
            last_telemetry_at=last_telemetry_at or "2026-03-22T12:00:00+00:00",
            metadata={"label": label or credential_id},
            db_path=self.db_path,
        )

    def test_acquire_returns_healthy_eligible_credential(self) -> None:
        self._sync_credential("cred-a", health_score=95.0, utilization_pct=12.0)
        self._sync_credential("cred-b", health_score=80.0, utilization_pct=5.0)

        result = acquire_broker_lease(
            machine_id="machine-1",
            agent_id="agent-1",
            db_path=self.db_path,
        )

        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["lease"]["credential_id"], "cred-a")
        self.assertEqual(get_broker_credential("cred-a", db_path=self.db_path)["state"], "leased")

    def test_acquire_denies_when_only_exhausted_or_over_threshold_credentials_exist(self) -> None:
        self._sync_credential("cred-a", utilization_pct=100.0)
        self._sync_credential("cred-b", utilization_pct=99.0)

        result = acquire_broker_lease(
            machine_id="machine-1",
            agent_id="agent-1",
            db_path=self.db_path,
        )

        self.assertEqual(result["status"], "denied")
        self.assertEqual(result["reason"], "no_eligible_credentials_available")

    def test_telemetry_at_100_marks_credential_exhausted(self) -> None:
        self._sync_credential("cred-a")
        lease = acquire_broker_lease(machine_id="m1", agent_id="a1", db_path=self.db_path)["lease"]

        record_broker_lease_telemetry(
            lease_id=lease["id"],
            machine_id="m1",
            agent_id="a1",
            captured_at="2026-03-22T13:00:00+00:00",
            requests_count=10,
            tokens_in=100,
            tokens_out=50,
            utilization_pct=100.0,
            quota_remaining=0,
            rate_limit_remaining=0,
            status="ok",
            last_success_at="2026-03-22T13:00:00+00:00",
            last_error_at=None,
            error_rate_1h=0.0,
            db_path=self.db_path,
        )

        credential = get_broker_credential("cred-a", db_path=self.db_path)
        self.assertEqual(credential["state"], "exhausted")
        self.assertIsNotNone(credential["exhausted_at"])

    def test_telemetry_at_100_revokes_active_lease(self) -> None:
        self._sync_credential("cred-a")
        lease = acquire_broker_lease(machine_id="m1", agent_id="a1", db_path=self.db_path)["lease"]

        result = record_broker_lease_telemetry(
            lease_id=lease["id"],
            machine_id="m1",
            agent_id="a1",
            captured_at="2026-03-22T13:00:00+00:00",
            requests_count=1,
            tokens_in=1,
            tokens_out=1,
            utilization_pct=100.0,
            quota_remaining=0,
            rate_limit_remaining=0,
            status="ok",
            last_success_at="2026-03-22T13:00:00+00:00",
            last_error_at=None,
            error_rate_1h=0.0,
            db_path=self.db_path,
        )

        self.assertEqual(result["lease"]["state"], "revoked")
        self.assertEqual(result["lease"]["reason"], "credential_exhausted")

    def test_renew_fails_on_exhausted_revoked_lease(self) -> None:
        self._sync_credential("cred-a")
        lease = acquire_broker_lease(machine_id="m1", agent_id="a1", db_path=self.db_path)["lease"]
        record_broker_lease_telemetry(
            lease_id=lease["id"],
            machine_id="m1",
            agent_id="a1",
            captured_at="2026-03-22T13:00:00+00:00",
            requests_count=1,
            tokens_in=1,
            tokens_out=1,
            utilization_pct=100.0,
            quota_remaining=0,
            rate_limit_remaining=0,
            status="ok",
            last_success_at="2026-03-22T13:00:00+00:00",
            last_error_at=None,
            error_rate_1h=0.0,
            db_path=self.db_path,
        )

        renewed = renew_broker_lease(
            lease_id=lease["id"],
            machine_id="m1",
            agent_id="a1",
            db_path=self.db_path,
        )

        self.assertEqual(renewed["status"], "denied")
        self.assertIn("lease_not_renewable", renewed["reason"])

    def test_rotate_returns_new_lease_when_eligible_replacement_exists(self) -> None:
        self._sync_credential("cred-a", health_score=95.0)
        self._sync_credential("cred-b", health_score=90.0)
        lease = acquire_broker_lease(machine_id="m1", agent_id="a1", db_path=self.db_path)["lease"]

        result = rotate_broker_lease(
            lease_id=lease["id"],
            machine_id="m1",
            agent_id="a1",
            reason="approaching_utilization_threshold",
            db_path=self.db_path,
        )

        self.assertEqual(result["status"], "ok")
        self.assertNotEqual(result["lease"]["id"], lease["id"])
        self.assertEqual(get_broker_lease(lease["id"], db_path=self.db_path)["replacement_lease_id"], result["lease"]["id"])

    def test_rotate_denies_when_no_eligible_replacement_exists(self) -> None:
        self._sync_credential("cred-a")
        lease = acquire_broker_lease(machine_id="m1", agent_id="a1", db_path=self.db_path)["lease"]

        result = rotate_broker_lease(
            lease_id=lease["id"],
            machine_id="m1",
            agent_id="a1",
            reason="approaching_utilization_threshold",
            db_path=self.db_path,
        )

        self.assertEqual(result["status"], "denied")
        self.assertEqual(result["reason"], "no_eligible_credentials_available")

    def test_rotate_never_returns_exhausted_or_over_threshold_credentials(self) -> None:
        self._sync_credential("cred-a", health_score=95.0)
        self._sync_credential("cred-b", utilization_pct=100.0, health_score=99.0)
        self._sync_credential("cred-c", utilization_pct=99.0, health_score=98.0)
        self._sync_credential("cred-d", utilization_pct=20.0, health_score=80.0)
        lease = acquire_broker_lease(machine_id="m1", agent_id="a1", db_path=self.db_path)["lease"]

        result = rotate_broker_lease(
            lease_id=lease["id"],
            machine_id="m1",
            agent_id="a1",
            reason="approaching_utilization_threshold",
            db_path=self.db_path,
        )

        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["lease"]["credential_id"], "cred-d")

    def test_above_threshold_credentials_are_not_assigned(self) -> None:
        self._sync_credential("cred-a", utilization_pct=99.0)
        self._sync_credential("cred-b", utilization_pct=94.0)

        result = acquire_broker_lease(
            machine_id="machine-1",
            agent_id="agent-1",
            db_path=self.db_path,
        )

        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["lease"]["credential_id"], "cred-b")

    def test_weekly_reset_does_not_restore_assignability_until_confirmed(self) -> None:
        self._sync_credential(
            "cred-a",
            utilization_pct=100.0,
            weekly_reset_at="2026-03-22T10:00:00+00:00",
            last_telemetry_at="2026-03-22T09:00:00+00:00",
        )

        denied = acquire_broker_lease(
            machine_id="machine-1",
            agent_id="agent-1",
            now=datetime(2026, 3, 22, 12, 0, tzinfo=timezone.utc),
            db_path=self.db_path,
        )
        self.assertEqual(denied["status"], "denied")

        self._sync_credential(
            "cred-a",
            utilization_pct=12.0,
            weekly_reset_at="2026-03-29T10:00:00+00:00",
            last_telemetry_at="2026-03-22T12:05:00+00:00",
        )
        acquired = acquire_broker_lease(
            machine_id="machine-1",
            agent_id="agent-1",
            now=datetime(2026, 3, 22, 12, 6, tzinfo=timezone.utc),
            db_path=self.db_path,
        )
        self.assertEqual(acquired["status"], "ok")

    def test_get_lease_returns_latest_telemetry_summary(self) -> None:
        self._sync_credential("cred-a")
        lease = acquire_broker_lease(machine_id="m1", agent_id="a1", db_path=self.db_path)["lease"]
        record_broker_lease_telemetry(
            lease_id=lease["id"],
            machine_id="m1",
            agent_id="a1",
            captured_at="2026-03-22T13:00:00+00:00",
            requests_count=50,
            tokens_in=1000,
            tokens_out=500,
            utilization_pct=88.0,
            quota_remaining=25000,
            rate_limit_remaining=100,
            status="ok",
            last_success_at="2026-03-22T13:00:00+00:00",
            last_error_at=None,
            error_rate_1h=0.0,
            db_path=self.db_path,
        )

        status = get_broker_lease_status(lease["id"], db_path=self.db_path)

        self.assertEqual(status["latest_utilization_pct"], 88.0)
        self.assertEqual(status["latest_quota_remaining"], 25000)
        self.assertEqual(status["credential_state"], "leased")

    def test_high_utilization_telemetry_marks_replacement_required_before_exhaustion(self) -> None:
        self._sync_credential("cred-a")
        lease = acquire_broker_lease(machine_id="m1", agent_id="a1", db_path=self.db_path)["lease"]

        result = record_broker_lease_telemetry(
            lease_id=lease["id"],
            machine_id="m1",
            agent_id="a1",
            captured_at="2026-03-22T13:00:00+00:00",
            requests_count=5,
            tokens_in=100,
            tokens_out=50,
            utilization_pct=95.0,
            quota_remaining=5000,
            rate_limit_remaining=10,
            status="ok",
            last_success_at="2026-03-22T13:00:00+00:00",
            last_error_at=None,
            error_rate_1h=0.0,
            db_path=self.db_path,
        )

        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["lease"]["state"], "rotation_required")
        status = get_broker_lease_status(lease["id"], db_path=self.db_path)
        self.assertTrue(status["replacement_required"])

    def test_release_transitions_lease_state_correctly(self) -> None:
        self._sync_credential("cred-a")
        lease = acquire_broker_lease(machine_id="m1", agent_id="a1", db_path=self.db_path)["lease"]

        result = release_broker_lease(
            lease_id=lease["id"],
            machine_id="m1",
            agent_id="a1",
            reason="done",
            db_path=self.db_path,
        )

        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["lease"]["state"], "released")
        self.assertEqual(get_broker_credential("cred-a", db_path=self.db_path)["state"], "available")

    def test_ownership_validation_blocks_another_machine_agent(self) -> None:
        self._sync_credential("cred-a")
        lease = acquire_broker_lease(machine_id="m1", agent_id="a1", db_path=self.db_path)["lease"]

        renewed = renew_broker_lease(
            lease_id=lease["id"],
            machine_id="m2",
            agent_id="a2",
            db_path=self.db_path,
        )
        released = release_broker_lease(
            lease_id=lease["id"],
            machine_id="m2",
            agent_id="a2",
            db_path=self.db_path,
        )

        self.assertEqual(renewed["status"], "denied")
        self.assertEqual(released["status"], "denied")
        self.assertEqual(get_broker_lease(lease["id"], db_path=self.db_path)["state"], "active")

    def test_materialize_updates_delivery_metadata_for_owned_active_lease(self) -> None:
        self._sync_credential("cred-a")
        lease = acquire_broker_lease(machine_id="m1", agent_id="a1", db_path=self.db_path)["lease"]

        result = materialize_broker_lease(
            lease_id=lease["id"],
            machine_id="m1",
            agent_id="a1",
            db_path=self.db_path,
        )

        self.assertEqual(result["status"], "ok")
        metadata = result["lease"]["metadata"]
        self.assertEqual(metadata["delivery_count"], 1)
        self.assertIn("first_materialized_at", metadata)
        self.assertIn("last_materialized_at", metadata)

    def test_materialize_denies_for_wrong_owner(self) -> None:
        self._sync_credential("cred-a")
        lease = acquire_broker_lease(machine_id="m1", agent_id="a1", db_path=self.db_path)["lease"]

        result = materialize_broker_lease(
            lease_id=lease["id"],
            machine_id="m2",
            agent_id="a2",
            db_path=self.db_path,
        )

        self.assertEqual(result["status"], "denied")
        self.assertEqual(result["reason"], "lease_not_found_or_not_owned")


if __name__ == "__main__":
    unittest.main()
