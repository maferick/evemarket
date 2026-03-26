from __future__ import annotations

import unittest

from orchestrator.job_runner import PHP_BRIDGED_JOB_KEYS
from orchestrator.processor_registry import PYTHON_PROCESSOR_JOB_KEYS, run_registered_processor
from orchestrator.worker_registry import DISABLED_WORKER_JOBS, WORKER_JOB_DEFINITIONS

TARGET_JOB_KEYS = [
    "market_hub_current_sync",
    "alliance_current_sync",
    "market_hub_historical_sync",
    "alliance_historical_sync",
    "current_state_refresh_sync",
    "analytics_bucket_1h_sync",
    "analytics_bucket_1d_sync",
    "activity_priority_summary_sync",
    "dashboard_summary_sync",
    "loss_demand_summary_sync",
    "doctrine_intelligence_sync",
    "deal_alerts_sync",
    "rebuild_ai_briefings",
    "forecasting_ai_sync",
    "compute_buy_all",
    "compute_signals",
]


class PythonSyncProcessorParityTests(unittest.TestCase):
    class _DbStub:
        def fetch_one(self, _query: str):
            return {"now_utc": "2026-03-26 00:00:00"}

    def test_jobs_have_python_processor_bindings(self) -> None:
        for key in TARGET_JOB_KEYS:
            self.assertIn(key, PYTHON_PROCESSOR_JOB_KEYS)

    def test_jobs_not_in_php_bridge_allowlist(self) -> None:
        for key in TARGET_JOB_KEYS:
            self.assertNotIn(key, PHP_BRIDGED_JOB_KEYS)

    def test_worker_registry_has_active_definitions(self) -> None:
        for key in TARGET_JOB_KEYS:
            self.assertIn(key, WORKER_JOB_DEFINITIONS)
            self.assertNotIn(key, DISABLED_WORKER_JOBS)

    def test_phase_sync_processors_have_no_placeholder_warning(self) -> None:
        db = self._DbStub()
        for key in TARGET_JOB_KEYS:
            if key.startswith("compute_"):
                continue
            result = run_registered_processor(key, db=db, raw_config={})
            self.assertEqual(result["status"], "success")
            self.assertEqual(result["warnings"], [])


if __name__ == "__main__":
    unittest.main()
