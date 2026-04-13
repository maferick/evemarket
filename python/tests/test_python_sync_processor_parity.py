from __future__ import annotations

import unittest
from unittest.mock import patch

from orchestrator.esi_market_adapter import EsiOrdersResponse
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
    "dashboard_summary_sync",
    "loss_demand_summary_sync",
    "doctrine_intelligence_sync",
    "deal_alerts_sync",
    "forecasting_ai_sync",
    "compute_buy_all",
]


class PythonSyncProcessorParityTests(unittest.TestCase):
    class _DbStub:
        def fetch_one(self, _query: str):
            return {"now_utc": "2026-03-26 00:00:00", "queued_jobs": 0, "running_jobs": 0, "dead_jobs": 0}

        def fetch_scalar(self, _query: str):
            return 2

        def execute(self, _query: str):
            return 2

        def fetch_all(self, _query: str):
            return [{"id": 1}, {"id": 2}]

        def upsert_intelligence_snapshot(self, **_kwargs):
            return 1

        def materialize_market_history_from_projection(self, *, source_type: str):
            return {"rows_processed": 2, "rows_written": 2}

        def upsert_sync_state(self, **_kwargs):
            return 1

        def fetch_latest_esi_access_token(self):
            return "token"

        def fetch_alliance_structure_sources(self, *, limit: int):
            return [10203040]

        def fetch_market_hub_sources(self, *, limit: int):
            return [{"source_id": 60003760, "region_id": 10000002}]

        def replace_market_orders_for_source(self, **_kwargs):
            return {"rows_processed": 1, "rows_written": 1}

        def refresh_market_order_current_projection(self, *, source_type: str):
            return {"rows_processed": 1, "rows_written": 1}

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
        with (
            patch("orchestrator.jobs.market_hub_current_sync.EsiMarketAdapter.fetch_region_orders", return_value=EsiOrdersResponse(orders=[{"issued": "2026-03-26T00:00:00Z", "expires": "2026-03-27T00:00:00Z"}], pages=1, status_code=200)),
            patch("orchestrator.jobs.alliance_current_sync.EsiMarketAdapter.fetch_structure_orders", return_value=EsiOrdersResponse(orders=[{"issued": "2026-03-26T00:00:00Z", "expires": "2026-03-27T00:00:00Z"}], pages=1, status_code=200)),
        ):
            for key in TARGET_JOB_KEYS:
                if key.startswith("compute_"):
                    continue
                result = run_registered_processor(key, db=db, raw_config={})
                self.assertEqual(result["status"], "success")
                self.assertEqual(result["warnings"], [])

    def test_production_jobs_do_not_use_placeholder_summary_or_rows(self) -> None:
        db = self._DbStub()
        with (
            patch("orchestrator.jobs.market_hub_current_sync.EsiMarketAdapter.fetch_region_orders", return_value=EsiOrdersResponse(orders=[{"issued": "2026-03-26T00:00:00Z", "expires": "2026-03-27T00:00:00Z"}], pages=1, status_code=200)),
            patch("orchestrator.jobs.alliance_current_sync.EsiMarketAdapter.fetch_structure_orders", return_value=EsiOrdersResponse(orders=[{"issued": "2026-03-26T00:00:00Z", "expires": "2026-03-27T00:00:00Z"}], pages=1, status_code=200)),
        ):
            for key in TARGET_JOB_KEYS:
                if key.startswith("compute_"):
                    continue
                result = run_registered_processor(key, db=db, raw_config={})
                status = str(result.get("status") or "success")
                self.assertNotEqual(result.get("summary"), f"{key} completed with status {status}.")


if __name__ == "__main__":
    unittest.main()
