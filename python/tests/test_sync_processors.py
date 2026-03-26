from __future__ import annotations

import sqlite3
import unittest
from dataclasses import dataclass
from typing import Any
from unittest.mock import patch

from orchestrator.esi_market_adapter import EsiOrdersResponse
from orchestrator.jobs.activity_priority_summary_sync import run_activity_priority_summary_sync
from orchestrator.jobs.alliance_current_sync import run_alliance_current_sync
from orchestrator.jobs.alliance_historical_sync import run_alliance_historical_sync
from orchestrator.jobs.analytics_bucket_1d_sync import run_analytics_bucket_1d_sync
from orchestrator.jobs.analytics_bucket_1h_sync import run_analytics_bucket_1h_sync
from orchestrator.jobs.current_state_refresh_sync import run_current_state_refresh_sync
from orchestrator.jobs.dashboard_summary_sync import run_dashboard_summary_sync
from orchestrator.jobs.deal_alerts_sync import run_deal_alerts_sync
from orchestrator.jobs.doctrine_intelligence_sync import run_doctrine_intelligence_sync
from orchestrator.jobs.forecasting_ai_sync import run_forecasting_ai_sync
from orchestrator.jobs.loss_demand_summary_sync import run_loss_demand_summary_sync
from orchestrator.jobs.market_hub_current_sync import run_market_hub_current_sync
from orchestrator.jobs.market_hub_historical_sync import run_market_hub_historical_sync
from orchestrator.jobs.rebuild_ai_briefings import run_rebuild_ai_briefings


class _SpyDb:
    def __init__(self) -> None:
        self.fetch_scalar_values: list[int] = [0]
        self.fetch_all_values: list[list[dict[str, Any]]] = [[]]
        self.fetch_one_values: list[dict[str, Any]] = [{"now_utc": "2026-03-26 00:00:00"}]
        self.execute_values: list[int] = [0]
        self.upsert_snapshot_values: list[int] = [0]
        self.history_stats = {"rows_processed": 0, "rows_written": 0}
        self.replace_stats = {"rows_processed": 0, "rows_written": 0}
        self.projection_stats = {"rows_processed": 0, "rows_written": 0}
        self.alliance_structures: list[int] = []
        self.market_hub_sources: list[dict[str, int]] = []
        self.access_token: str | None = None
        self.region_by_system_id: dict[int, int] = {}
        self.calls: list[tuple[str, Any]] = []

    def fetch_one(self, query: str) -> dict[str, Any]:
        self.calls.append(("fetch_one", query))
        return self.fetch_one_values.pop(0) if self.fetch_one_values else {"now_utc": "2026-03-26 00:00:00"}

    def fetch_scalar(self, query: str) -> int:
        self.calls.append(("fetch_scalar", query))
        return int(self.fetch_scalar_values.pop(0) if self.fetch_scalar_values else 0)

    def execute(self, query: str) -> int:
        self.calls.append(("execute", query))
        return int(self.execute_values.pop(0) if self.execute_values else 0)

    def fetch_all(self, query: str) -> list[dict[str, Any]]:
        self.calls.append(("fetch_all", query))
        return self.fetch_all_values.pop(0) if self.fetch_all_values else []

    def upsert_intelligence_snapshot(self, **kwargs: Any) -> int:
        self.calls.append(("upsert_intelligence_snapshot", kwargs))
        return int(self.upsert_snapshot_values.pop(0) if self.upsert_snapshot_values else 0)

    def materialize_market_history_from_projection(self, *, source_type: str) -> dict[str, int]:
        self.calls.append(("materialize_market_history_from_projection", source_type))
        return dict(self.history_stats)

    def upsert_sync_state(self, **kwargs: Any) -> int:
        self.calls.append(("upsert_sync_state", kwargs))
        return 1

    def fetch_latest_esi_access_token(self) -> str | None:
        self.calls.append(("fetch_latest_esi_access_token", None))
        return self.access_token

    def fetch_alliance_structure_sources(self, *, limit: int) -> list[int]:
        self.calls.append(("fetch_alliance_structure_sources", limit))
        return list(self.alliance_structures)

    def fetch_market_hub_sources(self, *, limit: int) -> list[dict[str, int]]:
        self.calls.append(("fetch_market_hub_sources", limit))
        return list(self.market_hub_sources)

    def fetch_region_id_for_system(self, *, system_id: int) -> int:
        self.calls.append(("fetch_region_id_for_system", system_id))
        return int(self.region_by_system_id.get(system_id, 0))

    def replace_market_orders_for_source(self, **kwargs: Any) -> dict[str, int]:
        self.calls.append(("replace_market_orders_for_source", kwargs))
        return dict(self.replace_stats)

    def refresh_market_order_current_projection(self, *, source_type: str) -> dict[str, int]:
        self.calls.append(("refresh_market_order_current_projection", source_type))
        return dict(self.projection_stats)


class SyncProcessorCoverageTests(unittest.TestCase):
    def test_sql_backed_sync_processors_cover_fetch_and_write_paths(self) -> None:
        jobs = [
            (run_current_state_refresh_sync, [4], [4], 1, "upsert_intelligence_snapshot", 4, 4),
            (run_analytics_bucket_1h_sync, [6], [3, 2], 0, None, 6, 5),
            (run_analytics_bucket_1d_sync, [7], [4, 1], 0, None, 7, 5),
            (run_activity_priority_summary_sync, [8], [5], 0, None, 8, 5),
            (run_deal_alerts_sync, [10], [6], 0, None, 10, 6),
            (run_doctrine_intelligence_sync, [9], [4, 3], 0, None, 9, 7),
            (run_dashboard_summary_sync, [2, 11], [], 1, "upsert_intelligence_snapshot", 20, 1),
            (run_loss_demand_summary_sync, [], [], 1, "upsert_intelligence_snapshot", 2, 1),
            (run_forecasting_ai_sync, [], [], 1, "upsert_intelligence_snapshot", 3, 1),
            (run_rebuild_ai_briefings, [5], [4], 1, "upsert_intelligence_snapshot", 5, 4),
        ]

        for runner, scalar_values, execute_values, snapshot_writes, write_api, expected_processed, expected_written in jobs:
            with self.subTest(job=runner.__name__):
                db = _SpyDb()
                db.fetch_scalar_values = list(scalar_values) or [0]
                db.execute_values = list(execute_values) or [0]
                db.fetch_all_values = [[{"id": 1}, {"id": 2}]] if runner is run_loss_demand_summary_sync else [[{"id": 1}, {"id": 2}, {"id": 3}]]
                if runner is run_dashboard_summary_sync:
                    db.fetch_one_values = [{"queued_jobs": 4, "running_jobs": 2, "dead_jobs": 1}, {"now_utc": "2026-03-26 00:00:00"}]
                db.upsert_snapshot_values = [snapshot_writes]

                result = runner(db)

                self.assertEqual(result["status"], "success")
                self.assertEqual(result["rows_processed"], expected_processed)
                self.assertEqual(result["rows_written"], expected_written)
                self.assertTrue(any(call[0] == "fetch_one" for call in db.calls))
                if scalar_values:
                    self.assertTrue(any(call[0] == "fetch_scalar" for call in db.calls))
                if execute_values:
                    self.assertTrue(any(call[0] == "execute" for call in db.calls))
                if write_api is not None:
                    self.assertTrue(any(call[0] == write_api for call in db.calls))

    def test_market_hub_and_alliance_processors_fetch_sources_and_write_projection(self) -> None:
        db = _SpyDb()
        db.market_hub_sources = [{"source_id": 60003760, "region_id": 10000002}]
        db.alliance_structures = [10203040]
        db.access_token = "token"
        db.replace_stats = {"rows_processed": 2, "rows_written": 2}
        db.projection_stats = {"rows_processed": 1, "rows_written": 1}

        with (
            patch("orchestrator.jobs.market_hub_current_sync.EsiMarketAdapter.fetch_region_orders", return_value=EsiOrdersResponse(orders=[{"issued": "2026-03-26T00:00:00Z", "expires": "2026-03-27T00:00:00Z"}], pages=1, status_code=200)) as region_fetch,
            patch("orchestrator.jobs.alliance_current_sync.EsiMarketAdapter.fetch_structure_orders", return_value=EsiOrdersResponse(orders=[{"issued": "2026-03-26T00:00:00Z", "expires": "2026-03-27T00:00:00Z"}], pages=1, status_code=200)) as structure_fetch,
        ):
            market_result = run_market_hub_current_sync(db)
            alliance_result = run_alliance_current_sync(db)

        region_fetch.assert_called_once()
        structure_fetch.assert_called_once()
        self.assertEqual(market_result["rows_processed"], 3)
        self.assertEqual(market_result["rows_written"], 3)
        self.assertEqual(alliance_result["rows_processed"], 3)
        self.assertEqual(alliance_result["rows_written"], 3)
        self.assertTrue(any(call[0] == "replace_market_orders_for_source" for call in db.calls))
        self.assertTrue(any(call[0] == "refresh_market_order_current_projection" for call in db.calls))

    def test_historical_sync_processors_write_sync_state(self) -> None:
        db = _SpyDb()
        db.history_stats = {"rows_processed": 4, "rows_written": 4}

        market_result = run_market_hub_historical_sync(db)
        alliance_result = run_alliance_historical_sync(db)

        self.assertEqual(market_result["rows_processed"], 4)
        self.assertEqual(market_result["rows_written"], 4)
        self.assertEqual(alliance_result["rows_processed"], 4)
        self.assertEqual(alliance_result["rows_written"], 4)
        self.assertGreaterEqual(sum(1 for call in db.calls if call[0] == "upsert_sync_state"), 2)

    def test_market_hub_structure_source_can_resolve_region_and_filter_location(self) -> None:
        db = _SpyDb()
        db.market_hub_sources = [{"source_id": 102030405060, "region_id": 0, "source_kind": "structure"}]
        db.access_token = "token"
        db.region_by_system_id = {30000142: 10000002}
        db.replace_stats = {"rows_processed": 1, "rows_written": 1}
        db.projection_stats = {"rows_processed": 1, "rows_written": 1}

        fixture_orders = [
            {"location_id": 102030405060, "issued": "2026-03-26T00:00:00Z", "expires": "2026-03-27T00:00:00Z"},
            {"location_id": 60003760, "issued": "2026-03-26T00:00:00Z", "expires": "2026-03-27T00:00:00Z"},
        ]
        with (
            patch("orchestrator.jobs.market_hub_current_sync.EsiMarketAdapter.fetch_structure_metadata", return_value={"solar_system_id": 30000142}) as structure_meta_fetch,
            patch("orchestrator.jobs.market_hub_current_sync.EsiMarketAdapter.fetch_region_orders", return_value=EsiOrdersResponse(orders=fixture_orders, pages=1, status_code=200)) as region_fetch,
            patch("orchestrator.jobs.market_hub_current_sync.EsiMarketAdapter.fetch_structure_orders") as structure_orders_fetch,
        ):
            result = run_market_hub_current_sync(db)

        self.assertEqual(result["status"], "success")
        self.assertEqual(result["rows_processed"], 2)
        self.assertEqual(result["rows_written"], 2)
        structure_meta_fetch.assert_called_once()
        region_fetch.assert_called_once_with(region_id=10000002, order_type="all", page=1)
        structure_orders_fetch.assert_not_called()

    def test_failure_paths_emit_meaningful_status_and_warnings(self) -> None:
        db_source_unavailable = _SpyDb()
        db_source_unavailable.market_hub_sources = [{"source_id": 60003760, "region_id": 10000002}]
        db_source_unavailable.projection_stats = {"rows_processed": 0, "rows_written": 0}

        with patch("orchestrator.jobs.market_hub_current_sync.EsiMarketAdapter.fetch_region_orders", side_effect=RuntimeError("esi unavailable")):
            unavailable_result = run_market_hub_current_sync(db_source_unavailable)

        self.assertEqual(unavailable_result["status"], "success")
        self.assertTrue(any("fetch failed" in warning for warning in unavailable_result["warnings"]))

        db_malformed = _SpyDb()
        db_malformed.market_hub_sources = [{"source_id": 60003760, "region_id": 10000002}]
        db_malformed.projection_stats = {"rows_processed": 0, "rows_written": 0}
        with patch(
            "orchestrator.jobs.market_hub_current_sync.EsiMarketAdapter.fetch_region_orders",
            return_value=EsiOrdersResponse(orders=["invalid-order"], pages=1, status_code=200),
        ):
            malformed_result = run_market_hub_current_sync(db_malformed)

        self.assertEqual(malformed_result["status"], "failed")
        self.assertTrue(any("ValueError" in warning for warning in malformed_result["warnings"]))

        db_write_error = _SpyDb()
        db_write_error.fetch_scalar_values = [2]
        with patch.object(db_write_error, "execute", side_effect=RuntimeError("write failed")):
            write_error_result = run_deal_alerts_sync(db_write_error)

        self.assertEqual(write_error_result["status"], "failed")
        self.assertTrue(any("RuntimeError: write failed" in warning for warning in write_error_result["warnings"]))


@dataclass
class _SqliteAllianceFixtureDb:
    connection: sqlite3.Connection

    @classmethod
    def build(cls) -> "_SqliteAllianceFixtureDb":
        conn = sqlite3.connect(":memory:")
        conn.execute("CREATE TABLE market_orders_current (source_type TEXT, source_id INTEGER, observed_at TEXT, issued TEXT, expires TEXT)")
        conn.execute("CREATE TABLE market_order_current_projection (source_type TEXT, source_id INTEGER, row_count INTEGER)")
        conn.execute("CREATE TABLE sync_state (dataset_key TEXT PRIMARY KEY, status TEXT, row_count INTEGER)")
        return cls(conn)

    def fetch_one(self, _query: str) -> dict[str, Any]:
        return {"now_utc": "2026-03-26 00:00:00"}

    def fetch_latest_esi_access_token(self) -> str | None:
        return "token"

    def fetch_alliance_structure_sources(self, *, limit: int) -> list[int]:
        return [10203040][:limit]

    def replace_market_orders_for_source(self, *, source_type: str, source_id: int, observed_at: str, orders: list[dict[str, Any]]) -> dict[str, int]:
        for order in orders:
            self.connection.execute(
                "INSERT INTO market_orders_current (source_type, source_id, observed_at, issued, expires) VALUES (?, ?, ?, ?, ?)",
                (source_type, source_id, observed_at, str(order.get("issued") or ""), str(order.get("expires") or "")),
            )
        self.connection.commit()
        return {"rows_processed": len(orders), "rows_written": len(orders)}

    def refresh_market_order_current_projection(self, *, source_type: str) -> dict[str, int]:
        row = self.connection.execute(
            "SELECT COUNT(*) FROM market_orders_current WHERE source_type = ?", (source_type,)
        ).fetchone()
        processed = int(row[0] if row else 0)
        self.connection.execute("DELETE FROM market_order_current_projection WHERE source_type = ?", (source_type,))
        self.connection.execute(
            "INSERT INTO market_order_current_projection (source_type, source_id, row_count) VALUES (?, ?, ?)",
            (source_type, 10203040, processed),
        )
        self.connection.commit()
        return {"rows_processed": processed, "rows_written": 1 if processed > 0 else 0}

    def upsert_sync_state(self, *, dataset_key: str, status: str, row_count: int, cursor: str | None) -> int:
        self.connection.execute(
            "INSERT INTO sync_state (dataset_key, status, row_count) VALUES (?, ?, ?) ON CONFLICT(dataset_key) DO UPDATE SET status=excluded.status, row_count=excluded.row_count",
            (dataset_key, status, row_count),
        )
        self.connection.commit()
        return 1


class AllianceCurrentSyncIntegrationTests(unittest.TestCase):
    def test_alliance_current_sync_e2e_with_sqlite_fixture_layer(self) -> None:
        db = _SqliteAllianceFixtureDb.build()
        fixture_orders = [
            {"issued": "2026-03-26T00:00:00Z", "expires": "2026-03-27T00:00:00Z"},
            {"issued": "2026-03-26T01:00:00Z", "expires": "2026-03-27T01:00:00Z"},
        ]
        with patch(
            "orchestrator.jobs.alliance_current_sync.EsiMarketAdapter.fetch_structure_orders",
            return_value=EsiOrdersResponse(orders=fixture_orders, pages=1, status_code=200),
        ):
            result = run_alliance_current_sync(db)

        projected = db.connection.execute("SELECT row_count FROM market_order_current_projection WHERE source_type = 'alliance_structure'").fetchone()
        sync_state = db.connection.execute("SELECT status, row_count FROM sync_state WHERE dataset_key = 'alliance.structure.orders.current'").fetchone()

        self.assertEqual(result["status"], "success")
        self.assertEqual(result["rows_processed"], 4)
        self.assertEqual(result["rows_written"], 3)
        self.assertEqual(int(projected[0]), 2)
        self.assertEqual(sync_state[0], "success")


if __name__ == "__main__":
    unittest.main()
