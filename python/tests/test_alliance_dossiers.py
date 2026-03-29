"""Tests for alliance dossier computation logic."""

from __future__ import annotations

import importlib.util
import json
import sys
import unittest
from pathlib import Path
from types import ModuleType
from unittest.mock import MagicMock, patch

# Load the module directly to avoid full orchestrator import chain
_MODULE_PATH = Path(__file__).resolve().parents[1] / "orchestrator" / "jobs" / "compute_alliance_dossiers.py"


def _load_module() -> ModuleType:
    spec = importlib.util.spec_from_file_location("compute_alliance_dossiers", str(_MODULE_PATH))
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    # Provide stubs for imports
    for stub_name in ("orchestrator.db", "orchestrator.job_result", "orchestrator.json_utils",
                       "orchestrator.job_utils"):
        if stub_name not in sys.modules:
            sys.modules[stub_name] = MagicMock()
    spec.loader.exec_module(mod)
    return mod


_mod = _load_module()
_load_geographic_summary = _mod._load_geographic_summary
_load_behavior_metrics = _mod._load_behavior_metrics
_query_co_presence_neo4j = _mod._query_co_presence_neo4j
_query_enemies_neo4j = _mod._query_enemies_neo4j
_compute_trend = _mod._compute_trend


class TestGeographicSummary(unittest.TestCase):
    def test_basic_geographic_summary(self) -> None:
        db = MagicMock()
        db.fetch_all.return_value = [
            {"system_id": 30001, "system_name": "Jita", "region_id": 10001,
             "region_name": "The Forge", "constellation_id": 20001,
             "constellation_name": "Kimotoro", "battle_count": 10},
            {"system_id": 30002, "system_name": "Perimeter", "region_id": 10001,
             "region_name": "The Forge", "constellation_id": 20001,
             "constellation_name": "Kimotoro", "battle_count": 5},
            {"system_id": 30100, "system_name": "Amarr", "region_id": 10002,
             "region_name": "Domain", "constellation_id": 20050,
             "constellation_name": "Throne Worlds", "battle_count": 3},
        ]

        result = _load_geographic_summary(db, 100)

        self.assertEqual(result["primary_region_id"], 10001)
        self.assertEqual(result["primary_system_id"], 30001)
        self.assertEqual(len(result["top_regions"]), 2)
        self.assertEqual(result["top_regions"][0]["region_name"], "The Forge")
        self.assertEqual(result["top_regions"][0]["battle_count"], 15)  # 10 + 5
        self.assertEqual(len(result["top_systems"]), 3)

    def test_empty_geographic_summary(self) -> None:
        db = MagicMock()
        db.fetch_all.return_value = []

        result = _load_geographic_summary(db, 999)

        self.assertIsNone(result["primary_region_id"])
        self.assertIsNone(result["primary_system_id"])
        self.assertEqual(result["top_regions"], [])
        self.assertEqual(result["top_systems"], [])


class TestBehaviorMetrics(unittest.TestCase):
    def test_committed_posture(self) -> None:
        db = MagicMock()
        db.fetch_one.return_value = {
            "avg_centrality": 0.5, "avg_visibility": 0.4,
            "high_sustain_count": 60, "low_sustain_count": 10, "total_appearances": 100,
        }

        result = _load_behavior_metrics(db, 100)

        self.assertEqual(result["posture"], "committed")
        self.assertAlmostEqual(result["avg_engagement_rate"], 0.6, places=2)

    def test_opportunistic_posture(self) -> None:
        db = MagicMock()
        db.fetch_one.return_value = {
            "avg_centrality": 0.2, "avg_visibility": 0.3,
            "high_sustain_count": 5, "low_sustain_count": 45, "total_appearances": 100,
        }

        result = _load_behavior_metrics(db, 200)

        self.assertEqual(result["posture"], "opportunistic")
        self.assertAlmostEqual(result["avg_token_participation"], 0.45, places=2)

    def test_infrequent_posture(self) -> None:
        db = MagicMock()
        db.fetch_one.return_value = {
            "avg_centrality": 0, "avg_visibility": 0,
            "high_sustain_count": 1, "low_sustain_count": 1, "total_appearances": 3,
        }

        result = _load_behavior_metrics(db, 300)

        self.assertEqual(result["posture"], "infrequent")

    def test_no_data(self) -> None:
        db = MagicMock()
        db.fetch_one.return_value = None

        result = _load_behavior_metrics(db, 400)

        self.assertEqual(result["posture"], "infrequent")
        self.assertEqual(result["avg_engagement_rate"], 0.0)


class TestNeo4jCoPresence(unittest.TestCase):
    def test_graceful_degradation_no_neo4j(self) -> None:
        result = _query_co_presence_neo4j(None, 100)
        self.assertEqual(result, [])

    def test_graceful_degradation_on_exception(self) -> None:
        client = MagicMock()
        client.query.side_effect = Exception("Connection refused")

        result = _query_co_presence_neo4j(client, 100)
        self.assertEqual(result, [])

    def test_returns_parsed_results(self) -> None:
        client = MagicMock()
        client.query.return_value = [
            {"co_alliance_id": 200, "co_battles": 5, "co_pilots": 12},
            {"co_alliance_id": 300, "co_battles": 3, "co_pilots": 7},
        ]

        result = _query_co_presence_neo4j(client, 100)

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["alliance_id"], 200)
        self.assertEqual(result[0]["co_battles"], 5)


class TestNeo4jEnemies(unittest.TestCase):
    def test_graceful_degradation_no_neo4j(self) -> None:
        result = _query_enemies_neo4j(None, 100)
        self.assertEqual(result, [])

    def test_graceful_degradation_on_exception(self) -> None:
        client = MagicMock()
        client.query.side_effect = RuntimeError("Timeout")

        result = _query_enemies_neo4j(client, 100)
        self.assertEqual(result, [])

    def test_returns_parsed_results(self) -> None:
        client = MagicMock()
        client.query.return_value = [
            {"enemy_id": 500, "engagements": 8},
        ]

        result = _query_enemies_neo4j(client, 100)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["alliance_id"], 500)
        self.assertEqual(result[0]["engagements"], 8)


class TestTrendComputation(unittest.TestCase):
    def test_rising_trend(self) -> None:
        db = MagicMock()
        db.fetch_one.return_value = {"battles_7d": 10, "battles_8_30d": 3, "battles_31_90d": 5}

        result = _compute_trend(db, 100)

        self.assertEqual(result["activity_trend"], "rising")
        self.assertEqual(result["battles_7d"], 10)

    def test_declining_trend(self) -> None:
        db = MagicMock()
        db.fetch_one.return_value = {"battles_7d": 0, "battles_8_30d": 15, "battles_31_90d": 20}

        result = _compute_trend(db, 200)

        self.assertEqual(result["activity_trend"], "declining")

    def test_stable_trend(self) -> None:
        db = MagicMock()
        db.fetch_one.return_value = {"battles_7d": 3, "battles_8_30d": 10, "battles_31_90d": 20}

        result = _compute_trend(db, 300)

        self.assertEqual(result["activity_trend"], "stable")


if __name__ == "__main__":
    unittest.main()
