"""Tests for threat corridor computation logic."""

from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path
from types import ModuleType
from unittest.mock import MagicMock

_MODULE_PATH = Path(__file__).resolve().parents[1] / "orchestrator" / "jobs" / "compute_threat_corridors.py"


def _load_module() -> ModuleType:
    spec = importlib.util.spec_from_file_location("compute_threat_corridors", str(_MODULE_PATH))
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    for stub_name in ("orchestrator.db", "orchestrator.job_result", "orchestrator.json_utils",
                       "orchestrator.job_utils"):
        if stub_name not in sys.modules:
            sys.modules[stub_name] = MagicMock()
    spec.loader.exec_module(mod)
    return mod


_mod = _load_module()
_corridor_hash = _mod._corridor_hash
_score_corridor = _mod._score_corridor
_find_connected_corridors_neo4j = _mod._find_connected_corridors_neo4j


class TestCorridorHash(unittest.TestCase):
    def test_deterministic(self) -> None:
        h1 = _corridor_hash([100, 200, 300])
        h2 = _corridor_hash([100, 200, 300])
        self.assertEqual(h1, h2)

    def test_order_independent(self) -> None:
        """Hash uses sorted system IDs, so order doesn't matter."""
        h1 = _corridor_hash([300, 100, 200])
        h2 = _corridor_hash([100, 200, 300])
        self.assertEqual(h1, h2)

    def test_different_corridors_differ(self) -> None:
        h1 = _corridor_hash([100, 200])
        h2 = _corridor_hash([100, 300])
        self.assertNotEqual(h1, h2)


class TestScoreCorridor(unittest.TestCase):
    def _make_system_data(self) -> dict:
        return {
            100: {
                "system_id": 100, "system_name": "Alpha", "region_id": 10,
                "alliances": {1: {"battle_count": 5}, 2: {"battle_count": 3}},
                "total_battles": 8, "recent_battles": 4,
                "first_activity": "2026-01-01", "last_activity": "2026-03-25",
            },
            200: {
                "system_id": 200, "system_name": "Beta", "region_id": 10,
                "alliances": {1: {"battle_count": 2}, 3: {"battle_count": 6}},
                "total_battles": 8, "recent_battles": 6,
                "first_activity": "2026-02-01", "last_activity": "2026-03-28",
            },
            300: {
                "system_id": 300, "system_name": "Gamma", "region_id": 10,
                "alliances": {4: {"battle_count": 1}},
                "total_battles": 1, "recent_battles": 0,
                "first_activity": "2026-01-15", "last_activity": "2026-01-15",
            },
        }

    def test_basic_scoring(self) -> None:
        system_data = self._make_system_data()
        tracked = {1}  # Alliance 1 is friendly

        result = _score_corridor([100, 200], system_data, tracked)

        self.assertEqual(result["corridor_length"], 2)
        self.assertEqual(result["total_battles"], 16)  # 8 + 8
        self.assertEqual(result["recent_battles"], 10)  # 4 + 6
        self.assertGreater(result["corridor_score"], 0)
        self.assertEqual(result["system_names"], ["Alpha", "Beta"])
        # Hostile alliances: 2, 3 (not in tracked={1})
        self.assertIn(2, result["hostile_alliance_ids"])
        self.assertIn(3, result["hostile_alliance_ids"])

    def test_longer_corridor_scores_higher(self) -> None:
        system_data = self._make_system_data()
        tracked = {1}

        score_2 = _score_corridor([100, 200], system_data, tracked)
        score_3 = _score_corridor([100, 200, 300], system_data, tracked)

        # The 3-system corridor has lower avg battles but the length factor should still differ
        self.assertNotEqual(score_2["corridor_score"], score_3["corridor_score"])

    def test_choke_points_identified(self) -> None:
        system_data = self._make_system_data()
        tracked = set()

        result = _score_corridor([100, 200, 300], system_data, tracked)

        # Systems 100 and 200 have 8 battles each, avg across 3 is ~5.7, threshold ~8.5
        # So they're at or above the threshold — both should be choke points
        # System 300 (1 battle) should NOT be a choke point
        self.assertNotIn(300, result["choke_systems"])

    def test_no_tracked_alliances(self) -> None:
        """When no alliances are tracked, all are treated as hostile."""
        system_data = self._make_system_data()
        result = _score_corridor([100], system_data, set())

        # All alliances in system 100 (1 and 2) should be hostile
        self.assertIn(1, result["hostile_alliance_ids"])
        self.assertIn(2, result["hostile_alliance_ids"])


class TestNeo4jCorridorDiscovery(unittest.TestCase):
    def test_graceful_degradation_no_client(self) -> None:
        result = _find_connected_corridors_neo4j(None, [100, 200])
        self.assertEqual(result, [])

    def test_graceful_degradation_empty_systems(self) -> None:
        client = MagicMock()
        result = _find_connected_corridors_neo4j(client, [])
        self.assertEqual(result, [])

    def test_graceful_degradation_on_exception(self) -> None:
        client = MagicMock()
        client.query.side_effect = Exception("Neo4j down")

        result = _find_connected_corridors_neo4j(client, [100, 200])
        self.assertEqual(result, [])

    def test_returns_corridor_lists(self) -> None:
        client = MagicMock()
        client.query.return_value = [
            {"corridor_systems": [100, 200, 300], "corridor_length": 3},
        ]

        result = _find_connected_corridors_neo4j(client, [100, 200, 300])

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], [100, 200, 300])


if __name__ == "__main__":
    unittest.main()
