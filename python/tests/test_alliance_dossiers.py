"""Tests for alliance dossier computation logic.

Covers:
- Geographic summary aggregation (killmail-based)
- Behavior metrics and posture classification (killmail-based)
- Neo4j co-presence and enemy queries (including graceful degradation)
- SQL fallback queries (contract alignment with Neo4j results)
- Trend computation (killmail-based)
- Payload contract integrity (producer keys match consumer expectations)
"""

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
_load_ship_summary = _mod._load_ship_summary
_query_co_presence_neo4j = _mod._query_co_presence_neo4j
_query_co_presence_sql = _mod._query_co_presence_sql
_query_enemies_neo4j = _mod._query_enemies_neo4j
_query_enemies_sql = _mod._query_enemies_sql
_compute_trend = _mod._compute_trend
_classify_fleet_function = _mod._classify_fleet_function

# ── Canonical payload key contracts ──────────────────────────────────────
# These are the keys the PHP consumer expects.  Tests below assert that
# every producer function returns exactly these keys.

CO_PRESENT_REQUIRED_KEYS = {"alliance_id", "shared_battles", "shared_pilots", "source"}
ENEMY_REQUIRED_KEYS = {"alliance_id", "engagements", "source"}
REGION_REQUIRED_KEYS = {"region_id", "region_name", "killmail_count"}
SYSTEM_REQUIRED_KEYS = {"system_id", "system_name", "region_name", "killmail_count"}
SHIP_CLASS_REQUIRED_KEYS = {"class", "count"}
SHIP_TYPE_REQUIRED_KEYS = {"type_id", "name", "fleet_function", "count"}
BEHAVIOR_REQUIRED_KEYS = {"kills_per_week", "avg_gang_size", "solo_ratio",
                          "total_kills", "total_losses", "kill_loss_ratio",
                          "posture", "active_pilots"}
TREND_REQUIRED_KEYS = {"killmails_7d", "killmails_8_30d", "killmails_31_90d",
                       "isk_destroyed_7d", "isk_destroyed_8_30d", "isk_destroyed_31_90d",
                       "activity_trend"}
POSTURE_VALUES = {"aggressive", "opportunistic", "infrequent", "balanced"}


class TestGeographicSummary(unittest.TestCase):
    def test_basic_geographic_summary(self) -> None:
        db = MagicMock()
        db.fetch_all.return_value = [
            {"system_id": 30001, "system_name": "Jita", "region_id": 10001,
             "region_name": "The Forge",
             "killmail_count": 10},
            {"system_id": 30002, "system_name": "Perimeter", "region_id": 10001,
             "region_name": "The Forge",
             "killmail_count": 5},
            {"system_id": 30100, "system_name": "Amarr", "region_id": 10002,
             "region_name": "Domain",
             "killmail_count": 3},
        ]

        result = _load_geographic_summary(db, 100)

        self.assertEqual(result["primary_region_id"], 10001)
        self.assertEqual(result["primary_system_id"], 30001)
        self.assertEqual(len(result["top_regions"]), 2)
        self.assertEqual(result["top_regions"][0]["region_name"], "The Forge")
        self.assertEqual(result["top_regions"][0]["killmail_count"], 15)  # 10 + 5
        self.assertEqual(len(result["top_systems"]), 3)

        # Contract: all region items have required keys
        for r in result["top_regions"]:
            self.assertTrue(REGION_REQUIRED_KEYS.issubset(r.keys()), f"Missing keys: {REGION_REQUIRED_KEYS - r.keys()}")
        for s in result["top_systems"]:
            self.assertTrue(SYSTEM_REQUIRED_KEYS.issubset(s.keys()), f"Missing keys: {SYSTEM_REQUIRED_KEYS - s.keys()}")

    def test_empty_geographic_summary(self) -> None:
        db = MagicMock()
        db.fetch_all.return_value = []

        result = _load_geographic_summary(db, 999)

        self.assertIsNone(result["primary_region_id"])
        self.assertIsNone(result["primary_system_id"])
        self.assertEqual(result["top_regions"], [])
        self.assertEqual(result["top_systems"], [])


class TestShipSummary(unittest.TestCase):
    def test_ship_class_and_type_keys(self) -> None:
        db = MagicMock()
        db.fetch_all.return_value = [
            {"ship_type_id": 24690, "ship_name": "Maelstrom", "group_name": "Battleship",
             "market_group_name": "Standard Battleships", "usage_count": 20},
            {"ship_type_id": 11987, "ship_name": "Guardian", "group_name": "Logistics Cruiser",
             "market_group_name": "Logistics", "usage_count": 15},
            {"ship_type_id": 22474, "ship_name": "Vulture", "group_name": "Command Ship",
             "market_group_name": "Command Ships", "usage_count": 5},
        ]

        result = _load_ship_summary(db, 100)

        # Contract: ship class items
        for sc in result["top_ship_classes"]:
            self.assertTrue(SHIP_CLASS_REQUIRED_KEYS.issubset(sc.keys()), f"Missing keys: {SHIP_CLASS_REQUIRED_KEYS - sc.keys()}")

        # Contract: ship type items
        for st in result["top_ship_types"]:
            self.assertTrue(SHIP_TYPE_REQUIRED_KEYS.issubset(st.keys()), f"Missing keys: {SHIP_TYPE_REQUIRED_KEYS - st.keys()}")

        # Verify fleet function derivation
        type_by_id = {t["type_id"]: t for t in result["top_ship_types"]}
        self.assertEqual(type_by_id[24690]["fleet_function"], "dps")
        self.assertEqual(type_by_id[11987]["fleet_function"], "logistics")
        self.assertEqual(type_by_id[22474]["fleet_function"], "command")

    def test_capital_classification(self) -> None:
        db = MagicMock()
        db.fetch_all.return_value = [
            {"ship_type_id": 19720, "ship_name": "Revelation", "group_name": "Dreadnought",
             "market_group_name": "Capital Ships", "usage_count": 3},
        ]

        result = _load_ship_summary(db, 100)
        self.assertEqual(result["top_ship_types"][0]["fleet_function"], "capital")
        self.assertEqual(result["top_ship_classes"][0]["class"], "capital")


class TestFleetFunctionClassification(unittest.TestCase):
    def test_logistics(self) -> None:
        self.assertEqual(_classify_fleet_function("Logistics Cruiser", ""), "logistics")
        self.assertEqual(_classify_fleet_function("Force Auxiliary", "Capital Ships"), "logistics")

    def test_capital(self) -> None:
        self.assertEqual(_classify_fleet_function("Dreadnought", ""), "capital")
        self.assertEqual(_classify_fleet_function("Carrier", "Capital Ships"), "capital")
        self.assertEqual(_classify_fleet_function("Titan", ""), "capital")

    def test_command(self) -> None:
        self.assertEqual(_classify_fleet_function("Command Ship", ""), "command")
        self.assertEqual(_classify_fleet_function("Command Destroyer", ""), "command")

    def test_dps_default(self) -> None:
        self.assertEqual(_classify_fleet_function("Battleship", "Standard Battleships"), "dps")
        self.assertEqual(_classify_fleet_function("Interceptor", ""), "dps")
        self.assertEqual(_classify_fleet_function("", ""), "dps")


class TestBehaviorMetrics(unittest.TestCase):
    def test_aggressive_posture(self) -> None:
        db = MagicMock()
        # First call: total kills + active pilots
        # Second call: gang size stats
        # Third call: losses
        db.fetch_one.side_effect = [
            {"total_kills": 1000, "active_pilots": 50, "earliest_kill": "2026-01-01", "latest_kill": "2026-04-01"},
            {"avg_gang_size": 15.0, "solo_kills": 10, "total_counted": 1000},
            {"total_losses": 200},
        ]

        result = _load_behavior_metrics(db, 100)

        self.assertEqual(result["posture"], "aggressive")
        self.assertIn(result["posture"], POSTURE_VALUES)
        self.assertTrue(BEHAVIOR_REQUIRED_KEYS.issubset(result.keys()))
        self.assertGreater(result["kills_per_week"], 0)
        self.assertGreater(result["avg_gang_size"], 0)
        self.assertEqual(result["total_kills"], 1000)
        self.assertEqual(result["total_losses"], 200)

    def test_opportunistic_posture(self) -> None:
        db = MagicMock()
        db.fetch_one.side_effect = [
            {"total_kills": 200, "active_pilots": 15, "earliest_kill": "2026-01-01", "latest_kill": "2026-04-01"},
            {"avg_gang_size": 5.0, "solo_kills": 30, "total_counted": 200},
            {"total_losses": 100},
        ]

        result = _load_behavior_metrics(db, 200)

        self.assertEqual(result["posture"], "opportunistic")
        self.assertIn(result["posture"], POSTURE_VALUES)

    def test_infrequent_posture(self) -> None:
        db = MagicMock()
        db.fetch_one.side_effect = [
            {"total_kills": 5, "active_pilots": 3, "earliest_kill": "2026-03-01", "latest_kill": "2026-03-15"},
            {"avg_gang_size": 3.0, "solo_kills": 2, "total_counted": 5},
            {"total_losses": 2},
        ]

        result = _load_behavior_metrics(db, 300)

        self.assertEqual(result["posture"], "infrequent")
        self.assertIn(result["posture"], POSTURE_VALUES)

    def test_balanced_posture(self) -> None:
        db = MagicMock()
        db.fetch_one.side_effect = [
            {"total_kills": 50, "active_pilots": 20, "earliest_kill": "2026-01-01", "latest_kill": "2026-04-01"},
            {"avg_gang_size": 12.0, "solo_kills": 5, "total_counted": 50},
            {"total_losses": 30},
        ]

        result = _load_behavior_metrics(db, 400)

        self.assertEqual(result["posture"], "balanced")
        self.assertIn(result["posture"], POSTURE_VALUES)

    def test_no_data(self) -> None:
        db = MagicMock()
        db.fetch_one.side_effect = [None, None, None]

        result = _load_behavior_metrics(db, 500)

        self.assertEqual(result["posture"], "infrequent")
        self.assertEqual(result["kills_per_week"], 0)
        self.assertTrue(BEHAVIOR_REQUIRED_KEYS.issubset(result.keys()))


class TestNeo4jCoPresence(unittest.TestCase):
    def test_graceful_degradation_no_neo4j(self) -> None:
        result = _query_co_presence_neo4j(None, 100)
        self.assertEqual(result, [])

    def test_graceful_degradation_on_exception(self) -> None:
        client = MagicMock()
        client.query.side_effect = Exception("Connection refused")

        result = _query_co_presence_neo4j(client, 100)
        self.assertEqual(result, [])

    def test_returns_canonical_keys(self) -> None:
        """Neo4j co-presence results use the canonical contract keys."""
        client = MagicMock()
        client.query.return_value = [
            {"co_alliance_id": 200, "shared_battles": 5, "shared_pilots": 12},
            {"co_alliance_id": 300, "shared_battles": 3, "shared_pilots": 7},
        ]

        result = _query_co_presence_neo4j(client, 100)

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["alliance_id"], 200)
        self.assertEqual(result[0]["shared_battles"], 5)
        self.assertEqual(result[0]["shared_pilots"], 12)
        self.assertEqual(result[0]["source"], "neo4j")
        # Full contract check
        for item in result:
            self.assertTrue(CO_PRESENT_REQUIRED_KEYS.issubset(item.keys()),
                          f"Missing keys: {CO_PRESENT_REQUIRED_KEYS - item.keys()}")

    def test_empty_result_returns_empty_list(self) -> None:
        client = MagicMock()
        client.query.return_value = []

        result = _query_co_presence_neo4j(client, 100)

        self.assertEqual(result, [])
        self.assertEqual(client.query.call_count, 1)


class TestSqlCoPresence(unittest.TestCase):
    def test_returns_canonical_keys(self) -> None:
        """SQL co-presence fallback uses the same keys as Neo4j."""
        db = MagicMock()
        db.fetch_all.return_value = [
            {"co_alliance_id": 200, "shared_battles": 5, "shared_pilots": 12},
        ]

        result = _query_co_presence_sql(db, 100)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["alliance_id"], 200)
        self.assertEqual(result[0]["shared_battles"], 5)
        self.assertEqual(result[0]["shared_pilots"], 12)
        self.assertEqual(result[0]["source"], "sql")
        for item in result:
            self.assertTrue(CO_PRESENT_REQUIRED_KEYS.issubset(item.keys()),
                          f"Missing keys: {CO_PRESENT_REQUIRED_KEYS - item.keys()}")

    def test_key_parity_with_neo4j(self) -> None:
        """SQL and Neo4j return the same key set (except source value)."""
        db = MagicMock()
        db.fetch_all.return_value = [
            {"co_alliance_id": 300, "shared_battles": 3, "shared_pilots": 5},
        ]
        client = MagicMock()
        client.query.return_value = [
            {"co_alliance_id": 300, "shared_battles": 3, "shared_pilots": 5},
        ]

        sql_result = _query_co_presence_sql(db, 100)
        neo4j_result = _query_co_presence_neo4j(client, 100)

        self.assertEqual(set(sql_result[0].keys()), set(neo4j_result[0].keys()),
                        "SQL and Neo4j co-presence results must have identical key sets")

    def test_empty_result(self) -> None:
        db = MagicMock()
        db.fetch_all.return_value = []

        result = _query_co_presence_sql(db, 999)
        self.assertEqual(result, [])


class TestNeo4jEnemies(unittest.TestCase):
    def test_graceful_degradation_no_neo4j(self) -> None:
        result = _query_enemies_neo4j(None, 100)
        self.assertEqual(result, [])

    def test_graceful_degradation_on_exception(self) -> None:
        client = MagicMock()
        client.query.side_effect = RuntimeError("Timeout")

        result = _query_enemies_neo4j(client, 100)
        self.assertEqual(result, [])

    def test_returns_canonical_keys(self) -> None:
        """Neo4j enemy results use the canonical contract keys."""
        client = MagicMock()
        client.query.return_value = [
            {"enemy_id": 500, "engagements": 8},
        ]

        result = _query_enemies_neo4j(client, 100)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["alliance_id"], 500)
        self.assertEqual(result[0]["engagements"], 8)
        self.assertEqual(result[0]["source"], "neo4j")
        for item in result:
            self.assertTrue(ENEMY_REQUIRED_KEYS.issubset(item.keys()),
                          f"Missing keys: {ENEMY_REQUIRED_KEYS - item.keys()}")

    def test_empty_result_returns_empty_list(self) -> None:
        client = MagicMock()
        client.query.return_value = []

        result = _query_enemies_neo4j(client, 100)

        self.assertEqual(result, [])
        self.assertEqual(client.query.call_count, 1)


class TestSqlEnemies(unittest.TestCase):
    def test_returns_canonical_keys(self) -> None:
        """SQL enemy fallback uses the same keys as Neo4j."""
        db = MagicMock()
        db.fetch_all.return_value = [
            {"enemy_id": 500, "engagements": 8},
        ]

        result = _query_enemies_sql(db, 100)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["alliance_id"], 500)
        self.assertEqual(result[0]["engagements"], 8)
        self.assertEqual(result[0]["source"], "sql")
        for item in result:
            self.assertTrue(ENEMY_REQUIRED_KEYS.issubset(item.keys()),
                          f"Missing keys: {ENEMY_REQUIRED_KEYS - item.keys()}")

    def test_key_parity_with_neo4j(self) -> None:
        """SQL and Neo4j return the same key set (except source value)."""
        db = MagicMock()
        db.fetch_all.return_value = [
            {"enemy_id": 500, "engagements": 8},
        ]
        client = MagicMock()
        client.query.return_value = [
            {"enemy_id": 500, "engagements": 8},
        ]

        sql_result = _query_enemies_sql(db, 100)
        neo4j_result = _query_enemies_neo4j(client, 100)

        self.assertEqual(set(sql_result[0].keys()), set(neo4j_result[0].keys()),
                        "SQL and Neo4j enemy results must have identical key sets")


class TestTrendComputation(unittest.TestCase):
    def test_rising_trend(self) -> None:
        db = MagicMock()
        db.fetch_one.return_value = {
            "killmails_7d": 50, "killmails_8_30d": 15, "killmails_31_90d": 30,
            "isk_destroyed_7d": 1e10, "isk_destroyed_8_30d": 5e9, "isk_destroyed_31_90d": 1e10,
        }

        result = _compute_trend(db, 100)

        self.assertEqual(result["activity_trend"], "rising")
        self.assertEqual(result["killmails_7d"], 50)
        self.assertTrue(TREND_REQUIRED_KEYS.issubset(result.keys()))

    def test_declining_trend(self) -> None:
        db = MagicMock()
        db.fetch_one.return_value = {
            "killmails_7d": 0, "killmails_8_30d": 60, "killmails_31_90d": 100,
            "isk_destroyed_7d": 0, "isk_destroyed_8_30d": 5e10, "isk_destroyed_31_90d": 1e11,
        }

        result = _compute_trend(db, 200)

        self.assertEqual(result["activity_trend"], "declining")

    def test_stable_trend(self) -> None:
        db = MagicMock()
        db.fetch_one.return_value = {
            "killmails_7d": 10, "killmails_8_30d": 30, "killmails_31_90d": 80,
            "isk_destroyed_7d": 1e9, "isk_destroyed_8_30d": 3e9, "isk_destroyed_31_90d": 8e9,
        }

        result = _compute_trend(db, 300)

        self.assertEqual(result["activity_trend"], "stable")

    def test_trend_values_are_valid(self) -> None:
        """Activity trend must be one of the documented values."""
        for data, expected in [
            ({"killmails_7d": 50, "killmails_8_30d": 15, "killmails_31_90d": 30,
              "isk_destroyed_7d": 1e10, "isk_destroyed_8_30d": 5e9, "isk_destroyed_31_90d": 1e10}, "rising"),
            ({"killmails_7d": 0, "killmails_8_30d": 60, "killmails_31_90d": 100,
              "isk_destroyed_7d": 0, "isk_destroyed_8_30d": 5e10, "isk_destroyed_31_90d": 1e11}, "declining"),
            ({"killmails_7d": 10, "killmails_8_30d": 30, "killmails_31_90d": 80,
              "isk_destroyed_7d": 1e9, "isk_destroyed_8_30d": 3e9, "isk_destroyed_31_90d": 8e9}, "stable"),
        ]:
            db = MagicMock()
            db.fetch_one.return_value = data
            result = _compute_trend(db, 100)
            self.assertIn(result["activity_trend"], {"rising", "declining", "stable"},
                         f"Invalid trend value: {result['activity_trend']}")


class TestPostureValues(unittest.TestCase):
    """Verify that all posture values produced by _load_behavior_metrics are
    within the documented set and will have color mappings in the PHP consumer."""

    def test_all_postures_are_documented(self) -> None:
        """Every possible posture output must be in POSTURE_VALUES."""
        test_cases = [
            # aggressive: high kills/week + large gang size
            ([{"total_kills": 1000, "active_pilots": 50, "earliest_kill": "2026-01-01", "latest_kill": "2026-04-01"},
              {"avg_gang_size": 15.0, "solo_kills": 10, "total_counted": 1000},
              {"total_losses": 200}], "aggressive"),
            # opportunistic: moderate kills/week + small gang size
            ([{"total_kills": 200, "active_pilots": 15, "earliest_kill": "2026-01-01", "latest_kill": "2026-04-01"},
              {"avg_gang_size": 5.0, "solo_kills": 30, "total_counted": 200},
              {"total_losses": 100}], "opportunistic"),
            # infrequent: very few kills
            ([{"total_kills": 5, "active_pilots": 3, "earliest_kill": "2026-03-01", "latest_kill": "2026-03-15"},
              {"avg_gang_size": 3.0, "solo_kills": 2, "total_counted": 5},
              {"total_losses": 2}], "infrequent"),
            # balanced: moderate kills/week + large gang size
            ([{"total_kills": 50, "active_pilots": 20, "earliest_kill": "2026-01-01", "latest_kill": "2026-04-01"},
              {"avg_gang_size": 12.0, "solo_kills": 5, "total_counted": 50},
              {"total_losses": 30}], "balanced"),
        ]
        for side_effects, expected in test_cases:
            db = MagicMock()
            db.fetch_one.side_effect = side_effects
            result = _load_behavior_metrics(db, 100)
            self.assertEqual(result["posture"], expected)
            self.assertIn(result["posture"], POSTURE_VALUES,
                         f"Posture '{result['posture']}' not in documented set {POSTURE_VALUES}")


if __name__ == "__main__":
    unittest.main()
