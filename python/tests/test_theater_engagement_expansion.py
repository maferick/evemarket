"""Tests for theater engagement expansion and sub-threshold battle absorption.

Validates the core principle: bad source coverage cannot be repaired by good
formulas.  The event set must be complete before calculating ISK.
"""

from __future__ import annotations

import sys
import types
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

# ── Bootstrap: stub out heavy dependencies so we can import theater_analysis
# and theater_clustering without a full DB stack.
_PYTHON_DIR = str(Path(__file__).resolve().parents[1])
if _PYTHON_DIR not in sys.path:
    sys.path.insert(0, _PYTHON_DIR)

# Stub the heavy DB module before importing the jobs package
_stub_db = types.ModuleType("orchestrator.db")
_stub_db.SupplyCoreDb = MagicMock  # type: ignore[attr-defined]
sys.modules.setdefault("orchestrator.db", _stub_db)

# Stub the __init__ that imports everything — replace with an empty module
_stub_jobs_init = types.ModuleType("orchestrator.jobs")
_stub_jobs_init.__path__ = [str(Path(_PYTHON_DIR) / "orchestrator" / "jobs")]  # type: ignore[attr-defined]
sys.modules["orchestrator.jobs"] = _stub_jobs_init

# Now import the specific modules we need
from orchestrator.jobs.theater_analysis import (  # noqa: E402
    _compute_alliance_summary,
    _compute_timeline,
    _load_expanded_killmails,
    ENGAGEMENT_EXPANSION_MARGIN_SECONDS,
)
from orchestrator.jobs.theater_clustering import (  # noqa: E402
    _absorb_sub_threshold_battles,
    _cluster_battles,
    MIN_CLUSTERING_SEED_PARTICIPANTS,
    THEATER_TIME_WINDOW_SECONDS,
)


def _km(
    killmail_id: int,
    system_id: int,
    killmail_time: str,
    battle_id: str,
    victim_character_id: int = 0,
    victim_corporation_id: int = 0,
    victim_alliance_id: int = 0,
    victim_ship_type_id: int = 0,
    total_value: float = 0.0,
    attacker_character_id: int = 0,
    attacker_corporation_id: int = 0,
    attacker_alliance_id: int = 0,
    attacker_ship_type_id: int = 0,
    attacker_damage_done: float = 0.0,
    attacker_final_blow: int = 0,
    zkb_npc: int = 0,
) -> dict[str, Any]:
    """Build a killmail row matching the DB schema."""
    return {
        "killmail_id": killmail_id,
        "sequence_id": killmail_id,
        "system_id": system_id,
        "killmail_time": killmail_time,
        "battle_id": battle_id,
        "victim_character_id": victim_character_id,
        "victim_corporation_id": victim_corporation_id,
        "victim_alliance_id": victim_alliance_id,
        "victim_ship_type_id": victim_ship_type_id,
        "total_value": total_value,
        "attacker_character_id": attacker_character_id,
        "attacker_corporation_id": attacker_corporation_id,
        "attacker_alliance_id": attacker_alliance_id,
        "attacker_ship_type_id": attacker_ship_type_id,
        "attacker_damage_done": attacker_damage_done,
        "attacker_final_blow": attacker_final_blow,
        "zkb_npc": zkb_npc,
    }


def _battle(
    battle_id: str,
    system_id: int,
    started_at: str,
    ended_at: str,
    participant_count: int,
    constellation_id: int = 1,
    region_id: int = 1,
    system_name: str = "Test",
    security: float = 0.0,
) -> dict[str, Any]:
    return {
        "battle_id": battle_id,
        "system_id": system_id,
        "started_at": started_at,
        "ended_at": ended_at,
        "duration_seconds": 900,
        "participant_count": participant_count,
        "battle_size_class": "small",
        "constellation_id": constellation_id,
        "region_id": region_id,
        "system_name": system_name,
        "security": security,
    }


class TestCleanTwoSideFight(unittest.TestCase):
    """Case 1: Clean two-side fight. Totals must match exactly."""

    def test_two_side_isk_totals(self) -> None:
        kms = [
            _km(1, 30000001, "2026-04-01 10:05:00", "b1",
                victim_character_id=10, victim_alliance_id=100, total_value=500.0,
                attacker_character_id=20, attacker_alliance_id=200, attacker_final_blow=1,
                attacker_damage_done=100),
            _km(2, 30000001, "2026-04-01 10:06:00", "b1",
                victim_character_id=20, victim_alliance_id=200, total_value=800.0,
                attacker_character_id=10, attacker_alliance_id=100, attacker_final_blow=1,
                attacker_damage_done=100),
        ]

        bp_rows = [
            {"character_id": 10, "side_key": "a:100", "alliance_id": 100, "corporation_id": 0},
            {"character_id": 20, "side_key": "a:200", "alliance_id": 200, "corporation_id": 0},
        ]
        side_configuration = {
            "friendly_alliance_ids": {100},
            "friendly_corporation_ids": set(),
            "opponent_alliance_ids": {200},
            "opponent_corporation_ids": set(),
        }
        char_sides = {10: "friendly", 20: "opponent"}

        summary = _compute_alliance_summary(kms, bp_rows, side_configuration, char_sides)

        friendly = [s for s in summary if s["alliance_id"] == 100][0]
        opponent = [s for s in summary if s["alliance_id"] == 200][0]

        # ISK totals must reconcile
        self.assertEqual(friendly["total_isk_lost"], 500.0)
        self.assertEqual(friendly["total_isk_killed"], 800.0)
        self.assertEqual(opponent["total_isk_lost"], 800.0)
        self.assertEqual(opponent["total_isk_killed"], 500.0)

        total_destroyed = friendly["total_isk_lost"] + opponent["total_isk_lost"]
        self.assertEqual(total_destroyed, 1300.0)


class TestThirdPartyFinalBlow(unittest.TestCase):
    """Case 2: Third-party final blow. Killmail must still be in event set
    and losses must count correctly."""

    def test_third_party_final_blow_still_counts(self) -> None:
        kms = [
            # Enemy victim, third-party gets final blow
            _km(1, 30000001, "2026-04-01 10:05:00", "b1",
                victim_character_id=20, victim_alliance_id=200, total_value=1000.0,
                attacker_character_id=30, attacker_alliance_id=300, attacker_final_blow=1,
                attacker_damage_done=50),
            # Same killmail, friendly attacker (not final blow, but did 95% damage)
            _km(1, 30000001, "2026-04-01 10:05:00", "b1",
                victim_character_id=20, victim_alliance_id=200, total_value=1000.0,
                attacker_character_id=10, attacker_alliance_id=100, attacker_final_blow=0,
                attacker_damage_done=950),
        ]

        bp_rows = [
            {"character_id": 10, "side_key": "a:100", "alliance_id": 100, "corporation_id": 0},
            {"character_id": 20, "side_key": "a:200", "alliance_id": 200, "corporation_id": 0},
            {"character_id": 30, "side_key": "a:300", "alliance_id": 300, "corporation_id": 0},
        ]
        side_configuration = {
            "friendly_alliance_ids": {100},
            "friendly_corporation_ids": set(),
            "opponent_alliance_ids": {200},
            "opponent_corporation_ids": set(),
        }
        char_sides = {10: "friendly", 20: "opponent", 30: "third_party"}

        summary = _compute_alliance_summary(kms, bp_rows, side_configuration, char_sides)

        opponent = [s for s in summary if s["alliance_id"] == 200][0]
        friendly = [s for s in summary if s["alliance_id"] == 100][0]

        # Enemy STILL lost 1000 ISK regardless of who got final blow
        self.assertEqual(opponent["total_isk_lost"], 1000.0)
        # Friendly gets ISK killed credit (they participated)
        self.assertEqual(friendly["total_isk_killed"], 1000.0)


class TestThirdPartyVictimInSameEngagement(unittest.TestCase):
    """Case 3: Third-party victim in same fight. Must increase total_isk_destroyed
    but not distort side efficiency."""

    def test_third_party_loss_in_timeline(self) -> None:
        char_sides = {10: "friendly", 20: "opponent", 30: "third_party"}
        start = datetime(2026, 4, 1, 10, 0, 0, tzinfo=UTC)
        end = datetime(2026, 4, 1, 10, 15, 0, tzinfo=UTC)

        kms = [
            _km(1, 30000001, "2026-04-01 10:05:00", "b1",
                victim_character_id=20, victim_alliance_id=200, total_value=500.0,
                attacker_character_id=10, attacker_alliance_id=100, attacker_final_blow=1),
            _km(2, 30000001, "2026-04-01 10:06:00", "b1",
                victim_character_id=30, victim_alliance_id=300, total_value=100.0,
                attacker_character_id=10, attacker_alliance_id=100, attacker_final_blow=1),
        ]

        timeline = _compute_timeline(kms, char_sides, start, end)
        total_isk = sum(b["isk_destroyed"] for b in timeline)

        # Total ISK includes both (opponent victim + third party victim)
        self.assertEqual(total_isk, 600.0)

        # Side A ISK = opponent losses only (500), not third-party losses
        side_a_isk = sum(b["side_a_isk"] for b in timeline)
        self.assertEqual(side_a_isk, 500.0)

        side_b_isk = sum(b["side_b_isk"] for b in timeline)
        self.assertEqual(side_b_isk, 0.0)

        # Third party ISK = total - side_a - side_b = 100
        self.assertEqual(total_isk - side_a_isk - side_b_isk, 100.0)


class TestSubThresholdBattleClustering(unittest.TestCase):
    """Case 4: Same engagement, indirect overlap. A battle below the threshold
    must still end up in the same theater via clustering."""

    def test_cluster_merges_same_constellation_battles(self) -> None:
        battles = [
            _battle("b1", 30000001, "2026-04-01 10:00:00", "2026-04-01 10:14:59",
                    participant_count=15, constellation_id=100),
            _battle("b2", 30000002, "2026-04-01 10:10:00", "2026-04-01 10:24:59",
                    participant_count=12, constellation_id=100),
        ]
        participants = {
            "b1": {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
            "b2": {5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
        }

        groups = _cluster_battles(battles, participants)

        self.assertEqual(len(groups), 1)
        self.assertEqual(len(list(groups.values())[0]), 2)


class TestMultiWaveEngagement(unittest.TestCase):
    """Case 6: Multi-wave engagement within theater time window."""

    def test_multi_wave_merged_into_single_theater(self) -> None:
        battles = [
            _battle("b1", 30000001, "2026-04-01 10:00:00", "2026-04-01 10:14:59",
                    participant_count=25, constellation_id=100),
            _battle("b2", 30000001, "2026-04-01 10:20:00", "2026-04-01 10:34:59",
                    participant_count=30, constellation_id=100),
            _battle("b3", 30000001, "2026-04-01 10:40:00", "2026-04-01 10:54:59",
                    participant_count=20, constellation_id=100),
        ]
        participants = {
            "b1": set(range(1, 26)),
            "b2": set(range(10, 41)),
            "b3": set(range(20, 41)),
        }

        groups = _cluster_battles(battles, participants)

        self.assertEqual(len(groups), 1)
        self.assertEqual(len(list(groups.values())[0]), 3)


class TestExpansionDiagnostics(unittest.TestCase):
    """Verify expansion diagnostics report correct counts on empty inputs."""

    def test_expansion_returns_diagnostics_on_empty(self) -> None:
        db_mock = MagicMock()
        db_mock.fetch_all.return_value = []

        rows, diag = _load_expanded_killmails(db_mock, [], [], "", "")

        self.assertEqual(rows, [])
        self.assertEqual(diag["base_killmail_ids"], 0)
        self.assertEqual(diag["expanded_killmail_ids"], 0)
        self.assertEqual(diag["expansion_new_killmail_ids"], 0)


class TestSubThresholdAbsorptionEmpty(unittest.TestCase):
    """Verify sub-threshold absorption handles empty/trivial cases."""

    def test_empty_groups_produces_no_absorptions(self) -> None:
        db_mock = MagicMock()
        db_mock.fetch_all.return_value = []

        diagnostics = _absorb_sub_threshold_battles(
            db_mock, {}, {}, "2026-04-01 10:00:00",
        )

        self.assertEqual(diagnostics, {})


if __name__ == "__main__":
    unittest.main()
