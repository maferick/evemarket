"""Tests for per-character battle stats ledger and group rollup.

Validates the canonical model: build per-character stats from killmails first,
then roll up into groups.  All invariants must hold.
"""

from __future__ import annotations

import sys
import types
import unittest
from collections import defaultdict
from pathlib import Path
from typing import Any

# ── Bootstrap: stub heavy dependencies ─────────────────────────────────────
_PYTHON_DIR = str(Path(__file__).resolve().parents[1])
if _PYTHON_DIR not in sys.path:
    sys.path.insert(0, _PYTHON_DIR)

from unittest.mock import MagicMock

_stub_db = types.ModuleType("orchestrator.db")
_stub_db.SupplyCoreDb = MagicMock  # type: ignore[attr-defined]
sys.modules.setdefault("orchestrator.db", _stub_db)

_stub_jobs_init = types.ModuleType("orchestrator.jobs")
_stub_jobs_init.__path__ = [str(Path(_PYTHON_DIR) / "orchestrator" / "jobs")]  # type: ignore[attr-defined]
sys.modules["orchestrator.jobs"] = _stub_jobs_init

from orchestrator.jobs.theater_analysis import (  # noqa: E402
    _build_character_ledger,
    _compute_alliance_summary,
    _empty_ledger_entry,
)


# ── Helpers ────────────────────────────────────────────────────────────────

def _km(
    killmail_id: int,
    victim_character_id: int = 0,
    victim_alliance_id: int = 0,
    victim_corporation_id: int = 0,
    total_value: float = 0.0,
    attacker_character_id: int = 0,
    attacker_alliance_id: int = 0,
    attacker_corporation_id: int = 0,
    attacker_damage_done: float = 0.0,
    attacker_final_blow: int = 0,
) -> dict[str, Any]:
    """Build a killmail row matching the LEFT JOIN schema."""
    return {
        "killmail_id": killmail_id,
        "sequence_id": killmail_id,
        "system_id": 30000001,
        "killmail_time": "2026-04-01 10:05:00",
        "battle_id": "b1",
        "victim_character_id": victim_character_id,
        "victim_corporation_id": victim_corporation_id,
        "victim_alliance_id": victim_alliance_id,
        "victim_ship_type_id": 0,
        "total_value": total_value,
        "attacker_character_id": attacker_character_id,
        "attacker_corporation_id": attacker_corporation_id,
        "attacker_alliance_id": attacker_alliance_id,
        "attacker_ship_type_id": 0,
        "attacker_damage_done": attacker_damage_done,
        "attacker_final_blow": attacker_final_blow,
        "zkb_npc": 0,
    }


# ── Test: Single attacker killmail ─────────────────────────────────────────

class TestSingleAttackerKillmail(unittest.TestCase):
    """One killmail, one victim, one attacker with final blow."""

    def test_single_attacker(self) -> None:
        kms = [
            _km(1, victim_character_id=10, victim_alliance_id=100,
                total_value=100.0,
                attacker_character_id=20, attacker_alliance_id=200,
                attacker_final_blow=1, attacker_damage_done=500),
        ]

        ledger = _build_character_ledger(kms)

        # Victim
        self.assertEqual(ledger[10]["losses"], 1)
        self.assertEqual(ledger[10]["isk_lost"], 100.0)
        self.assertEqual(ledger[10]["final_kills"], 0)
        self.assertEqual(ledger[10]["isk_killed"], 0.0)
        self.assertEqual(ledger[10]["contributed_kills"], 0)

        # Attacker
        self.assertEqual(ledger[20]["final_kills"], 1)
        self.assertEqual(ledger[20]["contributed_kills"], 1)
        self.assertEqual(ledger[20]["isk_killed"], 100.0)
        self.assertEqual(ledger[20]["losses"], 0)
        self.assertEqual(ledger[20]["isk_lost"], 0.0)
        self.assertEqual(ledger[20]["damage_done"], 500.0)


# ── Test: Multi-attacker killmail ──────────────────────────────────────────

class TestMultiAttackerKillmail(unittest.TestCase):
    """One killmail, one victim, two attackers — only final blow gets ISK."""

    def test_multi_attacker(self) -> None:
        kms = [
            # Attacker B gets final blow
            _km(1, victim_character_id=10, victim_alliance_id=100,
                total_value=500.0,
                attacker_character_id=20, attacker_alliance_id=200,
                attacker_final_blow=1, attacker_damage_done=900),
            # Attacker C assisted (same killmail, NOT final blow)
            _km(1, victim_character_id=10, victim_alliance_id=100,
                total_value=500.0,
                attacker_character_id=30, attacker_alliance_id=200,
                attacker_final_blow=0, attacker_damage_done=100),
        ]

        ledger = _build_character_ledger(kms)

        # Victim: 1 loss, full ISK
        self.assertEqual(ledger[10]["losses"], 1)
        self.assertEqual(ledger[10]["isk_lost"], 500.0)

        # Final blow attacker: 1 final kill, 1 contributed kill, full ISK
        self.assertEqual(ledger[20]["final_kills"], 1)
        self.assertEqual(ledger[20]["contributed_kills"], 1)
        self.assertEqual(ledger[20]["isk_killed"], 500.0)

        # Assisting attacker: 0 final kills, 1 contributed kill, 0 ISK
        self.assertEqual(ledger[30]["final_kills"], 0)
        self.assertEqual(ledger[30]["contributed_kills"], 1)
        self.assertEqual(ledger[30]["isk_killed"], 0.0)


# ── Test: Zero-damage attacker still gets contributed_kills ────────────────

class TestZeroDamageAttacker(unittest.TestCase):
    """An attacker listed on the killmail with damage_done=0 (e.g. ewar)
    still gets contributed_kills += 1."""

    def test_zero_damage_attacker(self) -> None:
        kms = [
            _km(1, victim_character_id=10, victim_alliance_id=100,
                total_value=500.0,
                attacker_character_id=20, attacker_alliance_id=200,
                attacker_final_blow=1, attacker_damage_done=1000),
            # Zero-damage attacker (ewar/booster)
            _km(1, victim_character_id=10, victim_alliance_id=100,
                total_value=500.0,
                attacker_character_id=30, attacker_alliance_id=200,
                attacker_final_blow=0, attacker_damage_done=0),
        ]

        ledger = _build_character_ledger(kms)

        # Zero-damage attacker still counted
        self.assertEqual(ledger[30]["contributed_kills"], 1)
        self.assertEqual(ledger[30]["final_kills"], 0)
        self.assertEqual(ledger[30]["isk_killed"], 0.0)
        self.assertEqual(ledger[30]["damage_done"], 0.0)


# ── Test: Multi-killmail aggregation ───────────────────────────────────────

class TestMultiKillmailAggregation(unittest.TestCase):
    """Multiple killmails — character stats accumulate correctly."""

    def test_multi_killmail_character_accumulation(self) -> None:
        kms = [
            # KM 1: A kills B
            _km(1, victim_character_id=20, victim_alliance_id=200,
                total_value=300.0,
                attacker_character_id=10, attacker_alliance_id=100,
                attacker_final_blow=1, attacker_damage_done=100),
            # KM 2: B kills A
            _km(2, victim_character_id=10, victim_alliance_id=100,
                total_value=700.0,
                attacker_character_id=20, attacker_alliance_id=200,
                attacker_final_blow=1, attacker_damage_done=200),
        ]

        ledger = _build_character_ledger(kms)

        # Character 10: 1 final kill (KM1) + 1 loss (KM2)
        self.assertEqual(ledger[10]["final_kills"], 1)
        self.assertEqual(ledger[10]["isk_killed"], 300.0)
        self.assertEqual(ledger[10]["losses"], 1)
        self.assertEqual(ledger[10]["isk_lost"], 700.0)

        # Character 20: 1 final kill (KM2) + 1 loss (KM1)
        self.assertEqual(ledger[20]["final_kills"], 1)
        self.assertEqual(ledger[20]["isk_killed"], 700.0)
        self.assertEqual(ledger[20]["losses"], 1)
        self.assertEqual(ledger[20]["isk_lost"], 300.0)


# ── Test: Group rollup from ledger ─────────────────────────────────────────

class TestGroupRollup(unittest.TestCase):
    """Alliance summary must be exact rollup of character ledger."""

    def test_group_totals_match_character_sums(self) -> None:
        kms = [
            # KM 1: Alliance 200 victim, Alliance 100 attacker (final blow)
            _km(1, victim_character_id=20, victim_alliance_id=200,
                total_value=500.0,
                attacker_character_id=10, attacker_alliance_id=100,
                attacker_final_blow=1, attacker_damage_done=100),
            # KM 1: Alliance 100 also has a second attacker
            _km(1, victim_character_id=20, victim_alliance_id=200,
                total_value=500.0,
                attacker_character_id=11, attacker_alliance_id=100,
                attacker_final_blow=0, attacker_damage_done=50),
            # KM 2: Alliance 100 victim, Alliance 200 attacker
            _km(2, victim_character_id=10, victim_alliance_id=100,
                total_value=800.0,
                attacker_character_id=20, attacker_alliance_id=200,
                attacker_final_blow=1, attacker_damage_done=200),
        ]

        ledger = _build_character_ledger(kms)

        bp_rows = [
            {"character_id": 10, "alliance_id": 100, "corporation_id": 0},
            {"character_id": 11, "alliance_id": 100, "corporation_id": 0},
            {"character_id": 20, "alliance_id": 200, "corporation_id": 0},
        ]
        side_configuration = {
            "friendly_alliance_ids": {100},
            "friendly_corporation_ids": set(),
            "opponent_alliance_ids": {200},
            "opponent_corporation_ids": set(),
        }
        char_sides = {10: "friendly", 11: "friendly", 20: "opponent"}

        summary = _compute_alliance_summary(ledger, bp_rows, side_configuration, char_sides)

        friendly = [s for s in summary if s["alliance_id"] == 100][0]
        opponent = [s for s in summary if s["alliance_id"] == 200][0]

        # Friendly group: char 10 (1 final kill, 1 loss) + char 11 (0 final, 0 loss)
        self.assertEqual(friendly["total_kills"], 1)         # final kills
        self.assertEqual(friendly["total_contributed_kills"], 2)  # both chars on KM1
        self.assertEqual(friendly["total_losses"], 1)
        self.assertEqual(friendly["total_isk_killed"], 500.0)
        self.assertEqual(friendly["total_isk_lost"], 800.0)

        # Opponent group: char 20 (1 final kill, 1 loss)
        self.assertEqual(opponent["total_kills"], 1)
        self.assertEqual(opponent["total_contributed_kills"], 1)
        self.assertEqual(opponent["total_losses"], 1)
        self.assertEqual(opponent["total_isk_killed"], 800.0)
        self.assertEqual(opponent["total_isk_lost"], 500.0)


# ── Test: Invariants ───────────────────────────────────────────────────────

class TestInvariants(unittest.TestCase):
    """Verify invariants hold across the full battle."""

    def test_isk_and_count_invariants(self) -> None:
        """For N killmails:
        sum(isk_lost)   == total_destroyed
        sum(isk_killed) == total_destroyed
        sum(losses)     == N
        sum(final_kills) == N
        sum(contributed_kills) >= N
        """
        kms = [
            # KM 1: 3 attackers
            _km(1, victim_character_id=10, victim_alliance_id=100,
                total_value=100.0,
                attacker_character_id=20, attacker_alliance_id=200,
                attacker_final_blow=1, attacker_damage_done=80),
            _km(1, victim_character_id=10, victim_alliance_id=100,
                total_value=100.0,
                attacker_character_id=30, attacker_alliance_id=200,
                attacker_final_blow=0, attacker_damage_done=15),
            _km(1, victim_character_id=10, victim_alliance_id=100,
                total_value=100.0,
                attacker_character_id=40, attacker_alliance_id=300,
                attacker_final_blow=0, attacker_damage_done=5),
            # KM 2: 1 attacker
            _km(2, victim_character_id=20, victim_alliance_id=200,
                total_value=250.0,
                attacker_character_id=10, attacker_alliance_id=100,
                attacker_final_blow=1, attacker_damage_done=300),
            # KM 3: 2 attackers
            _km(3, victim_character_id=30, victim_alliance_id=200,
                total_value=50.0,
                attacker_character_id=10, attacker_alliance_id=100,
                attacker_final_blow=1, attacker_damage_done=40),
            _km(3, victim_character_id=30, victim_alliance_id=200,
                total_value=50.0,
                attacker_character_id=40, attacker_alliance_id=300,
                attacker_final_blow=0, attacker_damage_done=10),
        ]

        ledger = _build_character_ledger(kms)

        total_destroyed = 100.0 + 250.0 + 50.0  # 400.0
        num_killmails = 3

        sum_isk_lost = sum(e["isk_lost"] for e in ledger.values())
        sum_isk_killed = sum(e["isk_killed"] for e in ledger.values())
        sum_losses = sum(e["losses"] for e in ledger.values())
        sum_final_kills = sum(e["final_kills"] for e in ledger.values())
        sum_contributed = sum(e["contributed_kills"] for e in ledger.values())

        self.assertEqual(sum_isk_lost, total_destroyed,
                         "sum(isk_lost) must equal total destroyed ISK")
        self.assertEqual(sum_isk_killed, total_destroyed,
                         "sum(isk_killed) must equal total destroyed ISK")
        self.assertEqual(sum_losses, num_killmails,
                         "sum(losses) must equal number of killmails")
        self.assertEqual(sum_final_kills, num_killmails,
                         "sum(final_kills) must equal number of killmails")
        self.assertGreaterEqual(sum_contributed, num_killmails,
                                "sum(contributed_kills) must be >= number of killmails")

    def test_group_rollup_preserves_invariants(self) -> None:
        """Group rollup must not break ISK invariants."""
        kms = [
            _km(1, victim_character_id=10, victim_alliance_id=100,
                total_value=1000.0,
                attacker_character_id=20, attacker_alliance_id=200,
                attacker_final_blow=1, attacker_damage_done=500),
            _km(1, victim_character_id=10, victim_alliance_id=100,
                total_value=1000.0,
                attacker_character_id=30, attacker_alliance_id=300,
                attacker_final_blow=0, attacker_damage_done=500),
            _km(2, victim_character_id=20, victim_alliance_id=200,
                total_value=600.0,
                attacker_character_id=10, attacker_alliance_id=100,
                attacker_final_blow=1, attacker_damage_done=600),
        ]

        ledger = _build_character_ledger(kms)

        bp_rows = [
            {"character_id": 10, "alliance_id": 100, "corporation_id": 0},
            {"character_id": 20, "alliance_id": 200, "corporation_id": 0},
            {"character_id": 30, "alliance_id": 300, "corporation_id": 0},
        ]
        side_configuration = {
            "friendly_alliance_ids": {100},
            "friendly_corporation_ids": set(),
            "opponent_alliance_ids": {200},
            "opponent_corporation_ids": set(),
        }
        char_sides = {10: "friendly", 20: "opponent", 30: "third_party"}

        summary = _compute_alliance_summary(ledger, bp_rows, side_configuration, char_sides)

        total_destroyed = 1000.0 + 600.0
        group_isk_killed = sum(s["total_isk_killed"] for s in summary)
        group_isk_lost = sum(s["total_isk_lost"] for s in summary)
        group_kills = sum(s["total_kills"] for s in summary)
        group_losses = sum(s["total_losses"] for s in summary)

        self.assertEqual(group_isk_killed, total_destroyed)
        self.assertEqual(group_isk_lost, total_destroyed)
        self.assertEqual(group_kills, 2)   # 2 killmails
        self.assertEqual(group_losses, 2)  # 2 killmails


# ── Test: Third-party final blow ───────────────────────────────────────────

class TestThirdPartyFinalBlow(unittest.TestCase):
    """Third-party gets the final blow — ISK killed goes to them, not
    to the group that dealt the most damage."""

    def test_third_party_gets_isk_killed(self) -> None:
        kms = [
            # Third party gets final blow
            _km(1, victim_character_id=20, victim_alliance_id=200,
                total_value=1000.0,
                attacker_character_id=30, attacker_alliance_id=300,
                attacker_final_blow=1, attacker_damage_done=50),
            # Friendly did 95% damage but no final blow
            _km(1, victim_character_id=20, victim_alliance_id=200,
                total_value=1000.0,
                attacker_character_id=10, attacker_alliance_id=100,
                attacker_final_blow=0, attacker_damage_done=950),
        ]

        ledger = _build_character_ledger(kms)

        # Third party char gets the ISK killed
        self.assertEqual(ledger[30]["final_kills"], 1)
        self.assertEqual(ledger[30]["isk_killed"], 1000.0)

        # Friendly char gets contributed kill but no ISK killed
        self.assertEqual(ledger[10]["contributed_kills"], 1)
        self.assertEqual(ledger[10]["final_kills"], 0)
        self.assertEqual(ledger[10]["isk_killed"], 0.0)


if __name__ == "__main__":
    unittest.main()
