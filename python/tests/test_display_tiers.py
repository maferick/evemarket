"""Tests for orchestrator.display_tiers (tier slot reservation helpers)."""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

# Bootstrap: ensure orchestrator package is importable without pulling
# in its heavy runtime dependencies.
_PYTHON_DIR = str(Path(__file__).resolve().parents[1])
if _PYTHON_DIR not in sys.path:
    sys.path.insert(0, _PYTHON_DIR)

from orchestrator.display_tiers import (  # noqa: E402
    DEFAULT_DISPLAY_TIER,
    DISPLAY_TIERS,
    VALID_DISPLAY_TIERS,
    describe_tier_slots,
    get_display_tier,
    parse_tier_slots,
    tier_capacity_allows,
    validate_display_tier_parity,
)


class GetDisplayTierTests(unittest.TestCase):
    def test_known_jobs_return_declared_tier(self) -> None:
        self.assertEqual(get_display_tier("market_hub_current_sync"), 1)
        self.assertEqual(get_display_tier("entity_metadata_resolve_sync"), 2)
        self.assertEqual(get_display_tier("compute_graph_sync"), 3)
        self.assertEqual(get_display_tier("compute_battle_rollups"), 4)
        self.assertEqual(get_display_tier("dashboard_summary_sync"), 5)
        self.assertEqual(get_display_tier("cache_expiry_cleanup_sync"), 6)

    def test_unknown_job_defaults_to_analytics(self) -> None:
        self.assertEqual(get_display_tier("nonexistent_job"), DEFAULT_DISPLAY_TIER)
        self.assertEqual(DEFAULT_DISPLAY_TIER, 5)

    def test_all_declared_tiers_are_in_valid_range(self) -> None:
        for job, tier in DISPLAY_TIERS.items():
            with self.subTest(job=job):
                self.assertIn(tier, VALID_DISPLAY_TIERS)


class ParseTierSlotsTests(unittest.TestCase):
    def test_empty_spec_returns_empty_dict(self) -> None:
        self.assertEqual(parse_tier_slots("", max_parallel=6), {})
        self.assertEqual(parse_tier_slots("   ", max_parallel=6), {})

    def test_single_pair(self) -> None:
        self.assertEqual(parse_tier_slots("1:2", max_parallel=6), {1: 2})

    def test_multiple_pairs(self) -> None:
        self.assertEqual(
            parse_tier_slots("1:2,3:2", max_parallel=6),
            {1: 2, 3: 2},
        )

    def test_whitespace_tolerated(self) -> None:
        self.assertEqual(
            parse_tier_slots(" 1 : 2 ,  3 : 1 ", max_parallel=6),
            {1: 2, 3: 1},
        )

    def test_trailing_comma_tolerated(self) -> None:
        self.assertEqual(parse_tier_slots("1:2,", max_parallel=6), {1: 2})

    def test_rejects_missing_colon(self) -> None:
        with self.assertRaisesRegex(ValueError, "expected 'tier:slots'"):
            parse_tier_slots("1=2", max_parallel=6)

    def test_rejects_non_integer(self) -> None:
        with self.assertRaisesRegex(ValueError, "integer"):
            parse_tier_slots("1:foo", max_parallel=6)

    def test_rejects_unknown_tier(self) -> None:
        with self.assertRaisesRegex(ValueError, "unknown tier 7"):
            parse_tier_slots("7:1", max_parallel=6)
        with self.assertRaisesRegex(ValueError, "unknown tier 0"):
            parse_tier_slots("0:1", max_parallel=6)

    def test_rejects_negative_slots(self) -> None:
        with self.assertRaisesRegex(ValueError, "negative slot count"):
            parse_tier_slots("1:-1", max_parallel=6)

    def test_rejects_duplicate_tier(self) -> None:
        with self.assertRaisesRegex(ValueError, "specified more than once"):
            parse_tier_slots("1:1,1:2", max_parallel=6)

    def test_rejects_over_subscription(self) -> None:
        # Sum of reservations strictly greater than max_parallel is invalid.
        with self.assertRaisesRegex(ValueError, "exceeds"):
            parse_tier_slots("1:3,3:3,5:1", max_parallel=6)

    def test_exactly_max_parallel_is_allowed(self) -> None:
        # Sum == max_parallel leaves a zero shared pool, which is
        # operationally harsh (non-reserved tiers never dispatch) but
        # legitimate for strict per-tier isolation.  Accept it.
        self.assertEqual(
            parse_tier_slots("1:3,3:3", max_parallel=6),
            {1: 3, 3: 3},
        )


class TierCapacityAllowsTests(unittest.TestCase):
    """Dead-simple reserved-pool model:

    * Reserved tiers get their first ``reserved[T]`` slots for free.
    * Everything else uses a shared pool of ``max_parallel - sum(reserved)``.
    """

    def test_disabled_reservations_always_allow_under_cap(self) -> None:
        self.assertTrue(
            tier_capacity_allows(
                job_key="compute_graph_sync",
                in_flight_by_tier={3: 5},
                in_flight_total=5,
                max_parallel=6,
                reserved_slots={},
            )
        )

    def test_disabled_reservations_respect_max_parallel(self) -> None:
        self.assertFalse(
            tier_capacity_allows(
                job_key="compute_graph_sync",
                in_flight_by_tier={3: 6},
                in_flight_total=6,
                max_parallel=6,
                reserved_slots={},
            )
        )

    def test_reserved_tier_gets_its_reservation_even_when_pool_full(self) -> None:
        # Shared pool (size 2) is fully occupied by tier 5 jobs, and tier 1
        # hasn't used any of its reserved slots yet.  A tier 1 dispatch
        # must still be allowed because it comes out of its own reservation.
        self.assertTrue(
            tier_capacity_allows(
                job_key="market_hub_current_sync",  # tier 1
                in_flight_by_tier={5: 2},
                in_flight_total=2,
                max_parallel=4,
                reserved_slots={1: 2},
            )
        )

    def test_non_reserved_tier_cannot_exceed_shared_pool(self) -> None:
        # max_parallel=6, reserved={1:2, 3:2} -> shared pool = 2.
        # Tier 5 already has 2 in flight (filling the shared pool).
        # Another tier 5 dispatch must be denied.
        self.assertFalse(
            tier_capacity_allows(
                job_key="dashboard_summary_sync",  # tier 5
                in_flight_by_tier={1: 0, 3: 0, 5: 2},
                in_flight_total=2,
                max_parallel=6,
                reserved_slots={1: 2, 3: 2},
            )
        )

    def test_reserved_tier_over_reservation_uses_shared_pool(self) -> None:
        # Tier 1 has already consumed its 2 reserved slots and is trying
        # to burst beyond.  The shared pool has room (0/2 used), so allow.
        self.assertTrue(
            tier_capacity_allows(
                job_key="market_hub_current_sync",
                in_flight_by_tier={1: 2},
                in_flight_total=2,
                max_parallel=6,
                reserved_slots={1: 2, 3: 2},
            )
        )

    def test_reserved_tier_over_reservation_blocked_when_pool_full(self) -> None:
        # Tier 1 already used its 2 reserved slots; shared pool also fully
        # consumed by tier 5.  Tier 1 cannot burst further.
        self.assertFalse(
            tier_capacity_allows(
                job_key="market_hub_current_sync",
                in_flight_by_tier={1: 2, 5: 2},
                in_flight_total=4,
                max_parallel=6,
                reserved_slots={1: 2, 3: 2},
            )
        )

    def test_cap_respected_across_multiple_over_reservations(self) -> None:
        # Tier 1 and tier 3 both burst into the shared pool (1 slot each
        # over their reservations).  That's 2/2 of the shared pool. A
        # third dispatch from any tier must be denied.
        self.assertFalse(
            tier_capacity_allows(
                job_key="dashboard_summary_sync",  # tier 5
                in_flight_by_tier={1: 3, 3: 3},
                in_flight_total=6,
                max_parallel=6,
                reserved_slots={1: 2, 3: 2},
            )
        )

    def test_always_denied_when_total_at_max(self) -> None:
        self.assertFalse(
            tier_capacity_allows(
                job_key="market_hub_current_sync",
                in_flight_by_tier={1: 2, 3: 2, 5: 2},
                in_flight_total=6,
                max_parallel=6,
                reserved_slots={1: 2, 3: 2},
            )
        )


class DescribeTierSlotsTests(unittest.TestCase):
    def test_disabled_summary(self) -> None:
        self.assertIn("disabled", describe_tier_slots({}, max_parallel=6))

    def test_enabled_summary_mentions_each_reserved_tier_and_shared_pool(self) -> None:
        summary = describe_tier_slots({1: 2, 3: 2}, max_parallel=6)
        self.assertIn("T1 reserved=2", summary)
        self.assertIn("T3 reserved=2", summary)
        self.assertIn("shared pool=2", summary)


class ValidateDisplayTierParityTests(unittest.TestCase):
    def test_matching_maps_return_no_issues(self) -> None:
        issues = validate_display_tier_parity(dict(DISPLAY_TIERS))
        self.assertEqual(issues, [])

    def test_mismatched_tier_reported(self) -> None:
        php_map = dict(DISPLAY_TIERS)
        php_map["market_hub_current_sync"] = 3  # was 1 in python
        issues = validate_display_tier_parity(php_map)
        self.assertTrue(
            any("market_hub_current_sync" in msg and "1 != php tier 3" in msg
                for msg in issues),
            f"expected mismatch report, got: {issues}",
        )

    def test_job_only_in_python_reported(self) -> None:
        php_map = {k: v for k, v in DISPLAY_TIERS.items()
                   if k != "market_hub_current_sync"}
        issues = validate_display_tier_parity(php_map)
        self.assertTrue(
            any("market_hub_current_sync" in msg and "not in php" in msg
                for msg in issues),
            f"expected python-only report, got: {issues}",
        )

    def test_job_only_in_php_reported(self) -> None:
        php_map = dict(DISPLAY_TIERS)
        php_map["brand_new_job"] = 4
        issues = validate_display_tier_parity(php_map)
        self.assertTrue(
            any("brand_new_job" in msg and "not in python" in msg
                for msg in issues),
            f"expected php-only report, got: {issues}",
        )


if __name__ == "__main__":
    unittest.main()
