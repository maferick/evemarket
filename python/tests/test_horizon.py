"""Tests for the incremental horizon helpers in orchestrator.horizon."""
from __future__ import annotations

import sys
import unittest
from datetime import datetime
from pathlib import Path
from typing import Any

# Bootstrap: ensure the orchestrator package is importable without
# pulling in its heavy runtime dependencies.
_PYTHON_DIR = str(Path(__file__).resolve().parents[1])
if _PYTHON_DIR not in sys.path:
    sys.path.insert(0, _PYTHON_DIR)

from orchestrator import horizon  # noqa: E402
from orchestrator.horizon import (  # noqa: E402
    DEFAULT_HORIZON_SECONDS,
    DEFAULT_REPAIR_WINDOW_SECONDS,
    HorizonState,
    STALL_THRESHOLD,
    advance_cursor_with_stall_detection,
    format_cursor,
    freshness_status,
    horizon_state_get,
    parse_cursor,
    read_from_cursor,
    rewind_cursor,
    should_use_incremental_only,
)


class FakeDb:
    """Minimal stand-in for SupplyCoreDb used in horizon helper tests.

    Models a single sync_state row per dataset_key and records the
    execute() calls that target it so the tests can assert on the
    outbound SQL shape without spinning up MariaDB.
    """

    def __init__(self, rows: dict[str, dict[str, Any]] | None = None) -> None:
        self.rows: dict[str, dict[str, Any]] = {k: dict(v) for k, v in (rows or {}).items()}
        self.executes: list[tuple[str, tuple[Any, ...]]] = []

    def fetch_one(self, sql: str, params: Any = None) -> dict[str, Any] | None:
        key = self._extract_dataset_key(params)
        if key is None or key not in self.rows:
            return None
        return dict(self.rows[key])

    def fetch_all(self, sql: str, params: Any = None) -> list[dict[str, Any]]:
        return [dict(row) for row in self.rows.values()]

    def execute(self, sql: str, params: Any = None) -> int:
        params_tuple = tuple(params or ())
        self.executes.append((sql, params_tuple))

        key = self._extract_dataset_key(params)
        if key is None or key not in self.rows:
            return 0

        row = self.rows[key]
        upper = sql.upper()

        # Very small UPDATE simulator: enough for the horizon helpers.
        # Explicit rewind: stall_cursor is literal NULL (not a parameter).
        if "LAST_CURSOR = %S" in upper and "STALL_CURSOR = NULL" in upper:
            new_cursor, event_time, _dataset_key = params_tuple
            row["last_cursor"] = new_cursor
            row["watermark_event_time"] = event_time
            row["stall_cursor"] = None
            row["stall_count"] = 0
            return 1

        # Forward advance: stall_cursor comes in as a parameter.
        if "LAST_CURSOR = %S" in upper and "STALL_CURSOR = %S" in upper:
            new_cursor, event_time, stall_cursor, _dataset_key = params_tuple
            row["last_cursor"] = new_cursor
            row["watermark_event_time"] = event_time
            row["stall_cursor"] = stall_cursor
            if "STALL_COUNT = 0" in upper:
                row["stall_count"] = 0
            else:
                row["stall_count"] = int(row.get("stall_count") or 0) + 1
            return 1

        if "BACKFILL_COMPLETE = 1" in upper:
            row["backfill_complete"] = 1
            row["backfill_proposed_at"] = None
            row["backfill_proposed_reason"] = None
            return 1

        if "BACKFILL_PROPOSED_AT = COALESCE" in upper:
            reason, _dataset_key = params_tuple
            if int(row.get("backfill_complete") or 0) == 0:
                row.setdefault("backfill_proposed_at", "2026-04-10 00:00:00")
                row["backfill_proposed_reason"] = reason
                return 1
            return 0

        if "BACKFILL_COMPLETE = 0" in upper and "BACKFILL_PROPOSED_AT = NULL" in upper:
            row["backfill_complete"] = 0
            row["backfill_proposed_at"] = None
            row["backfill_proposed_reason"] = None
            return 1

        if "BACKFILL_PROPOSED_AT = NULL" in upper:
            row["backfill_proposed_at"] = None
            row["backfill_proposed_reason"] = None
            return 1

        return 0

    @staticmethod
    def _extract_dataset_key(params: Any) -> str | None:
        if params is None:
            return None
        if isinstance(params, (list, tuple)) and params:
            return str(params[-1])
        return None


def _make_row(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "dataset_key": "test.dataset",
        "sync_mode": "incremental",
        "status": "success",
        "last_success_at": datetime(2026, 4, 10, 12, 0, 0),
        "last_cursor": "2026-04-10 10:00:00|100",
        "watermark_event_time": datetime(2026, 4, 10, 10, 0, 0),
        "backfill_complete": 1,
        "backfill_proposed_at": None,
        "backfill_proposed_reason": None,
        "incremental_horizon_seconds": DEFAULT_HORIZON_SECONDS,
        "repair_window_seconds": DEFAULT_REPAIR_WINDOW_SECONDS,
        "stall_cursor": "2026-04-10 10:00:00|100",
        "stall_count": 0,
    }
    base.update(overrides)
    return base


class CursorParsingTests(unittest.TestCase):
    def test_parse_cursor_happy_path(self) -> None:
        self.assertEqual(parse_cursor("2026-04-10 12:34:56|42"), ("2026-04-10 12:34:56", 42))

    def test_parse_cursor_handles_empty_input(self) -> None:
        self.assertEqual(parse_cursor(None), ("", 0))
        self.assertEqual(parse_cursor(""), ("", 0))
        self.assertEqual(parse_cursor("   "), ("", 0))

    def test_parse_cursor_handles_missing_id(self) -> None:
        self.assertEqual(parse_cursor("2026-04-10 12:00:00"), ("2026-04-10 12:00:00", 0))

    def test_parse_cursor_clamps_negative_id(self) -> None:
        self.assertEqual(parse_cursor("2026-04-10 12:00:00|-5"), ("2026-04-10 12:00:00", 0))

    def test_format_cursor_round_trip(self) -> None:
        self.assertEqual(
            format_cursor("2026-04-10 09:15:00", 7),
            "2026-04-10 09:15:00|7",
        )


class HorizonStateTests(unittest.TestCase):
    def test_horizon_state_from_row_casts_types(self) -> None:
        row = _make_row(backfill_complete=1, stall_count="3")
        state = HorizonState.from_row(row)
        self.assertTrue(state.backfill_complete)
        self.assertEqual(state.stall_count, 3)
        self.assertEqual(state.incremental_horizon_seconds, DEFAULT_HORIZON_SECONDS)

    def test_should_use_incremental_only_requires_gate_and_success(self) -> None:
        self.assertFalse(should_use_incremental_only(None))

        gated_off = HorizonState.from_row(_make_row(backfill_complete=0))
        self.assertFalse(should_use_incremental_only(gated_off))

        never_ran = HorizonState.from_row(_make_row(last_success_at=None))
        self.assertFalse(should_use_incremental_only(never_ran))

        ready = HorizonState.from_row(_make_row())
        self.assertTrue(should_use_incremental_only(ready))


class ReadFromCursorTests(unittest.TestCase):
    def test_returns_raw_cursor_when_backfill_incomplete(self) -> None:
        state = HorizonState.from_row(_make_row(backfill_complete=0))
        self.assertEqual(
            read_from_cursor(state, "2026-04-10 10:00:00|100"),
            "2026-04-10 10:00:00|100",
        )

    def test_applies_repair_window_when_gated_on(self) -> None:
        state = HorizonState.from_row(_make_row(repair_window_seconds=3600))
        result = read_from_cursor(state, "2026-04-10 10:00:00|100")
        # 1h earlier, id reset to 0.
        self.assertEqual(result, "2026-04-10 09:00:00|0")

    def test_repair_window_default_is_24_hours(self) -> None:
        state = HorizonState.from_row(_make_row())
        result = read_from_cursor(state, "2026-04-10 10:00:00|100")
        self.assertEqual(result, "2026-04-09 10:00:00|0")

    def test_empty_cursor_returns_epoch(self) -> None:
        state = HorizonState.from_row(_make_row())
        self.assertEqual(read_from_cursor(state, None), "1970-01-01 00:00:00|0")
        self.assertEqual(read_from_cursor(state, ""), "1970-01-01 00:00:00|0")

    def test_clamps_at_epoch_boundary(self) -> None:
        state = HorizonState.from_row(_make_row(repair_window_seconds=10 * 365 * 86400))
        result = read_from_cursor(state, "1971-01-01 00:00:00|5")
        timestamp, _id = parse_cursor(result)
        self.assertEqual(timestamp, "1970-01-01 00:00:00")

    def test_repair_window_zero_returns_raw_cursor(self) -> None:
        state = HorizonState.from_row(_make_row(repair_window_seconds=0))
        self.assertEqual(
            read_from_cursor(state, "2026-04-10 10:00:00|100"),
            "2026-04-10 10:00:00|100",
        )


class AdvanceCursorTests(unittest.TestCase):
    def test_advance_updates_cursor_and_resets_stall(self) -> None:
        db = FakeDb({"test.dataset": _make_row(last_cursor="2026-04-10 10:00:00|100", stall_count=2)})
        advance_cursor_with_stall_detection(db, "test.dataset", "2026-04-10 11:00:00|200")

        updated = db.rows["test.dataset"]
        self.assertEqual(updated["last_cursor"], "2026-04-10 11:00:00|200")
        self.assertEqual(updated["watermark_event_time"], "2026-04-10 11:00:00")
        self.assertEqual(updated["stall_count"], 0)
        self.assertEqual(updated["stall_cursor"], "2026-04-10 11:00:00|200")

    def test_non_advance_increments_stall_counter(self) -> None:
        db = FakeDb({"test.dataset": _make_row(last_cursor="2026-04-10 10:00:00|100", stall_count=1)})
        advance_cursor_with_stall_detection(db, "test.dataset", "2026-04-10 10:00:00|100")

        updated = db.rows["test.dataset"]
        self.assertEqual(updated["last_cursor"], "2026-04-10 10:00:00|100")
        self.assertEqual(updated["stall_count"], 2)
        self.assertEqual(updated["stall_cursor"], "2026-04-10 10:00:00|100")

    def test_rewind_attempt_is_ignored(self) -> None:
        db = FakeDb({"test.dataset": _make_row(last_cursor="2026-04-10 12:00:00|500")})
        advance_cursor_with_stall_detection(db, "test.dataset", "2026-04-10 10:00:00|100")

        # No UPDATE should have been recorded.
        updates = [
            call for call in db.executes
            if "LAST_CURSOR" in call[0].upper() and "STALL_COUNT = " in call[0].upper()
        ]
        self.assertEqual(updates, [])
        self.assertEqual(db.rows["test.dataset"]["last_cursor"], "2026-04-10 12:00:00|500")

    def test_explicit_rewind_bypasses_monotonic_guard(self) -> None:
        db = FakeDb({"test.dataset": _make_row(last_cursor="2026-04-10 12:00:00|500", stall_count=4)})
        rewind_cursor(db, "test.dataset", "2026-04-05 00:00:00|0")

        updated = db.rows["test.dataset"]
        self.assertEqual(updated["last_cursor"], "2026-04-05 00:00:00|0")
        self.assertEqual(updated["watermark_event_time"], "2026-04-05 00:00:00")
        self.assertEqual(updated["stall_count"], 0)
        self.assertIsNone(updated["stall_cursor"])


class FreshnessStatusTests(unittest.TestCase):
    NOW = datetime(2026, 4, 10, 12, 0, 0)

    def test_none_state_is_backfilling(self) -> None:
        result = freshness_status(None, now=self.NOW)
        self.assertEqual(result["horizon_status"], "backfilling")
        self.assertIsNone(result["freshness_lag_seconds"])
        self.assertFalse(result["is_caught_up_24h"])

    def test_caught_up_inside_horizon(self) -> None:
        state = HorizonState.from_row(_make_row(
            watermark_event_time=datetime(2026, 4, 10, 1, 0, 0),  # 11h ago
        ))
        result = freshness_status(state, now=self.NOW)
        self.assertEqual(result["horizon_status"], "caught_up")
        self.assertTrue(result["is_caught_up_24h"])
        self.assertEqual(result["freshness_lag_seconds"], 11 * 3600)

    def test_catching_up_beyond_horizon(self) -> None:
        state = HorizonState.from_row(_make_row(
            watermark_event_time=datetime(2026, 4, 8, 12, 0, 0),  # 48h ago
        ))
        result = freshness_status(state, now=self.NOW)
        self.assertEqual(result["horizon_status"], "catching_up")
        self.assertFalse(result["is_caught_up_24h"])

    def test_stalled_flag_wins_over_caught_up(self) -> None:
        state = HorizonState.from_row(_make_row(
            watermark_event_time=datetime(2026, 4, 10, 10, 0, 0),  # 2h ago
            stall_count=STALL_THRESHOLD,
        ))
        result = freshness_status(state, now=self.NOW)
        self.assertEqual(result["horizon_status"], "stalled")
        self.assertTrue(result["is_stalled"])

    def test_failed_status_is_stopped(self) -> None:
        state = HorizonState.from_row(_make_row(
            status="failed",
            watermark_event_time=datetime(2026, 4, 10, 11, 0, 0),
        ))
        result = freshness_status(state, now=self.NOW)
        self.assertEqual(result["horizon_status"], "stopped")

    def test_backfilling_when_gate_off(self) -> None:
        state = HorizonState.from_row(_make_row(
            backfill_complete=0,
            watermark_event_time=datetime(2026, 4, 10, 11, 0, 0),
        ))
        result = freshness_status(state, now=self.NOW)
        self.assertEqual(result["horizon_status"], "backfilling")


class HorizonStateGetTests(unittest.TestCase):
    def test_returns_none_for_missing_dataset(self) -> None:
        db = FakeDb()
        self.assertIsNone(horizon_state_get(db, "nope"))

    def test_returns_state_for_existing_row(self) -> None:
        db = FakeDb({"test.dataset": _make_row()})
        state = horizon_state_get(db, "test.dataset")
        self.assertIsNotNone(state)
        assert state is not None
        self.assertEqual(state.dataset_key, "test.dataset")
        self.assertTrue(state.backfill_complete)


if __name__ == "__main__":
    unittest.main()
