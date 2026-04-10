"""Tests for the detect_backfill_complete auto-approval loop.

Covers the two-stage flow:
  1. Fresh sync_state row  -> proposal stamped, gate not flipped.
  2. Proposal age < soak    -> skipped, proposal intact.
  3. Proposal age >= soak   -> gate auto-flipped (if health still OK).
  4. Regression during soak -> recheck fails, gate stays closed.
  5. auto_approve_blocked   -> excluded from the approval scan entirely.

The detector itself runs against a small in-memory ``FakeDb`` that
models just enough of the ``sync_state`` / ``sync_runs`` query shapes
used by the job. We intentionally avoid spinning up MariaDB here --
the column additions / schema migrations are covered separately.
"""
from __future__ import annotations

import importlib.util
import sys
import types
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

_PYTHON_DIR = Path(__file__).resolve().parents[1]
if str(_PYTHON_DIR) not in sys.path:
    sys.path.insert(0, str(_PYTHON_DIR))

# Load detect_backfill_complete.py as a standalone module without going
# through ``orchestrator.jobs.__init__`` (which eagerly imports every
# job in the catalog, dragging pymysql into the test environment).
#
# We stub minimal placeholder packages for ``orchestrator`` /
# ``orchestrator.jobs`` so the relative imports inside
# detect_backfill_complete.py (``from ..horizon import ...`` and
# ``from .sync_runtime import ...``) resolve. The horizon module loads
# cleanly because it has no heavy deps; sync_runtime is stubbed with a
# no-op ``run_sync_phase_job`` that just calls the processor.

def _bootstrap_detector() -> types.ModuleType:
    orchestrator_pkg = sys.modules.get("orchestrator")
    if orchestrator_pkg is None:
        orchestrator_pkg = types.ModuleType("orchestrator")
        orchestrator_pkg.__path__ = [str(_PYTHON_DIR / "orchestrator")]  # type: ignore[attr-defined]
        sys.modules["orchestrator"] = orchestrator_pkg

    # Load the real horizon module so HorizonState + propose/approve
    # helpers match production behavior.
    if "orchestrator.horizon" not in sys.modules:
        horizon_spec = importlib.util.spec_from_file_location(
            "orchestrator.horizon",
            _PYTHON_DIR / "orchestrator" / "horizon.py",
        )
        assert horizon_spec is not None and horizon_spec.loader is not None
        horizon_mod = importlib.util.module_from_spec(horizon_spec)
        sys.modules["orchestrator.horizon"] = horizon_mod
        horizon_spec.loader.exec_module(horizon_mod)

    # Stub the db module so ``from ..db import SupplyCoreDb`` succeeds.
    if "orchestrator.db" not in sys.modules:
        db_stub = types.ModuleType("orchestrator.db")
        db_stub.SupplyCoreDb = object  # type: ignore[attr-defined]
        sys.modules["orchestrator.db"] = db_stub

    # Stub the jobs package with a minimal sync_runtime that just calls
    # the processor inline (production wraps the processor in phase
    # metadata; here we only care about the meta dict the processor
    # returns).
    jobs_pkg = sys.modules.get("orchestrator.jobs")
    if jobs_pkg is None:
        jobs_pkg = types.ModuleType("orchestrator.jobs")
        jobs_pkg.__path__ = [str(_PYTHON_DIR / "orchestrator" / "jobs")]  # type: ignore[attr-defined]
        sys.modules["orchestrator.jobs"] = jobs_pkg

    if "orchestrator.jobs.sync_runtime" not in sys.modules:
        sync_runtime_stub = types.ModuleType("orchestrator.jobs.sync_runtime")

        def run_sync_phase_job(db, *, job_key, phase, objective, processor):  # noqa: ANN001
            return processor(db)

        sync_runtime_stub.run_sync_phase_job = run_sync_phase_job  # type: ignore[attr-defined]
        sys.modules["orchestrator.jobs.sync_runtime"] = sync_runtime_stub

    spec = importlib.util.spec_from_file_location(
        "orchestrator.jobs.detect_backfill_complete",
        _PYTHON_DIR / "orchestrator" / "jobs" / "detect_backfill_complete.py",
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["orchestrator.jobs.detect_backfill_complete"] = module
    spec.loader.exec_module(module)
    return module


detector = _bootstrap_detector()


class DetectorFakeDb:
    """In-memory stand-in for SupplyCoreDb focused on the detector's
    query shapes.

    Understands four query patterns used by detect_backfill_complete:
      - ``SELECT ... FROM sync_state WHERE backfill_complete = 0 AND backfill_proposed_at IS NULL``
      - ``SELECT ... FROM sync_state WHERE backfill_complete = 0 AND backfill_proposed_at IS NOT NULL AND auto_approve_blocked = 0``
      - ``SELECT UTC_TIMESTAMP() AS now_utc``
      - ``SELECT TIMESTAMPDIFF(SECOND, %s, UTC_TIMESTAMP()) AS lag_seconds, TIMESTAMPDIFF(SECOND, %s, UTC_TIMESTAMP()) AS soak_seconds``
      - ``SELECT run_status FROM sync_runs WHERE dataset_key = %s ORDER BY started_at DESC LIMIT %s``

    Plus the two execute shapes from propose/approve_backfill_complete.
    """

    def __init__(
        self,
        *,
        now: datetime,
        sync_state: dict[str, dict[str, Any]],
        sync_runs: dict[str, list[dict[str, Any]]] | None = None,
    ) -> None:
        self.now = now
        self.sync_state: dict[str, dict[str, Any]] = {
            key: dict(row) for key, row in sync_state.items()
        }
        self.sync_runs = sync_runs or {}
        self.executes: list[tuple[str, tuple[Any, ...]]] = []

    # -- fetch -----------------------------------------------------------

    def fetch_all(self, sql: str, params: Any = None) -> list[dict[str, Any]]:
        compact = " ".join(sql.split()).upper()
        params_tuple = tuple(params or ())

        if "FROM SYNC_STATE" in compact and "BACKFILL_PROPOSED_AT IS NULL" in compact:
            # Pass 1: candidates to propose.
            return [
                dict(row) for row in self.sync_state.values()
                if int(row.get("backfill_complete") or 0) == 0
                and row.get("backfill_proposed_at") is None
            ]

        if (
            "FROM SYNC_STATE" in compact
            and "BACKFILL_PROPOSED_AT IS NOT NULL" in compact
            and "AUTO_APPROVE_BLOCKED = 0" in compact
        ):
            # Pass 2: candidates to auto-approve.
            rows: list[dict[str, Any]] = []
            for row in self.sync_state.values():
                if int(row.get("backfill_complete") or 0) != 0:
                    continue
                if row.get("backfill_proposed_at") is None:
                    continue
                if int(row.get("auto_approve_blocked") or 0) != 0:
                    continue
                copy = dict(row)
                proposed_at = row.get("backfill_proposed_at")
                if isinstance(proposed_at, datetime):
                    age = int((self.now - proposed_at).total_seconds())
                else:
                    age = 0
                copy["proposal_age_seconds"] = age
                rows.append(copy)
            return rows

        if "FROM SYNC_RUNS" in compact:
            # SELECT run_status FROM sync_runs WHERE dataset_key = %s ORDER BY started_at DESC LIMIT %s
            dataset_key = str(params_tuple[0]) if params_tuple else ""
            limit = int(params_tuple[1]) if len(params_tuple) > 1 else 0
            runs = self.sync_runs.get(dataset_key, [])
            return [dict(r) for r in runs[:limit]]

        raise AssertionError(f"Unexpected fetch_all SQL: {compact}")

    def fetch_one(self, sql: str, params: Any = None) -> dict[str, Any] | None:
        compact = " ".join(sql.split()).upper()
        params_tuple = tuple(params or ())

        if "UTC_TIMESTAMP() AS NOW_UTC" in compact:
            return {"now_utc": self.now}

        if "LAG_SECONDS" in compact and "SOAK_SECONDS" in compact:
            watermark = params_tuple[0]
            last_success = params_tuple[1]
            lag = int((self.now - watermark).total_seconds()) if isinstance(watermark, datetime) else 0
            soak = int((self.now - last_success).total_seconds()) if isinstance(last_success, datetime) else 0
            return {"lag_seconds": lag, "soak_seconds": soak}

        raise AssertionError(f"Unexpected fetch_one SQL: {compact}")

    # -- execute ---------------------------------------------------------

    def execute(self, sql: str, params: Any = None) -> int:
        params_tuple = tuple(params or ())
        self.executes.append((sql, params_tuple))
        compact = " ".join(sql.split()).upper()

        if "BACKFILL_PROPOSED_AT = COALESCE" in compact:
            reason, dataset_key = params_tuple
            row = self.sync_state.get(str(dataset_key))
            if row is None or int(row.get("backfill_complete") or 0) != 0:
                return 0
            # COALESCE(existing, UTC_TIMESTAMP()): only stamp when unset.
            if row.get("backfill_proposed_at") is None:
                row["backfill_proposed_at"] = self.now
            row["backfill_proposed_reason"] = reason
            return 1

        if "BACKFILL_COMPLETE = 1" in compact:
            (dataset_key,) = params_tuple
            row = self.sync_state.get(str(dataset_key))
            if row is None:
                return 0
            row["backfill_complete"] = 1
            row["backfill_proposed_at"] = None
            row["backfill_proposed_reason"] = None
            return 1

        raise AssertionError(f"Unexpected execute SQL: {compact}")


NOW = datetime(2026, 4, 10, 12, 0, 0)


def _healthy_state(
    dataset_key: str,
    *,
    backfill_proposed_at: datetime | None = None,
    stall_count: int = 0,
    auto_approve_blocked: int = 0,
    watermark_offset: timedelta = timedelta(hours=2),
    success_offset: timedelta = timedelta(hours=30),
) -> dict[str, Any]:
    """Build a sync_state row that passes every health criterion."""
    return {
        "dataset_key": dataset_key,
        "sync_mode": "incremental",
        "status": "success",
        "last_success_at": NOW - success_offset,
        "last_cursor": "2026-04-10 10:00:00|100",
        "watermark_event_time": NOW - watermark_offset,
        "backfill_complete": 0,
        "backfill_proposed_at": backfill_proposed_at,
        "backfill_proposed_reason": None,
        "incremental_horizon_seconds": 86400,
        "repair_window_seconds": 86400,
        "stall_cursor": "2026-04-10 10:00:00|100",
        "stall_count": stall_count,
        "auto_approve_blocked": auto_approve_blocked,
    }


def _clean_runs(count: int = 5) -> list[dict[str, Any]]:
    return [{"run_status": "success"} for _ in range(count)]


class ProposalPassTests(unittest.TestCase):
    def test_healthy_dataset_is_proposed_not_approved(self) -> None:
        db = DetectorFakeDb(
            now=NOW,
            sync_state={"test.dataset": _healthy_state("test.dataset")},
            sync_runs={"test.dataset": _clean_runs()},
        )

        result = detector._processor(db)

        self.assertEqual(result["meta"]["proposed"][0]["dataset_key"], "test.dataset")
        self.assertEqual(result["meta"]["auto_approved"], [])
        self.assertEqual(db.sync_state["test.dataset"]["backfill_complete"], 0)
        self.assertEqual(
            db.sync_state["test.dataset"]["backfill_proposed_at"], NOW
        )

    def test_stalled_dataset_is_skipped(self) -> None:
        db = DetectorFakeDb(
            now=NOW,
            sync_state={"test.dataset": _healthy_state("test.dataset", stall_count=5)},
            sync_runs={"test.dataset": _clean_runs()},
        )

        result = detector._processor(db)

        self.assertEqual(result["meta"]["proposed"], [])
        self.assertEqual(result["meta"]["propose_skipped_count"], 1)

    def test_fresh_proposal_in_same_run_is_not_auto_approved(self) -> None:
        # Propose pass stamps backfill_proposed_at = NOW, so the approval
        # pass sees proposal_age_seconds = 0 and skips on the soak check.
        db = DetectorFakeDb(
            now=NOW,
            sync_state={"test.dataset": _healthy_state("test.dataset")},
            sync_runs={"test.dataset": _clean_runs()},
        )

        result = detector._processor(db)

        self.assertEqual(len(result["meta"]["proposed"]), 1)
        self.assertEqual(result["meta"]["auto_approved"], [])
        self.assertEqual(db.sync_state["test.dataset"]["backfill_complete"], 0)


class AutoApproveSoakTests(unittest.TestCase):
    def test_soaked_proposal_is_auto_approved(self) -> None:
        old_proposal = NOW - timedelta(seconds=detector._AUTO_APPROVE_SOAK_SECONDS + 60)
        db = DetectorFakeDb(
            now=NOW,
            sync_state={
                "test.dataset": _healthy_state(
                    "test.dataset",
                    backfill_proposed_at=old_proposal,
                )
            },
            sync_runs={"test.dataset": _clean_runs()},
        )

        result = detector._processor(db)

        self.assertEqual(result["meta"]["proposed"], [])
        self.assertEqual(len(result["meta"]["auto_approved"]), 1)
        self.assertEqual(
            result["meta"]["auto_approved"][0]["dataset_key"], "test.dataset"
        )
        self.assertEqual(db.sync_state["test.dataset"]["backfill_complete"], 1)
        self.assertIsNone(db.sync_state["test.dataset"]["backfill_proposed_at"])

    def test_proposal_still_soaking_is_skipped(self) -> None:
        recent_proposal = NOW - timedelta(seconds=detector._AUTO_APPROVE_SOAK_SECONDS - 3600)
        db = DetectorFakeDb(
            now=NOW,
            sync_state={
                "test.dataset": _healthy_state(
                    "test.dataset",
                    backfill_proposed_at=recent_proposal,
                )
            },
            sync_runs={"test.dataset": _clean_runs()},
        )

        result = detector._processor(db)

        self.assertEqual(result["meta"]["auto_approved"], [])
        self.assertEqual(result["meta"]["approve_skipped_count"], 1)
        skip = result["meta"]["approve_skipped"][0]
        self.assertEqual(skip["dataset_key"], "test.dataset")
        self.assertTrue(skip["reason"].startswith("soaking:"))
        # Gate stays closed, proposal stays intact.
        self.assertEqual(db.sync_state["test.dataset"]["backfill_complete"], 0)
        self.assertEqual(
            db.sync_state["test.dataset"]["backfill_proposed_at"], recent_proposal
        )


class AutoApproveRecheckTests(unittest.TestCase):
    def test_recheck_fails_when_dataset_regressed_into_stall(self) -> None:
        old_proposal = NOW - timedelta(seconds=detector._AUTO_APPROVE_SOAK_SECONDS + 3600)
        db = DetectorFakeDb(
            now=NOW,
            sync_state={
                "test.dataset": _healthy_state(
                    "test.dataset",
                    backfill_proposed_at=old_proposal,
                    stall_count=detector._MAX_STALL + 1,
                )
            },
            sync_runs={"test.dataset": _clean_runs()},
        )

        result = detector._processor(db)

        self.assertEqual(result["meta"]["auto_approved"], [])
        skip = result["meta"]["approve_skipped"][0]
        self.assertTrue(skip["reason"].startswith("recheck_failed:"))
        # Gate stays closed; proposal stays intact so the admin can see
        # the regression in the review queue.
        self.assertEqual(db.sync_state["test.dataset"]["backfill_complete"], 0)
        self.assertEqual(
            db.sync_state["test.dataset"]["backfill_proposed_at"], old_proposal
        )

    def test_recheck_fails_when_recent_run_failed(self) -> None:
        old_proposal = NOW - timedelta(seconds=detector._AUTO_APPROVE_SOAK_SECONDS + 60)
        runs = _clean_runs(4) + [{"run_status": "failed"}]
        db = DetectorFakeDb(
            now=NOW,
            sync_state={
                "test.dataset": _healthy_state(
                    "test.dataset",
                    backfill_proposed_at=old_proposal,
                )
            },
            sync_runs={"test.dataset": runs},
        )

        result = detector._processor(db)

        self.assertEqual(result["meta"]["auto_approved"], [])
        skip = result["meta"]["approve_skipped"][0]
        self.assertIn("recent_failure", skip["reason"])
        self.assertEqual(db.sync_state["test.dataset"]["backfill_complete"], 0)

    def test_recheck_fails_when_watermark_drifted_outside_horizon(self) -> None:
        old_proposal = NOW - timedelta(seconds=detector._AUTO_APPROVE_SOAK_SECONDS + 60)
        db = DetectorFakeDb(
            now=NOW,
            sync_state={
                "test.dataset": _healthy_state(
                    "test.dataset",
                    backfill_proposed_at=old_proposal,
                    watermark_offset=timedelta(hours=48),  # 48h > 24h horizon
                )
            },
            sync_runs={"test.dataset": _clean_runs()},
        )

        result = detector._processor(db)

        self.assertEqual(result["meta"]["auto_approved"], [])
        skip = result["meta"]["approve_skipped"][0]
        self.assertIn("behind_horizon", skip["reason"])


class AutoApproveBlockTests(unittest.TestCase):
    def test_blocked_dataset_is_not_scanned_for_approval(self) -> None:
        old_proposal = NOW - timedelta(seconds=detector._AUTO_APPROVE_SOAK_SECONDS * 10)
        db = DetectorFakeDb(
            now=NOW,
            sync_state={
                "test.dataset": _healthy_state(
                    "test.dataset",
                    backfill_proposed_at=old_proposal,
                    auto_approve_blocked=1,
                )
            },
            sync_runs={"test.dataset": _clean_runs()},
        )

        result = detector._processor(db)

        # Excluded from the approval scan by the SQL WHERE clause.
        self.assertEqual(result["meta"]["auto_approved"], [])
        self.assertEqual(result["meta"]["approve_skipped_count"], 0)
        # Gate stays closed, proposal unchanged.
        self.assertEqual(db.sync_state["test.dataset"]["backfill_complete"], 0)
        self.assertEqual(
            db.sync_state["test.dataset"]["backfill_proposed_at"], old_proposal
        )


class SoakConstantSanityTests(unittest.TestCase):
    def test_auto_approve_soak_is_longer_than_initial_soak(self) -> None:
        # The approval soak should dominate the initial propose soak so
        # a dataset that clears both gates has had time to run cleanly
        # *and* sit in the review queue.
        self.assertGreaterEqual(
            detector._AUTO_APPROVE_SOAK_SECONDS,
            detector._SOAK_SECONDS,
        )

    def test_auto_approve_soak_is_at_least_24_hours(self) -> None:
        self.assertGreaterEqual(detector._AUTO_APPROVE_SOAK_SECONDS, 24 * 3600)


if __name__ == "__main__":
    unittest.main()
