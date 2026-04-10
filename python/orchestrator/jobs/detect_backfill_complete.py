"""Backfill-complete proposal detector.

Scans ``sync_state`` for datasets that look ready to transition from
full/backfill + incremental into incremental-only horizon mode, and
*proposes* them for admin review by stamping ``backfill_proposed_at``
and ``backfill_proposed_reason``.

The detector intentionally never flips ``backfill_complete`` itself --
humans approve via ``bin/horizon-approve.php`` (or the settings UI).
This matches the plan in ``/root/.claude/plans/breezy-toasting-wind.md``
(section 4 -- "Backfill-done gate").

Heuristics for proposing a dataset
----------------------------------
A dataset becomes a candidate when **all** of the following hold:

1. ``backfill_complete = 0`` and no pending proposal yet.
2. ``last_success_at`` is older than the soak period (default 24h).
3. At least ``_MIN_CLEAN_RUNS`` consecutive recent ``sync_runs`` rows
   are ``run_status = 'success'`` (no recent failures).
4. The cursor has advanced at least once inside the soak window
   (``stall_count < _MAX_STALL`` guards against idle/stuck datasets).
5. The freshness watermark is inside the dataset's incremental horizon
   window (i.e. the job is already caught up to its SLA).

Datasets that already have a pending proposal are left alone so repeated
runs don't clobber an admin's review queue.
"""
from __future__ import annotations

import logging
from typing import Any

from ..db import SupplyCoreDb
from ..horizon import (
    DEFAULT_HORIZON_SECONDS,
    HorizonState,
    propose_backfill_complete,
)
from .sync_runtime import run_sync_phase_job

logger = logging.getLogger(__name__)


# --- Tunables ---------------------------------------------------------------

#: How long a dataset must have been running cleanly in incremental mode
#: before the detector is willing to propose it. 24h matches the default
#: incremental horizon.
_SOAK_SECONDS = 24 * 3600

#: Minimum number of recent ``sync_runs`` rows that must all be clean
#: (``run_status = 'success'``) for the detector to propose a dataset.
_MIN_CLEAN_RUNS = 5

#: Stall-counter ceiling. A dataset that has stalled this many runs or
#: more is obviously not ready to go incremental-only.
_MAX_STALL = 2


def _processor(db: SupplyCoreDb) -> dict[str, Any]:
    rows = db.fetch_all(
        """
        SELECT dataset_key,
               sync_mode,
               status,
               last_success_at,
               last_cursor,
               watermark_event_time,
               backfill_complete,
               backfill_proposed_at,
               incremental_horizon_seconds,
               repair_window_seconds,
               stall_cursor,
               stall_count
          FROM sync_state
         WHERE backfill_complete = 0
           AND backfill_proposed_at IS NULL
        """,
    )

    proposed: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []

    for row in rows:
        state = HorizonState.from_row(row)
        verdict = _evaluate_candidate(db, state)

        if verdict["ready"]:
            reason = str(verdict["reason"])
            propose_backfill_complete(db, state.dataset_key, reason)
            proposed.append({
                "dataset_key": state.dataset_key,
                "reason": reason,
            })
            logger.info(
                "Proposed backfill_complete for dataset %s (%s)",
                state.dataset_key,
                reason,
            )
        else:
            skipped.append({
                "dataset_key": state.dataset_key,
                "reason": verdict["reason"],
            })

    summary = (
        f"Scanned {len(rows)} candidate dataset(s); "
        f"proposed {len(proposed)}, skipped {len(skipped)}."
    )

    return {
        "rows_processed": len(rows),
        "rows_written": len(proposed),
        "warnings": [],
        "summary": summary,
        "meta": {
            "dataset_key": "detect_backfill_complete",
            "proposed": proposed,
            "skipped_count": len(skipped),
            "soak_seconds": _SOAK_SECONDS,
            "min_clean_runs": _MIN_CLEAN_RUNS,
        },
    }


def _evaluate_candidate(db: SupplyCoreDb, state: HorizonState) -> dict[str, Any]:
    """Decide whether a dataset is ready to be proposed. Pure function of
    the state row plus a small bounded query against ``sync_runs``.

    Returns a dict with ``ready: bool`` and ``reason: str``.
    """
    if state.last_success_at is None:
        return {"ready": False, "reason": "never_succeeded"}

    if state.last_cursor is None or state.last_cursor.strip() == "":
        return {"ready": False, "reason": "no_cursor"}

    if state.stall_count >= _MAX_STALL:
        return {"ready": False, "reason": f"stalled:{state.stall_count}"}

    if state.watermark_event_time is None:
        return {"ready": False, "reason": "no_watermark"}

    # Must be caught up to the configured horizon before we're willing
    # to switch over to incremental-only.
    horizon_seconds = state.incremental_horizon_seconds or DEFAULT_HORIZON_SECONDS
    now_row = db.fetch_one("SELECT UTC_TIMESTAMP() AS now_utc") or {}
    now_utc = now_row.get("now_utc")
    if now_utc is None:
        return {"ready": False, "reason": "no_db_clock"}

    lag_row = db.fetch_one(
        """
        SELECT TIMESTAMPDIFF(SECOND, %s, UTC_TIMESTAMP()) AS lag_seconds,
               TIMESTAMPDIFF(SECOND, %s, UTC_TIMESTAMP()) AS soak_seconds
        """,
        (state.watermark_event_time, state.last_success_at),
    ) or {}

    lag_seconds = int(lag_row.get("lag_seconds") or 0)
    soak_seconds = int(lag_row.get("soak_seconds") or 0)

    if lag_seconds > horizon_seconds:
        return {
            "ready": False,
            "reason": f"behind_horizon:{lag_seconds}s>{horizon_seconds}s",
        }

    if soak_seconds < _SOAK_SECONDS:
        return {
            "ready": False,
            "reason": f"soaking:{soak_seconds}s<{_SOAK_SECONDS}s",
        }

    recent = db.fetch_all(
        """
        SELECT run_status
          FROM sync_runs
         WHERE dataset_key = %s
         ORDER BY started_at DESC
         LIMIT %s
        """,
        (state.dataset_key, _MIN_CLEAN_RUNS),
    )

    if len(recent) < _MIN_CLEAN_RUNS:
        return {
            "ready": False,
            "reason": f"too_few_runs:{len(recent)}<{_MIN_CLEAN_RUNS}",
        }

    if any(str(r.get("run_status")) != "success" for r in recent):
        return {"ready": False, "reason": "recent_failure"}

    return {
        "ready": True,
        "reason": (
            f"clean_soak:{soak_seconds}s;lag:{lag_seconds}s;"
            f"runs:{_MIN_CLEAN_RUNS}/{_MIN_CLEAN_RUNS}"
        ),
    }


def run_detect_backfill_complete(db: SupplyCoreDb) -> dict[str, Any]:
    return run_sync_phase_job(
        db,
        job_key="detect_backfill_complete",
        phase="A",
        objective="propose horizon-ready datasets for admin review",
        processor=_processor,
    )
