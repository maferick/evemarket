"""Backfill-complete proposal detector and auto-approver.

This job runs two passes against ``sync_state`` every time it runs:

1. **Propose** new horizon-ready datasets by stamping
   ``backfill_proposed_at`` and ``backfill_proposed_reason`` on datasets
   that meet the health criteria but have no pending proposal yet.

2. **Auto-approve** existing proposals that have been soaking long
   enough *and* still satisfy the same health criteria at approval time.
   Any dataset with ``auto_approve_blocked = 1`` is left alone; the
   proposal will sit in the review queue until a human acts.

The detector intentionally never flips ``backfill_complete`` on a
brand-new proposal -- every dataset goes through the propose → soak →
re-check → approve cycle. This matches the plan in
``/root/.claude/plans/breezy-toasting-wind.md`` (section 4 -- "Backfill-
done gate") and extends it with a self-driving approval loop.

Timeline for a well-behaved dataset once backfill finishes
----------------------------------------------------------
::

    T+0         Backfill completes, job starts running cleanly in
                full/backfill + incremental mode.
    T+24h       First detect_backfill_complete pass meets all criteria
                and proposes the dataset (``backfill_proposed_at``
                stamped). Dataset shows up in the admin review queue.
    T+24h..T+72h  Proposal soaks. Any human can approve early via
                ``bin/horizon-approve.php``, block forever via
                ``bin/horizon-block.php``, reject via
                ``bin/horizon-reject.php``, or just wait.
    T+72h       Second detect_backfill_complete pass re-runs the health
                check, confirms still-ready and proposal age ≥
                ``_AUTO_APPROVE_SOAK_SECONDS``, and auto-flips
                ``backfill_complete = 1``. Next run uses
                incremental-only horizon mode.

The re-check at approval time is the critical safety layer: if the
dataset has regressed (stall, failed run, lag outside the SLA) between
proposal and auto-approval, the auto-approver leaves the proposal
pending so the freshness dashboard reflects the regression.

Heuristics for proposing / approving a dataset
-----------------------------------------------
A dataset becomes a candidate when **all** of the following hold:

1. ``backfill_complete = 0``.
2. ``last_success_at`` is older than the soak period (default 24h).
3. At least ``_MIN_CLEAN_RUNS`` consecutive recent ``sync_runs`` rows
   are ``run_status = 'success'`` (no recent failures).
4. The cursor has advanced at least once inside the soak window
   (``stall_count < _MAX_STALL`` guards against idle/stuck datasets).
5. The freshness watermark is inside the dataset's incremental horizon
   window (i.e. the job is already caught up to its SLA).

Auto-approval additionally requires:

6. ``backfill_proposed_at`` is at least ``_AUTO_APPROVE_SOAK_SECONDS``
   old (default 48h after the proposal).
7. ``auto_approve_blocked = 0`` (opt-out column on ``sync_state``).
"""
from __future__ import annotations

import logging
from typing import Any

from ..db import SupplyCoreDb
from ..horizon import (
    DEFAULT_HORIZON_SECONDS,
    HorizonState,
    approve_backfill_complete,
    propose_backfill_complete,
)
from .sync_runtime import run_sync_phase_job

logger = logging.getLogger(__name__)


# --- Tunables ---------------------------------------------------------------

#: How long a dataset must have been running cleanly in incremental mode
#: before the detector is willing to propose it. 24h matches the default
#: incremental horizon.
_SOAK_SECONDS = 24 * 3600

#: How long a proposal must sit in the admin review queue before
#: auto-approval kicks in. 48h gives a human two weekdays to eyeball
#: the freshness dashboard and either approve early, block, or reject
#: before the system decides for itself. The re-check of the health
#: criteria at approval time (see ``_evaluate_candidate``) is the
#: critical safety layer -- if anything regressed during the soak, the
#: auto-approver leaves the proposal pending.
_AUTO_APPROVE_SOAK_SECONDS = 48 * 3600

#: Minimum number of recent ``sync_runs`` rows that must all be clean
#: (``run_status = 'success'``) for the detector to propose a dataset.
_MIN_CLEAN_RUNS = 5

#: Stall-counter ceiling. A dataset that has stalled this many runs or
#: more is obviously not ready to go incremental-only.
_MAX_STALL = 2


_COMMON_STATE_COLUMNS = """
    dataset_key,
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
    stall_count,
    auto_approve_blocked
""".strip()


def _processor(db: SupplyCoreDb) -> dict[str, Any]:
    # Pass 1: propose new candidates.
    propose_rows = db.fetch_all(
        f"""
        SELECT {_COMMON_STATE_COLUMNS}
          FROM sync_state
         WHERE backfill_complete = 0
           AND backfill_proposed_at IS NULL
        """,
    )

    proposed: list[dict[str, Any]] = []
    propose_skipped: list[dict[str, Any]] = []

    for row in propose_rows:
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
            propose_skipped.append({
                "dataset_key": state.dataset_key,
                "reason": verdict["reason"],
            })

    # Pass 2: auto-approve soaked proposals that still look healthy.
    #
    # We re-fetch here instead of chaining through pass-1 results so
    # that proposals made on earlier runs (not just this run) are
    # considered, and so that a dataset proposed in pass 1 cannot be
    # instantly approved in pass 2 within the same job invocation --
    # the soak check (``proposal_age_seconds >= _AUTO_APPROVE_SOAK_SECONDS``)
    # will naturally reject it because ``backfill_proposed_at`` was
    # just stamped to ``UTC_TIMESTAMP()``.
    approve_rows = db.fetch_all(
        f"""
        SELECT {_COMMON_STATE_COLUMNS},
               TIMESTAMPDIFF(SECOND, backfill_proposed_at, UTC_TIMESTAMP())
                   AS proposal_age_seconds
          FROM sync_state
         WHERE backfill_complete = 0
           AND backfill_proposed_at IS NOT NULL
           AND auto_approve_blocked = 0
        """,
    )

    approved: list[dict[str, Any]] = []
    approve_skipped: list[dict[str, Any]] = []

    for row in approve_rows:
        state = HorizonState.from_row(row)
        proposal_age = int(row.get("proposal_age_seconds") or 0)

        if proposal_age < _AUTO_APPROVE_SOAK_SECONDS:
            approve_skipped.append({
                "dataset_key": state.dataset_key,
                "reason": f"soaking:{proposal_age}s<{_AUTO_APPROVE_SOAK_SECONDS}s",
            })
            continue

        # Re-run the same health check at approval time. This is the
        # safety layer: if the dataset regressed (stall, failed run,
        # lag outside SLA) since the proposal, leave it pending so the
        # freshness dashboard reflects the regression and a human can
        # decide what to do.
        verdict = _evaluate_candidate(db, state)
        if not verdict["ready"]:
            approve_skipped.append({
                "dataset_key": state.dataset_key,
                "reason": f"recheck_failed:{verdict['reason']}",
            })
            logger.info(
                "Auto-approve re-check failed for dataset %s: %s",
                state.dataset_key,
                verdict["reason"],
            )
            continue

        approve_backfill_complete(db, state.dataset_key)
        approved.append({
            "dataset_key": state.dataset_key,
            "proposal_age_seconds": proposal_age,
            "reason": str(verdict["reason"]),
        })
        logger.info(
            "Auto-approved backfill_complete for dataset %s (age=%ds; %s)",
            state.dataset_key,
            proposal_age,
            verdict["reason"],
        )

    rows_processed = len(propose_rows) + len(approve_rows)
    rows_written = len(proposed) + len(approved)
    summary = (
        f"Scanned {len(propose_rows)} new + {len(approve_rows)} pending; "
        f"proposed {len(proposed)}, auto-approved {len(approved)}, "
        f"skipped {len(propose_skipped) + len(approve_skipped)}."
    )

    return {
        "rows_processed": rows_processed,
        "rows_written": rows_written,
        "warnings": [],
        "summary": summary,
        "meta": {
            "dataset_key": "detect_backfill_complete",
            "proposed": proposed,
            "auto_approved": approved,
            "propose_skipped_count": len(propose_skipped),
            "approve_skipped_count": len(approve_skipped),
            "approve_skipped": approve_skipped,
            "soak_seconds": _SOAK_SECONDS,
            "auto_approve_soak_seconds": _AUTO_APPROVE_SOAK_SECONDS,
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
