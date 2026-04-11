"""Lane-aware tier-by-tier loop runner.

Replaces the database-queue-backed worker pool with a straightforward approach:

  1. Compute execution tiers from the job dependency graph.
  2. For each tier, run all due jobs in parallel (respecting concurrency groups).
  3. Wait for every job in the tier to finish before starting the next tier.
  4. When all tiers are done, start over immediately.

Two loops run concurrently inside the same process:

  - **Fast loop**: jobs with ``opportunistic_background=False``.
  - **Background loop**: jobs with ``opportunistic_background=True``.

When ``--lane`` is specified, only jobs matching that lane are included.
This allows running multiple lane-specific systemd services for workload
isolation (realtime, ingestion, compute, maintenance) while keeping a
single codebase.  Without ``--lane``, all jobs run (backward compatible).
"""

from __future__ import annotations

import argparse
import math
import os
import signal
import socket
import time
import threading
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed, wait, FIRST_COMPLETED, Future
from dataclasses import dataclass, field
from pathlib import Path
from datetime import datetime, timezone
from typing import Any

from .config import load_php_runtime_config, resolve_app_root
from .db import SupplyCoreDb
from .display_tiers import (
    describe_tier_slots,
    get_display_tier,
    parse_tier_slots,
    tier_capacity_allows,
)
from .job_result import JobResult
from .json_utils import json_dumps_safe, make_json_safe
from .logging_utils import LoggerAdapter, configure_logging
from .processor_registry import PYTHON_PROCESSOR_JOB_KEYS, audit_enabled_python_jobs, run_registered_processor
from .scheduling_graph import build_graph, _topological_tiers
from .worker_registry import WORKER_JOB_DEFINITIONS
from .worker_runtime import resident_memory_bytes, utc_now_iso


# Re-use the UI refresh mapping and finalize logic from worker_pool.
from .worker_pool import _finalize_job, _UI_REFRESH_JOB_MAP


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class JobOutcome:
    """Result of running a single job inside the loop."""
    job_key: str
    status: str           # "success", "failed", "skipped"
    duration_seconds: float
    error: str | None = None
    result: dict[str, Any] = field(default_factory=dict)
    hog_risk: bool = False


_REALTIME_WARN_SECONDS = 15.0  # Warn if a realtime job exceeds this runtime.


def _run_single_job(
    job_key: str,
    db: SupplyCoreDb,
    raw_config: dict[str, Any],
    logger: LoggerAdapter,
    timeout_seconds: int,
    lane: str = "",
) -> JobOutcome:
    """Execute one job synchronously and return its outcome."""
    started = time.monotonic()
    try:
        result = run_registered_processor(job_key, db, raw_config)
        elapsed = time.monotonic() - started
        result = make_json_safe(result)

        # Back-fill timing if the processor didn't provide it.
        if not result.get("duration_ms"):
            result["duration_ms"] = int(elapsed * 1000)
        if not result.get("started_at"):
            result["started_at"] = utc_now_iso()
        if not result.get("finished_at"):
            result["finished_at"] = utc_now_iso()
        result.setdefault("meta", {})
        result["meta"]["execution_language"] = "python"
        result["meta"]["runner"] = "loop_runner"

        status = str(result.get("status") or "success")

        _finalize_job(db, job_key, result, logger)

        # Realtime lane governance: warn if a job is drifting heavy.
        if lane == "realtime" and elapsed > _REALTIME_WARN_SECONDS:
            logger.warning(
                f"realtime job {job_key} took {elapsed:.1f}s (>{_REALTIME_WARN_SECONDS}s threshold)",
                payload={
                    "event": "loop_runner.realtime_slow",
                    "job_key": job_key,
                    "duration_seconds": round(elapsed, 1),
                    "threshold_seconds": _REALTIME_WARN_SECONDS,
                },
            )

        # Advance next_due_at so the job isn't re-dispatched before its
        # configured interval.  If the job signalled has_more (it has
        # remaining work to drain), set next_due_at to now so it's
        # picked up on the very next cycle instead of waiting.
        try:
            db.update_sync_schedule_status(
                job_key=job_key,
                status=status,
                snapshot_json=json_dumps_safe(result),
                duration_seconds=elapsed,
            )
            db.update_effective_interval(job_key)
            if result.get("has_more"):
                db.advance_next_due_at(job_key)
            else:
                db.advance_next_due_at_by_interval(job_key)
        except Exception:
            pass

        return JobOutcome(
            job_key=job_key,
            status=status,
            duration_seconds=elapsed,
            result=result,
        )
    except Exception as exc:
        elapsed = time.monotonic() - started
        fail_result = JobResult.failed(
            job_key=job_key,
            error=exc,
            meta={"execution_language": "python", "runner": "loop_runner"},
        ).to_dict()

        _finalize_job(db, job_key, fail_result, logger)
        try:
            db.update_sync_schedule_status(
                job_key=job_key,
                status="failed",
                snapshot_json=json_dumps_safe(fail_result),
                duration_seconds=elapsed,
            )
            db.update_effective_interval(job_key)
            db.advance_next_due_at_by_interval(job_key)
        except Exception:
            pass

        return JobOutcome(
            job_key=job_key,
            status="failed",
            duration_seconds=elapsed,
            error=str(exc),
            result=fail_result,
        )


# ---------------------------------------------------------------------------
# Tier execution
# ---------------------------------------------------------------------------

def _split_tier_by_concurrency(
    tier_jobs: list[str],
    graph_nodes: dict,
) -> tuple[list[str], dict[str, list[str]]]:
    """Split a tier into independent jobs and concurrency-grouped jobs.

    Returns:
        (independent_jobs, grouped_jobs) where grouped_jobs maps
        concurrency_group -> [job_key, ...] (must run sequentially within group).
    """
    independent: list[str] = []
    groups: dict[str, list[str]] = {}

    for job_key in tier_jobs:
        node = graph_nodes.get(job_key)
        cg = node.concurrency_group if node else ""
        if not cg:
            independent.append(job_key)
        else:
            groups.setdefault(cg, []).append(job_key)

    return independent, groups


def _run_concurrency_group(
    group_jobs: list[str],
    db: SupplyCoreDb,
    raw_config: dict[str, Any],
    logger: LoggerAdapter,
    definitions: dict[str, dict[str, Any]],
    shutdown_event: threading.Event,
    lane: str = "",
) -> list[JobOutcome]:
    """Run jobs in a concurrency group sequentially."""
    outcomes: list[JobOutcome] = []
    for job_key in group_jobs:
        if shutdown_event.is_set():
            break
        timeout = int(definitions.get(job_key, {}).get("timeout_seconds", 300))
        outcome = _run_single_job(job_key, db, raw_config, logger, timeout, lane=lane)
        outcomes.append(outcome)
        logger.info(
            f"job finished: {job_key}",
            payload={
                "event": "loop_runner.job_finished",
                "job_key": job_key,
                "status": outcome.status,
                "duration_seconds": round(outcome.duration_seconds, 1),
                "error": outcome.error,
            },
        )
    return outcomes


def _tier_timeout_seconds(
    tier_jobs: list[str],
    definitions: dict[str, dict[str, Any]],
    groups: dict[str, list[str]],
    max_parallel: int = 1,
) -> int:
    """Compute the maximum wall-clock time a tier should be allowed to run.

    A tier is made up of "parallel units": each independent job is one
    unit (its cost = its own timeout), and each concurrency group is one
    unit whose members run sequentially under a single lock (cost = sum
    of members' timeouts).

    How those units map to wall-clock time depends on how many of them
    can run at once, which is capped by the executor's ``max_parallel``:

      * ``max_parallel >= len(units)`` — everything runs in parallel and
        the budget is ``max(units)``.
      * ``max_parallel <= 1``         — fully serialized, budget is
        ``sum(units)``.
      * otherwise — the longest unit pins the lower bound and the rest
        of the work is distributed across the available slots.

    Finally, a 2-minute buffer is added for thread startup, DB
    connection overhead, etc.  Prior to this fix the formula assumed
    unbounded parallelism (``max(units) + 120``), which silently
    starved ``--max-parallel 1`` deployments where multiple heavy jobs
    share a tier.
    """
    independent_units = [
        int(definitions.get(jk, {}).get("timeout_seconds", 300))
        for jk in tier_jobs
        if not any(jk in g for g in groups.values())
    ]
    group_units = [
        sum(int(definitions.get(jk, {}).get("timeout_seconds", 300)) for jk in gj)
        for gj in groups.values()
    ]
    units = independent_units + group_units
    if not units:
        return 300 + 120

    longest = max(units)
    parallel = max(1, int(max_parallel))
    if parallel <= 1:
        budget = sum(units)
    elif parallel >= len(units):
        budget = longest
    else:
        remainder = sum(units) - longest
        budget = longest + math.ceil(remainder / parallel)

    return budget + 120


def _append_scheduler_cycle_report(path: Path, payload: dict[str, Any]) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as fh:
            fh.write(json_dumps_safe(payload) + "\n")
    except OSError:
        pass


def run_tier(
    tier_index: int,
    tier_jobs: list[str],
    db: SupplyCoreDb,
    raw_config: dict[str, Any],
    logger: LoggerAdapter,
    definitions: dict[str, dict[str, Any]],
    graph_nodes: dict,
    max_parallel: int,
    shutdown_event: threading.Event,
    lane: str = "",
) -> list[JobOutcome]:
    """Run all jobs in a single tier, respecting concurrency groups.

    Independent jobs run in parallel.  Jobs sharing a concurrency group run
    sequentially within that group, but different groups run in parallel with
    each other and with independent jobs.

    A tier-level timeout prevents a single hung job from blocking all
    downstream tiers indefinitely.  Timed-out threads continue running in
    the background but the scheduler advances to the next tier.
    """
    if not tier_jobs or shutdown_event.is_set():
        return []

    independent, groups = _split_tier_by_concurrency(tier_jobs, graph_nodes)
    tier_timeout = _tier_timeout_seconds(tier_jobs, definitions, groups, max_parallel)

    logger.info(
        f"tier {tier_index}: {len(tier_jobs)} jobs "
        f"({len(independent)} independent, {len(groups)} concurrency groups, "
        f"timeout {tier_timeout}s)",
        payload={
            "event": "loop_runner.tier_start",
            "tier": tier_index,
            "total_jobs": len(tier_jobs),
            "independent_count": len(independent),
            "concurrency_groups": {k: v for k, v in groups.items()},
            "tier_timeout_seconds": tier_timeout,
        },
    )

    all_outcomes: list[JobOutcome] = []
    futures: list[Future] = []

    # Use explicit pool management instead of context manager so we can
    # abandon timed-out threads without blocking on shutdown(wait=True).
    pool = ThreadPoolExecutor(max_workers=max_parallel)
    try:
        # Submit each independent job as its own task.
        for job_key in independent:
            if shutdown_event.is_set():
                break
            timeout = int(definitions.get(job_key, {}).get("timeout_seconds", 300))
            fut = pool.submit(_run_single_job, job_key, db, raw_config, logger, timeout, lane)
            fut.job_key = job_key  # type: ignore[attr-defined]
            futures.append(fut)

        # Submit each concurrency group as a single sequential task.
        for group_name, group_jobs in groups.items():
            if shutdown_event.is_set():
                break
            fut = pool.submit(
                _run_concurrency_group,
                group_jobs, db, raw_config, logger, definitions, shutdown_event, lane,
            )
            fut.job_key = f"[group:{group_name}]"  # type: ignore[attr-defined]
            futures.append(fut)

        # Wait for everything in this tier — but with a hard timeout so a
        # single hung job cannot block all downstream tiers forever.
        completed_futures: set[Future] = set()
        try:
            for fut in as_completed(futures, timeout=tier_timeout):
                completed_futures.add(fut)
                try:
                    result = fut.result()
                    if isinstance(result, list):
                        all_outcomes.extend(result)
                    elif isinstance(result, JobOutcome):
                        all_outcomes.append(result)
                        logger.info(
                            f"job finished: {result.job_key}",
                            payload={
                                "event": "loop_runner.job_finished",
                                "job_key": result.job_key,
                                "status": result.status,
                                "duration_seconds": round(result.duration_seconds, 1),
                                "error": result.error,
                            },
                        )
                except Exception as exc:
                    job_label = getattr(fut, "job_key", "unknown")
                    logger.warning(
                        f"unexpected error in tier {tier_index}",
                        payload={
                            "event": "loop_runner.tier_error",
                            "tier": tier_index,
                            "job": job_label,
                            "error": str(exc),
                        },
                    )
        except TimeoutError:
            # Identify which jobs are still running and record them as
            # timed-out so the tier can advance.
            for fut in futures:
                if fut not in completed_futures and not fut.done():
                    job_label = getattr(fut, "job_key", "unknown")
                    logger.error(
                        f"tier {tier_index}: job {job_label} exceeded tier timeout "
                        f"({tier_timeout}s) — advancing to next tier",
                        payload={
                            "event": "loop_runner.tier_timeout",
                            "tier": tier_index,
                            "job": job_label,
                            "tier_timeout_seconds": tier_timeout,
                        },
                    )
                    all_outcomes.append(JobOutcome(
                        job_key=job_label,
                        status="failed",
                        duration_seconds=float(tier_timeout),
                        error=f"Exceeded tier timeout of {tier_timeout}s",
                    ))
    finally:
        # Shut down the pool without waiting for stuck threads.  Running
        # threads will continue in the background but the scheduler is
        # free to proceed with the next tier.
        pool.shutdown(wait=False, cancel_futures=True)

    return all_outcomes


# ---------------------------------------------------------------------------
# Dependency-aware cycle (no tier barriers)
# ---------------------------------------------------------------------------

def _run_cycle_dependency_aware(
    loop_name: str,
    due_keys: set[str],
    db: SupplyCoreDb,
    raw_config: dict[str, Any],
    logger: LoggerAdapter,
    definitions: dict[str, dict[str, Any]],
    graph_nodes: dict[str, Any],
    max_parallel: int,
    shutdown_event: threading.Event,
    lane: str = "",
    memory_abort_bytes: int | None = None,
    reserved_tier_slots: dict[int, int] | None = None,
) -> list[JobOutcome]:
    """Run all due jobs respecting dependencies but without tier barriers.

    Instead of waiting for an entire tier to complete, each job starts as
    soon as *its own* dependencies have finished.  This dramatically
    reduces idle time when a tier contains a mix of fast and slow jobs.

    Concurrency groups are respected via per-group locks so that jobs
    sharing a physical resource still serialise.

    ``reserved_tier_slots`` (optional) lets operators guarantee a minimum
    share of worker slots per display tier (T1–T6).  When set, jobs from
    over-saturated non-reserved tiers are held back at the dispatcher so a
    flood of one tier can't starve the others.  See
    ``orchestrator.display_tiers.tier_capacity_allows`` for the algorithm.
    """
    all_outcomes: list[JobOutcome] = []
    if not due_keys or shutdown_event.is_set():
        return all_outcomes

    reserved = reserved_tier_slots or {}

    # Build per-job dependency sets (only deps within this cycle's due jobs).
    deps: dict[str, set[str]] = {}
    for key in due_keys:
        node = graph_nodes.get(key)
        if node:
            deps[key] = set(node.depends_on) & due_keys
        else:
            deps[key] = set()

    completed: set[str] = set()
    remaining: set[str] = set(due_keys)
    in_flight: dict[Future, str] = {}
    # Per-display-tier in-flight counter used for reservation enforcement.
    # Only populated when ``reserved`` is non-empty.
    in_flight_by_tier: dict[int, int] = defaultdict(int)

    # Per-concurrency-group locks to prevent overlapping execution.
    group_locks: dict[str, threading.Lock] = defaultdict(threading.Lock)

    def _run_with_group_lock(job_key: str) -> JobOutcome:
        cg = definitions.get(job_key, {}).get("concurrency_group") or ""
        timeout = int(definitions.get(job_key, {}).get("timeout_seconds", 300))
        if cg:
            with group_locks[cg]:
                return _run_single_job(job_key, db, raw_config, logger, timeout)
        return _run_single_job(job_key, db, raw_config, logger, timeout)

    pool = ThreadPoolExecutor(max_workers=max_parallel)
    memory_aborted = False
    try:
        while (remaining or in_flight) and not shutdown_event.is_set():
            # Memory gate: with --no-tier-barriers the dispatcher will keep
            # feeding new jobs into the pool as soon as deps are satisfied,
            # so a single cycle can push RSS past the systemd MemoryMax
            # before the next cycle-start check runs.  Re-check here and,
            # if we've crossed the abort threshold, stop dispatching new
            # work and drain what's in flight so we exit cleanly instead
            # of being OOM-killed mid-cycle.
            if not memory_aborted and memory_abort_bytes is not None:
                mem = resident_memory_bytes()
                if mem >= memory_abort_bytes:
                    memory_aborted = True
                    logger.warning(
                        f"{loop_name}: memory abort threshold reached mid-cycle "
                        f"({mem / (1024**3):.1f} GiB), draining {len(in_flight)} "
                        f"in-flight job(s) before shutdown",
                        payload={
                            "event": "loop_runner.memory_abort_midcycle",
                            "loop": loop_name,
                            "memory_bytes": mem,
                            "in_flight": len(in_flight),
                            "remaining": len(remaining),
                        },
                    )

            # Find jobs whose deps are all satisfied — but don't dispatch
            # any more once we've tripped the memory gate.
            ready: list[str] = [] if memory_aborted else [
                k for k in remaining if deps[k] <= completed
            ]

            # When tier reservations are active, dispatch lower-tier work
            # first (T1 ingestion → T6 maintenance) so reserved tiers can
            # claim their guaranteed slots before non-reserved tiers eat
            # the shared pool.  Secondary sort by job_key keeps dispatch
            # deterministic for tests.
            if reserved and ready:
                ready.sort(key=lambda k: (get_display_tier(k), k))
            else:
                ready.sort(key=lambda k: (0 if str(definitions.get(k, {}).get("priority", "")).lower() == "high" else 1, k))

            held_back_by_tier_cap = 0
            for key in ready:
                is_high = str(definitions.get(key, {}).get("priority", "")).lower() == "high"
                if max_parallel > 1 and not is_high:
                    high_ready_waiting = any(
                        str(definitions.get(candidate, {}).get("priority", "")).lower() == "high"
                        for candidate in ready
                    )
                    high_in_flight = sum(
                        1
                        for inflight_key in in_flight.values()
                        if str(definitions.get(inflight_key, {}).get("priority", "")).lower() == "high"
                    )
                    if high_ready_waiting and high_in_flight == 0 and len(in_flight) >= max_parallel - 1:
                        continue
                if not tier_capacity_allows(
                    job_key=key,
                    in_flight_by_tier=in_flight_by_tier,
                    in_flight_total=len(in_flight),
                    max_parallel=max_parallel,
                    reserved_slots=reserved,
                ):
                    held_back_by_tier_cap += 1
                    continue
                remaining.discard(key)
                fut = pool.submit(_run_with_group_lock, key)
                fut.job_key = key  # type: ignore[attr-defined]
                in_flight[fut] = key
                if reserved:
                    in_flight_by_tier[get_display_tier(key)] += 1

            if reserved and held_back_by_tier_cap:
                logger.info(
                    f"{loop_name}: {held_back_by_tier_cap} job(s) held back "
                    f"by tier capacity cap (in_flight={len(in_flight)}, "
                    f"by_tier={dict(in_flight_by_tier)})",
                    payload={
                        "event": "loop_runner.tier_capacity_backpressure",
                        "loop": loop_name,
                        "held_back": held_back_by_tier_cap,
                        "in_flight_total": len(in_flight),
                        "in_flight_by_tier": dict(in_flight_by_tier),
                        "reserved_tier_slots": dict(reserved),
                    },
                )

            if not in_flight:
                if memory_aborted:
                    # In-flight drained after tripping the memory gate —
                    # request a graceful shutdown so systemd can restart us.
                    shutdown_event.set()
                elif remaining:
                    # Nothing running and nothing ready — remaining jobs have
                    # unsatisfied deps (likely failed upstream).  Skip them.
                    logger.warning(
                        f"{loop_name}: {len(remaining)} jobs blocked by unsatisfied deps, skipping",
                        payload={
                            "event": "loop_runner.deps_blocked",
                            "loop": loop_name,
                            "blocked_jobs": sorted(remaining),
                        },
                    )
                break

            # Wait for at least one future to complete.  The timeout doubles
            # as the mid-cycle memory-gate polling interval: when every
            # in-flight job is long-running (e.g. compute-bg backfills that
            # take minutes each) we still want to notice RSS creeping toward
            # the systemd MemoryMax before the kernel OOM-kills us, so we
            # loop back and re-check the gate on a short cadence instead of
            # waiting up to a full minute for a job to finish (#1000).
            done, _ = wait(in_flight.keys(), return_when=FIRST_COMPLETED, timeout=5)

            if not done:
                # Timeout on wait — just loop back to check shutdown_event
                # and the mid-cycle memory gate.
                continue

            for fut in done:
                key = in_flight.pop(fut)
                if reserved:
                    tier = get_display_tier(key)
                    in_flight_by_tier[tier] = max(0, in_flight_by_tier[tier] - 1)
                try:
                    outcome = fut.result()
                    all_outcomes.append(outcome)
                    logger.info(
                        f"job finished: {key}",
                        payload={
                            "event": "loop_runner.job_finished",
                            "job_key": key,
                            "status": outcome.status,
                            "duration_seconds": round(outcome.duration_seconds, 1),
                            "error": outcome.error,
                        },
                    )
                except Exception as exc:
                    all_outcomes.append(JobOutcome(
                        job_key=key, status="failed",
                        duration_seconds=0, error=str(exc),
                    ))
                    logger.warning(
                        f"unexpected error running {key}: {exc}",
                        payload={"event": "loop_runner.job_error", "job_key": key, "error": str(exc)},
                    )
                # Mark completed regardless of success/failure so dependents
                # can proceed (they check their own data freshness).
                completed.add(key)
    finally:
        pool.shutdown(wait=False, cancel_futures=True)

    return all_outcomes


# ---------------------------------------------------------------------------
# Main loops
# ---------------------------------------------------------------------------

def _select_jobs(
    definitions: dict[str, dict[str, Any]],
    background: bool,
    lane: str | None = None,
) -> dict[str, dict[str, Any]]:
    """Filter job definitions to either the fast loop or background loop.

    When *lane* is specified, only jobs assigned to that lane are included.
    """
    filtered = {
        key: defn
        for key, defn in definitions.items()
        if bool(defn.get("opportunistic_background", False)) == background
    }
    if lane:
        filtered = {
            key: defn
            for key, defn in filtered.items()
            if defn.get("lane", "compute-misc") == lane
        }
    return filtered


def _run_loop(
    loop_name: str,
    definitions: dict[str, dict[str, Any]],
    db: SupplyCoreDb,
    raw_config: dict[str, Any],
    logger: LoggerAdapter,
    max_parallel: int,
    shutdown_event: threading.Event,
    pause_between_cycles: float,
    run_once: bool = False,
    known_external_keys: set[str] | None = None,
    lane: str = "",
    memory_abort_bytes: int | None = None,
    use_tier_barriers: bool = True,
    reserved_tier_slots: dict[int, int] | None = None,
    report_path: Path | None = None,
) -> None:
    """Run the loop continuously (or once)."""
    graph_nodes = build_graph(definitions, known_external_keys=known_external_keys)
    tiers, _tier_map = _topological_tiers(graph_nodes)
    cycle = 0

    logger.info(
        f"{loop_name}: computed {len(tiers)} tiers from {len(definitions)} jobs",
        payload={
            "event": "loop_runner.graph_computed",
            "loop": loop_name,
            "total_jobs": len(definitions),
            "total_tiers": len(tiers),
            "tiers": {i: jobs for i, jobs in enumerate(tiers)},
        },
    )

    # Memory safety: abort threshold slightly below the systemd MemoryMax
    # so we can exit gracefully instead of being OOM-killed.
    if memory_abort_bytes is None:
        memory_abort_bytes = int(2.5 * 1024 * 1024 * 1024)  # 2.5 GiB default

    while not shutdown_event.is_set():
        cycle += 1
        cycle_start = time.monotonic()
        cycle_started_at = datetime.now(timezone.utc)

        mem = resident_memory_bytes()
        if mem >= memory_abort_bytes:
            logger.warning(
                f"{loop_name}: memory abort threshold reached ({mem / (1024**3):.1f} GiB), exiting for restart",
                payload={"event": "loop_runner.memory_abort", "loop": loop_name, "memory_bytes": mem},
            )
            shutdown_event.set()
            break

        logger.info(
            f"{loop_name}: cycle {cycle} starting",
            payload={"event": "loop_runner.cycle_start", "loop": loop_name, "cycle": cycle, "memory_bytes": mem},
        )

        total_success = 0
        total_failed = 0
        total_skipped = 0
        slowest_key = ""
        slowest_seconds = 0.0
        cycle_outcomes: list[JobOutcome] = []

        # Query the database once per cycle to find which jobs are actually
        # due.  This prevents re-running jobs that just finished and still
        # have time left on their configured interval.
        all_job_keys = [k for tier in tiers for k in tier]
        try:
            due_keys = db.fetch_due_job_keys(all_job_keys)
        except Exception:
            # If the query fails (e.g. table missing), fall back to running
            # everything so the system degrades gracefully.
            due_keys = set(all_job_keys)

        due_count = len(due_keys)
        skipped_not_due = len(all_job_keys) - due_count
        if skipped_not_due:
            logger.info(
                f"{loop_name}: {due_count} jobs due, {skipped_not_due} not yet due (skipped)",
                payload={
                    "event": "loop_runner.due_check",
                    "loop": loop_name,
                    "cycle": cycle,
                    "due_count": due_count,
                    "skipped_not_due": skipped_not_due,
                },
            )

        if use_tier_barriers:
            # Classic mode: run tiers sequentially, wait for each tier to
            # complete before starting the next.
            for tier_idx, tier_jobs in enumerate(tiers):
                if shutdown_event.is_set():
                    break

                # Only dispatch jobs that are actually due this cycle.
                tier_due = [k for k in tier_jobs if k in due_keys]
                if not tier_due:
                    continue

                # Mid-cycle memory gate (tier-barriers path).  The compute
                # lane runs with --memory-max-gb 1.5 / MemoryMax=2G and fires
                # several heavy compute_battle_* jobs in parallel inside
                # display tier T5.  Their combined RSS can easily blow past
                # the abort threshold between tiers, and without this check
                # we'd only notice at the *next* cycle-start — by which
                # time the kernel OOM-killer has already fired
                # (supplycore-lane-compute.service: Failed with result
                # 'oom-kill', auto-log #1008).  Re-check RSS before
                # dispatching each new tier so we shut down gracefully for
                # a systemd restart instead of being SIGKILLed.  PR #1015
                # / #1017 added the same gate to the dependency-aware
                # path; this mirrors it for the tier-barriers path used by
                # all the non-compute-bg lanes.
                mem = resident_memory_bytes()
                if mem >= memory_abort_bytes:
                    logger.warning(
                        f"{loop_name}: memory abort threshold reached mid-cycle "
                        f"({mem / (1024**3):.1f} GiB) at tier {tier_idx}/{len(tiers)}, "
                        "stopping dispatch and requesting shutdown",
                        payload={
                            "event": "loop_runner.memory_abort_midcycle",
                            "loop": loop_name,
                            "tier": tier_idx,
                            "total_tiers": len(tiers),
                            "memory_bytes": mem,
                            "memory_abort_bytes": memory_abort_bytes,
                        },
                    )
                    shutdown_event.set()
                    break

                outcomes = run_tier(
                    tier_index=tier_idx,
                    tier_jobs=tier_due,
                    db=db,
                    raw_config=raw_config,
                    logger=logger,
                    definitions=definitions,
                    graph_nodes=graph_nodes,
                    max_parallel=max_parallel,
                    shutdown_event=shutdown_event,
                    lane=lane,
                )

                for o in outcomes:
                    cycle_outcomes.append(o)
                    tier_budget = max(1.0, _tier_timeout_seconds(tier_due, definitions, _split_tier_by_concurrency(tier_due, graph_nodes)[1], max_parallel))
                    o.hog_risk = o.duration_seconds > (tier_budget * 0.5)
                    if o.status == "failed":
                        total_failed += 1
                    elif o.status == "skipped":
                        total_skipped += 1
                    else:
                        total_success += 1
                    if o.duration_seconds > slowest_seconds:
                        slowest_seconds = o.duration_seconds
                        slowest_key = o.job_key
        else:
            # Dependency-aware mode: each job starts as soon as its own
            # deps finish — no artificial tier barriers.
            outcomes = _run_cycle_dependency_aware(
                loop_name=loop_name,
                due_keys=due_keys,
                db=db,
                raw_config=raw_config,
                logger=logger,
                definitions=definitions,
                graph_nodes=graph_nodes,
                max_parallel=max_parallel,
                shutdown_event=shutdown_event,
                lane=lane,
                memory_abort_bytes=memory_abort_bytes,
                reserved_tier_slots=reserved_tier_slots,
            )
            for o in outcomes:
                cycle_outcomes.append(o)
                tier_budget = max(1.0, float(int(definitions.get(o.job_key, {}).get("timeout_seconds", 300))))
                o.hog_risk = o.duration_seconds > (tier_budget * 0.5)
                if o.status == "failed":
                    total_failed += 1
                elif o.status == "skipped":
                    total_skipped += 1
                else:
                    total_success += 1
                if o.duration_seconds > slowest_seconds:
                    slowest_seconds = o.duration_seconds
                    slowest_key = o.job_key

        cycle_elapsed = time.monotonic() - cycle_start
        logger.info(
            f"{loop_name}: cycle {cycle} finished in {cycle_elapsed:.1f}s "
            f"({total_success} ok, {total_failed} failed, {total_skipped} skipped)",
            payload={
                "event": "loop_runner.cycle_end",
                "loop": loop_name,
                "lane": lane or "all",
                "cycle": cycle,
                "jobs_considered": len(all_job_keys),
                "jobs_due": due_count,
                "jobs_dispatched": total_success + total_failed,
                "jobs_skipped_not_due": skipped_not_due,
                "duration_seconds": round(cycle_elapsed, 1),
                "success": total_success,
                "failed": total_failed,
                "skipped": total_skipped,
                "slowest_job_key": slowest_key,
                "slowest_job_seconds": round(slowest_seconds, 1),
            },
        )
        if report_path is not None:
            failures = [
                {
                    "job_key": o.job_key,
                    "error": o.error,
                    "duration_seconds": round(o.duration_seconds, 3),
                }
                for o in cycle_outcomes
                if o.status == "failed"
            ]
            # In tier-barrier mode outcomes are scoped per-tier; rebuild from counters and latest tier runs only.
            all_jobs_payload: list[dict[str, Any]] = []
            if use_tier_barriers:
                # best-effort snapshot from due keys; per-job statuses are emitted by job logs.
                for key in sorted(due_keys):
                    all_jobs_payload.append({"job_key": key, "status": "due"})
            else:
                for o in cycle_outcomes:
                    row = {
                        "job_key": o.job_key,
                        "status": o.status,
                        "duration_seconds": round(o.duration_seconds, 3),
                        "hog_risk": o.hog_risk,
                    }
                    if o.error:
                        row["error"] = o.error
                    all_jobs_payload.append(row)

            finished_at = datetime.now(timezone.utc)
            report_payload = {
                "cycle": cycle,
                "started_at": cycle_started_at.isoformat().replace("+00:00", "Z"),
                "finished_at": finished_at.isoformat().replace("+00:00", "Z"),
                "duration_seconds": round(cycle_elapsed, 3),
                "lane": lane or "all",
                "jobs_total": len(all_job_keys),
                "jobs_due": due_count,
                "jobs_ran": total_success + total_failed,
                "jobs_succeeded": total_success,
                "jobs_failed": total_failed,
                "jobs_skipped_not_due": skipped_not_due,
                "jobs_blocked_by_deps": max(0, due_count - (total_success + total_failed + total_skipped)),
                "slowest_job": slowest_key,
                "slowest_seconds": round(slowest_seconds, 3),
                "failures": failures,
                "memory_bytes": resident_memory_bytes(),
                "all_jobs": all_jobs_payload,
            }
            _append_scheduler_cycle_report(report_path, report_payload)

        if run_once:
            break

        # Brief pause between cycles to avoid hot-looping if everything is instant.
        if not shutdown_event.is_set() and pause_between_cycles > 0:
            shutdown_event.wait(timeout=pause_between_cycles)


# ---------------------------------------------------------------------------
# State file for external monitoring
# ---------------------------------------------------------------------------

def _write_state_file(path: Path, payload: dict[str, Any]) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json_dumps_safe(payload, indent=2) + "\n", encoding="utf-8")
    except OSError:
        pass  # best-effort; don't crash the runner over a heartbeat file


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Simple tier-by-tier loop runner")
    parser.add_argument("--app-root", default=resolve_app_root(__file__))
    parser.add_argument("--max-parallel", type=int, default=6,
                        help="Max concurrent jobs per tier (default: 6)")
    parser.add_argument("--fast-pause", type=float, default=5.0,
                        help="Seconds to pause between fast-loop cycles (default: 5)")
    parser.add_argument("--background-pause", type=float, default=30.0,
                        help="Seconds to pause between background-loop cycles (default: 30)")
    parser.add_argument("--once", action="store_true",
                        help="Run one cycle of each loop and exit")
    parser.add_argument("--fast-only", action="store_true",
                        help="Only run the fast loop (skip background jobs)")
    parser.add_argument("--background-only", action="store_true",
                        help="Only run the background loop (skip fast jobs)")
    parser.add_argument("--memory-max-gb", type=float, default=None,
                        help="Memory abort threshold in GiB (default: auto-detect from systemd or 2.5)")
    parser.add_argument("--no-tier-barriers", action="store_true",
                        help="Use dependency-aware dispatch instead of tier barriers (better for background loops)")
    parser.add_argument("--lane", default=None,
                        help="Only run jobs in this lane (realtime/ingestion/compute/maintenance)")
    parser.add_argument("--tier-slots", default="",
                        help="Reserve a minimum share of worker slots per display tier "
                             "(1–6). Format: 'tier:slots' pairs, comma-separated. "
                             "Example: '1:2,3:2' reserves 2 slots each for Tier 1 "
                             "(Data Ingestion) and Tier 3 (Graph). Non-reserved tiers "
                             "share the remaining pool. Only applies with "
                             "--no-tier-barriers. Total reserved must be < --max-parallel.")
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args(argv)


_VALID_LANES = {
    "realtime",
    "ingestion",
    "compute-graph",      # neo4j + graph analytics
    "compute-battle",     # battle rollups, theater, escalation, suspicion
    "compute-behavioral", # behavioral scoring, cohort, character pipeline
    "compute-cip",        # cip_* correlation/event pipeline
    "compute-misc",       # market/alliance/geo/AI/rollup catch-all
    "maintenance",
}


def _validate_lane_assignments(definitions: dict[str, dict[str, Any]]) -> list[str]:
    """Validate lane configuration at startup.  Returns list of issues (empty = ok)."""
    issues: list[str] = []
    for key, defn in definitions.items():
        if "lane" not in defn:
            issues.append(f"{key}: missing lane assignment")
        elif defn["lane"] not in _VALID_LANES:
            issues.append(f"{key}: unknown lane '{defn['lane']}'")
    # No concurrency group may span multiple lanes.
    group_lanes: dict[str, set[str]] = defaultdict(set)
    for key, defn in definitions.items():
        cg = defn.get("concurrency_group", "")
        if cg:
            group_lanes[cg].add(defn.get("lane", ""))
    for group, lanes in group_lanes.items():
        if len(lanes) > 1:
            issues.append(f"concurrency group '{group}' spans lanes: {sorted(lanes)}")
    return issues


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    app_root = Path(args.app_root).resolve()
    raw_config = load_php_runtime_config(app_root).raw
    worker_settings = dict(raw_config.get("workers") or {})

    log_file = Path(worker_settings.get("worker_log_file", app_root / "storage/logs/worker.log"))
    # When running in lane mode, use a per-lane log file so multiple
    # lane processes don't interleave writes in the same file.
    lane = args.lane or ""
    if lane:
        log_file = log_file.with_name(f"lane-{lane}.log")
    logger = configure_logging(verbose=args.verbose, log_file=log_file)
    worker_id = f"loop-{socket.gethostname()}-{os.getpid()}"

    db = SupplyCoreDb(dict(raw_config.get("db") or {}))

    # Ensure schedule rows exist from Python worker registry.
    ensure_summary = db.ensure_schedule_rows_from_registry(
        WORKER_JOB_DEFINITIONS,
        PYTHON_PROCESSOR_JOB_KEYS,
    )

    # Audit: warn on missing processor bindings (do not crash startup).
    audit = audit_enabled_python_jobs(db)
    if audit["issues"]:
        for issue in audit["issues"]:
            logger.warning(
                "python processor binding audit warning",
                payload={"event": "loop_runner.binding_audit.failed", "issue": issue},
            )
    logger.info(
        f"{ensure_summary['registered']} jobs registered, {ensure_summary['enabled']} enabled, "
        f"{ensure_summary['missing_processors']} missing processors, "
        f"{ensure_summary['disabled_by_operator']} disabled-by-operator",
        payload={"event": "loop_runner.schedule_registry_summary", **ensure_summary},
    )

    # Validate lane assignments before starting.
    lane_issues = _validate_lane_assignments(WORKER_JOB_DEFINITIONS)
    if lane_issues:
        for issue in lane_issues:
            logger.error(
                "lane assignment validation failure",
                payload={"event": "loop_runner.lane_audit.failed", "issue": issue},
            )
        return 1

    if lane and lane not in _VALID_LANES:
        logger.error(f"unknown lane '{lane}', valid lanes: {sorted(_VALID_LANES)}")
        return 1

    # Parse optional tier-slot reservations.  Only meaningful with
    # --no-tier-barriers (classic barrier mode is already tier-sequential),
    # but we parse unconditionally so bad specs fail fast on startup.
    try:
        reserved_tier_slots = parse_tier_slots(args.tier_slots, args.max_parallel)
    except ValueError as exc:
        logger.error(
            f"invalid --tier-slots: {exc}",
            payload={"event": "loop_runner.tier_slots.invalid", "error": str(exc)},
        )
        return 1
    if reserved_tier_slots and not args.no_tier_barriers:
        logger.warning(
            "--tier-slots has no effect without --no-tier-barriers "
            "(barrier mode already runs tiers sequentially); ignoring",
            payload={
                "event": "loop_runner.tier_slots.ignored",
                "reserved_tier_slots": reserved_tier_slots,
            },
        )
        reserved_tier_slots = {}
    if reserved_tier_slots:
        logger.info(
            describe_tier_slots(reserved_tier_slots, args.max_parallel),
            payload={
                "event": "loop_runner.tier_slots.active",
                "reserved_tier_slots": reserved_tier_slots,
                "max_parallel": args.max_parallel,
            },
        )

    state_file = Path(worker_settings.get("pool_state_file", app_root / "storage/run/loop-runner-heartbeat.json"))
    report_file = app_root / "storage/logs/scheduler-report.jsonl"
    # Per-lane state file so multiple lane processes don't overwrite each other.
    if lane:
        state_file = state_file.with_name(f"lane-{lane}-heartbeat.json")

    shutdown_event = threading.Event()

    def _handle_signal(signum: int, _frame: Any) -> None:
        sig_name = signal.Signals(signum).name
        logger.info(
            f"received {sig_name}, shutting down gracefully",
            payload={"event": "loop_runner.shutdown", "signal": sig_name},
        )
        shutdown_event.set()

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    logger.info(
        f"loop runner starting (worker_id={worker_id}, lane={lane or 'all'}, max_parallel={args.max_parallel})",
        payload={
            "event": "loop_runner.start",
            "worker_id": worker_id,
            "lane": lane or "all",
            "max_parallel": args.max_parallel,
            "fast_pause": args.fast_pause,
            "background_pause": args.background_pause,
            "fast_only": args.fast_only,
            "background_only": args.background_only,
        },
    )

    _write_state_file(state_file, {
        "ts": utc_now_iso(),
        "worker_id": worker_id,
        "status": "starting",
        "lane": lane or "all",
        "max_parallel": args.max_parallel,
    })

    fast_defs = _select_jobs(WORKER_JOB_DEFINITIONS, background=False, lane=lane or None)
    bg_defs = _select_jobs(WORKER_JOB_DEFINITIONS, background=True, lane=lane or None)

    logger.info(
        f"job split: {len(fast_defs)} fast-loop jobs, {len(bg_defs)} background jobs"
        + (f" (lane={lane})" if lane else ""),
        payload={
            "event": "loop_runner.job_split",
            "lane": lane or "all",
            "fast_jobs": sorted(fast_defs.keys()),
            "background_jobs": sorted(bg_defs.keys()),
        },
    )

    # Compute memory abort threshold from --memory-max-gb or auto-detect.
    if args.memory_max_gb is not None:
        mem_abort = int(args.memory_max_gb * 1024 * 1024 * 1024)
    else:
        mem_abort = int(2.5 * 1024 * 1024 * 1024)  # 2.5 GiB default

    threads: list[threading.Thread] = []

    # Each loop needs to know about the other loop's job keys AND all keys
    # from other lanes so that cross-loop/cross-lane dependencies are
    # silently stripped instead of logged as "unknown jobs" warnings.
    all_keys = set(WORKER_JOB_DEFINITIONS.keys())
    lane_keys = set(fast_defs.keys()) | set(bg_defs.keys())
    external_lane_keys = all_keys - lane_keys
    fast_keys = set(fast_defs.keys())
    bg_keys = set(bg_defs.keys())

    no_tier_barriers = args.no_tier_barriers

    if not args.background_only and fast_defs:
        fast_thread = threading.Thread(
            target=_run_loop,
            args=("fast", fast_defs, db, raw_config, logger,
                  args.max_parallel, shutdown_event, args.fast_pause, args.once),
            kwargs={"known_external_keys": bg_keys | external_lane_keys, "lane": lane,
                    "memory_abort_bytes": mem_abort,
                    "use_tier_barriers": not no_tier_barriers,
                    "reserved_tier_slots": reserved_tier_slots or None,
                    "report_path": report_file},
            name="fast-loop",
            daemon=True,
        )
        threads.append(fast_thread)

    if not args.fast_only and bg_defs:
        bg_thread = threading.Thread(
            target=_run_loop,
            args=("background", bg_defs, db, raw_config, logger,
                  args.max_parallel, shutdown_event, args.background_pause, args.once),
            kwargs={"known_external_keys": fast_keys | external_lane_keys, "lane": lane,
                    "memory_abort_bytes": mem_abort,
                    "use_tier_barriers": not no_tier_barriers,
                    "reserved_tier_slots": reserved_tier_slots or None,
                    "report_path": report_file},
            name="background-loop",
            daemon=True,
        )
        threads.append(bg_thread)

    for t in threads:
        t.start()

    # Write periodic heartbeat while running.
    try:
        while not shutdown_event.is_set() and any(t.is_alive() for t in threads):
            _write_state_file(state_file, {
                "ts": utc_now_iso(),
                "worker_id": worker_id,
                "status": "running",
                "lane": lane or "all",
                "max_parallel": args.max_parallel,
                "memory_usage_bytes": resident_memory_bytes(),
                "threads": [t.name for t in threads if t.is_alive()],
            })
            shutdown_event.wait(timeout=15)
    except KeyboardInterrupt:
        shutdown_event.set()

    # Wait for loops to finish.
    for t in threads:
        t.join(timeout=30)

    _write_state_file(state_file, {
        "ts": utc_now_iso(),
        "worker_id": worker_id,
        "status": "stopped",
    })

    logger.info("loop runner stopped", payload={"event": "loop_runner.stopped", "worker_id": worker_id})

    # When shutting down on a signal (SIGTERM/SIGINT) or a memory-abort
    # request, the inner ThreadPoolExecutors used by run_tier() and
    # _run_cycle_dependency_aware() were torn down with
    # `pool.shutdown(wait=False, cancel_futures=True)` so the dispatcher
    # could exit promptly without blocking on long-running jobs.  Their
    # already-running worker threads, however, remain registered in
    # concurrent.futures._threads_queues, and Python's atexit handler will
    # join them before the interpreter actually exits.  For compute-bg jobs
    # that routinely run for minutes (killmail backfills, CIP pipeline,
    # horizon forecasts) that join easily exceeds systemd
    # `TimeoutStopSec=90s`, which then trips
    # `Failed with result 'timeout'` / SIGKILL on the lane services
    # (#1001 lane-compute-bg, #1003 lane-compute).
    #
    # Force-exit instead so systemd records a clean stop.  All per-job DB
    # state was finalised by `_finalize_job` before this point and the JSON
    # log handlers flush line-by-line, so nothing material is lost.  The
    # `--once` path returns normally because shutdown_event stays unset
    # there.
    if shutdown_event.is_set():
        os._exit(0)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
