"""Simple tier-by-tier loop runner.

Replaces the database-queue-backed worker pool with a straightforward approach:

  1. Compute execution tiers from the job dependency graph.
  2. For each tier, run all jobs in parallel (respecting concurrency groups).
  3. Wait for every job in the tier to finish before starting the next tier.
  4. When all tiers are done, start over immediately.

Two loops run concurrently inside the same process:

  - **Fast loop**: jobs with ``opportunistic_background=False``.  These are the
    critical, frequent jobs (syncs, dashboards, compute).  A full cycle typically
    completes in a few minutes.
  - **Background loop**: jobs with ``opportunistic_background=True``.  These are
    slow, occasional jobs (historical backfills, forecasting, pruning).  They run
    independently so they never block the fast loop.

One process, one systemd service, no database queuing.
"""

from __future__ import annotations

import argparse
import os
import signal
import socket
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed, Future
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .config import load_php_runtime_config, resolve_app_root
from .db import SupplyCoreDb
from .job_result import JobResult
from .json_utils import json_dumps_safe, make_json_safe
from .logging_utils import LoggerAdapter, configure_logging
from .processor_registry import audit_enabled_python_jobs, run_registered_processor
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


def _run_single_job(
    job_key: str,
    db: SupplyCoreDb,
    raw_config: dict[str, Any],
    logger: LoggerAdapter,
    timeout_seconds: int,
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
        # Keep next_due_at in sync so the PHP scheduler and worker-pool
        # DAG planner see accurate timing.  Without this, next_due_at
        # stays permanently in the past, causing the PHP scheduler to
        # treat every job as perpetually overdue and the worker-pool to
        # queue stale cross-queue entries that block DAG resolution.
        try:
            db.advance_next_due_at_by_interval(job_key)
        except Exception:
            pass  # best-effort; row may not exist for sub-pipeline jobs

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
) -> list[JobOutcome]:
    """Run jobs in a concurrency group sequentially."""
    outcomes: list[JobOutcome] = []
    for job_key in group_jobs:
        if shutdown_event.is_set():
            break
        timeout = int(definitions.get(job_key, {}).get("timeout_seconds", 300))
        outcome = _run_single_job(job_key, db, raw_config, logger, timeout)
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
) -> list[JobOutcome]:
    """Run all jobs in a single tier, respecting concurrency groups.

    Independent jobs run in parallel.  Jobs sharing a concurrency group run
    sequentially within that group, but different groups run in parallel with
    each other and with independent jobs.
    """
    if not tier_jobs or shutdown_event.is_set():
        return []

    independent, groups = _split_tier_by_concurrency(tier_jobs, graph_nodes)

    logger.info(
        f"tier {tier_index}: {len(tier_jobs)} jobs "
        f"({len(independent)} independent, {len(groups)} concurrency groups)",
        payload={
            "event": "loop_runner.tier_start",
            "tier": tier_index,
            "total_jobs": len(tier_jobs),
            "independent_count": len(independent),
            "concurrency_groups": {k: v for k, v in groups.items()},
        },
    )

    all_outcomes: list[JobOutcome] = []
    futures: list[Future] = []

    with ThreadPoolExecutor(max_workers=max_parallel) as pool:
        # Submit each independent job as its own task.
        for job_key in independent:
            if shutdown_event.is_set():
                break
            timeout = int(definitions.get(job_key, {}).get("timeout_seconds", 300))
            fut = pool.submit(_run_single_job, job_key, db, raw_config, logger, timeout)
            fut.job_key = job_key  # type: ignore[attr-defined]
            futures.append(fut)

        # Submit each concurrency group as a single sequential task.
        for group_name, group_jobs in groups.items():
            if shutdown_event.is_set():
                break
            fut = pool.submit(
                _run_concurrency_group,
                group_jobs, db, raw_config, logger, definitions, shutdown_event,
            )
            fut.job_key = f"[group:{group_name}]"  # type: ignore[attr-defined]
            futures.append(fut)

        # Wait for everything in this tier.
        for fut in as_completed(futures):
            try:
                result = fut.result()
                if isinstance(result, list):
                    # Concurrency group returned a list of outcomes
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

    return all_outcomes


# ---------------------------------------------------------------------------
# Main loops
# ---------------------------------------------------------------------------

def _select_jobs(
    definitions: dict[str, dict[str, Any]],
    background: bool,
) -> dict[str, dict[str, Any]]:
    """Filter job definitions to either the fast loop or background loop."""
    return {
        key: defn
        for key, defn in definitions.items()
        if bool(defn.get("opportunistic_background", False)) == background
    }


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
) -> None:
    """Run the tier-by-tier loop continuously (or once)."""
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

    while not shutdown_event.is_set():
        cycle += 1
        cycle_start = time.monotonic()
        logger.info(
            f"{loop_name}: cycle {cycle} starting",
            payload={"event": "loop_runner.cycle_start", "loop": loop_name, "cycle": cycle},
        )

        total_success = 0
        total_failed = 0
        total_skipped = 0

        for tier_idx, tier_jobs in enumerate(tiers):
            if shutdown_event.is_set():
                break

            outcomes = run_tier(
                tier_index=tier_idx,
                tier_jobs=tier_jobs,
                db=db,
                raw_config=raw_config,
                logger=logger,
                definitions=definitions,
                graph_nodes=graph_nodes,
                max_parallel=max_parallel,
                shutdown_event=shutdown_event,
            )

            for o in outcomes:
                if o.status == "failed":
                    total_failed += 1
                elif o.status == "skipped":
                    total_skipped += 1
                else:
                    total_success += 1

        cycle_elapsed = time.monotonic() - cycle_start
        logger.info(
            f"{loop_name}: cycle {cycle} finished in {cycle_elapsed:.1f}s "
            f"({total_success} ok, {total_failed} failed, {total_skipped} skipped)",
            payload={
                "event": "loop_runner.cycle_end",
                "loop": loop_name,
                "cycle": cycle,
                "duration_seconds": round(cycle_elapsed, 1),
                "success": total_success,
                "failed": total_failed,
                "skipped": total_skipped,
            },
        )

        if run_once:
            break

        # Brief pause between cycles to avoid hot-looping if everything is instant.
        if not shutdown_event.is_set() and pause_between_cycles > 0:
            shutdown_event.wait(timeout=pause_between_cycles)


# ---------------------------------------------------------------------------
# State file for external monitoring
# ---------------------------------------------------------------------------

def _write_state_file(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json_dumps_safe(payload, indent=2) + "\n", encoding="utf-8")


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
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    app_root = Path(args.app_root).resolve()
    raw_config = load_php_runtime_config(app_root).raw
    worker_settings = dict(raw_config.get("workers") or {})

    log_file = Path(worker_settings.get("worker_log_file", app_root / "storage/logs/worker.log"))
    logger = configure_logging(verbose=args.verbose, log_file=log_file)
    worker_id = f"loop-{socket.gethostname()}-{os.getpid()}"

    db = SupplyCoreDb(dict(raw_config.get("db") or {}))

    # Audit: make sure all registered jobs have Python processors bound.
    audit = audit_enabled_python_jobs(db)
    if audit["issues"]:
        for issue in audit["issues"]:
            logger.error(
                "python processor binding audit failure",
                payload={"event": "loop_runner.binding_audit.failed", "issue": issue},
            )
        return 1

    state_file = Path(worker_settings.get("pool_state_file", app_root / "storage/run/loop-runner-heartbeat.json"))

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
        f"loop runner starting (worker_id={worker_id}, max_parallel={args.max_parallel})",
        payload={
            "event": "loop_runner.start",
            "worker_id": worker_id,
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
        "max_parallel": args.max_parallel,
    })

    fast_defs = _select_jobs(WORKER_JOB_DEFINITIONS, background=False)
    bg_defs = _select_jobs(WORKER_JOB_DEFINITIONS, background=True)

    logger.info(
        f"job split: {len(fast_defs)} fast-loop jobs, {len(bg_defs)} background jobs",
        payload={
            "event": "loop_runner.job_split",
            "fast_jobs": sorted(fast_defs.keys()),
            "background_jobs": sorted(bg_defs.keys()),
        },
    )

    threads: list[threading.Thread] = []

    # Each loop needs to know about the other loop's job keys so that
    # cross-loop dependencies are silently stripped instead of logged as
    # "unknown jobs" warnings.
    fast_keys = set(fast_defs.keys())
    bg_keys = set(bg_defs.keys())

    if not args.background_only and fast_defs:
        fast_thread = threading.Thread(
            target=_run_loop,
            args=("fast", fast_defs, db, raw_config, logger,
                  args.max_parallel, shutdown_event, args.fast_pause, args.once),
            kwargs={"known_external_keys": bg_keys},
            name="fast-loop",
            daemon=True,
        )
        threads.append(fast_thread)

    if not args.fast_only and bg_defs:
        bg_thread = threading.Thread(
            target=_run_loop,
            args=("background", bg_defs, db, raw_config, logger,
                  args.max_parallel, shutdown_event, args.background_pause, args.once),
            kwargs={"known_external_keys": fast_keys},
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
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
