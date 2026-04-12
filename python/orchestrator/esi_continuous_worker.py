"""Continuous ESI worker — runs ESI lookup jobs in a tight loop.

These jobs have large backlogs (700k+ characters) and should drain
continuously rather than waiting for scheduler intervals.  This worker
runs outside the scheduler's lane system, similar to the zKill stream
worker.

Jobs managed:
  - esi_character_queue_sync:   queue newly seen characters for ESI lookup
  - esi_affiliation_sync:       fetch character → alliance/corp affiliations
  - entity_metadata_resolve_sync: resolve entity IDs to names
  - evewho_enrichment_sync:     enrich characters via EveWho API

Each job runs in sequence, and if any returns has_more=True, the worker
loops immediately.  Otherwise it pauses briefly before the next cycle.
"""

from __future__ import annotations

import argparse
import os
import signal
import socket
import time
import threading
from pathlib import Path
from typing import Any

from .config import load_php_runtime_config, resolve_app_root
from .db import SupplyCoreDb
from .job_result import JobResult
from .json_utils import json_dumps_safe, make_json_safe
from .logging_utils import configure_logging
from .processor_registry import run_registered_processor
from .worker_pool import _finalize_job
from .worker_runtime import resident_memory_bytes, utc_now_iso


# Jobs to run continuously, in execution order.
# esi_character_queue feeds esi_affiliation which feeds the rest.
_CONTINUOUS_JOBS = [
    "esi_character_queue_sync",
    "esi_affiliation_sync",
    "entity_metadata_resolve_sync",
    "evewho_enrichment_sync",
]


def _run_one_job(
    job_key: str,
    db: SupplyCoreDb,
    raw_config: dict[str, Any],
    logger: Any,
) -> dict[str, Any]:
    """Run a single job processor and record its outcome in the DB."""
    started = time.monotonic()
    try:
        result = make_json_safe(run_registered_processor(job_key, db, raw_config))
        elapsed = time.monotonic() - started
        result.setdefault("duration_ms", int(elapsed * 1000))
        result.setdefault("started_at", utc_now_iso())
        result.setdefault("finished_at", utc_now_iso())
        result.setdefault("meta", {})
        result["meta"]["execution_language"] = "python"
        result["meta"]["runner"] = "esi_continuous_worker"
        status = str(result.get("status") or "success")

        _finalize_job(db, job_key, result, logger)

        try:
            db.update_sync_schedule_status(
                job_key=job_key,
                status=status,
                snapshot_json=json_dumps_safe(result),
                duration_seconds=elapsed,
            )
            db.update_effective_interval(job_key)
            db.update_duration_stats(job_key, elapsed, success=(status == "success"))
            # Always advance to now — we manage our own loop timing.
            db.advance_next_due_at(job_key)
        except Exception:
            pass

        logger.info(
            f"job finished: {job_key}",
            payload={
                "event": "esi_continuous.job_finished",
                "job_key": job_key,
                "status": status,
                "duration_seconds": round(elapsed, 1),
                "has_more": bool(result.get("has_more")),
                "summary": str(result.get("summary", ""))[:200],
            },
        )
        return result

    except Exception as exc:
        elapsed = time.monotonic() - started
        fail_result = JobResult.failed(
            job_key=job_key,
            error=exc,
            meta={"execution_language": "python", "runner": "esi_continuous_worker"},
        ).to_dict()
        _finalize_job(db, job_key, fail_result, logger)
        try:
            db.update_sync_schedule_status(
                job_key=job_key,
                status="failed",
                snapshot_json=json_dumps_safe(fail_result),
                duration_seconds=elapsed,
            )
            db.update_duration_stats(job_key, elapsed, success=False)
            db.advance_next_due_at(job_key)
        except Exception:
            pass

        logger.warning(
            f"job failed: {job_key}: {exc}",
            payload={
                "event": "esi_continuous.job_failed",
                "job_key": job_key,
                "error": str(exc),
                "duration_seconds": round(elapsed, 1),
            },
        )
        return fail_result


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run ESI lookup jobs in a continuous loop (outside the scheduler)."
    )
    parser.add_argument("--app-root", default=resolve_app_root(__file__))
    parser.add_argument("--idle-sleep", type=float, default=5.0,
                        help="Seconds to sleep when no job returned has_more (default: 5)")
    parser.add_argument("--error-backoff", type=float, default=10.0,
                        help="Seconds to sleep after an error (default: 10)")
    parser.add_argument("--memory-max-gb", type=float, default=2.0,
                        help="Memory abort threshold in GiB (default: 2.0)")
    parser.add_argument("--once", action="store_true",
                        help="Run one cycle and exit")
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    app_root = Path(args.app_root).resolve()
    raw_config = load_php_runtime_config(app_root).raw

    log_file = app_root / "storage/logs/esi-continuous.log"
    logger = configure_logging(
        logger_name="supplycore.esi_continuous",
        verbose=args.verbose,
        log_file=log_file,
        stdout_enabled=True,
    )

    db = SupplyCoreDb(dict(raw_config.get("db") or {}))
    identity = f"esi-continuous-{socket.gethostname()}-{os.getpid()}"
    memory_abort_bytes = int(args.memory_max_gb * 1024 * 1024 * 1024)

    shutdown_event = threading.Event()

    def _handle_signal(signum: int, _frame: Any) -> None:
        sig_name = signal.Signals(signum).name
        logger.info(
            f"received {sig_name}, shutting down",
            payload={"event": "esi_continuous.shutdown", "signal": sig_name},
        )
        shutdown_event.set()

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    logger.info(
        "ESI continuous worker started",
        payload={
            "event": "esi_continuous.started",
            "worker_id": identity,
            "jobs": _CONTINUOUS_JOBS,
            "idle_sleep": args.idle_sleep,
            "memory_max_gb": args.memory_max_gb,
        },
    )

    cycle = 0
    while not shutdown_event.is_set():
        cycle += 1

        # Memory gate.
        mem = resident_memory_bytes()
        if mem >= memory_abort_bytes:
            logger.warning(
                f"memory abort threshold reached ({mem / (1024**3):.1f} GiB), exiting for restart",
                payload={"event": "esi_continuous.memory_abort", "memory_bytes": mem},
            )
            return 1

        any_has_more = False
        cycle_had_error = False

        for job_key in _CONTINUOUS_JOBS:
            if shutdown_event.is_set():
                break

            result = _run_one_job(job_key, db, raw_config, logger)
            status = str(result.get("status") or "")

            if status == "failed":
                cycle_had_error = True
            if result.get("has_more"):
                any_has_more = True

        if args.once:
            return 0

        # If any job has more work, loop immediately.
        # If there was an error, back off a bit.
        # Otherwise idle briefly.
        if any_has_more:
            continue
        elif cycle_had_error:
            shutdown_event.wait(timeout=args.error_backoff)
        else:
            shutdown_event.wait(timeout=args.idle_sleep)

    logger.info(
        "ESI continuous worker stopped",
        payload={"event": "esi_continuous.stopped", "worker_id": identity, "cycles": cycle},
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
