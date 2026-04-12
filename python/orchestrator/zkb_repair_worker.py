"""Continuous zKB repair worker — drains the killmail zkb backlog.

Runs killmail_zkb_repair in a tight loop outside the scheduler.  The
repair job fetches missing zKillboard metadata (totalValue, points) for
killmail_events rows, at ~500 killmails per batch with 1s rate limiting.

With 15M+ killmails to process this needs its own dedicated daemon so
it doesn't block the maintenance lane or get killed by timeouts.

Each cycle runs one full processor invocation (which internally loops
through batches until it exhausts available work or hits a safety stop).
Then the daemon pauses briefly and runs again.
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


_JOB_KEY = "killmail_zkb_repair"


def _run_one_cycle(
    db: SupplyCoreDb,
    raw_config: dict[str, Any],
    logger: Any,
) -> dict[str, Any]:
    """Run the killmail_zkb_repair processor and record its outcome."""
    started = time.monotonic()
    try:
        result = make_json_safe(run_registered_processor(_JOB_KEY, db, raw_config))
        elapsed = time.monotonic() - started
        result.setdefault("duration_ms", int(elapsed * 1000))
        result.setdefault("started_at", utc_now_iso())
        result.setdefault("finished_at", utc_now_iso())
        result.setdefault("meta", {})
        result["meta"]["execution_language"] = "python"
        result["meta"]["runner"] = "zkb_repair_worker"
        status = str(result.get("status") or "success")

        _finalize_job(db, _JOB_KEY, result, logger)

        try:
            db.update_sync_schedule_status(
                job_key=_JOB_KEY,
                status=status,
                snapshot_json=json_dumps_safe(result),
                duration_seconds=elapsed,
            )
            db.update_effective_interval(_JOB_KEY)
            db.update_duration_stats(_JOB_KEY, elapsed, success=(status == "success"))
            db.advance_next_due_at(_JOB_KEY)
        except Exception:
            pass

        total_found = result.get("total_found", 0)
        total_updated = result.get("total_updated", 0)
        logger.info(
            f"cycle finished",
            payload={
                "event": "zkb_repair.cycle_finished",
                "status": status,
                "duration_seconds": round(elapsed, 1),
                "total_found": total_found,
                "total_updated": total_updated,
                "summary": str(result.get("summary", ""))[:200],
            },
        )
        return result

    except Exception as exc:
        elapsed = time.monotonic() - started
        fail_result = JobResult.failed(
            job_key=_JOB_KEY,
            error=exc,
            meta={"execution_language": "python", "runner": "zkb_repair_worker"},
        ).to_dict()
        _finalize_job(db, _JOB_KEY, fail_result, logger)
        try:
            db.update_sync_schedule_status(
                job_key=_JOB_KEY,
                status="failed",
                snapshot_json=json_dumps_safe(fail_result),
                duration_seconds=elapsed,
            )
            db.update_duration_stats(_JOB_KEY, elapsed, success=False)
            db.advance_next_due_at(_JOB_KEY)
        except Exception:
            pass

        logger.warning(
            f"cycle failed: {exc}",
            payload={
                "event": "zkb_repair.cycle_failed",
                "error": str(exc),
                "duration_seconds": round(elapsed, 1),
            },
        )
        return fail_result


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run killmail zKB repair in a continuous loop (outside the scheduler)."
    )
    parser.add_argument("--app-root", default=resolve_app_root(__file__))
    parser.add_argument("--idle-sleep", type=float, default=30.0,
                        help="Seconds to sleep between cycles (default: 30)")
    parser.add_argument("--error-backoff", type=float, default=60.0,
                        help="Seconds to sleep after an error (default: 60)")
    parser.add_argument("--memory-max-gb", type=float, default=1.0,
                        help="Memory abort threshold in GiB (default: 1.0)")
    parser.add_argument("--once", action="store_true",
                        help="Run one cycle and exit")
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    app_root = Path(args.app_root).resolve()
    raw_config = load_php_runtime_config(app_root).raw

    log_file = app_root / "storage/logs/zkb-repair.log"
    logger = configure_logging(
        logger_name="supplycore.zkb_repair",
        verbose=args.verbose,
        log_file=log_file,
        stdout_enabled=True,
    )

    db = SupplyCoreDb(dict(raw_config.get("db") or {}))
    identity = f"zkb-repair-{socket.gethostname()}-{os.getpid()}"
    memory_abort_bytes = int(args.memory_max_gb * 1024 * 1024 * 1024)

    shutdown_event = threading.Event()

    def _handle_signal(signum: int, _frame: Any) -> None:
        sig_name = signal.Signals(signum).name
        logger.info(
            f"received {sig_name}, shutting down",
            payload={"event": "zkb_repair.shutdown", "signal": sig_name},
        )
        shutdown_event.set()

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    logger.info(
        "zKB repair worker started",
        payload={
            "event": "zkb_repair.started",
            "worker_id": identity,
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
                payload={"event": "zkb_repair.memory_abort", "memory_bytes": mem},
            )
            return 1

        result = _run_one_cycle(db, raw_config, logger)
        status = str(result.get("status") or "")

        if args.once:
            return 0

        # The processor loops internally until it exhausts the batch or
        # hits a safety stop.  If it found zero rows, the backlog is
        # drained — sleep longer.  On error, back off.
        total_found = result.get("total_found", 0)
        if status == "failed":
            shutdown_event.wait(timeout=args.error_backoff)
        elif total_found == 0:
            # Nothing to repair — wait longer before checking again
            shutdown_event.wait(timeout=args.idle_sleep * 6)
        else:
            shutdown_event.wait(timeout=args.idle_sleep)

    logger.info(
        "zKB repair worker stopped",
        payload={"event": "zkb_repair.stopped", "worker_id": identity, "cycles": cycle},
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
