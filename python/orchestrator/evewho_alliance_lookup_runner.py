"""Dedicated continuous runner for EveWho alliance member lookups.

This runner is designed to be started and stopped manually from the CLI.
It continuously cycles through:

  1. Alliance member sync (Phase 1 org sweep + Phase 2 character discovery)
  2. Enrichment sync (batch-enrich queued characters)

It uses **half** of the EveWho API rate limit (5 req / 30 s) so the web
worker pool can use the other half for on-demand lookups without the two
processes exceeding the global 10 req / 30 s cap.

Usage::

    python -m orchestrator evewho-alliance-runner [--loop-sleep 30] [--once] [--verbose]

The runner writes a heartbeat state file to ``storage/run/evewho-runner-heartbeat.json``
so other processes can see whether it is alive.
"""
from __future__ import annotations

import argparse
import json
import os
import socket
import time
from pathlib import Path
from typing import Any

from .config import load_php_runtime_config, resolve_app_root
from .json_utils import json_dumps_safe
from .logging_utils import configure_logging
from .worker_runtime import resident_memory_bytes, utc_now_iso

# Half the global rate limit — the other half is reserved for the worker pool.
RUNNER_RATE_LIMIT_REQUESTS = 5


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the dedicated continuous EveWho alliance lookup runner.",
    )
    parser.add_argument("--app-root", default=resolve_app_root(__file__))
    parser.add_argument(
        "--loop-sleep", type=int, default=30,
        help="Seconds to sleep between cycles (default: 30).",
    )
    parser.add_argument("--once", action="store_true", help="Run one cycle then exit.")
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args(argv)


def _write_state_file(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json_dumps_safe(payload, indent=2) + "\n", encoding="utf-8")


def _run_one_cycle(
    db: Any,
    neo4j_raw: dict[str, Any] | None,
    runtime: dict[str, Any] | None,
    logger: Any,
) -> dict[str, Any]:
    """Execute one full alliance-sync + enrichment cycle.

    Returns a merged result dict from both sub-jobs.
    """
    from .evewho_adapter import EveWhoAdapter  # noqa: local import to avoid circular
    from .jobs.evewho_alliance_member_sync import run_evewho_alliance_member_sync
    from .jobs.evewho_enrichment_sync import run_evewho_enrichment_sync
    from .job_context import battle_runtime

    # Inject the halved rate limit into runtime config so the jobs
    # create adapters with the correct budget.
    runtime = dict(runtime or {})
    runtime["evewho_rate_limit_requests"] = RUNNER_RATE_LIMIT_REQUESTS

    logger.info("Starting alliance member sync cycle")
    alliance_result = run_evewho_alliance_member_sync(
        db, neo4j_raw, runtime,
    )
    alliance_status = str(alliance_result.get("status") or "unknown")
    logger.info(
        "Alliance member sync completed",
        payload={
            "event": "evewho_runner.alliance_sync_done",
            "status": alliance_status,
            "summary": alliance_result.get("summary", ""),
        },
    )

    logger.info("Starting enrichment sync cycle")
    enrichment_result = run_evewho_enrichment_sync(
        db, neo4j_raw, battle_runtime(runtime),
    )
    enrichment_status = str(enrichment_result.get("status") or "unknown")
    logger.info(
        "Enrichment sync completed",
        payload={
            "event": "evewho_runner.enrichment_sync_done",
            "status": enrichment_status,
            "summary": enrichment_result.get("summary", ""),
        },
    )

    # Merge into a single cycle result
    overall_status = "success"
    if alliance_status == "failed" or enrichment_status == "failed":
        overall_status = "failed"
    elif alliance_status == "skipped" and enrichment_status == "skipped":
        overall_status = "skipped"

    return {
        "status": overall_status,
        "alliance_sync": alliance_result,
        "enrichment_sync": enrichment_result,
    }


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    app_root = Path(args.app_root).resolve()
    config = load_php_runtime_config(app_root)
    worker_settings = config.raw.get("workers", {}) if isinstance(config.raw, dict) else {}

    log_file = Path(
        worker_settings.get(
            "evewho_runner_log_file",
            app_root / "storage/logs/evewho-runner.log",
        )
    )
    logger = configure_logging(
        logger_name="supplycore.evewho_runner",
        verbose=args.verbose,
        log_file=log_file,
        stdout_enabled=True,
    )

    state_file = Path(
        worker_settings.get(
            "evewho_runner_state_file",
            app_root / "storage/run/evewho-runner-heartbeat.json",
        )
    )
    abort_threshold = max(
        128 * 1024 * 1024,
        int(worker_settings.get("memory_abort_threshold_bytes", 512 * 1024 * 1024)),
    )
    error_backoff_seconds = max(5, int(worker_settings.get("evewho_runner_error_backoff_seconds", 15)))
    identity = f"{socket.gethostname()}-{os.getpid()}"

    from .db import SupplyCoreDb
    from .job_context import neo4j_runtime

    db = SupplyCoreDb(config.raw.get("db", {}))

    logger.info(
        "EveWho alliance lookup runner started",
        payload={
            "event": "evewho_runner.started",
            "worker_id": identity,
            "log_file": str(log_file),
            "state_file": str(state_file),
            "rate_limit_requests": RUNNER_RATE_LIMIT_REQUESTS,
            "loop_sleep": args.loop_sleep,
        },
    )

    cycle_count = 0

    while True:
        # Memory guard
        memory_usage = resident_memory_bytes()
        if memory_usage >= abort_threshold:
            logger.warning(
                "EveWho runner exiting: memory threshold exceeded",
                payload={
                    "event": "evewho_runner.memory_abort",
                    "worker_id": identity,
                    "memory_usage_bytes": memory_usage,
                },
            )
            return 1

        cycle_count += 1
        cycle_started = time.monotonic()

        try:
            result = _run_one_cycle(
                db,
                neo4j_runtime(config.raw),
                config.raw,
                logger,
            )
        except Exception as exc:
            logger.exception(
                "EveWho runner cycle failed",
                payload={
                    "event": "evewho_runner.cycle_failed",
                    "worker_id": identity,
                    "error": str(exc),
                },
            )
            result = {"status": "failed", "error": str(exc)}

            _write_state_file(state_file, {
                "ts": utc_now_iso(),
                "worker_id": identity,
                "cycle": cycle_count,
                "result": result,
                "memory_usage_bytes": resident_memory_bytes(),
            })

            if args.once:
                return 1
            time.sleep(error_backoff_seconds)
            continue

        cycle_duration_s = round(time.monotonic() - cycle_started, 1)

        _write_state_file(state_file, {
            "ts": utc_now_iso(),
            "worker_id": identity,
            "cycle": cycle_count,
            "cycle_duration_s": cycle_duration_s,
            "result": result,
            "memory_usage_bytes": resident_memory_bytes(),
        })

        logger.info(
            "EveWho runner cycle completed",
            payload={
                "event": "evewho_runner.cycle_completed",
                "worker_id": identity,
                "cycle": cycle_count,
                "cycle_duration_s": cycle_duration_s,
                "status": result.get("status"),
            },
        )

        if args.once:
            return 0 if result.get("status") != "failed" else 1

        # Sleep between cycles
        logger.info(
            "Sleeping %d seconds before next cycle",
            args.loop_sleep,
            payload={
                "event": "evewho_runner.sleeping",
                "loop_sleep": args.loop_sleep,
            },
        )
        time.sleep(args.loop_sleep)


if __name__ == "__main__":
    raise SystemExit(main())
