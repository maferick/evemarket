"""Dedicated continuous runner for the full-history killmail backfill.

Runs like the zKill or EveWho services: it loops continuously, processing
one backfill cycle per iteration.  When the backfill is fully caught up it
idles — sleeping between polls — until the ``killmail_backfill.full_history_start_date``
setting changes.  Changing that setting automatically resets progress and
triggers a fresh backfill from the new date.

Usage::

    python -m orchestrator killmail-backfill-runner [--loop-sleep 60] [--once] [--verbose]

The runner writes a heartbeat state file to
``storage/run/backfill-runner-heartbeat.json`` so other processes can see
whether it is alive and what it last processed.
"""
from __future__ import annotations

import argparse
import json
import os
import socket
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .bridge import PhpBridge
from .config import load_php_runtime_config, resolve_app_root
from .json_utils import json_dumps_safe
from .logging_utils import configure_logging
from .worker_runtime import resident_memory_bytes, utc_now_iso


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the dedicated continuous killmail full-history backfill runner.",
    )
    parser.add_argument("--app-root", default=resolve_app_root(__file__))
    parser.add_argument(
        "--loop-sleep", type=int, default=60,
        help="Seconds to sleep between cycles when already up to date (default: 60).",
    )
    parser.add_argument("--once", action="store_true", help="Run one cycle then exit.")
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args(argv)


def _write_state_file(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json_dumps_safe(payload, indent=2) + "\n", encoding="utf-8")


@dataclass
class _BackfillContext:
    app_root: Path
    php_binary: str


def _read_current_start_date(bridge: PhpBridge) -> str:
    """Ask the PHP bridge for the current configured backfill start date."""
    try:
        response = bridge.call("killmail-full-history-backfill-context")
        ctx = response.get("context") or {}
        return str(ctx.get("start_date") or "").strip()
    except Exception:
        return ""


def _reset_backfill_progress(bridge: PhpBridge, logger: Any) -> None:
    """Clear the persisted last-completed date so the next run starts fresh."""
    try:
        bridge.call("update-setting", payload={
            "key": "killmail_full_history_backfill_last_date",
            "value": "",
        })
        logger.info(
            "Backfill progress reset",
            payload={"event": "backfill_runner.progress_reset"},
        )
    except Exception as exc:
        logger.warning(
            "Failed to reset backfill progress",
            payload={"event": "backfill_runner.progress_reset_failed", "error": str(exc)},
        )


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    app_root = Path(args.app_root).resolve()
    runtime = load_php_runtime_config(app_root)
    worker_settings = runtime.raw.get("workers", {}) if isinstance(runtime.raw, dict) else {}

    log_file = Path(
        worker_settings.get(
            "backfill_runner_log_file",
            app_root / "storage/logs/backfill-runner.log",
        )
    )
    logger = configure_logging(
        logger_name="supplycore.backfill_runner",
        verbose=args.verbose,
        log_file=log_file,
        stdout_enabled=True,
    )

    state_file = Path(
        worker_settings.get(
            "backfill_runner_state_file",
            app_root / "storage/run/backfill-runner-heartbeat.json",
        )
    )
    abort_threshold = max(
        128 * 1024 * 1024,
        int(worker_settings.get("memory_abort_threshold_bytes", 512 * 1024 * 1024)),
    )
    error_backoff_seconds = max(5, int(worker_settings.get("backfill_runner_error_backoff_seconds", 15)))
    identity = f"{socket.gethostname()}-{os.getpid()}"

    bridge = PhpBridge(runtime.php_binary, app_root)
    ctx = _BackfillContext(app_root=app_root, php_binary=runtime.php_binary)

    # Track the start date seen at last cycle — detect setting changes.
    last_known_start_date: str | None = None

    logger.info(
        "Killmail backfill runner started",
        payload={
            "event": "backfill_runner.started",
            "worker_id": identity,
            "log_file": str(log_file),
            "state_file": str(state_file),
            "loop_sleep": args.loop_sleep,
        },
    )

    cycle_count = 0

    while True:
        # Memory guard
        memory_usage = resident_memory_bytes()
        if memory_usage >= abort_threshold:
            logger.warning(
                "Backfill runner exiting: memory threshold exceeded",
                payload={
                    "event": "backfill_runner.memory_abort",
                    "worker_id": identity,
                    "memory_usage_bytes": memory_usage,
                },
            )
            return 1

        cycle_count += 1
        cycle_started = time.monotonic()

        # --- Detect start-date setting changes ---
        current_start_date = _read_current_start_date(bridge)

        if last_known_start_date is not None and current_start_date != last_known_start_date:
            logger.info(
                "Backfill start date changed — resetting progress",
                payload={
                    "event": "backfill_runner.start_date_changed",
                    "worker_id": identity,
                    "previous": last_known_start_date,
                    "new": current_start_date,
                },
            )
            _reset_backfill_progress(bridge, logger)

        last_known_start_date = current_start_date

        # --- Run one backfill cycle ---
        try:
            from .jobs.killmail_full_history_backfill import run_killmail_full_history_backfill
            result = run_killmail_full_history_backfill(ctx)
        except Exception as exc:
            logger.exception(
                "Backfill runner cycle failed",
                payload={
                    "event": "backfill_runner.cycle_failed",
                    "worker_id": identity,
                    "error": str(exc),
                },
            )
            result = {"status": "failed", "error": str(exc)}

            _write_state_file(state_file, {
                "ts": utc_now_iso(),
                "worker_id": identity,
                "cycle": cycle_count,
                "start_date": current_start_date,
                "result": result,
                "memory_usage_bytes": resident_memory_bytes(),
            })

            if args.once:
                return 1
            time.sleep(error_backoff_seconds)
            continue

        cycle_duration_s = round(time.monotonic() - cycle_started, 1)
        status = str(result.get("status") or "unknown")
        up_to_date = status == "success" and "Already up to date" in str(result.get("message") or "")

        _write_state_file(state_file, {
            "ts": utc_now_iso(),
            "worker_id": identity,
            "cycle": cycle_count,
            "cycle_duration_s": cycle_duration_s,
            "start_date": current_start_date,
            "up_to_date": up_to_date,
            "result": result,
            "memory_usage_bytes": resident_memory_bytes(),
        })

        logger.info(
            "Backfill runner cycle completed",
            payload={
                "event": "backfill_runner.cycle_completed",
                "worker_id": identity,
                "cycle": cycle_count,
                "cycle_duration_s": cycle_duration_s,
                "status": status,
                "start_date": current_start_date,
                "up_to_date": up_to_date,
                "months_processed": result.get("months_processed", 0),
                "written": result.get("written", 0),
            },
        )

        if args.once:
            return 0 if status != "failed" else 1

        if up_to_date:
            logger.info(
                "Backfill is up to date — idling until start date changes",
                payload={
                    "event": "backfill_runner.idle",
                    "worker_id": identity,
                    "loop_sleep": args.loop_sleep,
                },
            )

        time.sleep(args.loop_sleep)


if __name__ == "__main__":
    raise SystemExit(main())
