from __future__ import annotations

import argparse
import json
import os
import socket
import time
from pathlib import Path
from typing import Any

from .config import load_php_runtime_config
from .jobs import run_killmail_r2z2_stream
from .logging_utils import configure_logging
from .worker_runtime import resident_memory_bytes, utc_now_iso


class ZKillContext:
    def __init__(self, app_root: Path, php_binary: str, timeout_seconds: int, memory_abort_threshold_bytes: int, logger):
        self.app_root = app_root
        self.php_binary = php_binary
        self.timeout_seconds = timeout_seconds
        self.memory_abort_threshold_bytes = memory_abort_threshold_bytes
        self.batch_size = 1000
        self.schedule_id = 0
        self.job_key = "killmail_r2z2_sync"
        self.logger = logger

    def emit(self, event: str, payload: dict[str, object]) -> None:
        self.logger.info(event, payload={"event": event, **payload})


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the dedicated continuous zKill worker.")
    parser.add_argument("--app-root", default=str(Path(__file__).resolve().parents[2]))
    parser.add_argument("--poll-sleep", type=int, default=10)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args(argv)


def _write_state_file(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _read_state_file(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        decoded = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return decoded if isinstance(decoded, dict) else {}


def _result_meta(result: dict[str, Any]) -> dict[str, Any]:
    meta = result.get("meta")
    return meta if isinstance(meta, dict) else {}


def _no_progress_state(previous_state: dict[str, Any], result: dict[str, Any], threshold: int) -> dict[str, Any]:
    meta = _result_meta(result)
    cursor_after = str(meta.get("cursor_after") or result.get("cursor") or "").strip()
    rows_written = int(result.get("rows_written") or 0)
    previous_result = previous_state.get("result")
    previous_meta = previous_result.get("meta") if isinstance(previous_result, dict) else {}
    previous_cursor_after = ""
    if isinstance(previous_meta, dict):
        previous_cursor_after = str(previous_meta.get("cursor_after") or previous_result.get("cursor") or "").strip()
    repeated_count = int(previous_state.get("same_cursor_no_progress_count") or 0)
    if rows_written == 0 and cursor_after != "" and cursor_after == previous_cursor_after:
        repeated_count += 1
    else:
        repeated_count = 0

    stuck_detected = repeated_count >= max(1, threshold)
    return {
        "same_cursor_no_progress_count": repeated_count,
        "stuck_threshold": max(1, threshold),
        "stuck_detected": stuck_detected,
    }


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    app_root = Path(args.app_root).resolve()
    runtime = load_php_runtime_config(app_root)
    worker_settings = runtime.raw.get("workers", {}) if isinstance(runtime.raw, dict) else {}
    log_file = Path(worker_settings.get("zkill_log_file", app_root / "storage/logs/zkill.log"))
    logger = configure_logging(
        logger_name="supplycore.zkill",
        verbose=args.verbose,
        log_file=log_file,
        stdout_enabled=True,
    )
    state_file = Path(worker_settings.get("zkill_state_file", app_root / "storage/run/zkill-heartbeat.json"))
    pause_threshold = max(128 * 1024 * 1024, int(worker_settings.get("memory_pause_threshold_bytes", 384 * 1024 * 1024)))
    abort_threshold = max(pause_threshold, int(worker_settings.get("memory_abort_threshold_bytes", 512 * 1024 * 1024)))
    stuck_threshold = max(2, int(worker_settings.get("zkill_stuck_cursor_threshold", 3)))
    identity = f"{socket.gethostname()}-{os.getpid()}"

    logger.info(
        "zkill worker started",
        payload={
            "event": "zkill.started",
            "worker_id": identity,
            "log_file": str(log_file),
            "state_file": str(state_file),
            "stuck_threshold": stuck_threshold,
        },
    )

    while True:
        memory_usage = resident_memory_bytes()
        if memory_usage >= abort_threshold:
            logger.warning(
                "zkill worker exiting because memory threshold was exceeded",
                payload={"event": "zkill.memory_abort", "worker_id": identity, "memory_usage_bytes": memory_usage},
            )
            return 1
        if memory_usage >= pause_threshold:
            logger.warning(
                "zkill worker pausing because memory threshold was reached",
                payload={"event": "zkill.memory_pause", "worker_id": identity, "memory_usage_bytes": memory_usage},
            )
            time.sleep(min(30, args.poll_sleep))

        context = ZKillContext(
            app_root=app_root,
            php_binary=runtime.php_binary,
            timeout_seconds=max(30, int(worker_settings.get("claim_ttl_seconds", 300))),
            memory_abort_threshold_bytes=abort_threshold,
            logger=logger,
        )
        previous_state = _read_state_file(state_file)
        try:
            result = run_killmail_r2z2_stream(context)
        except Exception:
            logger.exception(
                "zkill loop failed",
                payload={
                    "event": "zkill.loop_failed",
                    "worker_id": identity,
                    "log_file": str(log_file),
                    "state_file": str(state_file),
                },
            )
            raise

        no_progress = _no_progress_state(previous_state, result, stuck_threshold)
        meta = _result_meta(result)
        state_payload = {
            "ts": utc_now_iso(),
            "worker_id": identity,
            "log_file": str(log_file),
            "state_file": str(state_file),
            "result": result,
            "memory_usage_bytes": resident_memory_bytes(),
            **no_progress,
        }
        _write_state_file(state_file, state_payload)
        logger.info(
            "zkill loop completed",
            payload={
                "event": "zkill.loop_completed",
                "worker_id": identity,
                "log_file": str(log_file),
                "state_file": str(state_file),
                "status": result.get("status"),
                "rows_seen": result.get("rows_seen"),
                "rows_matched": meta.get("rows_matched"),
                "rows_filtered_out": meta.get("rows_filtered_out"),
                "rows_skipped_existing": meta.get("rows_skipped_existing"),
                "rows_write_attempted": meta.get("rows_write_attempted"),
                "rows_written": result.get("rows_written"),
                "rows_failed": meta.get("rows_failed"),
                "first_sequence_seen": meta.get("first_sequence_seen"),
                "last_sequence_seen": meta.get("last_sequence_seen"),
                "cursor_before": meta.get("cursor_before"),
                "cursor_after": meta.get("cursor_after") or result.get("cursor"),
                "reason_for_zero_write": meta.get("reason_for_zero_write"),
                "checkpoint_state": meta.get("checkpoint_state"),
                "same_cursor_no_progress_count": no_progress["same_cursor_no_progress_count"],
                "stuck_detected": no_progress["stuck_detected"],
                "stuck_threshold": no_progress["stuck_threshold"],
                "outcome_reason": meta.get("outcome_reason"),
            },
        )
        if no_progress["stuck_detected"]:
            logger.warning(
                "zkill worker detected repeated no-progress cursor state",
                payload={
                    "event": "zkill.no_progress_detected",
                    "worker_id": identity,
                    "cursor_after": meta.get("cursor_after") or result.get("cursor"),
                    "same_cursor_no_progress_count": no_progress["same_cursor_no_progress_count"],
                    "stuck_threshold": no_progress["stuck_threshold"],
                    "reason_for_zero_write": meta.get("reason_for_zero_write"),
                },
            )
        if args.once:
            return 0
        sleep_for = max(3, args.poll_sleep if int(result.get("rows_written") or 0) == 0 else 3)
        time.sleep(sleep_for)


if __name__ == "__main__":
    raise SystemExit(main())
