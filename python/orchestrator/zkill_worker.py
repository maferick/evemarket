from __future__ import annotations

import argparse
import json
import os
import socket
import time
from pathlib import Path

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


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    app_root = Path(args.app_root).resolve()
    runtime = load_php_runtime_config(app_root)
    worker_settings = runtime.raw.get("workers", {}) if isinstance(runtime.raw, dict) else {}
    log_file = Path(worker_settings.get("zkill_log_file", app_root / "storage/logs/zkill.log"))
    logger = configure_logging(verbose=args.verbose, log_file=log_file)
    state_file = Path(worker_settings.get("zkill_state_file", app_root / "storage/run/zkill-heartbeat.json"))
    pause_threshold = max(128 * 1024 * 1024, int(worker_settings.get("memory_pause_threshold_bytes", 384 * 1024 * 1024)))
    abort_threshold = max(pause_threshold, int(worker_settings.get("memory_abort_threshold_bytes", 512 * 1024 * 1024)))
    identity = f"{socket.gethostname()}-{os.getpid()}"

    logger.info("zkill worker started", payload={"event": "zkill.started", "worker_id": identity})

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
        result = run_killmail_r2z2_stream(context)
        _write_state_file(
            state_file,
            {
                "ts": utc_now_iso(),
                "worker_id": identity,
                "result": result,
                "memory_usage_bytes": resident_memory_bytes(),
            },
        )
        logger.info(
            "zkill loop completed",
            payload={
                "event": "zkill.loop_completed",
                "worker_id": identity,
                "status": result.get("status"),
                "rows_seen": result.get("rows_seen"),
                "rows_written": result.get("rows_written"),
                "cursor": result.get("cursor"),
            },
        )
        if args.once:
            return 0
        sleep_for = max(3, args.poll_sleep if int(result.get("rows_written") or 0) == 0 else 3)
        time.sleep(sleep_for)


if __name__ == "__main__":
    raise SystemExit(main())
