from __future__ import annotations

import fcntl
import json
import os
import signal
import socket
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TextIO

from .config import load_php_runtime_config
from .health import HealthResult, run_php_healthcheck
from .json_utils import json_dumps_safe
from .logging_utils import LoggerAdapter
from .php_runner import ManagedPhpProcess


@dataclass(slots=True)
class SupervisorState:
    stop_requested: bool = False
    restart_requested: bool = False
    consecutive_health_failures: int = 0
    last_health: dict[str, Any] | None = None
    last_worker_exit_code: int | None = None
    last_worker_exit_at: str | None = None


class SupplyCoreSupervisor:
    def __init__(self, app_root: Path, logger: LoggerAdapter) -> None:
        self.logger = logger
        self.app_root = app_root.resolve()
        self.config = load_php_runtime_config(self.app_root)
        self.state = SupervisorState()
        self.lock_handle: TextIO | None = None
        self.worker: ManagedPhpProcess | None = None
        self._install_signal_handlers()

    def _install_signal_handlers(self) -> None:
        signal.signal(signal.SIGTERM, self._handle_stop)
        signal.signal(signal.SIGINT, self._handle_stop)
        signal.signal(signal.SIGHUP, self._handle_restart)

    def _handle_stop(self, signum: int, frame: Any) -> None:
        self.state.stop_requested = True
        self.logger.info("stop requested", payload={"event": "orchestrator.signal", "signal": signum})

    def _handle_restart(self, signum: int, frame: Any) -> None:
        self.state.restart_requested = True
        self.logger.info("restart requested", payload={"event": "orchestrator.signal", "signal": signum})

    def acquire_lock(self) -> None:
        self.config.state_dir.mkdir(parents=True, exist_ok=True)
        self.lock_handle = self.config.lock_file.open("a+")
        try:
            fcntl.flock(self.lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError(f"lock already held: {self.config.lock_file}") from exc

        self.lock_handle.seek(0)
        self.lock_handle.truncate(0)
        self.lock_handle.write(f"{os.getpid()}\n")
        self.lock_handle.flush()

    def _build_worker(self) -> ManagedPhpProcess:
        child_env = {
            "SUPPLYCORE_PYTHON_SUPERVISOR": "1",
            "SCHEDULER_SUPERVISOR_MODE": "python",
            "APP_ENV": str(self.config.raw["app"]["env"]),
            "APP_TIMEZONE": str(self.config.raw["app"]["timezone"]),
        }
        return ManagedPhpProcess(
            php_binary=self.config.php_binary,
            script_path=self.config.scheduler_daemon,
            cwd=self.config.app_root,
            env=child_env,
            logger=self.logger,
        )

    def start_worker(self) -> None:
        if self.worker is None:
            self.worker = self._build_worker()
        self.worker.start()
        self.state.consecutive_health_failures = 0

    def stop_worker(self) -> int | None:
        if self.worker is None:
            return None
        code = self.worker.terminate(self.config.worker_grace_seconds)
        self.worker.flush_output()
        self.state.last_worker_exit_code = code
        self.state.last_worker_exit_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        return code

    def refresh_config(self) -> None:
        self.config = load_php_runtime_config(self.app_root)

    def write_heartbeat(self, worker_status: dict[str, Any], health: HealthResult | None) -> None:
        self.config.state_dir.mkdir(parents=True, exist_ok=True)
        payload = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "pid": os.getpid(),
            "hostname": socket.gethostname(),
            "supervisor_mode": self.config.supervisor_mode,
            "worker": worker_status,
            "health": None if health is None else {
                "ok": health.ok,
                "exit_code": health.exit_code,
                "payload": health.payload,
                "error": health.error,
            },
            "consecutive_health_failures": self.state.consecutive_health_failures,
            "restart_requested": self.state.restart_requested,
            "stop_requested": self.state.stop_requested,
            "last_worker_exit_code": self.state.last_worker_exit_code,
            "last_worker_exit_at": self.state.last_worker_exit_at,
        }
        self.config.heartbeat_file.write_text(json_dumps_safe(payload, indent=2) + "\n", encoding="utf-8")

    def evaluate_health(self) -> HealthResult:
        return run_php_healthcheck(self.config.php_binary, self.config.scheduler_health, self.config.app_root)

    def run(self) -> int:
        self.acquire_lock()
        self.logger.info(
            "orchestrator booted",
            payload={
                "event": "orchestrator.started",
                "app_root": str(self.config.app_root),
                "lock_file": str(self.config.lock_file),
                "heartbeat_file": str(self.config.heartbeat_file),
            },
        )

        next_healthcheck_at = 0.0
        last_health: HealthResult | None = None

        while not self.state.stop_requested:
            if self.state.restart_requested:
                self.logger.info("reloading PHP-derived runtime config", payload={"event": "orchestrator.reload"})
                self.stop_worker()
                self.refresh_config()
                self.worker = self._build_worker()
                self.state.restart_requested = False

            if self.worker is None or self.worker.poll() is not None:
                if self.worker is not None and self.worker.poll() is not None:
                    exit_code = self.worker.poll()
                    self.worker.flush_output()
                    self.state.last_worker_exit_code = exit_code
                    self.state.last_worker_exit_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                    self.logger.warning(
                        "managed PHP worker exited",
                        payload={"event": "worker.exited", "exit_code": exit_code, "pid": self.worker.pid},
                    )
                self.worker = self._build_worker()
                self.start_worker()
                next_healthcheck_at = 0.0

            assert self.worker is not None
            self.worker.flush_output()
            now = time.time()
            if now >= next_healthcheck_at:
                last_health = self.evaluate_health()
                self.state.last_health = last_health.payload
                if last_health.ok:
                    self.state.consecutive_health_failures = 0
                    self.logger.info(
                        "healthcheck passed",
                        payload={"event": "worker.health.ok", "health": last_health.payload},
                    )
                else:
                    self.state.consecutive_health_failures += 1
                    self.logger.warning(
                        "healthcheck reported degraded scheduler state",
                        payload={
                            "event": "worker.health.failed",
                            "failures": self.state.consecutive_health_failures,
                            "exit_code": last_health.exit_code,
                            "health": last_health.payload,
                            "error": last_health.error,
                        },
                    )
                    if self.state.consecutive_health_failures >= self.config.max_consecutive_health_failures:
                        self.logger.warning(
                            "restarting PHP worker after repeated health failures",
                            payload={"event": "worker.restart.health_threshold"},
                        )
                        self.stop_worker()
                        self.worker = None
                        time.sleep(self.config.worker_start_backoff_seconds)
                        continue
                next_healthcheck_at = now + self.config.health_check_interval_seconds

            worker_status = {
                "pid": self.worker.pid,
                "started_at_unix": self.worker.started_at,
                "runtime_seconds": self.worker.status().runtime_seconds,
                "exit_code": self.worker.poll(),
                "script": str(self.config.scheduler_daemon),
            }
            self.write_heartbeat(worker_status, last_health)
            time.sleep(1)

        code = self.stop_worker() or 0
        self.logger.info("orchestrator stopped", payload={"event": "orchestrator.stopped", "exit_code": code})
        return code


def run_supervisor(app_root: Path, logger: LoggerAdapter) -> int:
    supervisor = SupplyCoreSupervisor(app_root=app_root, logger=logger)
    return supervisor.run()
