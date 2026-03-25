from __future__ import annotations

import json
import os
import shlex
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(slots=True)
class OrchestratorConfig:
    raw: dict[str, Any]

    @property
    def php_binary(self) -> str:
        return str(self.raw.get("paths", {}).get("php_binary", "php"))

    @property
    def scheduler_daemon(self) -> Path:
        return Path(self.raw.get("paths", {}).get("scheduler_daemon", self.app_root / "bin/scheduler_daemon.php"))

    @property
    def scheduler_health(self) -> Path:
        return Path(self.raw.get("paths", {}).get("scheduler_health", self.app_root / "bin/scheduler_health.php"))

    @property
    def scheduler_watchdog(self) -> Path:
        return Path(self.raw.get("paths", {}).get("scheduler_watchdog", self.app_root / "bin/scheduler_watchdog.php"))

    @property
    def state_dir(self) -> Path:
        return Path(self.raw.get("orchestrator", {}).get("state_dir", self.app_root / "storage/run"))

    @property
    def heartbeat_file(self) -> Path:
        return Path(self.raw.get("orchestrator", {}).get("heartbeat_file", self.app_root / "storage/run/orchestrator-heartbeat.json"))

    @property
    def lock_file(self) -> Path:
        return Path(self.raw.get("orchestrator", {}).get("lock_file", self.app_root / "storage/run/orchestrator.lock"))

    @property
    def log_file(self) -> Path:
        return Path(self.raw.get("paths", {}).get("log_file", self.app_root / "storage/logs/orchestrator.log"))

    @property
    def worker_grace_seconds(self) -> int:
        return int(self.raw.get("orchestrator", {}).get("worker_grace_seconds", 45))

    @property
    def health_check_interval_seconds(self) -> int:
        return int(self.raw.get("orchestrator", {}).get("health_check_interval_seconds", 15))

    @property
    def worker_start_backoff_seconds(self) -> int:
        return int(self.raw.get("orchestrator", {}).get("worker_start_backoff_seconds", 5))

    @property
    def max_consecutive_health_failures(self) -> int:
        return int(self.raw.get("orchestrator", {}).get("max_consecutive_health_failures", 3))

    @property
    def watchdog_grace_seconds(self) -> int:
        return int(self.raw.get("scheduler", {}).get("watchdog_grace_seconds", 60))

    @property
    def supervisor_mode(self) -> str:
        return str(self.raw.get("scheduler", {}).get("supervisor_mode", "python"))

    @property
    def app_root(self) -> Path:
        return Path(self.raw["paths"]["app_root"]).resolve()


def _env_int(key: str, default: int, minimum: int | None = None) -> int:
    try:
        value = int(os.getenv(key, str(default)).strip())
    except Exception:
        value = default
    if minimum is not None:
        value = max(minimum, value)
    return value


def _load_dotenv_defaults(app_root: Path) -> None:
    env_path = app_root / ".env"
    if not env_path.is_file():
        return

    try:
        lines = env_path.read_text(encoding="utf-8").splitlines()
    except Exception:
        return

    for raw_line in lines:
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].strip()
        if "=" not in line:
            continue

        key, raw_value = line.split("=", 1)
        key = key.strip()
        if not key:
            continue

        value = raw_value.strip()
        if not value:
            os.environ.setdefault(key, "")
            continue

        try:
            parsed = shlex.split(value, comments=True, posix=True)
        except ValueError:
            parsed = [value]
        os.environ.setdefault(key, parsed[0] if parsed else "")


def _load_live_php_runtime_config(app_root: Path) -> dict[str, Any]:
    script_path = app_root / "bin/orchestrator_config.php"
    if not script_path.is_file():
        return {}

    php_binary = os.getenv("PHP_BINARY", "php").strip() or "php"
    try:
        completed = subprocess.run(
            [php_binary, str(script_path)],
            cwd=str(app_root),
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except Exception:
        return {}

    payload = (completed.stdout or "").strip()
    if payload == "":
        return {}

    try:
        decoded = json.loads(payload)
    except Exception:
        return {}

    return decoded if isinstance(decoded, dict) else {}


def load_php_runtime_config(app_root: Path) -> OrchestratorConfig:
    app_root = app_root.resolve()
    _load_dotenv_defaults(app_root)
    runtime_file = Path(os.getenv("SUPPLYCORE_ORCHESTRATOR_CONFIG_JSON", app_root / "storage/run/orchestrator-runtime.json"))
    raw: dict[str, Any] = {}
    if runtime_file.is_file():
        try:
            raw = json.loads(runtime_file.read_text(encoding="utf-8"))
        except Exception:
            raw = {}
    if not isinstance(raw, dict):
        raw = {}

    live_raw = _load_live_php_runtime_config(app_root)

    defaults = {
        "app": {"name": "SupplyCore", "env": os.getenv("APP_ENV", "development"), "timezone": os.getenv("APP_TIMEZONE", "UTC")},
        "db": {
            "host": os.getenv("DB_HOST", "127.0.0.1"),
            "port": _env_int("DB_PORT", 3306),
            "database": os.getenv("DB_DATABASE", "supplycore"),
            "username": os.getenv("DB_USERNAME", "supplycore"),
            "password": os.getenv("DB_PASSWORD", ""),
            "charset": os.getenv("DB_CHARSET", "utf8mb4"),
            "socket": os.getenv("DB_SOCKET", ""),
        },
        "neo4j": {
            "enabled": os.getenv("NEO4J_ENABLED", "0") == "1",
            "url": os.getenv("NEO4J_URL", "http://127.0.0.1:7474").rstrip("/"),
            "username": os.getenv("NEO4J_USERNAME", "neo4j"),
            "password": os.getenv("NEO4J_PASSWORD", ""),
            "database": os.getenv("NEO4J_DATABASE", "neo4j"),
            "timeout_seconds": _env_int("NEO4J_TIMEOUT_SECONDS", 15, 3),
        },
        "influx": {
            "enabled": os.getenv("INFLUXDB_ENABLED", "0") == "1",
            "read_enabled": os.getenv("INFLUXDB_READ_ENABLED", "0") == "1",
            "url": os.getenv("INFLUXDB_URL", "http://127.0.0.1:8086").rstrip("/"),
            "org": os.getenv("INFLUXDB_ORG", ""),
            "bucket": os.getenv("INFLUXDB_BUCKET", "supplycore_rollups"),
            "token": os.getenv("INFLUXDB_TOKEN", ""),
            "timeout_seconds": _env_int("INFLUXDB_TIMEOUT_SECONDS", 15, 3),
        },
        "battle_intelligence": {"log_file": str(app_root / "storage/logs/battle-intelligence.log")},
        "paths": {"app_root": str(app_root), "log_file": str(app_root / "storage/logs/orchestrator.log"), "php_binary": "php", "scheduler_daemon": str(app_root / "bin/scheduler_daemon.php"), "scheduler_watchdog": str(app_root / "bin/scheduler_watchdog.php"), "scheduler_health": str(app_root / "bin/scheduler_health.php")},
        "orchestrator": {"state_dir": str(app_root / "storage/run"), "heartbeat_file": str(app_root / "storage/run/orchestrator-heartbeat.json"), "lock_file": str(app_root / "storage/run/orchestrator.lock"), "health_check_interval_seconds": 15, "worker_grace_seconds": 45, "worker_start_backoff_seconds": 5, "max_consecutive_health_failures": 3},
        "scheduler": {"watchdog_grace_seconds": 60, "supervisor_mode": "python"},
        "workers": {
            "claim_ttl_seconds": _env_int("SUPPLYCORE_WORKER_CLAIM_TTL_SECONDS", 300, 30),
            "idle_sleep_seconds": _env_int("SUPPLYCORE_WORKER_IDLE_SLEEP_SECONDS", 10, 0),
            "sync_idle_sleep_seconds": _env_int("SUPPLYCORE_SYNC_IDLE_SLEEP_SECONDS", 8, 0),
            "compute_idle_sleep_seconds": _env_int("SUPPLYCORE_COMPUTE_IDLE_SLEEP_SECONDS", 15, 0),
            "memory_pause_threshold_bytes": _env_int("SUPPLYCORE_WORKER_MEMORY_PAUSE_THRESHOLD_BYTES", 402653184, 134217728),
            "memory_abort_threshold_bytes": _env_int("SUPPLYCORE_WORKER_MEMORY_ABORT_THRESHOLD_BYTES", 536870912, 268435456),
            "retry_backoff_seconds": _env_int("SUPPLYCORE_WORKER_RETRY_BACKOFF_SECONDS", 30, 5),
            "worker_log_file": str(app_root / "storage/logs/worker.log"),
            "compute_log_file": str(app_root / "storage/logs/compute.log"),
            "pool_state_file": str(app_root / "storage/run/worker-pool-heartbeat.json"),
            "zkill_log_file": str(app_root / "storage/logs/zkill.log"),
            "zkill_state_file": str(app_root / "storage/run/zkill-heartbeat.json"),
        },
    }

    merged = defaults | raw | live_raw
    for key in ("app", "db", "neo4j", "influx", "battle_intelligence", "paths", "orchestrator", "scheduler", "workers"):
        merged[key] = {
            **defaults.get(key, {}),
            **(raw.get(key, {}) if isinstance(raw.get(key), dict) else {}),
            **(live_raw.get(key, {}) if isinstance(live_raw.get(key), dict) else {}),
        }
    return OrchestratorConfig(raw=merged)
