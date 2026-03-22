from __future__ import annotations

import json
import os
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(slots=True)
class OrchestratorConfig:
    raw: dict[str, Any]

    @property
    def app_root(self) -> Path:
        return Path(self.raw["paths"]["app_root"]).resolve()

    @property
    def php_binary(self) -> str:
        return str(self.raw["paths"]["php_binary"])

    @property
    def scheduler_daemon(self) -> Path:
        return Path(self.raw["paths"]["scheduler_daemon"]).resolve()

    @property
    def scheduler_health(self) -> Path:
        return Path(self.raw["paths"]["scheduler_health"]).resolve()

    @property
    def scheduler_watchdog(self) -> Path:
        return Path(self.raw["paths"]["scheduler_watchdog"]).resolve()

    @property
    def state_dir(self) -> Path:
        return Path(self.raw["orchestrator"]["state_dir"]).resolve()

    @property
    def heartbeat_file(self) -> Path:
        return Path(self.raw["orchestrator"]["heartbeat_file"]).resolve()

    @property
    def lock_file(self) -> Path:
        return Path(self.raw["orchestrator"]["lock_file"]).resolve()

    @property
    def worker_grace_seconds(self) -> int:
        return int(self.raw["orchestrator"]["worker_grace_seconds"])

    @property
    def health_check_interval_seconds(self) -> int:
        return int(self.raw["orchestrator"]["health_check_interval_seconds"])

    @property
    def worker_start_backoff_seconds(self) -> int:
        return int(self.raw["orchestrator"]["worker_start_backoff_seconds"])

    @property
    def max_consecutive_health_failures(self) -> int:
        return int(self.raw["orchestrator"]["max_consecutive_health_failures"])

    @property
    def watchdog_grace_seconds(self) -> int:
        return int(self.raw["scheduler"]["watchdog_grace_seconds"])

    @property
    def supervisor_mode(self) -> str:
        return str(self.raw["scheduler"]["supervisor_mode"])


def _php_binary_candidates() -> list[str]:
    candidates: list[str] = []

    for env_key in ("SUPPLYCORE_PHP_BINARY", "ORCHESTRATOR_PHP_BINARY", "PHP_BINARY"):
        env_value = os.environ.get(env_key, "").strip()
        if env_value != "":
            candidates.append(env_value)

    candidates.extend([
        "php8.4",
        "php8.3",
        "php8.2",
        "php8.1",
        "php8.0",
        "php",
    ])

    seen: set[str] = set()
    ordered: list[str] = []
    for candidate in candidates:
        if candidate in seen:
            continue
        seen.add(candidate)
        ordered.append(candidate)

    return ordered


def _resolve_php_binary() -> str:
    version_probe = 'echo PHP_MAJOR_VERSION, ".", PHP_MINOR_VERSION;'
    fallback_binary: str | None = None

    for candidate in _php_binary_candidates():
        resolved = shutil.which(candidate) if os.path.sep not in candidate else candidate
        if not resolved or not Path(resolved).exists():
            continue

        if fallback_binary is None:
            fallback_binary = resolved

        completed = subprocess.run(
            [resolved, "-r", version_probe],
            check=False,
            capture_output=True,
            text=True,
        )
        if completed.returncode != 0:
            continue

        version = completed.stdout.strip()
        if version != "" and tuple(int(part) for part in version.split(".", 1)) >= (8, 0):
            return resolved

    if fallback_binary is not None:
        return fallback_binary

    return "php"


def load_php_runtime_config(app_root: Path) -> OrchestratorConfig:
    bridge = app_root / "bin" / "orchestrator_config.php"
    php_binary = _resolve_php_binary()

    try:
        completed = subprocess.run(
            [php_binary, str(bridge)],
            cwd=str(app_root),
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as error:
        stderr = (error.stderr or "").strip()
        stdout = (error.stdout or "").strip()
        details = stderr if stderr != "" else stdout
        if details == "":
            details = "PHP bridge exited without diagnostic output."
        raise RuntimeError(
            f"Failed to load orchestrator PHP runtime config via {php_binary}: {details}"
        ) from error

    payload = json.loads(completed.stdout)
    return OrchestratorConfig(raw=payload)
