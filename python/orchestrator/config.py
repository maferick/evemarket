from __future__ import annotations

import json
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


def load_php_runtime_config(app_root: Path) -> OrchestratorConfig:
    bridge = app_root / "bin" / "orchestrator_config.php"
    completed = subprocess.run(
        ["php", str(bridge)],
        cwd=str(app_root),
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(completed.stdout)
    return OrchestratorConfig(raw=payload)
