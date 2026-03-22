from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(slots=True)
class HealthResult:
    ok: bool
    exit_code: int
    payload: dict[str, Any]
    error: str | None = None


def run_php_healthcheck(php_binary: str, script_path: Path, cwd: Path) -> HealthResult:
    try:
        completed = subprocess.run(
            [php_binary, str(script_path)],
            cwd=str(cwd),
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError as exc:
        return HealthResult(ok=False, exit_code=255, payload={}, error=str(exc))

    stdout = completed.stdout.strip()
    payload: dict[str, Any] = {}
    if stdout:
        try:
            payload = json.loads(stdout)
        except json.JSONDecodeError:
            payload = {"raw_stdout": stdout}

    ok = completed.returncode == 0 and bool(payload.get("is_healthy"))
    return HealthResult(ok=ok, exit_code=completed.returncode, payload=payload, error=completed.stderr.strip() or None)
