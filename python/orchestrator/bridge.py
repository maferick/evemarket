from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any


class PhpBridge:
    def __init__(self, php_binary: str, app_root: Path):
        self.php_binary = php_binary
        self.script_path = app_root / "bin" / "python_scheduler_bridge.php"
        self.app_root = app_root

    def call(self, action: str, *, args: list[str] | None = None, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        command = [self.php_binary, str(self.script_path), f"--action={action}", *(args or [])]
        completed = subprocess.run(
            command,
            cwd=str(self.app_root),
            input=(json.dumps(payload) if payload is not None else None),
            capture_output=True,
            text=True,
            check=False,
        )
        if completed.returncode != 0:
            detail = completed.stderr.strip() or completed.stdout.strip() or "PHP bridge failed without output."
            raise RuntimeError(detail)
        response = json.loads(completed.stdout)
        if not isinstance(response, dict) or not response.get("ok", False):
            raise RuntimeError(str(response.get("error", "PHP bridge returned an invalid response.")))
        return response
