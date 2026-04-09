from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any

from .json_utils import make_json_safe


class PhpBridge:
    def __init__(self, php_binary: str, app_root: Path):
        self.php_binary = php_binary
        self.script_path = app_root / "bin" / "python_scheduler_bridge.php"
        self.app_root = app_root

    #: Default subprocess timeout (seconds).  Generous enough for any normal
    #: bridge action but prevents indefinite hangs when the PHP process blocks
    #: on a database lock or external call.
    DEFAULT_TIMEOUT_SECONDS = 120

    def call(
        self,
        action: str,
        *,
        args: list[str] | None = None,
        payload: dict[str, Any] | None = None,
        timeout: int | None = None,
    ) -> dict[str, Any]:
        effective_timeout = timeout if timeout is not None else self.DEFAULT_TIMEOUT_SECONDS
        command = [self.php_binary, str(self.script_path), f"--action={action}", *(args or [])]
        try:
            completed = subprocess.run(
                command,
                cwd=str(self.app_root),
                input=(json.dumps(make_json_safe(payload)) if payload is not None else None),
                capture_output=True,
                text=True,
                check=False,
                timeout=effective_timeout,
            )
        except subprocess.TimeoutExpired as exc:
            raise RuntimeError(
                f"PHP bridge action '{action}' timed out after {effective_timeout}s"
            ) from exc
        if completed.returncode != 0:
            detail = completed.stderr.strip() or completed.stdout.strip() or "PHP bridge failed without output."
            raise RuntimeError(detail)
        response = json.loads(completed.stdout)
        if not isinstance(response, dict) or not response.get("ok", False):
            raise RuntimeError(str(response.get("error", "PHP bridge returned an invalid response.")))
        return response
