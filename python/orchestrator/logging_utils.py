from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .json_utils import json_dumps_safe, make_json_safe


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname.lower(),
            "logger": record.name,
            "message": record.getMessage(),
        }
        extra = getattr(record, "payload", None)
        if isinstance(extra, dict):
            payload.update(make_json_safe(extra))
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json_dumps_safe(payload)


class LoggerAdapter(logging.LoggerAdapter):
    def process(self, msg: str, kwargs: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        payload = kwargs.pop("payload", {})
        extra = kwargs.setdefault("extra", {})
        existing = extra.get("payload", {})
        extra["payload"] = {**existing, **self.extra, **payload}
        return msg, kwargs


def configure_logging(
    *,
    logger_name: str = "supplycore.orchestrator",
    verbose: bool = False,
    log_file: Path | None = None,
    stdout_enabled: bool = True,
) -> LoggerAdapter:
    logger = logging.getLogger(logger_name)
    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        handler.close()
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)

    if stdout_enabled:
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(JsonFormatter())
        logger.addHandler(stream_handler)

    if log_file is not None:
        should_attach_file_handler = True
        try:
            stdout_target = Path(os.readlink("/proc/self/fd/1")).resolve()
            if stdout_target == log_file.resolve():
                should_attach_file_handler = False
        except OSError:
            should_attach_file_handler = True

        if should_attach_file_handler:
            try:
                log_file.parent.mkdir(parents=True, exist_ok=True)
                file_handler = logging.FileHandler(log_file, encoding="utf-8")
                file_handler.setFormatter(JsonFormatter())
                logger.addHandler(file_handler)
            except PermissionError:
                # Fall back gracefully — stdout handler is already attached,
                # so logs still reach journalctl / systemd output.
                pass

    logger.propagate = False
    return LoggerAdapter(logger, {})
