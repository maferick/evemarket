"""CIP Incident Capture — structured failure recording for diagnostics.

Wraps CIP job execution to capture full error context on failure:
  - Exception type and message
  - Full traceback
  - SQL query if available (from OperationalError args)
  - Structured context (rows processed, calibration state, etc.)
  - Hostname and git SHA

Usage in processor_registry.py:
    Instead of: (run_cip_fusion, lambda db, cfg: (db,))
    Use:        (wrap_cip_job("cip_fusion", run_cip_fusion), lambda db, cfg: (db,))
"""

from __future__ import annotations

import logging
import os
import socket
import traceback
from functools import wraps
from typing import Any, Callable

from ..json_utils import json_dumps_safe

logger = logging.getLogger(__name__)

# Try to read git SHA from common locations
_GIT_SHA: str | None = None
for _sha_path in ["/var/www/SupplyCore/.git-sha", "/var/www/SupplyCore/REVISION"]:
    try:
        with open(_sha_path) as f:
            _GIT_SHA = f.read().strip()[:50]
            break
    except OSError:
        pass
if _GIT_SHA is None:
    try:
        import subprocess
        _GIT_SHA = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd="/var/www/SupplyCore",
            stderr=subprocess.DEVNULL,
            timeout=5,
        ).decode().strip()[:50]
    except Exception:
        pass


def _extract_sql_from_error(exc: Exception) -> str | None:
    """Try to extract the SQL query from a database error."""
    # pymysql OperationalError often has the query in args
    if hasattr(exc, 'args') and len(exc.args) >= 2:
        msg = str(exc.args[1]) if len(exc.args) > 1 else str(exc.args[0])
        if 'SQL' in msg or 'query' in msg.lower():
            return msg
    # Check for __cause__ chain
    cause = exc.__cause__
    if cause and hasattr(cause, 'args'):
        return str(cause.args)
    return None


def record_incident(
    db: Any,
    job_key: str,
    exc: Exception,
    context: dict[str, Any] | None = None,
) -> None:
    """Record a CIP job failure to the incident log.

    Best-effort: if the INSERT itself fails (e.g. DB down), we log and move on.
    """
    try:
        tb = traceback.format_exception(type(exc), exc, exc.__traceback__)
        tb_text = "".join(tb)
        error_type = type(exc).__name__
        error_message = str(exc)
        sql_query = _extract_sql_from_error(exc)
        context_json = json_dumps_safe(context) if context else None
        hostname = socket.gethostname()

        db.execute(
            """INSERT INTO cip_incident_log
                   (job_key, error_type, error_message, traceback,
                    sql_query, context_json, hostname, git_sha)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
            [
                job_key, error_type, error_message, tb_text,
                sql_query, context_json, hostname, _GIT_SHA,
            ],
        )
        logger.info("CIP incident recorded for %s: %s", job_key, error_type)
    except Exception as log_exc:
        # If we can't write to the incident log, don't mask the original error
        logger.warning(
            "Failed to record CIP incident for %s: %s",
            job_key, str(log_exc),
        )


def wrap_cip_job(job_key: str, fn: Callable) -> Callable:
    """Decorator that wraps a CIP job to capture failures to the incident log.

    The wrapped function still raises the original exception so the worker
    pool handles it normally — this just adds structured capture.
    """
    @wraps(fn)
    def wrapper(db: Any, *args: Any, **kwargs: Any) -> Any:
        try:
            return fn(db, *args, **kwargs)
        except Exception as exc:
            # Capture the incident
            record_incident(db, job_key, exc, context={
                "args_count": len(args),
                "kwargs_keys": list(kwargs.keys()),
            })
            # Re-raise so the worker pool handles it normally
            raise

    return wrapper
