from __future__ import annotations

import json
import socket
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from .db import SupplyCoreDb


@dataclass(slots=True)
class JobRun:
    job_name: str
    run_id: str
    started_at: float


def _utc_now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")


def acquire_job_lock(db: SupplyCoreDb, lock_key: str, ttl_seconds: int = 300) -> str | None:
    owner = f"{socket.gethostname()}:{uuid.uuid4().hex}"
    ttl = max(30, min(3600, int(ttl_seconds)))
    rows = db.execute(
        """
        INSERT INTO compute_job_locks (lock_key, owner_key, acquired_at, expires_at)
        VALUES (%s, %s, UTC_TIMESTAMP(), DATE_ADD(UTC_TIMESTAMP(), INTERVAL %s SECOND))
        ON DUPLICATE KEY UPDATE
            owner_key = IF(expires_at <= UTC_TIMESTAMP(), VALUES(owner_key), owner_key),
            acquired_at = IF(expires_at <= UTC_TIMESTAMP(), VALUES(acquired_at), acquired_at),
            expires_at = IF(expires_at <= UTC_TIMESTAMP(), VALUES(expires_at), expires_at)
        """,
        (lock_key, owner, ttl),
    )
    if rows <= 0:
        return None
    row = db.fetch_one("SELECT owner_key, expires_at FROM compute_job_locks WHERE lock_key = %s LIMIT 1", (lock_key,))
    if not row or str(row.get("owner_key") or "") != owner:
        return None
    return owner


def refresh_job_lock(db: SupplyCoreDb, lock_key: str, owner_key: str, ttl_seconds: int = 300) -> None:
    ttl = max(30, min(3600, int(ttl_seconds)))
    db.execute(
        """
        UPDATE compute_job_locks
        SET expires_at = DATE_ADD(UTC_TIMESTAMP(), INTERVAL %s SECOND),
            updated_at = CURRENT_TIMESTAMP
        WHERE lock_key = %s AND owner_key = %s
        """,
        (ttl, lock_key, owner_key),
    )


def release_job_lock(db: SupplyCoreDb, lock_key: str, owner_key: str) -> None:
    db.execute("DELETE FROM compute_job_locks WHERE lock_key = %s AND owner_key = %s", (lock_key, owner_key))


def start_job_run(db: SupplyCoreDb, job_name: str) -> JobRun:
    run_id = f"{job_name}:{uuid.uuid4().hex}"
    db.execute(
        """
        INSERT INTO job_runs (job_name, run_key, status, started_at, meta_json)
        VALUES (%s, %s, 'running', UTC_TIMESTAMP(), %s)
        ON DUPLICATE KEY UPDATE started_at = UTC_TIMESTAMP(), status = 'running', error_text = NULL
        """,
        (job_name, run_id, json.dumps({"job_name": job_name}, separators=(",", ":"))),
    )
    return JobRun(job_name=job_name, run_id=run_id, started_at=time.time())


def finish_job_run(
    db: SupplyCoreDb,
    job: JobRun,
    *,
    status: str,
    rows_processed: int,
    rows_written: int,
    error_text: str | None = None,
    meta: dict[str, Any] | None = None,
) -> None:
    duration_ms = max(0, int((time.time() - job.started_at) * 1000))
    payload = {
        "job_name": job.job_name,
        "duration_ms": duration_ms,
        "rows_processed": max(0, int(rows_processed)),
        "rows_written": max(0, int(rows_written)),
        "errors": error_text or "",
        **(meta or {}),
    }
    print(json.dumps(payload, separators=(",", ":"), ensure_ascii=False))

    db.execute(
        """
        UPDATE job_runs
        SET status = %s,
            duration_ms = %s,
            rows_processed = %s,
            rows_written = %s,
            error_text = %s,
            meta_json = %s,
            finished_at = UTC_TIMESTAMP(),
            updated_at = CURRENT_TIMESTAMP
        WHERE run_key = %s
        LIMIT 1
        """,
        (
            status,
            duration_ms,
            max(0, int(rows_processed)),
            max(0, int(rows_written)),
            (error_text or "")[:500] if error_text else None,
            json.dumps(payload, separators=(",", ":"), ensure_ascii=False),
            job.run_id,
        ),
    )
