#!/usr/bin/env python3
"""Standalone CLI for ``compute_auto_buyall`` — the hourly safety-net.

The detector already chains into buyall via
``bin/python_compute_auto_doctrines.py``, so this wrapper exists purely so
that the scheduler can run buyall on its own interval if the detector is
idle.

**Serialization with compute_auto_doctrines.** ``_upsert_doctrines`` in
the detector rewrites ``auto_doctrine_modules`` per doctrine with a
DELETE+INSERT pair, each on its own autocommit connection. If
compute_auto_buyall reads modules for a doctrine during that window it
sees an empty list → zero demand → a degraded buy list gets written
and the /buy-all/ page renders blank. To prevent this we briefly
acquire the detector's lock before starting, which ensures we never
read mid-flight state. The acquire is immediately followed by a
release so we don't block a legitimate detector run that wants to
start right after us.
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path


def _wait_for_detector_idle(db, *, max_wait_seconds: int = 90, poll_interval: int = 2) -> int:
    """Poll the compute_auto_doctrines lock until it is free.

    Acquires the lock briefly to confirm it is idle, then releases it.
    Returns the number of seconds waited. Returns -1 if the timeout
    expired (caller should skip).
    """
    from orchestrator.job_utils import acquire_job_lock, release_job_lock

    elapsed = 0
    while elapsed < max_wait_seconds:
        probe = acquire_job_lock(db, "compute_auto_doctrines", ttl_seconds=5)
        if probe is not None:
            release_job_lock(db, "compute_auto_doctrines", probe)
            return elapsed
        time.sleep(poll_interval)
        elapsed += poll_interval
    return -1


def main() -> int:
    repo_root = Path(__file__).resolve().parents[1]
    python_root = repo_root / "python"
    if str(python_root) not in sys.path:
        sys.path.insert(0, str(python_root))

    from orchestrator.config import load_php_runtime_config
    from orchestrator.db import SupplyCoreDb
    from orchestrator.job_utils import acquire_job_lock, finish_job_run, release_job_lock, start_job_run
    from orchestrator.jobs.compute_auto_buyall import run_compute_auto_buyall

    config = load_php_runtime_config(repo_root)
    db = SupplyCoreDb(config.raw.get("db", {}))

    # Wait for compute_auto_doctrines to finish before reading its
    # output tables. Without this, we can read a doctrine mid-rewrite
    # and produce a degraded summary that the page will render as
    # "nothing to buy" until the next run.
    waited = _wait_for_detector_idle(db)
    if waited < 0:
        print(json.dumps({
            "status": "skipped",
            "reason": "compute_auto_doctrines_still_running",
            "waited_seconds": 90,
        }, ensure_ascii=False))
        return 0

    lock_owner = acquire_job_lock(db, "compute_auto_buyall", ttl_seconds=600)
    if lock_owner is None:
        print(json.dumps({"status": "skipped", "reason": "lock_not_acquired"}, ensure_ascii=False))
        return 0

    run = start_job_run(db, "compute_auto_buyall")
    try:
        result = run_compute_auto_buyall(db)
        finish_job_run(
            db,
            run,
            status="success",
            rows_processed=int(result.get("rows_processed") or 0),
            rows_written=int(result.get("rows_written") or 0),
            meta={"job_name": "compute_auto_buyall", "detector_wait_seconds": waited},
        )
        print(json.dumps({"job": "compute_auto_buyall", "detector_wait_seconds": waited, **result}, ensure_ascii=False, default=str))
        return 0
    except Exception as error:  # noqa: BLE001
        finish_job_run(
            db,
            run,
            status="failed",
            rows_processed=0,
            rows_written=0,
            error_text=str(error),
            meta={"job_name": "compute_auto_buyall"},
        )
        print(json.dumps({"status": "failed", "error": str(error)}, ensure_ascii=False))
        return 1
    finally:
        release_job_lock(db, "compute_auto_buyall", lock_owner)


if __name__ == "__main__":
    raise SystemExit(main())
