#!/usr/bin/env python3
"""CLI entry point for ``compute_auto_doctrines``.

Acquires the job lock, runs the detector, then chains
``compute_auto_buyall`` so the buy list refreshes automatically whenever
the doctrine set changes. Mirrors the lock/start/finish pattern used by
``bin/python_compute_buy_all.py``.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path


def _run_with_lock(db, job_name: str, run_fn, *, ttl_seconds: int, trigger: str | None = None) -> dict:
    from orchestrator.job_utils import acquire_job_lock, finish_job_run, release_job_lock, start_job_run

    lock_owner = acquire_job_lock(db, job_name, ttl_seconds=ttl_seconds)
    if lock_owner is None:
        return {"status": "skipped", "reason": "lock_not_acquired", "job": job_name}

    run = start_job_run(db, job_name)
    try:
        result = run_fn(db)
        finish_job_run(
            db,
            run,
            status="success",
            rows_processed=int(result.get("rows_processed") or 0),
            rows_written=int(result.get("rows_written") or 0),
            meta={"job_name": job_name, **({"trigger": trigger} if trigger else {})},
        )
        return {"job": job_name, **result}
    except Exception as error:  # noqa: BLE001
        finish_job_run(
            db,
            run,
            status="failed",
            rows_processed=0,
            rows_written=0,
            error_text=str(error),
            meta={"job_name": job_name, **({"trigger": trigger} if trigger else {})},
        )
        raise
    finally:
        release_job_lock(db, job_name, lock_owner)


def main() -> int:
    repo_root = Path(__file__).resolve().parents[1]
    python_root = repo_root / "python"
    if str(python_root) not in sys.path:
        sys.path.insert(0, str(python_root))

    from orchestrator.config import load_php_runtime_config
    from orchestrator.db import SupplyCoreDb
    from orchestrator.jobs.compute_auto_buyall import run_compute_auto_buyall
    from orchestrator.jobs.compute_auto_doctrines import run_compute_auto_doctrines

    config = load_php_runtime_config(repo_root)
    db = SupplyCoreDb(config.raw.get("db", {}))

    try:
        detector_result = _run_with_lock(
            db, "compute_auto_doctrines", run_compute_auto_doctrines, ttl_seconds=600,
        )
        print(json.dumps(detector_result, ensure_ascii=False, default=str))
    except Exception as error:  # noqa: BLE001
        print(json.dumps({"status": "failed", "error": str(error)}, ensure_ascii=False))
        return 1

    if detector_result.get("status") == "skipped":
        return 0

    try:
        buyall_result = _run_with_lock(
            db, "compute_auto_buyall", run_compute_auto_buyall,
            ttl_seconds=600, trigger="compute_auto_doctrines",
        )
        print(json.dumps(buyall_result, ensure_ascii=False, default=str))
    except Exception as error:  # noqa: BLE001
        print(json.dumps({"status": "failed", "job": "compute_auto_buyall", "error": str(error)}, ensure_ascii=False))
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
