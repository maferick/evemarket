#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
from pathlib import Path


def main() -> int:
    repo_root = Path(__file__).resolve().parents[1]
    python_root = repo_root / "python"
    if str(python_root) not in sys.path:
        sys.path.insert(0, str(python_root))

    from orchestrator.config import load_php_runtime_config
    from orchestrator.db import SupplyCoreDb
    from orchestrator.job_utils import acquire_job_lock, finish_job_run, release_job_lock, start_job_run
    from orchestrator.jobs.compute_graph_sync import run_compute_graph_sync

    config = load_php_runtime_config(repo_root)
    db = SupplyCoreDb(config.raw.get("db", {}))
    lock_owner = acquire_job_lock(db, "compute_graph_sync", ttl_seconds=600)
    if lock_owner is None:
        print(json.dumps({"status": "skipped", "reason": "lock_not_acquired"}, ensure_ascii=False))
        return 0

    run = start_job_run(db, "compute_graph_sync")
    try:
        result = run_compute_graph_sync(db, config.raw.get("neo4j", {}))
        status = str(result.get("status") or "success")
        finish_job_run(
            db,
            run,
            status="success" if status != "skipped" else "skipped",
            rows_processed=int(result.get("rows_processed") or 0),
            rows_written=int(result.get("rows_written") or 0),
            meta={"job_name": "compute_graph_sync"},
        )
        print(json.dumps({"status": "ok", **result}, ensure_ascii=False))
        return 0
    except Exception as error:  # noqa: BLE001
        finish_job_run(db, run, status="failed", rows_processed=0, rows_written=0, error_text=str(error), meta={"job_name": "compute_graph_sync"})
        print(json.dumps({"status": "failed", "error": str(error)}, ensure_ascii=False))
        return 1
    finally:
        release_job_lock(db, "compute_graph_sync", lock_owner)


if __name__ == "__main__":
    raise SystemExit(main())
