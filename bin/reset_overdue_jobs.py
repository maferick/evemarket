#!/usr/bin/env python3
"""Reset overdue job schedules so the loop runner picks them up immediately.

Usage:
    python bin/reset_overdue_jobs.py [--app-root /var/www/SupplyCore] [--dry-run]

What it does:
  1. Finds all sync_schedules rows whose next_due_at is in the past (overdue).
  2. Resets their next_due_at to UTC_TIMESTAMP() so the loop runner treats
     them as immediately due on the next cycle.
  3. Reaps stale running/queued worker_jobs rows left behind by crashed
     processes.

Run this after deploying scheduler fixes, then restart the lane services:
    systemctl restart supplycore-lane-compute supplycore-lane-realtime \
                      supplycore-lane-ingestion supplycore-lane-maintenance
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description="Reset overdue job schedules")
    parser.add_argument("--app-root", default=None,
                        help="Path to SupplyCore root (default: auto-detect)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be reset without making changes")
    parser.add_argument("--job-key", action="append", default=[],
                        help="Only reset specific job(s) by key (repeatable)")
    args = parser.parse_args()

    repo_root = Path(args.app_root).resolve() if args.app_root else Path(__file__).resolve().parents[1]
    python_root = repo_root / "python"
    if str(python_root) not in sys.path:
        sys.path.insert(0, str(python_root))

    from orchestrator.config import load_php_runtime_config
    from orchestrator.db import SupplyCoreDb

    raw_config = load_php_runtime_config(repo_root).raw
    db = SupplyCoreDb(dict(raw_config.get("db") or {}))

    # ── Step 1: Show overdue jobs ────────────────────────────────────────
    overdue_rows = db.fetch_all(
        """SELECT job_key,
                  next_due_at,
                  interval_seconds,
                  TIMESTAMPDIFF(SECOND, next_due_at, UTC_TIMESTAMP()) AS overdue_seconds
             FROM sync_schedules
            WHERE enabled = 1
              AND execution_mode = 'python'
              AND next_due_at IS NOT NULL
              AND next_due_at < UTC_TIMESTAMP()
            ORDER BY overdue_seconds DESC"""
    )

    if args.job_key:
        filter_keys = set(args.job_key)
        overdue_rows = [r for r in overdue_rows if r["job_key"] in filter_keys]

    if not overdue_rows:
        print("No overdue jobs found." + (" (filtered)" if args.job_key else ""))
        return 0

    print(f"Found {len(overdue_rows)} overdue job(s):\n")
    print(f"  {'Job Key':<45s} {'Overdue':>10s}  {'Interval':>10s}  Next Due At")
    print(f"  {'─' * 45} {'─' * 10}  {'─' * 10}  {'─' * 19}")
    for row in overdue_rows:
        overdue_s = int(row["overdue_seconds"] or 0)
        interval_s = int(row["interval_seconds"] or 0)
        if overdue_s >= 3600:
            overdue_str = f"{overdue_s / 3600:.1f}h"
        elif overdue_s >= 60:
            overdue_str = f"{overdue_s / 60:.0f}m"
        else:
            overdue_str = f"{overdue_s}s"
        interval_str = f"{interval_s / 60:.0f}m" if interval_s >= 60 else f"{interval_s}s"
        print(f"  {row['job_key']:<45s} {overdue_str:>10s}  {interval_str:>10s}  {row['next_due_at']}")

    if args.dry_run:
        print(f"\n[dry-run] Would reset {len(overdue_rows)} job(s). Pass without --dry-run to apply.")
        return 0

    # ── Step 2: Reset next_due_at for overdue jobs ───────────────────────
    if args.job_key:
        placeholders = ", ".join(["%s"] * len(overdue_rows))
        keys = tuple(r["job_key"] for r in overdue_rows)
        affected = db.execute(
            f"""UPDATE sync_schedules
                   SET next_due_at = UTC_TIMESTAMP()
                 WHERE enabled = 1
                   AND execution_mode = 'python'
                   AND next_due_at < UTC_TIMESTAMP()
                   AND job_key IN ({placeholders})""",
            keys,
        )
    else:
        affected = db.execute(
            """UPDATE sync_schedules
                  SET next_due_at = UTC_TIMESTAMP()
                WHERE enabled = 1
                  AND execution_mode = 'python'
                  AND next_due_at IS NOT NULL
                  AND next_due_at < UTC_TIMESTAMP()"""
        )
    print(f"\nReset next_due_at for {affected} job(s).")

    # ── Step 3: Reap stale running/queued worker_jobs ────────────────────
    reaped_running = db.reap_stale_running_jobs()
    reaped_queued = db.reap_stale_queued_jobs()
    if reaped_running or reaped_queued:
        print(f"Reaped {reaped_running} stale running + {reaped_queued} stale queued worker_jobs rows.")

    print("\nDone. Restart the lane services to pick up the reset schedules:")
    print("  systemctl restart supplycore-lane-compute supplycore-lane-realtime \\")
    print("                    supplycore-lane-ingestion supplycore-lane-maintenance")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
