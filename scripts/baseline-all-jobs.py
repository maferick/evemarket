#!/usr/bin/env python3
"""Run every registered job in dependency order and report time + memory.

Outputs a TSV report to stdout and a JSON file to storage/logs/baseline-report.json.

Usage:
    # From the project root (or on the server):
    .venv-orchestrator/bin/python scripts/baseline-all-jobs.py --app-root /var/www/SupplyCore

    # Dry-run (just print the execution order, don't run anything):
    .venv-orchestrator/bin/python scripts/baseline-all-jobs.py --dry-run

    # Run only specific lanes:
    .venv-orchestrator/bin/python scripts/baseline-all-jobs.py --lane compute-battle --lane compute-graph

    # Skip specific jobs:
    .venv-orchestrator/bin/python scripts/baseline-all-jobs.py --skip esi_affiliation_sync --skip evewho_enrichment_sync
"""
from __future__ import annotations

import argparse
import gc
import json
import os
import resource
import signal
import sys
import time
import traceback
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path


def _resident_mb() -> float:
    """Current RSS in MiB (Linux)."""
    usage = resource.getrusage(resource.RUSAGE_SELF)
    factor = 1024 if usage.ru_maxrss < 1024 * 1024 * 1024 else 1
    return (usage.ru_maxrss * factor) / (1024 * 1024)


def _topo_sort(definitions: dict) -> list[str]:
    """Kahn's algorithm — returns jobs in dependency order."""
    in_degree: dict[str, int] = {}
    dependents: dict[str, list[str]] = defaultdict(list)
    all_keys = set(definitions.keys())

    for key, defn in definitions.items():
        deps = [d for d in defn.get("depends_on", []) if d in all_keys]
        in_degree[key] = len(deps)
        for dep in deps:
            dependents[dep].append(key)

    queue: deque[str] = deque()
    for key in all_keys:
        if in_degree.get(key, 0) == 0:
            queue.append(key)

    # Stable sort within each tier: priority high > normal > low, then alphabetical
    priority_rank = {"high": 0, "normal": 1, "low": 2}

    ordered: list[str] = []
    while queue:
        # Sort the current frontier for deterministic output
        batch = sorted(
            queue,
            key=lambda k: (
                priority_rank.get(definitions[k].get("priority", "normal"), 1),
                k,
            ),
        )
        queue.clear()
        for key in batch:
            ordered.append(key)
            for dep in dependents[key]:
                in_degree[dep] -= 1
                if in_degree[dep] == 0:
                    queue.append(dep)

    # Anything left has a broken dep — append at end with a warning
    missing = all_keys - set(ordered)
    if missing:
        for m in sorted(missing):
            print(f"WARNING: {m} has unresolvable dependencies, appending at end", file=sys.stderr)
            ordered.append(m)

    return ordered


def main() -> int:
    parser = argparse.ArgumentParser(description="Baseline all jobs: run in dependency order, report time + memory.")
    parser.add_argument("--app-root", default=None, help="SupplyCore app root (default: auto-detect)")
    parser.add_argument("--lane", action="append", default=[], help="Only run jobs in these lanes (repeatable)")
    parser.add_argument("--skip", action="append", default=[], help="Skip these job keys (repeatable)")
    parser.add_argument("--dry-run", action="store_true", help="Print execution order without running")
    parser.add_argument("--stop-on-fail", action="store_true", help="Stop after first failure")
    parser.add_argument("--output", default=None, help="JSON output path (default: storage/logs/baseline-report.json)")
    args = parser.parse_args()

    # ── Bootstrap ──────────────────────────────────────────────────────
    # Add python/ to sys.path so orchestrator package is importable
    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent
    python_pkg = project_root / "python"
    if str(python_pkg) not in sys.path:
        sys.path.insert(0, str(python_pkg))

    from orchestrator.config import load_php_runtime_config, resolve_app_root
    from orchestrator.db import SupplyCoreDb
    from orchestrator.processor_registry import PYTHON_PROCESSOR_JOB_KEYS, run_registered_processor
    from orchestrator.worker_registry import WORKER_JOB_DEFINITIONS
    from orchestrator.json_utils import make_json_safe

    app_root = Path(args.app_root or resolve_app_root(__file__)).resolve()
    raw_config = load_php_runtime_config(app_root).raw

    # ── Build execution plan ──────────────────────────────────────────
    # Filter to jobs that have a Python processor
    runnable = {
        k: v for k, v in WORKER_JOB_DEFINITIONS.items()
        if k in PYTHON_PROCESSOR_JOB_KEYS
    }

    # Lane filter
    if args.lane:
        lanes = set(args.lane)
        runnable = {k: v for k, v in runnable.items() if v.get("lane", "") in lanes}

    # Skip filter
    skip_set = set(args.skip)
    runnable = {k: v for k, v in runnable.items() if k not in skip_set}

    execution_order = [k for k in _topo_sort(runnable) if k in runnable]

    print(f"\n{'='*90}")
    print(f"  SupplyCore Job Baseline — {len(execution_order)} jobs")
    print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    if args.lane:
        print(f"  Lanes: {', '.join(args.lane)}")
    if args.skip:
        print(f"  Skipping: {', '.join(args.skip)}")
    print(f"{'='*90}\n")

    # ── Dry run ───────────────────────────────────────────────────────
    if args.dry_run:
        print(f"{'#':>3}  {'Job Key':<50} {'Lane':<20} {'Timeout':>8}  {'MemLimit':>8}")
        print(f"{'─'*3}  {'─'*50} {'─'*20} {'─'*8}  {'─'*8}")
        for i, key in enumerate(execution_order, 1):
            defn = runnable[key]
            print(f"{i:3d}  {key:<50} {defn.get('lane','?'):<20} {defn.get('timeout_seconds',0):>7}s  {defn.get('memory_limit_mb',0):>6}MB")
        print(f"\nTotal: {len(execution_order)} jobs. Use without --dry-run to execute.\n")
        return 0

    # ── Run all jobs ──────────────────────────────────────────────────
    db = SupplyCoreDb(dict(raw_config.get("db") or {}))

    # Catch SIGINT/SIGTERM for clean shutdown
    stop = False
    def _signal_handler(signum, _frame):
        nonlocal stop
        print(f"\n\nReceived signal {signum}, finishing current job then stopping...\n", file=sys.stderr)
        stop = True
    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    results: list[dict] = []
    total_start = time.monotonic()

    header = f"{'#':>3}  {'Job Key':<45} {'Lane':<18} {'Status':<8} {'Duration':>10} {'ΔMem MB':>9} {'RSS MB':>8}  Summary"
    sep = f"{'─'*3}  {'─'*45} {'─'*18} {'─'*8} {'─'*10} {'─'*9} {'─'*8}  {'─'*40}"
    print(header)
    print(sep)

    for i, job_key in enumerate(execution_order, 1):
        if stop:
            break

        defn = runnable[job_key]
        lane = defn.get("lane", "?")

        gc.collect()
        mem_before = _resident_mb()
        t0 = time.monotonic()

        try:
            result = make_json_safe(run_registered_processor(job_key, db, raw_config))
            elapsed = time.monotonic() - t0
            mem_after = _resident_mb()
            status = str(result.get("status", "success"))
            summary = str(result.get("summary", ""))[:80]
            rows_processed = result.get("rows_processed", 0)
            rows_written = result.get("rows_written", 0)
        except Exception as exc:
            elapsed = time.monotonic() - t0
            mem_after = _resident_mb()
            status = "error"
            summary = str(exc)[:80]
            rows_processed = 0
            rows_written = 0
            result = {"status": "error", "error_text": str(exc)}
            if args.stop_on_fail:
                traceback.print_exc(file=sys.stderr)

        delta_mem = mem_after - mem_before

        # Format duration nicely
        if elapsed >= 60:
            dur_str = f"{elapsed/60:.1f}m"
        else:
            dur_str = f"{elapsed:.1f}s"

        status_icon = "OK" if status in ("success", "skipped") else "FAIL"

        print(f"{i:3d}  {job_key:<45} {lane:<18} {status_icon:<8} {dur_str:>10} {delta_mem:>+8.1f} {mem_after:>8.1f}  {summary}")

        results.append({
            "order": i,
            "job_key": job_key,
            "lane": lane,
            "status": status,
            "duration_seconds": round(elapsed, 2),
            "mem_before_mb": round(mem_before, 1),
            "mem_after_mb": round(mem_after, 1),
            "mem_delta_mb": round(delta_mem, 1),
            "rows_processed": rows_processed,
            "rows_written": rows_written,
            "summary": summary,
            "timeout_seconds": defn.get("timeout_seconds", 0),
            "memory_limit_mb": defn.get("memory_limit_mb", 0),
        })

        if args.stop_on_fail and status not in ("success", "skipped"):
            print(f"\n  Stopping after failure (--stop-on-fail).\n")
            break

    total_elapsed = time.monotonic() - total_start

    # ── Summary ───────────────────────────────────────────────────────
    print(sep)
    ok_count = sum(1 for r in results if r["status"] in ("success", "skipped"))
    fail_count = len(results) - ok_count
    total_mem = _resident_mb()

    if total_elapsed >= 60:
        total_dur_str = f"{total_elapsed/60:.1f}m"
    else:
        total_dur_str = f"{total_elapsed:.1f}s"

    print(f"\n  Ran {len(results)}/{len(execution_order)} jobs in {total_dur_str}")
    print(f"  OK: {ok_count}  Failed: {fail_count}  Final RSS: {total_mem:.0f} MB\n")

    # Top 10 slowest
    by_duration = sorted(results, key=lambda r: r["duration_seconds"], reverse=True)
    print("  Top 10 slowest:")
    for r in by_duration[:10]:
        d = r["duration_seconds"]
        dur = f"{d/60:.1f}m" if d >= 60 else f"{d:.1f}s"
        print(f"    {dur:>8}  {r['job_key']}")

    # Top 10 memory consumers
    by_mem = sorted(results, key=lambda r: r["mem_delta_mb"], reverse=True)
    print("\n  Top 10 memory growth:")
    for r in by_mem[:10]:
        print(f"    {r['mem_delta_mb']:>+7.1f} MB  {r['job_key']}")

    # Jobs that exceeded their configured timeout
    over_timeout = [r for r in results if r["duration_seconds"] > r["timeout_seconds"] and r["timeout_seconds"] > 0]
    if over_timeout:
        print(f"\n  ⚠ {len(over_timeout)} jobs exceeded their configured timeout:")
        for r in over_timeout:
            d = r["duration_seconds"]
            t = r["timeout_seconds"]
            dur = f"{d/60:.1f}m" if d >= 60 else f"{d:.1f}s"
            print(f"    {r['job_key']:<45} {dur:>8} (timeout: {t}s)")

    # Jobs that exceeded their memory limit
    over_mem = [r for r in results if r["mem_after_mb"] > r["memory_limit_mb"] and r["memory_limit_mb"] > 0]
    if over_mem:
        print(f"\n  ⚠ {len(over_mem)} jobs where RSS exceeded configured memory_limit_mb:")
        for r in over_mem:
            print(f"    {r['job_key']:<45} {r['mem_after_mb']:.0f} MB (limit: {r['memory_limit_mb']} MB)")

    print()

    # ── Write JSON report ─────────────────────────────────────────────
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_elapsed_seconds": round(total_elapsed, 2),
        "total_jobs": len(execution_order),
        "jobs_ran": len(results),
        "ok": ok_count,
        "failed": fail_count,
        "final_rss_mb": round(total_mem, 1),
        "lanes_filter": args.lane or None,
        "skip_filter": args.skip or None,
        "results": results,
    }

    output_path = Path(args.output) if args.output else app_root / "storage/logs/baseline-report.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(report, f, indent=2, default=str)
    print(f"  JSON report written to {output_path}\n")

    return 1 if fail_count > 0 else 0


if __name__ == "__main__":
    raise SystemExit(main())
