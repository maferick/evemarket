#!/usr/bin/env python3
"""One-off backfill: write ATTACKED_ON and VICTIM_OF edges into Neo4j.

Also patches any existing Killmail nodes that are missing the battle_id
property by re-running the killmail entity projection first.

Usage (from repo root):
    python scripts/backfill_neo4j_killmail_edges.py
    python scripts/backfill_neo4j_killmail_edges.py --app-root /path/to/supplycore
    python scripts/backfill_neo4j_killmail_edges.py --skip-entities   # skip node projection
    python scripts/backfill_neo4j_killmail_edges.py --batch-size 5000

The script resets the cursors for both jobs before running so it processes
the full history regardless of previous incremental state.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

# Allow running from repo root without installing the package.
_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT))

from python.orchestrator.config import load_php_runtime_config, resolve_app_root
from python.orchestrator.db import SupplyCoreDb
from python.orchestrator.job_context import neo4j_runtime
from python.orchestrator.jobs.graph_pipeline import (
    run_compute_graph_sync_killmail_entities,
    run_compute_graph_sync_killmail_edges,
)

_CURSOR_KEYS = (
    "graph_sync_killmail_entity_cursor",
    "graph_sync_killmail_attacker_edges_cursor",
    "graph_sync_killmail_victim_edges_cursor",
)


def _reset_cursors(db: SupplyCoreDb, keys: tuple[str, ...]) -> None:
    for key in keys:
        db.execute(
            "DELETE FROM sync_state WHERE dataset_key = %s",
            (key,),
        )


def _print_result(label: str, result: dict) -> None:
    status = result.get("status", "?")
    summary = result.get("summary", "")
    written = result.get("rows_written", 0)
    duration = result.get("duration_ms", 0)
    print(f"  [{status}] {label}: {summary} ({written} rows, {duration}ms)")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--app-root", default=resolve_app_root(), help="Path to SupplyCore app root")
    parser.add_argument("--skip-entities", action="store_true", help="Skip killmail node projection (entities already up to date)")
    parser.add_argument("--batch-size", type=int, default=2000, help="Neo4j write batch size (default: 2000)")
    args = parser.parse_args()

    app_root = Path(args.app_root).resolve()
    config = load_php_runtime_config(app_root)
    db = SupplyCoreDb(config.raw.get("db", {}))
    neo4j_raw = {**neo4j_runtime(config.raw), "batch_size": args.batch_size}

    print("=== Neo4j killmail edge backfill ===")
    print(f"App root : {app_root}")
    print(f"Batch    : {args.batch_size}")
    print()

    if not args.skip_entities:
        print("Step 1/2 — Resetting entity cursor and re-projecting Killmail nodes")
        print("         (adds battle_id property to existing nodes)...")
        _reset_cursors(db, ("graph_sync_killmail_entity_cursor",))
        t0 = time.perf_counter()
        result = run_compute_graph_sync_killmail_entities(db, neo4j_raw)
        _print_result("killmail entities", result)
        if result.get("status") == "failed":
            print("Entity projection failed — aborting.")
            return 1
        print()
    else:
        print("Step 1/2 — Skipped (--skip-entities)")
        print()

    print("Step 2/2 — Resetting edge cursors and writing ATTACKED_ON / VICTIM_OF edges...")
    _reset_cursors(db, (
        "graph_sync_killmail_attacker_edges_cursor",
        "graph_sync_killmail_victim_edges_cursor",
    ))
    result = run_compute_graph_sync_killmail_edges(db, neo4j_raw)
    _print_result("killmail edges", result)
    if result.get("status") == "failed":
        print("Edge sync failed.")
        return 1

    print()
    print("Done. Re-run compute_alliance_dossiers to pick up Neo4j-sourced data:")
    print("  python -m python.orchestrator compute-alliance-dossiers --app-root", app_root)
    return 0


if __name__ == "__main__":
    sys.exit(main())
