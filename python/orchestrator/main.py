from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any

from .config import load_php_runtime_config
from .job_context import battle_runtime, influx_runtime, neo4j_runtime
from .influx_export import main as run_influx_rollup_export
from .influx_inspect import inspect_main as run_influx_rollup_inspect
from .influx_inspect import sample_main as run_influx_rollup_sample
from .jobs.compute_buy_all import run_compute_buy_all
from .jobs.compute_graph_insights import run_compute_graph_insights
from .jobs.compute_graph_sync import run_compute_graph_sync
from .jobs.graph_pipeline import (
    run_compute_graph_derived_relationships,
    run_compute_graph_sync_battle_intelligence,
    run_compute_graph_sync_doctrine_dependency,
    run_compute_graph_prune,
    run_compute_graph_topology_metrics,
)
from .jobs.behavioral_intelligence_v2 import run_compute_behavioral_baselines, run_compute_suspicion_scores_v2
from .jobs.compute_signals import run_compute_signals
from .jobs.battle_intelligence import (
    run_compute_battle_actor_features,
    run_compute_battle_anomalies,
    run_compute_battle_rollups,
    run_compute_battle_target_metrics,
    run_compute_suspicion_scores,
)
from .logging_utils import configure_logging
from .rebuild_data_model import main as run_rebuild_data_model
from .supervisor import run_supervisor
from .worker_pool import main as run_worker_pool
from .zkill_worker import main as run_zkill_worker
from .processor_registry import run_registered_processor, PYTHON_PROCESSOR_JOB_KEYS
from .jobs.killmail_history_backfill import run_killmail_history_backfill


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run SupplyCore Python services")
    subparsers = parser.add_subparsers(dest="command")

    supervisor = subparsers.add_parser("supervisor", help="Run the legacy PHP scheduler supervisor")
    supervisor.add_argument("--app-root", default=str(Path(__file__).resolve().parents[2]))
    supervisor.add_argument("--verbose", action="store_true")

    worker_pool = subparsers.add_parser("worker-pool", help="Run the continuous worker pool")
    worker_pool.add_argument("--app-root", default=str(Path(__file__).resolve().parents[2]))
    worker_pool.add_argument("--worker-id", default="")
    worker_pool.add_argument("--queues", default="sync,compute")
    worker_pool.add_argument("--workload-classes", default="sync,compute")
    worker_pool.add_argument("--execution-modes", default="python,php")
    worker_pool.add_argument("--once", action="store_true")
    worker_pool.add_argument("--verbose", action="store_true")

    zkill = subparsers.add_parser("zkill-worker", help="Run the dedicated zKill continuous worker")
    zkill.add_argument("--app-root", default=str(Path(__file__).resolve().parents[2]))
    zkill.add_argument("--poll-sleep", type=int, default=10)
    zkill.add_argument("--once", action="store_true")
    zkill.add_argument("--verbose", action="store_true")

    rebuild = subparsers.add_parser("rebuild-data-model", help="Run the live-progress derived rebuild workflow")
    rebuild.add_argument("--app-root", default=str(Path(__file__).resolve().parents[2]))
    rebuild.add_argument("--mode", default="rebuild-all-derived")
    rebuild.add_argument("--window-days", type=int, default=30)
    rebuild.add_argument("--full-reset", action="store_true")
    rebuild.add_argument("--enable-partitioned-history", dest="enable_partitioned_history", action="store_true", default=True)
    rebuild.add_argument("--disable-partitioned-history", dest="enable_partitioned_history", action="store_false")

    influx_export = subparsers.add_parser("influx-rollup-export", help="Export selected historical rollups to InfluxDB")
    influx_export.add_argument("--app-root", default=str(Path(__file__).resolve().parents[2]))
    influx_export.add_argument("--dataset", action="append", default=[], help="Limit export to one or more dataset keys.")
    influx_export.add_argument("--full", action="store_true", help="Ignore checkpoints and export the full selected dataset(s).")
    influx_export.add_argument("--dry-run", action="store_true", help="Read and encode points without writing to InfluxDB.")
    influx_export.add_argument("--batch-size", type=int, default=0, help="Override Influx write batch size.")
    influx_export.add_argument("--verbose", action="store_true")

    influx_inspect = subparsers.add_parser("influx-rollup-inspect", help="Inspect measurement coverage in the InfluxDB rollup bucket")
    influx_inspect.add_argument("--app-root", default=str(Path(__file__).resolve().parents[2]))
    influx_inspect.add_argument("--dataset", action="append", default=[], help="Limit inspection to one or more dataset keys or measurement names.")
    influx_inspect.add_argument("--verbose", action="store_true")

    influx_sample = subparsers.add_parser("influx-rollup-sample", help="Fetch latest sample points from the InfluxDB rollup bucket")
    influx_sample.add_argument("--app-root", default=str(Path(__file__).resolve().parents[2]))
    influx_sample.add_argument("--dataset", action="append", default=[], help="Limit sample output to one or more dataset keys or measurement names.")
    influx_sample.add_argument("--limit", type=int, default=5, help="Number of latest points to return per measurement.")
    influx_sample.add_argument("--group-by", action="append", default=[], help="Optional tag keys to group summary output by.")
    influx_sample.add_argument("--verbose", action="store_true")

    compute_buy_all = subparsers.add_parser("compute-buy-all", help="Materialize Buy All planner data into precomputed MariaDB tables")
    compute_buy_all.add_argument("--app-root", default=str(Path(__file__).resolve().parents[2]))

    compute_signals = subparsers.add_parser("compute-signals", help="Generate precomputed intelligence signals into MariaDB")
    compute_signals.add_argument("--app-root", default=str(Path(__file__).resolve().parents[2]))
    compute_graph_sync = subparsers.add_parser("compute-graph-sync", help="Incrementally sync doctrine-fit-item graph into Neo4j")
    compute_graph_sync.add_argument("--app-root", default=str(Path(__file__).resolve().parents[2]))
    compute_graph_insights = subparsers.add_parser("compute-graph-insights", help="Compute graph-derived metrics and persist into MariaDB")
    compute_graph_insights.add_argument("--app-root", default=str(Path(__file__).resolve().parents[2]))
    compute_graph_sync_doctrine = subparsers.add_parser("compute-graph-sync-doctrine-dependency", help="Sync doctrine/fit/item anchors into Neo4j")
    compute_graph_sync_doctrine.add_argument("--app-root", default=str(Path(__file__).resolve().parents[2]))
    compute_graph_sync_battle = subparsers.add_parser("compute-graph-sync-battle-intelligence", help="Sync battle/actor anchors into Neo4j")
    compute_graph_sync_battle.add_argument("--app-root", default=str(Path(__file__).resolve().parents[2]))
    compute_graph_derived = subparsers.add_parser("compute-graph-derived-relationships", help="Build derived graph relationships")
    compute_graph_derived.add_argument("--app-root", default=str(Path(__file__).resolve().parents[2]))
    compute_graph_prune = subparsers.add_parser("compute-graph-prune", help="Prune stale low-signal graph edges")
    compute_graph_prune.add_argument("--app-root", default=str(Path(__file__).resolve().parents[2]))
    compute_graph_topology = subparsers.add_parser("compute-graph-topology-metrics", help="Materialize graph topology metrics to MariaDB")
    compute_graph_topology.add_argument("--app-root", default=str(Path(__file__).resolve().parents[2]))
    compute_behavioral = subparsers.add_parser("compute-behavioral-baselines", help="Compute character behavioral baselines")
    compute_behavioral.add_argument("--app-root", default=str(Path(__file__).resolve().parents[2]))
    compute_suspicion_v2 = subparsers.add_parser("compute-suspicion-scores-v2", help="Compute suspicion scoring v2")
    compute_suspicion_v2.add_argument("--app-root", default=str(Path(__file__).resolve().parents[2]))
    run_job = subparsers.add_parser("run-job", help="Run a Python-native recurring job by job key")
    run_job.add_argument("--app-root", default=str(Path(__file__).resolve().parents[2]))
    run_job.add_argument("--job-key", required=True)

    backfill = subparsers.add_parser("killmail-backfill", help="Backfill killmails from R2Z2 history API")
    backfill.add_argument("--app-root", default=str(Path(__file__).resolve().parents[2]))

    for command, help_text in [
        ("compute-battle-rollups", "Cluster killmails into deterministic battle rollups and participants"),
        ("compute-battle-target-metrics", "Build target-level sustain proxy metrics"),
        ("compute-battle-anomalies", "Compute side-level efficiency and anomaly scores"),
        ("compute-battle-actor-features", "Build actor-level battle feature rows and optional graph sync"),
        ("compute-suspicion-scores", "Compute character battle intelligence and suspicion scores"),
    ]:
        parser_job = subparsers.add_parser(command, help=help_text)
        parser_job.add_argument("--app-root", default=str(Path(__file__).resolve().parents[2]))
        parser_job.add_argument("--dry-run", action="store_true", help="Compute and log counters without writing MariaDB tables.")
    return parser.parse_args()


def _print_cli_result(command: str, started_at: float, result: dict[str, Any]) -> None:
    payload = {
        "command": command,
        "status": str(result.get("status") or "success"),
        "rows_processed": int(result.get("rows_processed") or 0),
        "rows_written": int(result.get("rows_written") or 0),
        "rows_would_write": int(result.get("rows_would_write") or result.get("rows_written") or 0),
        "duration_ms": max(0, int((time.monotonic() - started_at) * 1000)),
        "result": result,
    }
    print(json.dumps(payload, ensure_ascii=False, default=str))


def main() -> int:
    args = parse_args()
    command = args.command or "supervisor"
    if command == "worker-pool":
        return run_worker_pool([
            "--app-root", args.app_root,
            "--worker-id", args.worker_id,
            "--queues", args.queues,
            "--workload-classes", args.workload_classes,
            "--execution-modes", args.execution_modes,
            *( ["--once"] if args.once else [] ),
            *( ["--verbose"] if args.verbose else [] ),
        ])
    if command == "zkill-worker":
        return run_zkill_worker([
            "--app-root", args.app_root,
            "--poll-sleep", str(args.poll_sleep),
            *( ["--once"] if args.once else [] ),
            *( ["--verbose"] if args.verbose else [] ),
        ])
    if command == "rebuild-data-model":
        return run_rebuild_data_model([
            "--app-root", args.app_root,
            "--mode", args.mode,
            "--window-days", str(args.window_days),
            *( ["--full-reset"] if args.full_reset else [] ),
            *( ["--enable-partitioned-history"] if args.enable_partitioned_history else ["--disable-partitioned-history"] ),
        ])
    if command == "influx-rollup-export":
        return run_influx_rollup_export([
            "--app-root", args.app_root,
            *(sum([["--dataset", dataset] for dataset in args.dataset], [])),
            *( ["--full"] if args.full else [] ),
            *( ["--dry-run"] if args.dry_run else [] ),
            *( ["--batch-size", str(args.batch_size)] if args.batch_size > 0 else [] ),
            *( ["--verbose"] if args.verbose else [] ),
        ])
    if command == "influx-rollup-inspect":
        return run_influx_rollup_inspect([
            "--app-root", args.app_root,
            *(sum([["--dataset", dataset] for dataset in args.dataset], [])),
            *( ["--verbose"] if args.verbose else [] ),
        ])
    if command == "influx-rollup-sample":
        return run_influx_rollup_sample([
            "--app-root", args.app_root,
            *(sum([["--dataset", dataset] for dataset in args.dataset], [])),
            "--limit", str(args.limit),
            *(sum([["--group-by", group] for group in args.group_by], [])),
            *( ["--verbose"] if args.verbose else [] ),
        ])
    if command == "compute-buy-all":
        app_root = Path(args.app_root).resolve()
        config = load_php_runtime_config(app_root)
        from .db import SupplyCoreDb
        db = SupplyCoreDb(config.raw.get("db", {}))
        result = run_compute_buy_all(db)
        print(result)
        return 0
    if command == "compute-signals":
        app_root = Path(args.app_root).resolve()
        config = load_php_runtime_config(app_root)
        from .db import SupplyCoreDb
        db = SupplyCoreDb(config.raw.get("db", {}))
        result = run_compute_signals(db, influx_runtime(config.raw))
        print(result)
        return 0
    if command == "compute-graph-sync":
        app_root = Path(args.app_root).resolve()
        config = load_php_runtime_config(app_root)
        from .db import SupplyCoreDb
        db = SupplyCoreDb(config.raw.get("db", {}))
        result = run_compute_graph_sync(db, neo4j_runtime(config.raw))
        print(result)
        return 0
    if command == "compute-graph-insights":
        app_root = Path(args.app_root).resolve()
        config = load_php_runtime_config(app_root)
        from .db import SupplyCoreDb
        db = SupplyCoreDb(config.raw.get("db", {}))
        result = run_compute_graph_insights(db, neo4j_runtime(config.raw))
        print(result)
        return 0
    if command == "compute-graph-sync-doctrine-dependency":
        app_root = Path(args.app_root).resolve()
        config = load_php_runtime_config(app_root)
        from .db import SupplyCoreDb
        db = SupplyCoreDb(config.raw.get("db", {}))
        result = run_compute_graph_sync_doctrine_dependency(db, neo4j_runtime(config.raw))
        print(result)
        return 0
    if command == "compute-graph-sync-battle-intelligence":
        app_root = Path(args.app_root).resolve()
        config = load_php_runtime_config(app_root)
        from .db import SupplyCoreDb
        db = SupplyCoreDb(config.raw.get("db", {}))
        result = run_compute_graph_sync_battle_intelligence(db, neo4j_runtime(config.raw))
        print(result)
        return 0
    if command == "compute-graph-derived-relationships":
        app_root = Path(args.app_root).resolve()
        config = load_php_runtime_config(app_root)
        from .db import SupplyCoreDb
        db = SupplyCoreDb(config.raw.get("db", {}))
        result = run_compute_graph_derived_relationships(db, neo4j_runtime(config.raw))
        print(result)
        return 0
    if command == "compute-graph-prune":
        app_root = Path(args.app_root).resolve()
        config = load_php_runtime_config(app_root)
        from .db import SupplyCoreDb
        db = SupplyCoreDb(config.raw.get("db", {}))
        result = run_compute_graph_prune(db, neo4j_runtime(config.raw))
        print(result)
        return 0
    if command == "compute-graph-topology-metrics":
        app_root = Path(args.app_root).resolve()
        config = load_php_runtime_config(app_root)
        from .db import SupplyCoreDb
        db = SupplyCoreDb(config.raw.get("db", {}))
        result = run_compute_graph_topology_metrics(db, neo4j_runtime(config.raw))
        print(result)
        return 0
    if command == "compute-behavioral-baselines":
        app_root = Path(args.app_root).resolve()
        config = load_php_runtime_config(app_root)
        from .db import SupplyCoreDb
        db = SupplyCoreDb(config.raw.get("db", {}))
        result = run_compute_behavioral_baselines(db, battle_runtime(config.raw))
        print(result)
        return 0
    if command == "compute-suspicion-scores-v2":
        app_root = Path(args.app_root).resolve()
        config = load_php_runtime_config(app_root)
        from .db import SupplyCoreDb
        db = SupplyCoreDb(config.raw.get("db", {}))
        result = run_compute_suspicion_scores_v2(db, battle_runtime(config.raw))
        print(result)
        return 0
    if command == "killmail-backfill":
        from dataclasses import dataclass

        @dataclass
        class BackfillContext:
            app_root: Path
            php_binary: str

        app_root = Path(args.app_root).resolve()
        config = load_php_runtime_config(app_root)
        ctx = BackfillContext(app_root=app_root, php_binary=config.php_binary)
        result = run_killmail_history_backfill(ctx)
        print(json.dumps(result, default=str))
        return 0 if result.get("status") == "success" else 1

    if command == "run-job":
        app_root = Path(args.app_root).resolve()
        config = load_php_runtime_config(app_root)
        from .db import SupplyCoreDb
        db = SupplyCoreDb(config.raw.get("db", {}))
        job_key = str(args.job_key).strip()
        if job_key not in PYTHON_PROCESSOR_JOB_KEYS:
            print({"status": "failed", "error": f"No Python-native processor registered for {job_key}."})
            return 1
        result = run_registered_processor(job_key, db, config.raw)
        print(result)
        return 1 if str(result.get("status") or "success").lower() == "failed" else 0

    if command in {
        "compute-battle-rollups",
        "compute-battle-target-metrics",
        "compute-battle-anomalies",
        "compute-battle-actor-features",
        "compute-suspicion-scores",
    }:
        app_root = Path(args.app_root).resolve()
        config = load_php_runtime_config(app_root)
        from .db import SupplyCoreDb

        db = SupplyCoreDb(config.raw.get("db", {}))
        battle_job_runtime = battle_runtime(config.raw)
        started_at = time.monotonic()
        try:
            if command == "compute-battle-rollups":
                result = run_compute_battle_rollups(db, battle_job_runtime, dry_run=bool(args.dry_run))
            elif command == "compute-battle-target-metrics":
                result = run_compute_battle_target_metrics(db, battle_job_runtime, dry_run=bool(args.dry_run))
            elif command == "compute-battle-anomalies":
                result = run_compute_battle_anomalies(db, battle_job_runtime, dry_run=bool(args.dry_run))
            elif command == "compute-battle-actor-features":
                result = run_compute_battle_actor_features(
                    db,
                    neo4j_runtime(config.raw),
                    battle_job_runtime,
                    dry_run=bool(args.dry_run),
                )
            else:
                result = run_compute_suspicion_scores(db, battle_job_runtime, dry_run=bool(args.dry_run))
            _print_cli_result(command, started_at, result)
            return 0 if str(result.get("status") or "success") != "failed" else 1
        except Exception as exc:
            _print_cli_result(
                command,
                started_at,
                {"status": "failed", "error_text": str(exc), "rows_processed": 0, "rows_written": 0},
            )
            return 1

    app_root = Path(args.app_root).resolve()
    config = load_php_runtime_config(app_root)
    logger = configure_logging(verbose=args.verbose, log_file=config.log_file)
    return run_supervisor(app_root=app_root, logger=logger)
