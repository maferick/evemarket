from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any

from .config import load_php_runtime_config, resolve_app_root
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
from .loop_runner import main as run_loop_runner
from .supervisor import run_supervisor
from .worker_pool import main as run_worker_pool
from .zkill_worker import main as run_zkill_worker
from .evewho_alliance_lookup_runner import main as run_evewho_alliance_runner
from .killmail_backfill_runner import main as run_killmail_backfill_runner
from .processor_registry import run_registered_processor, PYTHON_PROCESSOR_JOB_KEYS
from .jobs.killmail_history_backfill import run_killmail_history_backfill
from .jobs.killmail_full_history_backfill import run_killmail_full_history_backfill


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run SupplyCore Python services")
    subparsers = parser.add_subparsers(dest="command")

    supervisor = subparsers.add_parser("supervisor", help="Run the legacy PHP scheduler supervisor")
    supervisor.add_argument("--app-root", default=resolve_app_root(__file__))
    supervisor.add_argument("--verbose", action="store_true")

    worker_pool = subparsers.add_parser("worker-pool", help="Run the continuous worker pool")
    worker_pool.add_argument("--app-root", default=resolve_app_root(__file__))
    worker_pool.add_argument("--worker-id", default="")
    worker_pool.add_argument("--queues", default="sync,compute")
    worker_pool.add_argument("--workload-classes", default="sync,compute")
    worker_pool.add_argument("--execution-modes", default="python,php")
    worker_pool.add_argument("--once", action="store_true")
    worker_pool.add_argument("--verbose", action="store_true")

    loop_runner = subparsers.add_parser("loop-runner", help="Simple tier-by-tier loop runner (replaces worker-pool)")
    loop_runner.add_argument("--app-root", default=resolve_app_root(__file__))
    loop_runner.add_argument("--max-parallel", type=int, default=6, help="Max concurrent jobs per tier (default: 6)")
    loop_runner.add_argument("--fast-pause", type=float, default=5.0, help="Seconds to pause between fast-loop cycles (default: 5)")
    loop_runner.add_argument("--background-pause", type=float, default=30.0, help="Seconds to pause between background-loop cycles (default: 30)")
    loop_runner.add_argument("--once", action="store_true", help="Run one cycle and exit")
    loop_runner.add_argument("--fast-only", action="store_true", help="Only run fast loop")
    loop_runner.add_argument("--background-only", action="store_true", help="Only run background loop")
    loop_runner.add_argument("--verbose", action="store_true")

    zkill = subparsers.add_parser("zkill-worker", help="Run the dedicated zKill continuous worker")
    zkill.add_argument("--app-root", default=resolve_app_root(__file__))
    zkill.add_argument("--poll-sleep", type=int, default=10)
    zkill.add_argument("--once", action="store_true")
    zkill.add_argument("--verbose", action="store_true")

    evewho_runner = subparsers.add_parser("evewho-alliance-runner", help="Run the dedicated continuous EveWho alliance lookup runner (uses half the API rate limit)")
    evewho_runner.add_argument("--app-root", default=resolve_app_root(__file__))
    evewho_runner.add_argument("--loop-sleep", type=int, default=30, help="Seconds to sleep between cycles (default: 30)")
    evewho_runner.add_argument("--once", action="store_true", help="Run one cycle then exit")
    evewho_runner.add_argument("--verbose", action="store_true")

    backfill_runner = subparsers.add_parser("killmail-backfill-runner", help="Run the dedicated continuous killmail full-history backfill runner (stops when caught up, resumes on start-date change)")
    backfill_runner.add_argument("--app-root", default=resolve_app_root(__file__))
    backfill_runner.add_argument("--loop-sleep", type=int, default=60, help="Seconds to sleep between cycles when up to date (default: 60)")
    backfill_runner.add_argument("--once", action="store_true", help="Run one cycle then exit")
    backfill_runner.add_argument("--verbose", action="store_true")

    rebuild = subparsers.add_parser("rebuild-data-model", help="Run the live-progress derived rebuild workflow")
    rebuild.add_argument("--app-root", default=resolve_app_root(__file__))
    rebuild.add_argument("--mode", default="rebuild-all-derived")
    rebuild.add_argument("--window-days", type=int, default=30)
    rebuild.add_argument("--full-reset", action="store_true")
    rebuild.add_argument("--enable-partitioned-history", dest="enable_partitioned_history", action="store_true", default=True)
    rebuild.add_argument("--disable-partitioned-history", dest="enable_partitioned_history", action="store_false")
    rebuild.add_argument("--verbose", action="store_true")

    influx_export = subparsers.add_parser("influx-rollup-export", help="Export selected historical rollups to InfluxDB")
    influx_export.add_argument("--app-root", default=resolve_app_root(__file__))
    influx_export.add_argument("--dataset", action="append", default=[], help="Limit export to one or more dataset keys.")
    influx_export.add_argument("--full", action="store_true", help="Ignore checkpoints and export the full selected dataset(s).")
    influx_export.add_argument("--dry-run", action="store_true", help="Read and encode points without writing to InfluxDB.")
    influx_export.add_argument("--batch-size", type=int, default=0, help="Override Influx write batch size.")
    influx_export.add_argument("--verbose", action="store_true")

    influx_inspect = subparsers.add_parser("influx-rollup-inspect", help="Inspect measurement coverage in the InfluxDB rollup bucket")
    influx_inspect.add_argument("--app-root", default=resolve_app_root(__file__))
    influx_inspect.add_argument("--dataset", action="append", default=[], help="Limit inspection to one or more dataset keys or measurement names.")
    influx_inspect.add_argument("--verbose", action="store_true")

    influx_sample = subparsers.add_parser("influx-rollup-sample", help="Fetch latest sample points from the InfluxDB rollup bucket")
    influx_sample.add_argument("--app-root", default=resolve_app_root(__file__))
    influx_sample.add_argument("--dataset", action="append", default=[], help="Limit sample output to one or more dataset keys or measurement names.")
    influx_sample.add_argument("--limit", type=int, default=5, help="Number of latest points to return per measurement.")
    influx_sample.add_argument("--group-by", action="append", default=[], help="Optional tag keys to group summary output by.")
    influx_sample.add_argument("--verbose", action="store_true")

    influx_validate = subparsers.add_parser("influx-validate", help="Validate InfluxDB data against MariaDB rollup tables for migration parity")
    influx_validate.add_argument("--app-root", default=resolve_app_root(__file__))
    influx_validate.add_argument("--dataset", action="append", default=[], help="Limit validation to one or more dataset keys.")
    influx_validate.add_argument("--days", type=int, default=14, help="Compare the last N days of data.")
    influx_validate.add_argument("--verbose", action="store_true")

    compute_buy_all = subparsers.add_parser("compute-buy-all", help="Materialize Buy All planner data into precomputed MariaDB tables")
    compute_buy_all.add_argument("--app-root", default=resolve_app_root(__file__))
    compute_buy_all.add_argument("--verbose", action="store_true")

    compute_signals = subparsers.add_parser("compute-signals", help="Generate precomputed intelligence signals into MariaDB")
    compute_signals.add_argument("--app-root", default=resolve_app_root(__file__))
    compute_signals.add_argument("--verbose", action="store_true")
    compute_ew = subparsers.add_parser("compute-economic-warfare", help="Compute economic warfare scores from opponent killmail data")
    compute_ew.add_argument("--app-root", default=resolve_app_root(__file__))
    compute_ew.add_argument("--verbose", action="store_true")
    graph_universe = subparsers.add_parser("graph-universe-sync", help="Sync universe topology (systems, stargates) into Neo4j")
    graph_universe.add_argument("--app-root", default=resolve_app_root(__file__))
    graph_universe.add_argument("--verbose", action="store_true")
    graph_killmails = subparsers.add_parser("compute-graph-sync-killmail-entities", help="Project killmail events as nodes into Neo4j")
    graph_killmails.add_argument("--app-root", default=resolve_app_root(__file__))
    graph_killmails.add_argument("--verbose", action="store_true")
    graph_killmail_edges = subparsers.add_parser("compute-graph-sync-killmail-edges", help="Sync ATTACKED_ON and VICTIM_OF edges from battle killmails into Neo4j")
    graph_killmail_edges.add_argument("--app-root", default=resolve_app_root(__file__))
    graph_killmail_edges.add_argument("--verbose", action="store_true")
    compute_graph_sync = subparsers.add_parser("compute-graph-sync", help="Incrementally sync doctrine-fit-item graph into Neo4j")
    compute_graph_sync.add_argument("--app-root", default=resolve_app_root(__file__))
    compute_graph_sync.add_argument("--verbose", action="store_true")
    compute_graph_insights = subparsers.add_parser("compute-graph-insights", help="Compute graph-derived metrics and persist into MariaDB")
    compute_graph_insights.add_argument("--app-root", default=resolve_app_root(__file__))
    compute_graph_insights.add_argument("--verbose", action="store_true")
    compute_graph_sync_doctrine = subparsers.add_parser("compute-graph-sync-doctrine-dependency", help="Sync doctrine/fit/item anchors into Neo4j")
    compute_graph_sync_doctrine.add_argument("--app-root", default=resolve_app_root(__file__))
    compute_graph_sync_doctrine.add_argument("--verbose", action="store_true")
    compute_graph_sync_battle = subparsers.add_parser("compute-graph-sync-battle-intelligence", help="Sync battle/actor anchors into Neo4j")
    compute_graph_sync_battle.add_argument("--app-root", default=resolve_app_root(__file__))
    compute_graph_sync_battle.add_argument("--verbose", action="store_true")
    compute_graph_derived = subparsers.add_parser("compute-graph-derived-relationships", help="Build derived graph relationships")
    compute_graph_derived.add_argument("--app-root", default=resolve_app_root(__file__))
    compute_graph_derived.add_argument("--verbose", action="store_true")
    compute_graph_prune = subparsers.add_parser("compute-graph-prune", help="Prune stale low-signal graph edges")
    compute_graph_prune.add_argument("--app-root", default=resolve_app_root(__file__))
    compute_graph_prune.add_argument("--verbose", action="store_true")
    compute_graph_topology = subparsers.add_parser("compute-graph-topology-metrics", help="Materialize graph topology metrics to MariaDB")
    compute_graph_topology.add_argument("--app-root", default=resolve_app_root(__file__))
    compute_graph_topology.add_argument("--verbose", action="store_true")
    compute_behavioral = subparsers.add_parser("compute-behavioral-baselines", help="Compute character behavioral baselines")
    compute_behavioral.add_argument("--app-root", default=resolve_app_root(__file__))
    compute_behavioral.add_argument("--verbose", action="store_true")
    compute_suspicion_v2 = subparsers.add_parser("compute-suspicion-scores-v2", help="Compute suspicion scoring v2")
    compute_suspicion_v2.add_argument("--app-root", default=resolve_app_root(__file__))
    compute_suspicion_v2.add_argument("--verbose", action="store_true")
    run_job = subparsers.add_parser("run-job", help="Run a Python-native recurring job by job key")
    run_job.add_argument("--app-root", default=resolve_app_root(__file__))
    run_job.add_argument("--job-key", required=True)
    run_job.add_argument("--verbose", action="store_true", help="Print step-by-step progress to stderr")

    backfill = subparsers.add_parser("killmail-backfill", help="Backfill killmails from R2Z2 history API")
    backfill.add_argument("--app-root", default=resolve_app_root(__file__))
    backfill.add_argument("--verbose", action="store_true")

    full_backfill = subparsers.add_parser("killmail-full-history-backfill", help="Backfill ALL killmails day-by-day from R2Z2 daily history dumps")
    full_backfill.add_argument("--app-root", default=resolve_app_root(__file__))
    full_backfill.add_argument("--verbose", action="store_true")

    for command, help_text in [
        ("compute-battle-rollups", "Cluster killmails into deterministic battle rollups and participants"),
        ("compute-battle-target-metrics", "Build target-level sustain proxy metrics"),
        ("compute-battle-anomalies", "Compute side-level efficiency and anomaly scores"),
        ("compute-battle-actor-features", "Build actor-level battle feature rows and optional graph sync"),
        ("compute-suspicion-scores", "Compute character battle intelligence and suspicion scores"),
    ]:
        parser_job = subparsers.add_parser(command, help=help_text)
        parser_job.add_argument("--app-root", default=resolve_app_root(__file__))
        parser_job.add_argument("--dry-run", action="store_true", help="Compute and log counters without writing MariaDB tables.")
        parser_job.add_argument("--verbose", action="store_true")

    evewho_sync = subparsers.add_parser("evewho-enrichment-sync", help="Batch-enrich characters from EveWho into Neo4j")
    evewho_sync.add_argument("--app-root", default=resolve_app_root(__file__))
    evewho_sync.add_argument("--dry-run", action="store_true", help="Compute without writing to Neo4j or MariaDB.")
    evewho_sync.add_argument("--verbose", action="store_true")

    scheduler_graph = subparsers.add_parser("scheduler-graph", help="Display the DAG-based job dependency graph and execution tiers")
    scheduler_graph.add_argument("--app-root", default=resolve_app_root(__file__))
    scheduler_graph.add_argument("--validate", action="store_true", help="Validate the graph and report issues")
    scheduler_graph.add_argument("--json", dest="output_json", action="store_true", help="Output graph as JSON instead of human-readable text")
    scheduler_graph.add_argument("--verbose", action="store_true")

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
    command = args.command
    if command is None:
        # No subcommand given – supply defaults that the supervisor subparser would have set.
        args.app_root = resolve_app_root(__file__)
        args.verbose = False
        command = "supervisor"
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
    if command == "loop-runner":
        return run_loop_runner([
            "--app-root", args.app_root,
            "--max-parallel", str(args.max_parallel),
            "--fast-pause", str(args.fast_pause),
            "--background-pause", str(args.background_pause),
            *( ["--once"] if args.once else [] ),
            *( ["--fast-only"] if args.fast_only else [] ),
            *( ["--background-only"] if args.background_only else [] ),
            *( ["--verbose"] if args.verbose else [] ),
        ])
    if command == "zkill-worker":
        return run_zkill_worker([
            "--app-root", args.app_root,
            "--poll-sleep", str(args.poll_sleep),
            *( ["--once"] if args.once else [] ),
            *( ["--verbose"] if args.verbose else [] ),
        ])
    if command == "evewho-alliance-runner":
        return run_evewho_alliance_runner([
            "--app-root", args.app_root,
            "--loop-sleep", str(args.loop_sleep),
            *( ["--once"] if args.once else [] ),
            *( ["--verbose"] if args.verbose else [] ),
        ])
    if command == "killmail-backfill-runner":
        return run_killmail_backfill_runner([
            "--app-root", args.app_root,
            "--loop-sleep", str(args.loop_sleep),
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
            *( ["--verbose"] if args.verbose else [] ),
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
    if command == "influx-validate":
        from .influx_validate import validate_main as run_influx_validate
        return run_influx_validate([
            "--app-root", args.app_root,
            *(sum([["--dataset", dataset] for dataset in args.dataset], [])),
            "--days", str(args.days),
            *( ["--verbose"] if args.verbose else [] ),
        ])
    if command == "compute-buy-all":
        app_root = Path(args.app_root).resolve()
        config = load_php_runtime_config(app_root)
        configure_logging(verbose=args.verbose, log_file=config.log_file)
        from .db import SupplyCoreDb
        db = SupplyCoreDb(config.raw.get("db", {}))
        result = run_compute_buy_all(db)
        print(result)
        return 0
    if command == "compute-signals":
        app_root = Path(args.app_root).resolve()
        config = load_php_runtime_config(app_root)
        configure_logging(verbose=args.verbose, log_file=config.log_file)
        from .db import SupplyCoreDb
        db = SupplyCoreDb(config.raw.get("db", {}))
        result = run_compute_signals(db, influx_runtime(config.raw))
        print(result)
        return 0
    if command == "compute-economic-warfare":
        app_root = Path(args.app_root).resolve()
        config = load_php_runtime_config(app_root)
        configure_logging(verbose=args.verbose, log_file=config.log_file)
        from .db import SupplyCoreDb
        db = SupplyCoreDb(config.raw.get("db", {}))
        from .jobs.compute_economic_warfare import run_compute_economic_warfare
        result = run_compute_economic_warfare(db, influx_runtime(config.raw))
        print(result)
        return 0
    if command == "graph-universe-sync":
        app_root = Path(args.app_root).resolve()
        config = load_php_runtime_config(app_root)
        configure_logging(verbose=args.verbose, log_file=config.log_file)
        from .db import SupplyCoreDb
        db = SupplyCoreDb(config.raw.get("db", {}))
        from .jobs.graph_universe_sync import run_graph_universe_sync
        result = run_graph_universe_sync(db, neo4j_runtime(config.raw))
        print(result)
        return 0
    if command == "compute-graph-sync-killmail-entities":
        app_root = Path(args.app_root).resolve()
        config = load_php_runtime_config(app_root)
        configure_logging(verbose=args.verbose, log_file=config.log_file)
        from .db import SupplyCoreDb
        db = SupplyCoreDb(config.raw.get("db", {}))
        from .jobs.graph_pipeline import run_compute_graph_sync_killmail_entities
        result = run_compute_graph_sync_killmail_entities(db, neo4j_runtime(config.raw))
        print(result)
        return 0
    if command == "compute-graph-sync-killmail-edges":
        app_root = Path(args.app_root).resolve()
        config = load_php_runtime_config(app_root)
        configure_logging(verbose=args.verbose, log_file=config.log_file)
        from .db import SupplyCoreDb
        db = SupplyCoreDb(config.raw.get("db", {}))
        from .jobs.graph_pipeline import run_compute_graph_sync_killmail_edges
        result = run_compute_graph_sync_killmail_edges(db, neo4j_runtime(config.raw))
        print(result)
        return 0
    if command == "compute-graph-sync":
        app_root = Path(args.app_root).resolve()
        config = load_php_runtime_config(app_root)
        configure_logging(verbose=args.verbose, log_file=config.log_file)
        from .db import SupplyCoreDb
        db = SupplyCoreDb(config.raw.get("db", {}))
        result = run_compute_graph_sync(db, neo4j_runtime(config.raw))
        print(result)
        return 0
    if command == "compute-graph-insights":
        app_root = Path(args.app_root).resolve()
        config = load_php_runtime_config(app_root)
        configure_logging(verbose=args.verbose, log_file=config.log_file)
        from .db import SupplyCoreDb
        db = SupplyCoreDb(config.raw.get("db", {}))
        result = run_compute_graph_insights(db, neo4j_runtime(config.raw), influx_runtime(config.raw))
        print(result)
        return 0
    if command == "compute-graph-sync-doctrine-dependency":
        app_root = Path(args.app_root).resolve()
        config = load_php_runtime_config(app_root)
        configure_logging(verbose=args.verbose, log_file=config.log_file)
        from .db import SupplyCoreDb
        db = SupplyCoreDb(config.raw.get("db", {}))
        result = run_compute_graph_sync_doctrine_dependency(db, neo4j_runtime(config.raw))
        print(result)
        return 0
    if command == "compute-graph-sync-battle-intelligence":
        app_root = Path(args.app_root).resolve()
        config = load_php_runtime_config(app_root)
        configure_logging(verbose=args.verbose, log_file=config.log_file)
        from .db import SupplyCoreDb
        db = SupplyCoreDb(config.raw.get("db", {}))
        result = run_compute_graph_sync_battle_intelligence(db, neo4j_runtime(config.raw))
        print(result)
        return 0
    if command == "compute-graph-derived-relationships":
        app_root = Path(args.app_root).resolve()
        config = load_php_runtime_config(app_root)
        configure_logging(verbose=args.verbose, log_file=config.log_file)
        from .db import SupplyCoreDb
        db = SupplyCoreDb(config.raw.get("db", {}))
        result = run_compute_graph_derived_relationships(db, neo4j_runtime(config.raw))
        print(result)
        return 0
    if command == "compute-graph-prune":
        app_root = Path(args.app_root).resolve()
        config = load_php_runtime_config(app_root)
        configure_logging(verbose=args.verbose, log_file=config.log_file)
        from .db import SupplyCoreDb
        db = SupplyCoreDb(config.raw.get("db", {}))
        result = run_compute_graph_prune(db, neo4j_runtime(config.raw))
        print(result)
        return 0
    if command == "compute-graph-topology-metrics":
        app_root = Path(args.app_root).resolve()
        config = load_php_runtime_config(app_root)
        configure_logging(verbose=args.verbose, log_file=config.log_file)
        from .db import SupplyCoreDb
        db = SupplyCoreDb(config.raw.get("db", {}))
        result = run_compute_graph_topology_metrics(db, neo4j_runtime(config.raw))
        print(result)
        return 0
    if command == "compute-behavioral-baselines":
        app_root = Path(args.app_root).resolve()
        config = load_php_runtime_config(app_root)
        configure_logging(verbose=args.verbose, log_file=config.log_file)
        from .db import SupplyCoreDb
        db = SupplyCoreDb(config.raw.get("db", {}))
        result = run_compute_behavioral_baselines(db, battle_runtime(config.raw))
        print(result)
        return 0
    if command == "compute-suspicion-scores-v2":
        app_root = Path(args.app_root).resolve()
        config = load_php_runtime_config(app_root)
        configure_logging(verbose=args.verbose, log_file=config.log_file)
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
        configure_logging(verbose=args.verbose, log_file=config.log_file)
        ctx = BackfillContext(app_root=app_root, php_binary=config.php_binary)
        result = run_killmail_history_backfill(ctx)
        print(json.dumps(result, default=str))
        return 0 if result.get("status") == "success" else 1

    if command == "killmail-full-history-backfill":
        from dataclasses import dataclass

        @dataclass
        class FullBackfillContext:
            app_root: Path
            php_binary: str

        app_root = Path(args.app_root).resolve()
        config = load_php_runtime_config(app_root)
        configure_logging(verbose=args.verbose, log_file=config.log_file)
        ctx = FullBackfillContext(app_root=app_root, php_binary=config.php_binary)
        result = run_killmail_full_history_backfill(ctx)
        print(json.dumps(result, default=str))
        return 0 if result.get("status") == "success" else 1

    if command == "run-job":
        app_root = Path(args.app_root).resolve()
        config = load_php_runtime_config(app_root)
        configure_logging(verbose=args.verbose, log_file=config.log_file)
        from .db import SupplyCoreDb
        from .bridge import PhpBridge
        db = SupplyCoreDb(config.raw.get("db", {}))
        job_key = str(args.job_key).strip()
        if job_key not in PYTHON_PROCESSOR_JOB_KEYS:
            print(json.dumps({"status": "failed", "error": f"No Python-native processor registered for {job_key}."}))
            return 1
        started_at = time.monotonic()
        try:
            result = run_registered_processor(job_key, db, config.raw, verbose=getattr(args, "verbose", False))
        except Exception as exc:
            import traceback
            result = {"status": "failed", "error_text": f"{type(exc).__name__}: {exc}", "traceback": traceback.format_exc(), "rows_processed": 0, "rows_written": 0}
        _print_cli_result(job_key, started_at, result)
        try:
            bridge = PhpBridge(config.php_binary, app_root)
            bridge.call("finalize-job-by-key", args=[f"--job-key={job_key}"], payload=result)
        except Exception:
            pass
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
        configure_logging(verbose=args.verbose, log_file=config.log_file)
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

    if command == "evewho-enrichment-sync":
        app_root = Path(args.app_root).resolve()
        config = load_php_runtime_config(app_root)
        configure_logging(verbose=args.verbose, log_file=config.log_file)
        from .db import SupplyCoreDb
        db = SupplyCoreDb(config.raw.get("db", {}))
        from .jobs.evewho_enrichment_sync import run_evewho_enrichment_sync
        started_at = time.monotonic()
        result = run_evewho_enrichment_sync(db, neo4j_runtime(config.raw), battle_runtime(config.raw), dry_run=bool(args.dry_run))
        _print_cli_result(command, started_at, result)
        return 0 if str(result.get("status") or "success") != "failed" else 1

    if command == "scheduler-graph":
        configure_logging(verbose=args.verbose)
        from .scheduling_graph import (
            build_graph,
            format_graph_summary,
            validate_graph,
        )
        from .worker_registry import WORKER_JOB_DEFINITIONS as _defs

        if args.validate:
            nodes = build_graph(_defs)
            issues = validate_graph(nodes)
            if issues:
                for issue in issues:
                    print(f"  ERROR: {issue}")
                return 1
            print(f"Graph valid: {len(nodes)} jobs, no cycles or missing dependencies.")
            return 0

        if args.output_json:
            from .scheduling_graph import _topological_tiers
            nodes = build_graph(_defs)
            tiers, job_tier = _topological_tiers(nodes)
            output = {
                "job_count": len(nodes),
                "tier_count": len(tiers),
                "tiers": {
                    str(i): tier_jobs for i, tier_jobs in enumerate(tiers)
                },
                "jobs": {
                    k: {
                        "depends_on": list(n.depends_on),
                        "concurrency_group": n.concurrency_group,
                        "priority": n.priority,
                        "resource_cost": n.resource_cost,
                        "tier": job_tier[k],
                    }
                    for k, n in sorted(nodes.items())
                },
            }
            print(json.dumps(output, indent=2))
            return 0

        print(format_graph_summary(_defs))
        return 0

    app_root = Path(args.app_root).resolve()
    config = load_php_runtime_config(app_root)
    logger = configure_logging(verbose=args.verbose, log_file=config.log_file)
    return run_supervisor(app_root=app_root, logger=logger)
