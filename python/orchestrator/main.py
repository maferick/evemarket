from __future__ import annotations

import argparse
from pathlib import Path

from .config import load_php_runtime_config
from .logging_utils import configure_logging
from .rebuild_data_model import main as run_rebuild_data_model
from .supervisor import run_supervisor
from .worker_pool import main as run_worker_pool
from .zkill_worker import main as run_zkill_worker


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
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    command = args.command or "supervisor"
    if command == "worker-pool":
        return run_worker_pool([
            "--app-root", args.app_root,
            "--worker-id", args.worker_id,
            "--queues", args.queues,
            "--workload-classes", args.workload_classes,
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

    app_root = Path(args.app_root).resolve()
    config = load_php_runtime_config(app_root)
    logger = configure_logging(verbose=args.verbose, log_file=config.log_file)
    return run_supervisor(app_root=app_root, logger=logger)
