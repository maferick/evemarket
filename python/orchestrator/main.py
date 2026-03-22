from __future__ import annotations

import argparse
from pathlib import Path

from .logging_utils import configure_logging
from .supervisor import run_supervisor


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the SupplyCore Python orchestrator")
    parser.add_argument(
        "--app-root",
        default=str(Path(__file__).resolve().parents[2]),
        help="Path to the SupplyCore repository/app root.",
    )
    parser.add_argument("--verbose", action="store_true", help="Enable verbose orchestrator logging.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    logger = configure_logging(verbose=args.verbose)
    return run_supervisor(app_root=Path(args.app_root), logger=logger)
