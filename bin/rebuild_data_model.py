#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path


def main() -> int:
    repo_root = Path(__file__).resolve().parents[1]
    python_root = repo_root / "python"
    if str(python_root) not in sys.path:
        sys.path.insert(0, str(python_root))

    from orchestrator.rebuild_data_model import main as rebuild_main

    return rebuild_main(sys.argv[1:])


if __name__ == "__main__":
    raise SystemExit(main())
