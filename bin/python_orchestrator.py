#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path


def main() -> int:
    repo_root = Path(__file__).resolve().parents[1]
    python_root = repo_root / "python"
    if str(python_root) not in sys.path:
        sys.path.insert(0, str(python_root))

    from orchestrator.main import main as orchestrator_main

    sys.argv = [sys.argv[0], *sys.argv[1:]]
    return orchestrator_main()


if __name__ == "__main__":
    raise SystemExit(main())
