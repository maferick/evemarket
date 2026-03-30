#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path


def main() -> int:
    repo_root = Path(__file__).resolve().parents[1]
    python_root = repo_root / "python"
    if str(python_root) not in sys.path:
        sys.path.insert(0, str(python_root))

    try:
        from orchestrator.main import main as orchestrator_main
    except Exception as exc:
        import traceback
        print(f"FATAL: Failed to import orchestrator: {exc}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        return 1

    sys.argv = [sys.argv[0], *sys.argv[1:]]

    try:
        return orchestrator_main()
    except Exception as exc:
        import traceback
        print(f"FATAL: Unhandled exception in orchestrator: {exc}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
