#!/usr/bin/env python3
"""Quick diagnostic: check why orchestrator services fail to start."""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


def main() -> int:
    repo_root = Path(__file__).resolve().parents[1]
    python_root = repo_root / "python"
    if str(python_root) not in sys.path:
        sys.path.insert(0, str(python_root))

    errors: list[str] = []

    # 1. Check Python imports
    print("=== 1. Testing Python imports ===")
    try:
        from orchestrator.main import main as _  # noqa: F401
        print("  OK: orchestrator.main imports successfully")
    except Exception as exc:
        errors.append(f"Import failed: {exc}")
        print(f"  FAIL: {exc}")
        import traceback
        traceback.print_exc()
        return 1

    # 2. Check PHP config
    print("\n=== 2. Testing PHP config ===")
    php_script = repo_root / "bin/orchestrator_config.php"
    if not php_script.is_file():
        errors.append(f"Missing: {php_script}")
        print(f"  FAIL: {php_script} not found")
    else:
        try:
            result = subprocess.run(
                ["php", str(php_script)],
                capture_output=True, text=True, timeout=10,
                cwd=str(repo_root),
            )
            if result.returncode != 0:
                errors.append(f"PHP config exit code {result.returncode}")
                print(f"  FAIL: exit code {result.returncode}")
                if result.stderr:
                    print(f"  STDERR: {result.stderr[:500]}")
                if result.stdout:
                    print(f"  STDOUT: {result.stdout[:500]}")
            else:
                try:
                    cfg = json.loads(result.stdout)
                    db_cfg = cfg.get("db", {})
                    pw = db_cfg.get("password", "")
                    print(f"  OK: PHP config loaded, db.password={'set' if pw else 'EMPTY'} ({len(pw)} chars)")
                    if not pw:
                        errors.append("DB password is empty in PHP config output")
                except json.JSONDecodeError as je:
                    errors.append(f"PHP config output is not valid JSON: {je}")
                    print(f"  FAIL: invalid JSON: {je}")
                    print(f"  Raw output: {result.stdout[:300]}")
        except Exception as exc:
            errors.append(f"PHP config execution error: {exc}")
            print(f"  FAIL: {exc}")

    # 3. Check Python config loading
    print("\n=== 3. Testing Python config loading ===")
    try:
        from orchestrator.config import load_php_runtime_config
        app_root = Path(sys.argv[1] if len(sys.argv) > 1 else str(repo_root)).resolve()
        config = load_php_runtime_config(app_root)
        db_cfg = config.raw.get("db", {})
        pw = db_cfg.get("password", "")
        print(f"  OK: Config loaded, db.password={'set' if pw else 'EMPTY'} ({len(pw)} chars)")
        if not pw:
            errors.append("DB password is empty in Python config")
    except Exception as exc:
        errors.append(f"Python config load failed: {exc}")
        print(f"  FAIL: {exc}")
        import traceback
        traceback.print_exc()

    # 4. Check DB connectivity
    print("\n=== 4. Testing DB connection ===")
    try:
        from orchestrator.db import SupplyCoreDb
        db = SupplyCoreDb(config.raw.get("db", {}))
        conn = db.connect()
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        conn.close()
        print("  OK: DB connection successful")
    except Exception as exc:
        errors.append(f"DB connection failed: {exc}")
        print(f"  FAIL: {exc}")

    # 5. Check local.php
    print("\n=== 5. Checking local.php ===")
    local_php = repo_root / "src/config/local.php"
    if local_php.is_file():
        print(f"  OK: {local_php} exists")
    else:
        print(f"  INFO: {local_php} does not exist (server may need it for DB password)")

    # Summary
    print(f"\n=== Summary: {len(errors)} issue(s) found ===")
    for err in errors:
        print(f"  - {err}")

    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
