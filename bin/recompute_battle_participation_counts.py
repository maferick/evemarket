#!/usr/bin/env python3
"""Recompute `battle_participants.participation_count` from raw killmail data.

The `compute_battle_rollups` job historically accumulated `participation_count`
via `ON DUPLICATE KEY UPDATE participation_count = participation_count + …` and
the cursor-rewind logic in `_validate_killmail_cursor` could cause the same
killmails to be processed multiple times across runs.  When that happens, the
per-character count drifts upward and the per-side SUM in
`compute_battle_anomalies` eventually overflows the column type, manifesting as
auto-log issues like:

    DataError: (1264, "Out of range value for column 'participant_count' at row N")

The 20260410 schema migration widens both columns to `BIGINT UNSIGNED` so the
crash can no longer happen, and `compute_battle_rollups` is now idempotent
(killmails with `battle_id IS NOT NULL` are excluded from the rollup SELECT).
This script is the one-shot cleanup for databases that already accumulated
inflated values: it rebuilds `participation_count` from scratch by counting how
many `killmail_events` rows reference each `(battle_id, character_id)` pair as
either victim or attacker.

Usage:
    python bin/recompute_battle_participation_counts.py [--app-root /var/www/SupplyCore]
                                                        [--dry-run]
                                                        [--limit 100000]
                                                        [--threshold 1000000]

Flags:
    --dry-run    Report drift without writing.
    --threshold  Only rewrite rows whose current count exceeds this value
                 (default: 0, i.e. rewrite every drift).
    --limit      Cap the number of rows updated per pass (default: unlimited).
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description="Recompute battle_participants.participation_count")
    parser.add_argument("--app-root", default=None,
                        help="Path to SupplyCore root (default: auto-detect)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Report drift without writing")
    parser.add_argument("--threshold", type=int, default=0,
                        help="Only rewrite rows whose stored count exceeds this value (default: 0)")
    parser.add_argument("--limit", type=int, default=0,
                        help="Maximum rows to update (0 = unlimited)")
    args = parser.parse_args()

    repo_root = Path(args.app_root).resolve() if args.app_root else Path(__file__).resolve().parents[1]
    python_root = repo_root / "python"
    if str(python_root) not in sys.path:
        sys.path.insert(0, str(python_root))

    from orchestrator.config import load_php_runtime_config
    from orchestrator.db import SupplyCoreDb

    raw_config = load_php_runtime_config(repo_root).raw
    db = SupplyCoreDb(dict(raw_config.get("db") or {}))

    # ── Step 1: Compute the actual count per (battle_id, character_id) ───
    # We union victim and attacker contributions, group, and compare against
    # the stored value.  The query is bounded by the rollup itself: only
    # killmails with `battle_id IS NOT NULL` participate.
    drift_rows = db.fetch_all(
        """
        SELECT bp.battle_id,
               bp.character_id,
               bp.participation_count AS stored,
               COALESCE(actual.total, 0) AS actual
          FROM battle_participants bp
          LEFT JOIN (
              SELECT battle_id, character_id, SUM(cnt) AS total
                FROM (
                    SELECT battle_id,
                           victim_character_id AS character_id,
                           COUNT(*) AS cnt
                      FROM killmail_events
                     WHERE battle_id IS NOT NULL
                       AND victim_character_id > 0
                  GROUP BY battle_id, victim_character_id
                  UNION ALL
                    SELECT ke.battle_id,
                           ka.character_id,
                           COUNT(*) AS cnt
                      FROM killmail_events ke
                      JOIN killmail_attackers ka ON ka.sequence_id = ke.sequence_id
                     WHERE ke.battle_id IS NOT NULL
                       AND ka.character_id > 0
                  GROUP BY ke.battle_id, ka.character_id
                ) parts
            GROUP BY battle_id, character_id
          ) actual
            ON actual.battle_id = bp.battle_id
           AND actual.character_id = bp.character_id
         WHERE bp.participation_count > %s
           AND bp.participation_count <> COALESCE(actual.total, 0)
         ORDER BY bp.participation_count DESC
        """,
        (max(0, args.threshold),),
    )

    if args.limit > 0:
        drift_rows = drift_rows[: args.limit]

    if not drift_rows:
        print("No drift detected; participation_count is already correct.")
        return 0

    print(f"Found {len(drift_rows)} battle_participants row(s) with drift.\n")
    print(f"  {'battle_id':<64s}  {'char_id':>12s}  {'stored':>15s}  {'actual':>15s}")
    print(f"  {'─' * 64}  {'─' * 12}  {'─' * 15}  {'─' * 15}")
    for row in drift_rows[:25]:
        print(
            f"  {str(row['battle_id']):<64s}  "
            f"{int(row['character_id']):>12d}  "
            f"{int(row['stored']):>15d}  "
            f"{int(row['actual']):>15d}"
        )
    if len(drift_rows) > 25:
        print(f"  … and {len(drift_rows) - 25} more")

    if args.dry_run:
        print(f"\n[dry-run] Would rewrite {len(drift_rows)} row(s). Pass without --dry-run to apply.")
        return 0

    # ── Step 2: Apply the corrected counts ───────────────────────────────
    update_sql = (
        "UPDATE battle_participants "
        "   SET participation_count = %s "
        " WHERE battle_id = %s AND character_id = %s"
    )
    written = 0
    for row in drift_rows:
        db.execute(update_sql, (int(row["actual"]), row["battle_id"], int(row["character_id"])))
        written += 1

    print(f"\nRewrote participation_count on {written} row(s).")
    print("Re-run `compute_battle_anomalies` to refresh `battle_side_metrics`.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
