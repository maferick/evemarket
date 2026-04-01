"""Battle type classification — categorize battles by tactical type.

Reads ``battle_rollups``, ``battle_participants``, ``killmail_events``,
and ``killmail_attackers`` to classify each battle into one of:

  * ``camp``       — few victims, many attackers, short duration, low diversity
  * ``roam``       — spread across multiple systems nearby, moderate size
  * ``defense``    — one side is local (has structures/sov), asymmetric sides
  * ``timer``      — high capital usage, large-scale, sustained duration
  * ``third_party`` — 3+ distinct alliances on 3+ sides
  * ``skirmish``   — default; small/medium fight not matching other patterns

Classification is heuristic-based using weighted feature scoring.
Output is written to ``battle_type_classifications``.
"""

from __future__ import annotations

import hashlib
import json
import time
from datetime import UTC, datetime
from typing import Any

from ..db import SupplyCoreDb
from ..job_result import JobResult
from ..job_utils import finish_job_run, start_job_run
from ..json_utils import json_dumps_safe

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BATCH_SIZE = 500

# Duration thresholds (seconds)
CAMP_MAX_DURATION = 120          # camps are usually very short
TIMER_MIN_DURATION = 600         # timer fights last 10+ minutes
TIMER_MIN_PARTICIPANTS = 50

# Side asymmetry
CAMP_ATTACKER_VICTIM_RATIO = 5.0  # 5:1 attackers to victims = camp
THIRD_PARTY_MIN_SIDES = 3

# Capital thresholds
TIMER_MIN_CAPITAL_RATIO = 0.10    # 10%+ capitals suggests a timer


def _now_sql() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")


def _classify(features: dict[str, Any]) -> tuple[str, float]:
    """Return (battle_type, confidence) based on extracted features."""
    duration = features.get("duration_seconds", 0)
    participant_count = features.get("participant_count", 0)
    unique_alliances = features.get("unique_alliances", 0)
    unique_sides = features.get("unique_sides", 0)
    attacker_count = features.get("attacker_count", 0)
    victim_count = features.get("victim_count", 0)
    capital_ratio = features.get("capital_ratio", 0.0)
    side_imbalance = features.get("side_imbalance", 0.0)
    system_count = features.get("system_count", 1)

    # Third-party detection: 3+ distinct alliance-sides
    if unique_sides >= THIRD_PARTY_MIN_SIDES and unique_alliances >= 3:
        confidence = min(1.0, 0.6 + 0.1 * (unique_sides - 3))
        return ("third_party", round(confidence, 4))

    # Camp detection: short, one-sided, few victims
    if (duration <= CAMP_MAX_DURATION
            and victim_count > 0
            and attacker_count / max(victim_count, 1) >= CAMP_ATTACKER_VICTIM_RATIO
            and participant_count < 50):
        confidence = min(1.0, 0.7 + 0.1 * (attacker_count / max(victim_count, 1) - 5))
        return ("camp", round(max(0.5, confidence), 4))

    # Timer detection: long, large, capital-heavy
    if (duration >= TIMER_MIN_DURATION
            and participant_count >= TIMER_MIN_PARTICIPANTS
            and capital_ratio >= TIMER_MIN_CAPITAL_RATIO):
        confidence = min(1.0, 0.6 + 0.15 * capital_ratio + 0.002 * participant_count)
        return ("timer", round(confidence, 4))

    # Defense detection: strong side imbalance, moderate+ size
    if side_imbalance >= 0.65 and participant_count >= 20:
        confidence = min(1.0, 0.5 + 0.3 * side_imbalance)
        return ("defense", round(confidence, 4))

    # Roam detection: multiple nearby systems, moderate size
    if system_count >= 2 and participant_count < 50:
        confidence = min(1.0, 0.5 + 0.1 * system_count)
        return ("roam", round(confidence, 4))

    # Default: skirmish
    confidence = 0.5
    return ("skirmish", confidence)


def run_battle_type_classification(
    db: SupplyCoreDb,
    runtime: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Classify unclassified battles by tactical type."""
    lock_key = "battle_type_classification"
    job = start_job_run(db, lock_key)
    started = time.perf_counter()
    rows_processed = 0
    rows_written = 0
    computed_at = _now_sql()

    try:
        while True:
            # Fetch battles not yet classified or outdated
            battles = db.fetch_all(
                """
                SELECT br.battle_id, br.system_id, br.started_at, br.ended_at,
                       br.duration_seconds, br.participant_count, br.battle_size_class
                FROM battle_rollups br
                LEFT JOIN battle_type_classifications btc ON btc.battle_id = br.battle_id
                WHERE btc.battle_id IS NULL
                ORDER BY br.started_at DESC
                LIMIT %s
                """,
                (BATCH_SIZE,),
            )
            if not battles:
                break

            battle_ids = [r["battle_id"] for r in battles]
            placeholders = ",".join(["%s"] * len(battle_ids))

            # Fetch participant details per battle
            participants = db.fetch_all(
                f"""
                SELECT battle_id, side_key, alliance_id, corporation_id,
                       ship_type_id, is_capital
                FROM battle_participants
                WHERE battle_id IN ({placeholders})
                """,
                battle_ids,
            )

            # Fetch attacker/victim counts per battle
            km_stats = db.fetch_all(
                f"""
                SELECT ke.battle_id,
                       COUNT(DISTINCT ka.character_id) AS attacker_count,
                       COUNT(DISTINCT ke.victim_character_id) AS victim_count
                FROM killmail_events ke
                LEFT JOIN killmail_attackers ka ON ka.sequence_id = ke.sequence_id
                WHERE ke.battle_id IN ({placeholders})
                GROUP BY ke.battle_id
                """,
                battle_ids,
            )

            # Fetch system count per battle (how many unique systems had killmails)
            sys_stats = db.fetch_all(
                f"""
                SELECT battle_id, COUNT(DISTINCT solar_system_id) AS system_count
                FROM killmail_events
                WHERE battle_id IN ({placeholders})
                GROUP BY battle_id
                """,
                battle_ids,
            )

            # Index lookup tables
            part_by_battle: dict[str, list[dict]] = {}
            for p in participants:
                part_by_battle.setdefault(p["battle_id"], []).append(p)

            km_by_battle = {r["battle_id"]: r for r in km_stats}
            sys_by_battle = {r["battle_id"]: r for r in sys_stats}

            insert_rows = []
            for battle in battles:
                bid = battle["battle_id"]
                parts = part_by_battle.get(bid, [])
                km = km_by_battle.get(bid, {})
                sys_info = sys_by_battle.get(bid, {})

                # Compute features
                unique_sides = len({p["side_key"] for p in parts if p.get("side_key")})
                unique_alliances = len({p["alliance_id"] for p in parts if p.get("alliance_id") and int(p["alliance_id"]) > 0})
                capital_count = sum(1 for p in parts if p.get("is_capital"))
                total = max(len(parts), 1)

                # Side imbalance: ratio of largest side to total
                side_counts: dict[str, int] = {}
                for p in parts:
                    sk = p.get("side_key", "unknown")
                    side_counts[sk] = side_counts.get(sk, 0) + 1
                max_side = max(side_counts.values()) if side_counts else 0
                side_imbalance = max_side / total if total > 0 else 0.0

                features = {
                    "duration_seconds": int(battle.get("duration_seconds") or 0),
                    "participant_count": int(battle.get("participant_count") or 0),
                    "unique_alliances": unique_alliances,
                    "unique_sides": unique_sides,
                    "attacker_count": int(km.get("attacker_count") or 0),
                    "victim_count": int(km.get("victim_count") or 0),
                    "capital_ratio": capital_count / total,
                    "capital_count": capital_count,
                    "side_imbalance": round(side_imbalance, 4),
                    "system_count": int(sys_info.get("system_count") or 1),
                    "battle_size_class": str(battle.get("battle_size_class") or "small"),
                }

                battle_type, confidence = _classify(features)

                insert_rows.append((
                    bid, battle_type, confidence,
                    json_dumps_safe(features), computed_at,
                    battle_type, confidence,
                    json_dumps_safe(features), computed_at,
                ))

            rows_processed += len(battles)

            if insert_rows:
                db.execute_many(
                    """
                    INSERT INTO battle_type_classifications
                        (battle_id, battle_type, confidence, features_json, computed_at)
                    VALUES (%s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        battle_type = %s, confidence = %s,
                        features_json = %s, computed_at = %s
                    """,
                    insert_rows,
                )
                rows_written += len(insert_rows)

        finish_job_run(db, job, status="success",
                       rows_processed=rows_processed, rows_written=rows_written)
        return {
            "status": "success",
            "rows_processed": rows_processed,
            "rows_written": rows_written,
            "duration_ms": int((time.perf_counter() - started) * 1000),
        }
    except Exception as exc:
        finish_job_run(db, job, status="failed",
                       rows_processed=rows_processed, rows_written=rows_written,
                       error_text=str(exc))
        return {"status": "failed", "error_text": str(exc),
                "rows_processed": rows_processed, "rows_written": rows_written}
