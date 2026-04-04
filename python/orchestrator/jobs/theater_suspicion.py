"""Theater Intelligence — Phase 5: Suspicion integration.

Joins existing character suspicion signals (from the intelligence pipeline)
with theater participants to produce per-theater suspicion summaries and
flag suspicious characters within theater context.

Populates: ``theater_suspicion_summary``
Updates:   ``theater_participants`` (suspicion_score, is_suspicious)
"""

from __future__ import annotations

import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from orchestrator.config import resolve_app_root  # noqa: F401
    from orchestrator.db import SupplyCoreDb
    from orchestrator.job_result import JobResult
    from orchestrator.json_utils import json_dumps_safe
    from orchestrator.job_utils import finish_job_run, start_job_run
else:
    from ..config import resolve_app_root  # noqa: F401
    from ..db import SupplyCoreDb
    from ..job_result import JobResult
    from ..json_utils import json_dumps_safe
    from ..job_utils import finish_job_run, start_job_run

SUSPICION_THRESHOLD = 0.5
BATCH_SIZE = 500


def _now_sql() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")


def _theater_log(runtime: dict[str, Any] | None, event: str, payload: dict[str, Any]) -> None:
    log_path = str(((runtime or {}).get("log_file") or "")).strip()
    if log_path == "":
        return
    path = Path(log_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    record = {"event": event, "timestamp": datetime.now(UTC).isoformat(), **payload}
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json_dumps_safe(record) + "\n")


def _load_theaters(db: SupplyCoreDb) -> list[dict[str, Any]]:
    return db.fetch_all("SELECT theater_id FROM theaters ORDER BY start_time ASC")


def _load_theater_participants(db: SupplyCoreDb, theater_id: str) -> list[dict[str, Any]]:
    return db.fetch_all(
        """
        SELECT character_id, alliance_id, side
        FROM theater_participants
        WHERE theater_id = %s
        """,
        (theater_id,),
    )


def _load_suspicion_scores(db: SupplyCoreDb, character_ids: list[int]) -> dict[int, float]:
    """Load the latest suspicion scores for the given characters.

    Tries character_suspicion_signals first (from intelligence pipeline),
    falls back to battle_actor_features if needed.
    """
    if not character_ids:
        return {}

    result: dict[int, float] = {}

    # Try character_suspicion_signals (primary source)
    for offset in range(0, len(character_ids), BATCH_SIZE):
        chunk = character_ids[offset:offset + BATCH_SIZE]
        placeholders = ",".join(["%s"] * len(chunk))
        rows = db.fetch_all(
            f"""
            SELECT character_id, suspicion_score
            FROM character_suspicion_signals
            WHERE character_id IN ({placeholders})
            """,
            tuple(chunk),
        )
        for r in rows:
            cid = int(r.get("character_id") or 0)
            score = float(r.get("suspicion_score") or 0)
            if cid > 0:
                result[cid] = score

    # For characters not in suspicion_signals, derive a score from battle_actor_features
    missing = [cid for cid in character_ids if cid not in result]
    if missing:
        for offset in range(0, len(missing), BATCH_SIZE):
            chunk = missing[offset:offset + BATCH_SIZE]
            placeholders = ",".join(["%s"] * len(chunk))
            rows = db.fetch_all(
                f"""
                SELECT character_id,
                       MAX(0.5 * COALESCE(centrality_score, 0) + 0.5 * COALESCE(visibility_score, 0)) AS derived_score
                FROM battle_actor_features
                WHERE character_id IN ({placeholders})
                GROUP BY character_id
                """,
                tuple(chunk),
            )
            for r in rows:
                cid = int(r.get("character_id") or 0)
                score = float(r.get("derived_score") or 0)
                if cid > 0:
                    result[cid] = score

    return result


def _load_tracked_alliance_ids(db: SupplyCoreDb) -> set[int]:
    """Load friendly alliance IDs from ESI contacts (positive standing)."""
    try:
        rows = db.fetch_all(
            "SELECT contact_id AS alliance_id FROM corp_contacts WHERE contact_type = 'alliance' AND standing > 0"
        )
        return {int(r["alliance_id"]) for r in rows if int(r.get("alliance_id") or 0) > 0}
    except Exception:
        return set()


# ── Entry point ─────────────────────────────────────────────────────────────

def run_theater_suspicion(
    db: SupplyCoreDb,
    runtime: dict[str, Any] | None = None,
    *,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Integrate suspicion signals into theater participants."""
    job = start_job_run(db, "theater_suspicion")
    started_monotonic = datetime.now(UTC)
    rows_processed = 0
    rows_written = 0
    computed_at = _now_sql()
    _theater_log(runtime, "theater_suspicion.job.started", {"dry_run": dry_run})

    try:
        theaters = _load_theaters(db)
        tracked_alliances = _load_tracked_alliance_ids(db)
        rows_processed = len(theaters)

        if not theaters:
            finish_job_run(db, job, status="success", rows_processed=0, rows_written=0)
            duration_ms = int((datetime.now(UTC) - started_monotonic).total_seconds() * 1000)
            return JobResult.success(
                job_key="theater_suspicion", summary="No theaters.", rows_processed=0, rows_written=0, duration_ms=duration_ms,
            ).to_dict()

        theaters_processed = 0
        for theater in theaters:
            theater_id = str(theater["theater_id"])
            participants = _load_theater_participants(db, theater_id)
            if not participants:
                continue

            char_ids = [int(p["character_id"]) for p in participants]
            suspicion_map = _load_suspicion_scores(db, char_ids)

            suspicious_count = 0
            tracked_suspicious_count = 0
            max_score = 0.0
            total_score = 0.0
            scored_count = 0

            if not dry_run:
                # Pre-compute all update tuples and stats in one pass
                update_batch: list[tuple[Any, ...]] = []
                for p in participants:
                    cid = int(p["character_id"])
                    aid = int(p.get("alliance_id") or 0)
                    score = suspicion_map.get(cid)
                    is_suspicious = 1 if (score is not None and score >= SUSPICION_THRESHOLD) else 0

                    if score is not None:
                        scored_count += 1
                        total_score += score
                        max_score = max(max_score, score)
                        if is_suspicious:
                            suspicious_count += 1
                            if aid in tracked_alliances:
                                tracked_suspicious_count += 1

                    update_batch.append((score, is_suspicious, theater_id, cid))

                with db.transaction_with_retry() as (_, cursor):
                    # Batch UPDATE all participants at once
                    if update_batch:
                        cursor.executemany(
                            """
                            UPDATE theater_participants
                            SET suspicion_score = %s, is_suspicious = %s
                            WHERE theater_id = %s AND character_id = %s
                            """,
                            update_batch,
                        )
                        rows_written += len(update_batch)

                    # Upsert theater_suspicion_summary
                    avg_score = (total_score / scored_count) if scored_count > 0 else 0.0
                    cursor.execute("DELETE FROM theater_suspicion_summary WHERE theater_id = %s", (theater_id,))
                    cursor.execute(
                        """
                        INSERT INTO theater_suspicion_summary (
                            theater_id, suspicious_character_count,
                            tracked_alliance_suspicious_count,
                            max_suspicion_score, avg_suspicion_score,
                            anomaly_flags_json, computed_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            theater_id, suspicious_count,
                            tracked_suspicious_count,
                            round(max_score, 4), round(avg_score, 4),
                            None,  # anomaly_flags_json — future use
                            computed_at,
                        ),
                    )
                    rows_written += 1
            else:
                # Dry run — just count
                for p in participants:
                    cid = int(p["character_id"])
                    score = suspicion_map.get(cid)
                    if score is not None:
                        scored_count += 1
                        total_score += score
                        max_score = max(max_score, score)
                        if score >= SUSPICION_THRESHOLD:
                            suspicious_count += 1

            theaters_processed += 1
            _theater_log(runtime, "theater_suspicion.theater_done", {
                "theater_id": theater_id,
                "participants": len(participants),
                "suspicious": suspicious_count,
                "max_score": round(max_score, 4),
            })

        finish_job_run(db, job, status="success", rows_processed=rows_processed, rows_written=rows_written,
                       meta={"theaters_processed": theaters_processed})

        duration_ms = int((datetime.now(UTC) - started_monotonic).total_seconds() * 1000)
        result = JobResult.success(
            job_key="theater_suspicion",
            summary=f"Integrated suspicion for {theaters_processed} theaters.",
            rows_processed=rows_processed,
            rows_written=0 if dry_run else rows_written,
            duration_ms=duration_ms,
            meta={"theaters_processed": theaters_processed, "dry_run": dry_run},
        ).to_dict()
        _theater_log(runtime, "theater_suspicion.job.success", result)
        return result

    except Exception as exc:
        finish_job_run(db, job, status="failed", rows_processed=rows_processed, rows_written=rows_written, error_text=str(exc))
        _theater_log(runtime, "theater_suspicion.job.failed", {"error": str(exc)})
        raise
