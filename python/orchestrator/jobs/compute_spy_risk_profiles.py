"""Character spy risk profiles — explainable per-character spy-risk score.

Phase 5 of the spy detection platform.  Fuses counterintel evidence, graph
centrality, identity-resolution context, and ring-case context into a
transparent weighted score with per-component evidence rows.

Output tables: ``character_spy_risk_profiles``, ``character_spy_risk_evidence``.
"""

from __future__ import annotations

import math
import time
import uuid
from datetime import UTC, datetime
from typing import Any

from ..db import SupplyCoreDb
from ..job_result import JobResult
from ..job_utils import finish_job_run, start_job_run
from ..json_utils import json_dumps_safe

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
MODEL_VERSION = "spy_risk_v1"
BATCH_SIZE = 500
MAX_EVIDENCE_PER_CHAR = 20

SEVERITY_THRESHOLDS = {"critical": 0.85, "high": 0.70, "medium": 0.50}

# Component weights — sum to 1.0
WEIGHTS = {
    "bridge_infiltration": 0.15,
    "pre_op_infiltration": 0.10,
    "hostile_overlap": 0.15,
    "temporal_coordination": 0.08,
    "identity_association": 0.12,
    "ring_context": 0.12,
    "behavioral_anomaly": 0.10,
    "org_movement": 0.10,
    "predicted_hostile_link": 0.08,
}


def _now_sql() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")


def _f(v: Any, default: float = 0.0) -> float:
    if v is None:
        return default
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _severity(score: float) -> str:
    if score >= SEVERITY_THRESHOLDS["critical"]:
        return "critical"
    if score >= SEVERITY_THRESHOLDS["high"]:
        return "high"
    if score >= SEVERITY_THRESHOLDS["medium"]:
        return "medium"
    return "monitor"


def _confidence_tier(score: float) -> str:
    if score >= 0.75:
        return "high"
    if score >= 0.50:
        return "medium"
    return "low"


# ---------------------------------------------------------------------------
# Main entry
# ---------------------------------------------------------------------------

def run_compute_spy_risk_profiles(
    db: SupplyCoreDb,
    neo4j_raw: dict[str, Any] | None = None,
    runtime: dict[str, Any] | None = None,
    *,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    lock_key = "compute_spy_risk_profiles"
    job = start_job_run(db, lock_key)
    started = time.perf_counter()
    computed_at = _now_sql()
    run_id = f"srp_{uuid.uuid4().hex[:16]}"
    runtime = runtime or {}
    max_chars = min(200_000, int(runtime.get("spy_risk_max_characters") or 50_000))

    rows_processed = 0
    rows_written = 0

    try:
        # Candidate universe: anyone with counterintel features
        char_ids = _fetch_candidate_ids(db, max_chars)
        if not char_ids:
            result = JobResult.success(
                job_key=lock_key, summary="No candidates.",
                rows_processed=0, rows_written=0,
                duration_ms=int((time.perf_counter() - started) * 1000),
                meta={"run_id": run_id},
            ).to_dict()
            finish_job_run(db, job, status="success", rows_processed=0, rows_written=0, meta=result)
            return result

        # Bulk-load all scoring data
        ci_features = _bulk_kv(db, "character_counterintel_features", "character_id", char_ids)
        ci_scores = _bulk_kv(db, "character_counterintel_scores", "character_id", char_ids)
        gml_features = _bulk_kv_latest(db, "graph_ml_features", "character_id", char_ids)
        gca = _bulk_kv(db, "graph_community_assignments", "character_id", char_ids)
        bhv = _bulk_kv(db, "character_behavioral_scores", "character_id", char_ids)
        org = _bulk_kv(db, "character_org_history_cache", "character_id", char_ids)
        id_links = _load_identity_max_scores(db, char_ids)
        case_scores = _load_case_scores(db, char_ids)
        link_preds = _load_hostile_link_scores(db, char_ids)

        # Compute percentile ranks across all candidates (for risk_percentile)
        all_scores: list[tuple[int, float]] = []

        profile_rows: list[tuple[Any, ...]] = []
        evidence_rows: list[tuple[Any, ...]] = []

        for cid in char_ids:
            rows_processed += 1
            components, evidences = _score_character(
                cid, ci_features.get(cid), ci_scores.get(cid),
                gml_features.get(cid), gca.get(cid), bhv.get(cid),
                org.get(cid), id_links.get(cid, 0.0),
                case_scores.get(cid, (0.0, None)),
                link_preds.get(cid, 0.0),
            )

            spy_risk = sum(WEIGHTS.get(k, 0) * v for k, v in components.items())
            spy_risk = min(1.0, max(0.0, spy_risk))
            all_scores.append((cid, spy_risk))

            # Evidence count as confidence proxy
            nonzero = sum(1 for v in components.values() if v > 0.01)
            conf = min(1.0, nonzero / len(WEIGHTS))

            case_ring_score, top_case_id = case_scores.get(cid, (0.0, None))

            explanation = json_dumps_safe({
                "top_evidence": evidences[:5],
                "component_scores": {k: round(v, 6) for k, v in components.items()},
            })

            profile_rows.append((
                cid, round(spy_risk, 6), 0.0,  # percentile filled below
                round(conf, 6), _confidence_tier(conf), _severity(spy_risk),
                round(components.get("bridge_infiltration", 0), 6),
                round(components.get("pre_op_infiltration", 0), 6),
                round(components.get("hostile_overlap", 0), 6),
                round(components.get("temporal_coordination", 0), 6),
                round(components.get("identity_association", 0), 6),
                round(components.get("ring_context", 0), 6),
                round(components.get("behavioral_anomaly", 0), 6),
                round(components.get("org_movement", 0), 6),
                round(components.get("predicted_hostile_link", 0), 6),
                top_case_id,
                explanation,
                json_dumps_safe(WEIGHTS),
                MODEL_VERSION, computed_at, run_id,
            ))

            for ev in evidences[:MAX_EVIDENCE_PER_CHAR]:
                evidence_rows.append((
                    cid, ev["key"], "all_time",
                    ev.get("value"), ev.get("expected"), ev.get("deviation"),
                    ev.get("z_score"), ev.get("percentile"),
                    ev.get("confidence_flag", "low"),
                    round(ev.get("contribution", 0), 6),
                    ev.get("text", ""),
                    json_dumps_safe(ev.get("payload")) if ev.get("payload") else None,
                    computed_at,
                ))

        # Compute percentile ranks
        sorted_scores = sorted(all_scores, key=lambda x: x[1])
        rank_map: dict[int, float] = {}
        total = len(sorted_scores)
        for idx, (cid, _) in enumerate(sorted_scores):
            rank_map[cid] = round(idx / max(1, total - 1), 6) if total > 1 else 0.5

        # Patch percentile into profile rows
        patched_profiles: list[tuple[Any, ...]] = []
        for row in profile_rows:
            cid = row[0]
            patched = list(row)
            patched[2] = rank_map.get(cid, 0.5)
            patched_profiles.append(tuple(patched))

        # Write profiles
        for i in range(0, len(patched_profiles), BATCH_SIZE):
            batch = patched_profiles[i : i + BATCH_SIZE]
            db.execute_many(
                """INSERT INTO character_spy_risk_profiles
                   (character_id, spy_risk_score, risk_percentile,
                    confidence_score, confidence_tier, severity_tier,
                    bridge_infiltration_score, pre_op_infiltration_score,
                    hostile_overlap_score, temporal_coordination_score,
                    identity_association_score, ring_context_score,
                    behavioral_anomaly_score, org_movement_score,
                    predicted_hostile_link_score, top_case_id,
                    explanation_json, component_weights_json,
                    model_version, computed_at, source_run_id)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                   ON DUPLICATE KEY UPDATE
                    spy_risk_score=VALUES(spy_risk_score),
                    risk_percentile=VALUES(risk_percentile),
                    confidence_score=VALUES(confidence_score),
                    confidence_tier=VALUES(confidence_tier),
                    severity_tier=VALUES(severity_tier),
                    bridge_infiltration_score=VALUES(bridge_infiltration_score),
                    pre_op_infiltration_score=VALUES(pre_op_infiltration_score),
                    hostile_overlap_score=VALUES(hostile_overlap_score),
                    temporal_coordination_score=VALUES(temporal_coordination_score),
                    identity_association_score=VALUES(identity_association_score),
                    ring_context_score=VALUES(ring_context_score),
                    behavioral_anomaly_score=VALUES(behavioral_anomaly_score),
                    org_movement_score=VALUES(org_movement_score),
                    predicted_hostile_link_score=VALUES(predicted_hostile_link_score),
                    top_case_id=VALUES(top_case_id),
                    explanation_json=VALUES(explanation_json),
                    component_weights_json=VALUES(component_weights_json),
                    model_version=VALUES(model_version),
                    computed_at=VALUES(computed_at),
                    source_run_id=VALUES(source_run_id)""",
                batch,
            )
            rows_written += len(batch)

        # Write evidence (delete+insert per character batch for freshness)
        for i in range(0, len(evidence_rows), BATCH_SIZE):
            batch = evidence_rows[i : i + BATCH_SIZE]
            batch_cids = list({r[0] for r in batch})
            ph = ",".join(["%s"] * len(batch_cids))
            db.execute(f"DELETE FROM character_spy_risk_evidence WHERE character_id IN ({ph})", tuple(batch_cids))
            db.execute_many(
                """INSERT INTO character_spy_risk_evidence
                   (character_id, evidence_key, window_label,
                    evidence_value, expected_value, deviation_value,
                    z_score, cohort_percentile, confidence_flag,
                    contribution_to_score, evidence_text,
                    evidence_payload_json, computed_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                batch,
            )

        duration_ms = int((time.perf_counter() - started) * 1000)
        result = JobResult.success(
            job_key=lock_key,
            summary=f"Scored {rows_processed} characters, wrote {rows_written} profiles.",
            rows_processed=rows_processed, rows_written=rows_written,
            duration_ms=duration_ms,
            meta={"run_id": run_id, "model_version": MODEL_VERSION,
                  "candidates": len(char_ids)},
        ).to_dict()
        finish_job_run(db, job, status="success", rows_processed=rows_processed,
                       rows_written=rows_written, meta=result)
        return result

    except Exception as exc:
        finish_job_run(db, job, status="failed", rows_processed=rows_processed,
                       rows_written=rows_written, error_text=str(exc))
        raise


# ---------------------------------------------------------------------------
# Per-character scoring
# ---------------------------------------------------------------------------

def _score_character(
    cid: int,
    ci_feat: dict[str, Any] | None,
    ci_score: dict[str, Any] | None,
    gml: dict[str, Any] | None,
    gca_row: dict[str, Any] | None,
    bhv_row: dict[str, Any] | None,
    org_row: dict[str, Any] | None,
    identity_max: float,
    case_info: tuple[float, int | None],
    hostile_link: float,
) -> tuple[dict[str, float], list[dict[str, Any]]]:
    ci_feat = ci_feat or {}
    ci_score = ci_score or {}
    gml = gml or {}
    gca_row = gca_row or {}
    bhv_row = bhv_row or {}
    org_row = org_row or {}

    evidences: list[dict[str, Any]] = []

    # 1. bridge_infiltration: is_bridge AND has cross-side links
    is_bridge = int(gca_row.get("is_bridge") or 0)
    bridge_score_raw = _f(ci_feat.get("graph_bridge_score"))
    bridge = min(1.0, is_bridge * 0.5 + bridge_score_raw * 0.5)
    if bridge > 0.01:
        evidences.append({"key": "bridge_infiltration", "value": bridge,
                          "contribution": WEIGHTS["bridge_infiltration"] * bridge,
                          "text": f"Bridge node (score={bridge_score_raw:.3f})", "confidence_flag": "medium"})

    # 2. pre_op_infiltration: anomalous presence rate
    pre_op = min(1.0, _f(ci_feat.get("anomalous_presence_rate")) * 2)
    if pre_op > 0.01:
        evidences.append({"key": "pre_op_infiltration", "value": pre_op,
                          "contribution": WEIGHTS["pre_op_infiltration"] * pre_op,
                          "text": f"Anomalous presence rate elevated", "confidence_flag": "medium"})

    # 3. hostile_overlap: copresence anomalous density
    hostile = min(1.0, _f(ci_feat.get("co_presence_anomalous_density")) * 2)
    if hostile > 0.01:
        evidences.append({"key": "hostile_overlap", "value": hostile,
                          "contribution": WEIGHTS["hostile_overlap"] * hostile,
                          "text": f"Co-presence anomalous density elevated", "confidence_flag": "medium"})

    # 4. temporal_coordination: behavioral regularity
    temporal = _f(bhv_row.get("temporal_regularity_score"))
    if temporal > 0.01:
        evidences.append({"key": "temporal_coordination", "value": temporal,
                          "contribution": WEIGHTS["temporal_coordination"] * temporal,
                          "text": f"Temporal regularity score={temporal:.3f}", "confidence_flag": "low"})

    # 5. identity_association: max link_score to other chars
    identity_assoc = min(1.0, identity_max)
    if identity_assoc > 0.01:
        evidences.append({"key": "identity_association", "value": identity_assoc,
                          "contribution": WEIGHTS["identity_association"] * identity_assoc,
                          "text": f"Identity link max_score={identity_assoc:.3f}", "confidence_flag": "medium"})

    # 6. ring_context: max ring_score of any case the char belongs to
    ring_score, top_case_id = case_info
    ring_ctx = min(1.0, ring_score)
    if ring_ctx > 0.01:
        evidences.append({"key": "ring_context", "value": ring_ctx,
                          "contribution": WEIGHTS["ring_context"] * ring_ctx,
                          "text": f"Member of spy case #{top_case_id} (ring_score={ring_score:.3f})",
                          "confidence_flag": "high"})

    # 7. behavioral_anomaly
    bhv_risk = _f(bhv_row.get("behavioral_risk_score"))
    if bhv_risk > 0.01:
        evidences.append({"key": "behavioral_anomaly", "value": bhv_risk,
                          "contribution": WEIGHTS["behavioral_anomaly"] * bhv_risk,
                          "text": f"Behavioral risk score={bhv_risk:.3f}", "confidence_flag": "low"})

    # 8. org_movement: corp hop frequency + short tenure
    hops = int(org_row.get("corp_hops_180d") or 0)
    short = int(org_row.get("short_tenure_hops_180d") or 0)
    org_move = min(1.0, (hops * 0.15 + short * 0.20))
    if org_move > 0.01:
        evidences.append({"key": "org_movement", "value": org_move,
                          "contribution": WEIGHTS["org_movement"] * org_move,
                          "text": f"Corp hops={hops}, short tenure={short} in 180d",
                          "confidence_flag": "low"})

    # 9. predicted_hostile_link
    pred_hostile = min(1.0, hostile_link)
    if pred_hostile > 0.01:
        evidences.append({"key": "predicted_hostile_link", "value": pred_hostile,
                          "contribution": WEIGHTS["predicted_hostile_link"] * pred_hostile,
                          "text": f"Predicted hostile link score={pred_hostile:.3f}",
                          "confidence_flag": "low"})

    components = {
        "bridge_infiltration": bridge,
        "pre_op_infiltration": pre_op,
        "hostile_overlap": hostile,
        "temporal_coordination": temporal,
        "identity_association": identity_assoc,
        "ring_context": ring_ctx,
        "behavioral_anomaly": bhv_risk,
        "org_movement": org_move,
        "predicted_hostile_link": pred_hostile,
    }

    # Sort evidences by contribution descending
    evidences.sort(key=lambda e: e.get("contribution", 0), reverse=True)

    return components, evidences


# ---------------------------------------------------------------------------
# Data loaders
# ---------------------------------------------------------------------------

def _fetch_candidate_ids(db: SupplyCoreDb, limit: int) -> list[int]:
    rows = db.fetch_all(
        """SELECT character_id FROM (
            SELECT character_id FROM character_counterintel_features
            UNION
            SELECT character_id FROM character_suspicion_scores
           ) u ORDER BY character_id LIMIT %s""",
        (limit,),
    )
    return [int(r["character_id"]) for r in rows if r.get("character_id")]


def _bulk_kv(db: SupplyCoreDb, table: str, key: str, ids: list[int]) -> dict[int, dict[str, Any]]:
    out: dict[int, dict[str, Any]] = {}
    for i in range(0, len(ids), BATCH_SIZE):
        batch = ids[i : i + BATCH_SIZE]
        ph = ",".join(["%s"] * len(batch))
        for r in db.fetch_all(f"SELECT * FROM {table} WHERE {key} IN ({ph})", tuple(batch)):
            cid = r.get(key)
            if cid is not None:
                out[int(cid)] = r
    return out


def _bulk_kv_latest(db: SupplyCoreDb, table: str, key: str, ids: list[int]) -> dict[int, dict[str, Any]]:
    """Load the freshest row per character from a table with computed_at."""
    out: dict[int, dict[str, Any]] = {}
    for i in range(0, len(ids), BATCH_SIZE):
        batch = ids[i : i + BATCH_SIZE]
        ph = ",".join(["%s"] * len(batch))
        rows = db.fetch_all(
            f"SELECT * FROM {table} WHERE {key} IN ({ph}) ORDER BY {key}, computed_at DESC",
            tuple(batch),
        )
        for r in rows:
            cid = r.get(key)
            if cid is not None and int(cid) not in out:
                out[int(cid)] = r
    return out


def _load_identity_max_scores(db: SupplyCoreDb, ids: list[int]) -> dict[int, float]:
    """Max identity link score per character."""
    out: dict[int, float] = {}
    for i in range(0, len(ids), BATCH_SIZE):
        batch = ids[i : i + BATCH_SIZE]
        ph = ",".join(["%s"] * len(batch))
        rows = db.fetch_all(
            f"""SELECT character_id_a AS cid, MAX(link_score) AS ms
                FROM character_identity_links WHERE character_id_a IN ({ph})
                GROUP BY character_id_a
                UNION ALL
                SELECT character_id_b AS cid, MAX(link_score) AS ms
                FROM character_identity_links WHERE character_id_b IN ({ph})
                GROUP BY character_id_b""",
            tuple(batch) + tuple(batch),
        )
        for r in rows:
            cid = int(r["cid"])
            ms = _f(r["ms"])
            out[cid] = max(out.get(cid, 0.0), ms)
    return out


def _load_case_scores(db: SupplyCoreDb, ids: list[int]) -> dict[int, tuple[float, int | None]]:
    """Max ring_score + case_id for each character's case membership."""
    out: dict[int, tuple[float, int | None]] = {}
    for i in range(0, len(ids), BATCH_SIZE):
        batch = ids[i : i + BATCH_SIZE]
        ph = ",".join(["%s"] * len(batch))
        rows = db.fetch_all(
            f"""SELECT m.character_id, c.ring_score, c.case_id
                FROM spy_network_case_members m
                JOIN spy_network_cases c ON c.case_id = m.case_id
                WHERE m.character_id IN ({ph})
                ORDER BY c.ring_score DESC""",
            tuple(batch),
        )
        for r in rows:
            cid = int(r["character_id"])
            if cid not in out:
                out[cid] = (_f(r["ring_score"]), int(r["case_id"]))
    return out


def _load_hostile_link_scores(db: SupplyCoreDb, ids: list[int]) -> dict[int, float]:
    """Mean confidence of top hostile link predictions per character."""
    out: dict[int, float] = {}
    for i in range(0, len(ids), BATCH_SIZE):
        batch = ids[i : i + BATCH_SIZE]
        ph = ",".join(["%s"] * len(batch))
        rows = db.fetch_all(
            f"""SELECT character_id_a AS cid, AVG(confidence) AS avg_conf
                FROM graph_ml_link_predictions
                WHERE character_id_a IN ({ph}) AND cross_side_count > 0
                GROUP BY character_id_a""",
            tuple(batch),
        )
        for r in rows:
            cid = int(r["cid"])
            out[cid] = min(1.0, _f(r["avg_conf"]))
    return out
