"""Spy feature snapshots — versioned per-character feature vectors.

Phase 2 of the spy detection platform. Builds reproducible, hash-addressable
feature vectors per character from the already-materialized upstream tables:

  * ``character_counterintel_features`` / ``character_counterintel_scores``
  * ``character_suspicion_scores``
  * ``character_behavioral_scores``
  * ``graph_ml_features`` (GDS centrality + FastRP embedding summary)
  * ``graph_community_assignments``
  * ``character_temporal_metrics``

Output: ``character_feature_snapshots`` (one row per character per run) and
``feature_set_definitions`` (catalog, seeded on first run).

The feature vector JSON is canonicalized (sorted keys, compact separators)
before hashing so Phase 6's skip-on-hash optimization works — identical
upstream state produces an identical hash.

This job does not write to Neo4j. All work is MariaDB-native.
"""

from __future__ import annotations

import hashlib
import json
import time
import uuid
from datetime import UTC, datetime
from typing import Any

from ..db import SupplyCoreDb
from ..job_result import JobResult
from ..job_utils import finish_job_run, start_job_run
from ..json_utils import json_dumps_safe

# ---------------------------------------------------------------------------
# Constants — schema identity for this feature set.
#
# Bump ``FEATURE_VERSION`` whenever the ordered field list changes. Old
# snapshots remain valid under their stamped version; new snapshots get the
# new version. This is what lets Phase 6 reproducibly retrain off historical
# feature vectors.
# ---------------------------------------------------------------------------

FEATURE_SET = "spy_v1"
FEATURE_VERSION = "1.0.0"
WINDOW_LABELS: tuple[str, ...] = ("all_time",)
BATCH_SIZE = 500

# Ordered (field, type, source_table, description) list. This is the
# machine-readable contract seeded into feature_set_definitions on first run.
# Order matters — the feature vector is built in this exact sequence so the
# canonical JSON (and therefore its hash) is deterministic.
_SPY_V1_FIELDS: list[dict[str, Any]] = [
    # Counterintel features (battle-scoped)
    {"field": "ci_anomalous_presence_rate", "type": "float", "source_table": "character_counterintel_features", "description": "Anomalous battle presence rate."},
    {"field": "ci_control_presence_rate", "type": "float", "source_table": "character_counterintel_features", "description": "Control battle presence rate."},
    {"field": "ci_enemy_same_hull_survival_lift", "type": "float", "source_table": "character_counterintel_features", "description": "Enemy same-hull survival lift."},
    {"field": "ci_enemy_sustain_lift", "type": "float", "source_table": "character_counterintel_features", "description": "Enemy sustain lift."},
    {"field": "ci_copresence_anomalous_density", "type": "float", "source_table": "character_counterintel_features", "description": "Anomalous co-presence density."},
    {"field": "ci_graph_bridge_score", "type": "float", "source_table": "character_counterintel_features", "description": "Graph bridge score."},
    {"field": "ci_corp_hop_frequency_180d", "type": "float", "source_table": "character_counterintel_features", "description": "180d corp hop frequency."},
    {"field": "ci_short_tenure_ratio_180d", "type": "float", "source_table": "character_counterintel_features", "description": "180d short-tenure ratio."},
    {"field": "ci_repeatability_score", "type": "float", "source_table": "character_counterintel_features", "description": "Pattern repeatability score."},
    # Counterintel fused score
    {"field": "ci_review_priority_score", "type": "float", "source_table": "character_counterintel_scores", "description": "Analyst review-priority fused score."},
    {"field": "ci_percentile_rank", "type": "float", "source_table": "character_counterintel_scores", "description": "Cohort percentile rank."},
    {"field": "ci_confidence_score", "type": "float", "source_table": "character_counterintel_scores", "description": "Score confidence."},
    {"field": "ci_evidence_count", "type": "int", "source_table": "character_counterintel_scores", "description": "Evidence rows backing the score."},
    # Suspicion v2
    {"field": "sus_recent", "type": "float", "source_table": "character_suspicion_scores", "description": "Recent suspicion score."},
    {"field": "sus_all_time", "type": "float", "source_table": "character_suspicion_scores", "description": "All-time suspicion score."},
    {"field": "sus_momentum", "type": "float", "source_table": "character_suspicion_scores", "description": "Suspicion momentum (recent-vs-baseline)."},
    {"field": "sus_percentile_rank", "type": "float", "source_table": "character_suspicion_scores", "description": "Suspicion percentile."},
    {"field": "sus_cross_side_rate", "type": "float", "source_table": "character_suspicion_scores", "description": "Observed cross-side rate."},
    {"field": "sus_enemy_efficiency_uplift", "type": "float", "source_table": "character_suspicion_scores", "description": "Enemy efficiency uplift attribution."},
    {"field": "sus_support_evidence_count", "type": "int", "source_table": "character_suspicion_scores", "description": "Supporting evidence rows."},
    # Behavioral (Lane 2)
    {"field": "bhv_risk_score", "type": "float", "source_table": "character_behavioral_scores", "description": "Behavioral risk score."},
    {"field": "bhv_fleet_absence_ratio", "type": "float", "source_table": "character_behavioral_scores", "description": "Fleet absence ratio."},
    {"field": "bhv_kill_concentration_score", "type": "float", "source_table": "character_behavioral_scores", "description": "Kill concentration."},
    {"field": "bhv_temporal_regularity_score", "type": "float", "source_table": "character_behavioral_scores", "description": "Temporal regularity."},
    {"field": "bhv_companion_consistency_score", "type": "float", "source_table": "character_behavioral_scores", "description": "Companion consistency."},
    {"field": "bhv_cross_side_small_rate", "type": "float", "source_table": "character_behavioral_scores", "description": "Small-engagement cross-side rate."},
    {"field": "bhv_asymmetry_preference", "type": "float", "source_table": "character_behavioral_scores", "description": "Asymmetry preference."},
    # Graph ML features (GDS-native)
    {"field": "gml_pagerank", "type": "float", "source_table": "graph_ml_features", "description": "PageRank score (time_window best available)."},
    {"field": "gml_betweenness", "type": "float", "source_table": "graph_ml_features", "description": "Betweenness centrality."},
    {"field": "gml_degree_centrality", "type": "int", "source_table": "graph_ml_features", "description": "Degree centrality."},
    {"field": "gml_kcore_level", "type": "int", "source_table": "graph_ml_features", "description": "K-core level."},
    {"field": "gml_is_articulation", "type": "int", "source_table": "graph_ml_features", "description": "Articulation point flag (0/1)."},
    {"field": "gml_embedding_anomaly", "type": "float", "source_table": "graph_ml_features", "description": "FastRP distance from alliance centroid."},
    # Community assignment
    {"field": "gca_community_id", "type": "int", "source_table": "graph_community_assignments", "description": "Leiden community id."},
    {"field": "gca_is_bridge", "type": "int", "source_table": "graph_community_assignments", "description": "Bridge node flag."},
    {"field": "gca_community_size", "type": "int", "source_table": "graph_community_assignments", "description": "Community size."},
    {"field": "gca_membership_score", "type": "float", "source_table": "graph_community_assignments", "description": "Community membership score."},
    # Temporal metrics (7d window)
    {"field": "tmp_battles_present_7d", "type": "int", "source_table": "character_temporal_metrics", "description": "7d battles present."},
    {"field": "tmp_kills_total_7d", "type": "int", "source_table": "character_temporal_metrics", "description": "7d kills total."},
    {"field": "tmp_losses_total_7d", "type": "int", "source_table": "character_temporal_metrics", "description": "7d losses total."},
    {"field": "tmp_co_presence_density_7d", "type": "float", "source_table": "character_temporal_metrics", "description": "7d co-presence density."},
]


def _now_sql() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")


def _canonical_json(obj: dict[str, Any]) -> str:
    """Deterministic JSON serialization for hashing.

    Keys are sorted and whitespace stripped. Two vectors with the same
    logical content produce the same bytes → the same SHA-256.
    """
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _vector_hash(vector: dict[str, Any]) -> str:
    payload = _canonical_json(vector).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _f(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _i(value: Any, default: int = 0) -> int:
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _seed_feature_set_definition(db: SupplyCoreDb, computed_at: str) -> None:
    """Idempotently insert the spy_v1 schema into feature_set_definitions.

    On every run. Cheap — upsert on (feature_set, feature_version).
    """
    schema_json = json_dumps_safe({"fields": _SPY_V1_FIELDS})
    db.execute(
        """
        INSERT INTO feature_set_definitions
            (feature_set, feature_version, schema_json, created_at, status)
        VALUES (%s, %s, %s, %s, 'active')
        ON DUPLICATE KEY UPDATE
            schema_json = VALUES(schema_json),
            status = 'active'
        """,
        (FEATURE_SET, FEATURE_VERSION, schema_json, computed_at),
    )


def _build_character_vector(
    character_id: int,
    ci_feat: dict[str, Any] | None,
    ci_score: dict[str, Any] | None,
    sus: dict[str, Any] | None,
    bhv: dict[str, Any] | None,
    gml: dict[str, Any] | None,
    gca: dict[str, Any] | None,
    tmp: dict[str, Any] | None,
) -> dict[str, Any]:
    """Assemble the ordered vector. Missing inputs default to 0.

    The field order MUST match ``_SPY_V1_FIELDS`` — iterated over the field
    list so a new field added to the schema naturally shows up in the vector
    (assuming this function is updated in lockstep; the schema JSON is the
    contract that downstream trainers rely on).
    """
    ci_feat = ci_feat or {}
    ci_score = ci_score or {}
    sus = sus or {}
    bhv = bhv or {}
    gml = gml or {}
    gca = gca or {}
    tmp = tmp or {}

    return {
        # Counterintel features
        "ci_anomalous_presence_rate": _f(ci_feat.get("anomalous_presence_rate")),
        "ci_control_presence_rate": _f(ci_feat.get("control_presence_rate")),
        "ci_enemy_same_hull_survival_lift": _f(ci_feat.get("enemy_same_hull_survival_lift")),
        "ci_enemy_sustain_lift": _f(ci_feat.get("enemy_sustain_lift")),
        "ci_copresence_anomalous_density": _f(ci_feat.get("co_presence_anomalous_density")),
        "ci_graph_bridge_score": _f(ci_feat.get("graph_bridge_score")),
        "ci_corp_hop_frequency_180d": _f(ci_feat.get("corp_hop_frequency_180d")),
        "ci_short_tenure_ratio_180d": _f(ci_feat.get("short_tenure_ratio_180d")),
        "ci_repeatability_score": _f(ci_feat.get("repeatability_score")),
        # Counterintel fused
        "ci_review_priority_score": _f(ci_score.get("review_priority_score")),
        "ci_percentile_rank": _f(ci_score.get("percentile_rank")),
        "ci_confidence_score": _f(ci_score.get("confidence_score")),
        "ci_evidence_count": _i(ci_score.get("evidence_count")),
        # Suspicion v2
        "sus_recent": _f(sus.get("suspicion_score_recent")),
        "sus_all_time": _f(sus.get("suspicion_score_all_time")),
        "sus_momentum": _f(sus.get("suspicion_momentum")),
        "sus_percentile_rank": _f(sus.get("percentile_rank")),
        "sus_cross_side_rate": _f(sus.get("cross_side_rate")),
        "sus_enemy_efficiency_uplift": _f(sus.get("enemy_efficiency_uplift")),
        "sus_support_evidence_count": _i(sus.get("support_evidence_count")),
        # Behavioral
        "bhv_risk_score": _f(bhv.get("behavioral_risk_score")),
        "bhv_fleet_absence_ratio": _f(bhv.get("fleet_absence_ratio")),
        "bhv_kill_concentration_score": _f(bhv.get("kill_concentration_score")),
        "bhv_temporal_regularity_score": _f(bhv.get("temporal_regularity_score")),
        "bhv_companion_consistency_score": _f(bhv.get("companion_consistency_score")),
        "bhv_cross_side_small_rate": _f(bhv.get("cross_side_small_rate")),
        "bhv_asymmetry_preference": _f(bhv.get("asymmetry_preference")),
        # Graph ML
        "gml_pagerank": _f(gml.get("pagerank_score")),
        "gml_betweenness": _f(gml.get("betweenness_score")),
        "gml_degree_centrality": _i(gml.get("degree_centrality")),
        "gml_kcore_level": _i(gml.get("kcore_level")),
        "gml_is_articulation": _i(gml.get("is_articulation_point")),
        "gml_embedding_anomaly": _f(gml.get("embedding_anomaly_score")),
        # Community
        "gca_community_id": _i(gca.get("community_id")),
        "gca_is_bridge": _i(gca.get("is_bridge")),
        "gca_community_size": _i(gca.get("community_size")),
        "gca_membership_score": _f(gca.get("membership_score")),
        # Temporal
        "tmp_battles_present_7d": _i(tmp.get("battles_present")),
        "tmp_kills_total_7d": _i(tmp.get("kills_total")),
        "tmp_losses_total_7d": _i(tmp.get("losses_total")),
        "tmp_co_presence_density_7d": _f(tmp.get("co_presence_density")),
    }


def _fetch_candidate_character_ids(db: SupplyCoreDb, limit: int) -> list[int]:
    """Candidate universe: characters with any fresh upstream signal.

    UNION across the three primary feature tables. We want a snapshot for
    every character that has something to snapshot, not just those in one
    source.
    """
    rows = db.fetch_all(
        """
        SELECT character_id FROM (
            SELECT character_id FROM character_counterintel_features
            UNION
            SELECT character_id FROM character_suspicion_scores
            UNION
            SELECT character_id FROM character_behavioral_scores
        ) AS u
        ORDER BY character_id ASC
        LIMIT %s
        """,
        (limit,),
    )
    return [int(r["character_id"]) for r in rows if r.get("character_id") is not None]


def _load_rows_keyed(
    db: SupplyCoreDb, sql: str, params: tuple[Any, ...], key: str
) -> dict[int, dict[str, Any]]:
    out: dict[int, dict[str, Any]] = {}
    for row in db.fetch_all(sql, params):
        cid = row.get(key)
        if cid is None:
            continue
        out[int(cid)] = row
    return out


def run_compute_spy_feature_snapshots(
    db: SupplyCoreDb,
    neo4j_raw: dict[str, Any] | None = None,
    runtime: dict[str, Any] | None = None,
    *,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Compute versioned per-character feature snapshots for feature_set='spy_v1'.

    Processes candidates in bounded chunks — the full character universe in
    one run, but in ``BATCH_SIZE`` slices so memory stays small. Hash-based
    skip logic is applied in Phase 6's scorer, not here; we always write a
    fresh snapshot so the time series is dense.
    """
    lock_key = "compute_spy_feature_snapshots"
    job = start_job_run(db, lock_key)
    started = time.perf_counter()
    computed_at = _now_sql()
    source_run_id = f"spy_v1_{uuid.uuid4().hex[:16]}"

    rows_processed = 0
    rows_written = 0
    unchanged_hash_count = 0
    runtime = runtime or {}
    max_characters = max(100, min(200_000, int(runtime.get("spy_snapshot_max_characters") or 50_000)))

    try:
        # Always refresh the catalog. Cheap upsert.
        _seed_feature_set_definition(db, computed_at)

        character_ids = _fetch_candidate_character_ids(db, max_characters)
        if not character_ids:
            result = JobResult.success(
                job_key=lock_key,
                summary="No candidate characters to snapshot.",
                rows_processed=0,
                rows_written=0,
                duration_ms=int((time.perf_counter() - started) * 1000),
                meta={
                    "feature_set": FEATURE_SET,
                    "feature_version": FEATURE_VERSION,
                    "source_run_id": source_run_id,
                    "computed_at": computed_at,
                },
            ).to_dict()
            finish_job_run(db, job, status="success", rows_processed=0, rows_written=0, meta=result)
            return result

        # Build a set of snapshots per window_label. v1 only has "all_time".
        for window_label in WINDOW_LABELS:
            for start_idx in range(0, len(character_ids), BATCH_SIZE):
                batch_ids = character_ids[start_idx : start_idx + BATCH_SIZE]
                placeholders = ",".join(["%s"] * len(batch_ids))

                ci_feat_rows = _load_rows_keyed(
                    db,
                    f"SELECT * FROM character_counterintel_features WHERE character_id IN ({placeholders})",
                    tuple(batch_ids),
                    "character_id",
                )
                ci_score_rows = _load_rows_keyed(
                    db,
                    f"SELECT * FROM character_counterintel_scores WHERE character_id IN ({placeholders})",
                    tuple(batch_ids),
                    "character_id",
                )
                sus_rows = _load_rows_keyed(
                    db,
                    f"SELECT * FROM character_suspicion_scores WHERE character_id IN ({placeholders})",
                    tuple(batch_ids),
                    "character_id",
                )
                bhv_rows = _load_rows_keyed(
                    db,
                    f"SELECT * FROM character_behavioral_scores WHERE character_id IN ({placeholders})",
                    tuple(batch_ids),
                    "character_id",
                )
                # graph_ml_features: pick the freshest row per character.
                # The UNIQUE KEY is (character_id, time_window, feature_version);
                # we pick the row with the largest computed_at to stay robust
                # across multi-window rollouts.
                gml_rows_raw = db.fetch_all(
                    f"""
                    SELECT character_id, pagerank_score, betweenness_score,
                           degree_centrality, kcore_level, is_articulation_point,
                           embedding_anomaly_score, computed_at
                    FROM graph_ml_features
                    WHERE character_id IN ({placeholders})
                    ORDER BY character_id ASC, computed_at DESC
                    """,
                    tuple(batch_ids),
                )
                gml_rows: dict[int, dict[str, Any]] = {}
                for row in gml_rows_raw:
                    cid = int(row.get("character_id") or 0)
                    if cid and cid not in gml_rows:
                        gml_rows[cid] = row

                gca_rows = _load_rows_keyed(
                    db,
                    f"SELECT * FROM graph_community_assignments WHERE character_id IN ({placeholders})",
                    tuple(batch_ids),
                    "character_id",
                )
                tmp_rows = _load_rows_keyed(
                    db,
                    f"""
                    SELECT * FROM character_temporal_metrics
                    WHERE character_id IN ({placeholders})
                      AND window_label = '7d'
                    """,
                    tuple(batch_ids),
                    "character_id",
                )

                # Fetch the previous hash per character (for unchanged-count
                # reporting only — we still write the new snapshot because
                # the time series is a contract with Phase 5/6 downstream).
                prev_rows = db.fetch_all(
                    f"""
                    SELECT cfs.character_id, cfs.feature_vector_hash
                    FROM character_feature_snapshots cfs
                    INNER JOIN (
                        SELECT character_id, MAX(computed_at) AS max_at
                        FROM character_feature_snapshots
                        WHERE feature_set = %s
                          AND feature_version = %s
                          AND window_label = %s
                          AND character_id IN ({placeholders})
                        GROUP BY character_id
                    ) latest
                      ON latest.character_id = cfs.character_id
                     AND latest.max_at = cfs.computed_at
                    WHERE cfs.feature_set = %s
                      AND cfs.feature_version = %s
                      AND cfs.window_label = %s
                    """,
                    (FEATURE_SET, FEATURE_VERSION, window_label, *batch_ids, FEATURE_SET, FEATURE_VERSION, window_label),
                )
                prev_hash_by_char: dict[int, str] = {
                    int(r["character_id"]): str(r.get("feature_vector_hash") or "")
                    for r in prev_rows
                }

                insert_rows: list[tuple[Any, ...]] = []
                for cid in batch_ids:
                    vector = _build_character_vector(
                        cid,
                        ci_feat_rows.get(cid),
                        ci_score_rows.get(cid),
                        sus_rows.get(cid),
                        bhv_rows.get(cid),
                        gml_rows.get(cid),
                        gca_rows.get(cid),
                        tmp_rows.get(cid),
                    )
                    canonical = _canonical_json(vector)
                    new_hash = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
                    if prev_hash_by_char.get(cid) == new_hash:
                        unchanged_hash_count += 1
                    insert_rows.append((
                        cid,
                        FEATURE_SET,
                        FEATURE_VERSION,
                        window_label,
                        canonical,
                        new_hash,
                        source_run_id,
                        computed_at,
                    ))
                    rows_processed += 1

                if insert_rows:
                    db.execute_many(
                        """
                        INSERT INTO character_feature_snapshots
                            (character_id, feature_set, feature_version, window_label,
                             feature_vector_json, feature_vector_hash, source_run_id,
                             computed_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                            feature_vector_json = VALUES(feature_vector_json),
                            feature_vector_hash = VALUES(feature_vector_hash),
                            source_run_id = VALUES(source_run_id)
                        """,
                        insert_rows,
                    )
                    rows_written += len(insert_rows)

        duration_ms = int((time.perf_counter() - started) * 1000)
        result = JobResult.success(
            job_key=lock_key,
            summary=(
                f"Wrote {rows_written} snapshots for {len(character_ids)} characters "
                f"({unchanged_hash_count} unchanged hashes)."
            ),
            rows_processed=rows_processed,
            rows_written=rows_written,
            duration_ms=duration_ms,
            meta={
                "feature_set": FEATURE_SET,
                "feature_version": FEATURE_VERSION,
                "source_run_id": source_run_id,
                "computed_at": computed_at,
                "candidate_characters": len(character_ids),
                "unchanged_hash_count": unchanged_hash_count,
                "window_labels": list(WINDOW_LABELS),
            },
        ).to_dict()
        finish_job_run(
            db,
            job,
            status="success",
            rows_processed=rows_processed,
            rows_written=rows_written,
            meta=result,
        )
        return result
    except Exception as exc:
        finish_job_run(
            db,
            job,
            status="failed",
            rows_processed=rows_processed,
            rows_written=rows_written,
            error_text=str(exc),
        )
        raise
