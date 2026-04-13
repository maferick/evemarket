"""Identity resolution — infer probable shared-operator / alt links.

Phase 3 of the spy detection platform.  Generates candidate character pairs
from cheap blocking signals (battle co-occurrence, shared community, GDS link
predictions), scores each pair on six bounded components, persists links above
threshold, and clusters high-confidence links into identity groups.

Output tables: ``character_identity_links``, ``character_identity_clusters``,
``character_identity_cluster_members``, ``identity_resolution_runs``.

This job does NOT write to Neo4j (gated by config flag, default off in v1).
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import time
import uuid
from datetime import UTC, datetime
from typing import Any

from ..db import SupplyCoreDb
from ..job_result import JobResult
from ..job_utils import finish_job_run, start_job_run
from ..json_utils import json_dumps_safe

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Tunables
# ---------------------------------------------------------------------------
MIN_BATTLE_COOCCURRENCE = 3
MAX_CANDIDATE_PAIRS = 50_000
LINK_PREDICTION_CONFIDENCE_FLOOR = 0.40
LINK_EMIT_THRESHOLD = 0.50
CLUSTER_EDGE_THRESHOLD = 0.65
MAX_CLUSTER_SIZE = 12
BATCH_SIZE = 500

# Component weights — sum to 1.0
WEIGHTS = {
    "org_history": 0.20,
    "copresence": 0.25,
    "temporal": 0.10,
    "cross_side": 0.15,
    "behavior_sim": 0.20,
    "embedding_sim": 0.10,
}

CONFIDENCE_TIERS = {"high": 0.75, "medium": 0.50}


def _now_sql() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")


def _tier(score: float) -> str:
    if score >= CONFIDENCE_TIERS["high"]:
        return "high"
    if score >= CONFIDENCE_TIERS["medium"]:
        return "medium"
    return "low"


def _f(v: Any, default: float = 0.0) -> float:
    if v is None:
        return default
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


# ---------------------------------------------------------------------------
# Candidate generation (blocking step)
# ---------------------------------------------------------------------------

def _generate_candidates(db: SupplyCoreDb, limit: int) -> dict[tuple[int, int], dict[str, Any]]:
    """Return {(a, b): context_dict} with a < b always."""
    pairs: dict[tuple[int, int], dict[str, Any]] = {}

    # Block 1: battle co-occurrence via precomputed copresence edges
    # Uses character_copresence_edges (populated by compute_copresence_edges)
    # instead of an expensive self-join on battle_participants.
    rows = db.fetch_all(
        """
        SELECT character_id_a AS a, character_id_b AS b,
               SUM(event_count) AS n
        FROM character_copresence_edges
        WHERE event_type IN ('same_battle','same_side')
        GROUP BY character_id_a, character_id_b
        HAVING n >= %s
        ORDER BY n DESC
        LIMIT %s
        """,
        (MIN_BATTLE_COOCCURRENCE, limit),
    )
    for r in rows:
        key = (int(r["a"]), int(r["b"]))
        pairs[key] = {"battle_count": int(r["n"])}

    remaining = limit - len(pairs)

    # Block 2: high-confidence GDS link predictions
    if remaining > 0:
        lp_rows = db.fetch_all(
            """
            SELECT character_id_a, character_id_b, confidence,
                   copresence_count, cross_side_count, embedding_similarity
            FROM graph_ml_link_predictions
            WHERE confidence >= %s
            ORDER BY confidence DESC
            LIMIT %s
            """,
            (LINK_PREDICTION_CONFIDENCE_FLOOR, remaining),
        )
        for r in lp_rows:
            a, b = int(r["character_id_a"]), int(r["character_id_b"])
            key = (min(a, b), max(a, b))
            ctx = pairs.get(key, {})
            ctx["lp_confidence"] = _f(r["confidence"])
            ctx["lp_copresence"] = int(r.get("copresence_count") or 0)
            ctx["lp_cross_side"] = int(r.get("cross_side_count") or 0)
            ctx["lp_embedding_sim"] = _f(r.get("embedding_similarity"))
            pairs[key] = ctx

    remaining = limit - len(pairs)

    # Block 3: same community where at least one is a bridge node
    if remaining > 0:
        comm_rows = db.fetch_all(
            """
            SELECT g1.character_id AS a, g2.character_id AS b
            FROM graph_community_assignments g1
            JOIN graph_community_assignments g2
              ON g1.community_id = g2.community_id
             AND g1.character_id < g2.character_id
            WHERE g1.is_bridge = 1 OR g2.is_bridge = 1
            LIMIT %s
            """,
            (remaining,),
        )
        for r in comm_rows:
            key = (int(r["a"]), int(r["b"]))
            if key not in pairs:
                pairs[key] = {"community_block": True}

    return pairs


# ---------------------------------------------------------------------------
# Component scorers
# ---------------------------------------------------------------------------

def _score_org_history(
    a: int, b: int,
    org_cache: dict[int, dict[str, Any]],
) -> float:
    ha, hb = org_cache.get(a), org_cache.get(b)
    if not ha or not hb:
        return 0.0
    score = 0.0
    if ha.get("current_corporation_id") and ha["current_corporation_id"] == hb.get("current_corporation_id"):
        score += 0.50
    if ha.get("current_alliance_id") and ha["current_alliance_id"] == hb.get("current_alliance_id"):
        score += 0.30
    # Both high-mobility characters
    hops_a = int(ha.get("corp_hops_180d") or 0)
    hops_b = int(hb.get("corp_hops_180d") or 0)
    if hops_a >= 2 and hops_b >= 2:
        score += 0.20
    return min(1.0, score)


def _score_copresence(ctx: dict[str, Any]) -> float:
    count = ctx.get("battle_count", 0) + ctx.get("lp_copresence", 0)
    if count <= 0:
        return 0.0
    return min(1.0, math.log(count + 1) / math.log(25))


def _score_temporal(
    a: int, b: int,
    temporal_cache: dict[int, dict[str, Any]],
) -> float:
    ta, tb = temporal_cache.get(a), temporal_cache.get(b)
    if not ta or not tb:
        return 0.0
    ea = _f(ta.get("engagement_rate_avg"))
    eb = _f(tb.get("engagement_rate_avg"))
    da = _f(ta.get("co_presence_density"))
    db_val = _f(tb.get("co_presence_density"))
    eng_sim = 1.0 - min(1.0, abs(ea - eb))
    cop_sim = 1.0 - min(1.0, abs(da - db_val))
    return (eng_sim + cop_sim) / 2.0


def _score_cross_side(a: int, b: int, ctx: dict[str, Any], typed_cache: dict[tuple[int, int], int]) -> float:
    count = ctx.get("lp_cross_side", 0)
    key = (min(a, b), max(a, b))
    count += typed_cache.get(key, 0)
    if count <= 0:
        return 0.0
    return min(1.0, count / 5.0)


def _cosine_sim(va: list[float], vb: list[float]) -> float:
    if len(va) != len(vb) or len(va) == 0:
        return 0.0
    dot = sum(x * y for x, y in zip(va, vb))
    mag_a = math.sqrt(sum(x * x for x in va))
    mag_b = math.sqrt(sum(x * x for x in vb))
    if mag_a < 1e-12 or mag_b < 1e-12:
        return 0.0
    return max(0.0, min(1.0, dot / (mag_a * mag_b)))


def _score_behavior_sim(
    a: int, b: int,
    snapshot_cache: dict[int, dict[str, Any]],
) -> float:
    sa, sb = snapshot_cache.get(a), snapshot_cache.get(b)
    if not sa or not sb:
        return 0.0
    try:
        va = list(json.loads(sa["feature_vector_json"]).values()) if isinstance(sa["feature_vector_json"], str) else list(sa["feature_vector_json"].values())
        vb = list(json.loads(sb["feature_vector_json"]).values()) if isinstance(sb["feature_vector_json"], str) else list(sb["feature_vector_json"].values())
        return _cosine_sim([_f(x) for x in va], [_f(x) for x in vb])
    except (json.JSONDecodeError, AttributeError, TypeError):
        return 0.0


def _score_embedding_sim(ctx: dict[str, Any]) -> float:
    return min(1.0, max(0.0, _f(ctx.get("lp_embedding_sim"))))


# ---------------------------------------------------------------------------
# Cluster construction (union-find)
# ---------------------------------------------------------------------------

class _UnionFind:
    def __init__(self) -> None:
        self.parent: dict[int, int] = {}
        self.rank: dict[int, int] = {}

    def find(self, x: int) -> int:
        if x not in self.parent:
            self.parent[x] = x
            self.rank[x] = 0
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, a: int, b: int) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra == rb:
            return
        if self.rank[ra] < self.rank[rb]:
            ra, rb = rb, ra
        self.parent[rb] = ra
        if self.rank[ra] == self.rank[rb]:
            self.rank[ra] += 1

    def components(self) -> dict[int, list[int]]:
        groups: dict[int, list[int]] = {}
        for node in self.parent:
            root = self.find(node)
            groups.setdefault(root, []).append(node)
        return groups


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_compute_identity_resolution(
    db: SupplyCoreDb,
    neo4j_raw: dict[str, Any] | None = None,
    runtime: dict[str, Any] | None = None,
    *,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    lock_key = "compute_identity_resolution"
    job = start_job_run(db, lock_key)
    started = time.perf_counter()
    computed_at = _now_sql()
    run_id = f"ir_{uuid.uuid4().hex[:16]}"
    runtime = runtime or {}
    max_pairs = min(MAX_CANDIDATE_PAIRS, int(runtime.get("ir_max_pairs") or MAX_CANDIDATE_PAIRS))

    rows_processed = 0
    rows_written = 0

    # Write run log entry
    db.execute(
        "INSERT INTO identity_resolution_runs (run_id, started_at, status) VALUES (%s, %s, 'running')",
        (run_id, computed_at),
    )

    try:
        # ── 1. Candidate generation ──────────────────────────────────
        candidates = _generate_candidates(db, max_pairs)
        if not candidates:
            _finish_run(db, run_id, "success", 0, 0, 0)
            result = JobResult.success(
                job_key=lock_key, summary="No candidate pairs found.",
                rows_processed=0, rows_written=0,
                duration_ms=int((time.perf_counter() - started) * 1000),
                meta={"run_id": run_id},
            ).to_dict()
            finish_job_run(db, job, status="success", rows_processed=0, rows_written=0, meta=result)
            return result

        # ── 2. Bulk-load scoring caches ──────────────────────────────
        all_char_ids = set()
        for a, b in candidates:
            all_char_ids.add(a)
            all_char_ids.add(b)
        char_list = sorted(all_char_ids)

        org_cache = _bulk_load(db, "character_org_history_cache", "character_id", char_list)
        temporal_cache = _bulk_load(db, "character_temporal_metrics", "character_id", char_list, extra_where="AND window_label = '7d'")
        snapshot_cache = _bulk_load_snapshots(db, char_list)
        typed_cache = _bulk_load_cross_side(db, char_list)

        # ── 3. Score each pair ───────────────────────────────────────
        link_rows: list[tuple[Any, ...]] = []
        # Diagnostics: track cache hit rates and per-component score sums so
        # operators can see which cache is empty / which component is dragging
        # the total score below the emit threshold.  Without these, a run that
        # scores 50k pairs but emits 0 links looks identical to a run with
        # genuinely low-similarity data.
        all_scores: list[float] = []
        component_sums = {"org_history": 0.0, "copresence": 0.0, "temporal": 0.0,
                          "cross_side": 0.0, "behavior_sim": 0.0, "embedding_sim": 0.0}
        component_nonzero = {k: 0 for k in component_sums}
        near_miss_count = 0  # pairs scoring [threshold - 0.10, threshold)
        zero_score_pairs = 0
        for (a, b), ctx in candidates.items():
            rows_processed += 1
            org = _score_org_history(a, b, org_cache)
            cop = _score_copresence(ctx)
            tmp = _score_temporal(a, b, temporal_cache)
            xsd = _score_cross_side(a, b, ctx, typed_cache)
            bsm = _score_behavior_sim(a, b, snapshot_cache)
            esm = _score_embedding_sim(ctx)

            component_sums["org_history"] += org
            component_sums["copresence"] += cop
            component_sums["temporal"] += tmp
            component_sums["cross_side"] += xsd
            component_sums["behavior_sim"] += bsm
            component_sums["embedding_sim"] += esm
            if org > 0: component_nonzero["org_history"] += 1
            if cop > 0: component_nonzero["copresence"] += 1
            if tmp > 0: component_nonzero["temporal"] += 1
            if xsd > 0: component_nonzero["cross_side"] += 1
            if bsm > 0: component_nonzero["behavior_sim"] += 1
            if esm > 0: component_nonzero["embedding_sim"] += 1

            link_score = (
                WEIGHTS["org_history"] * org
                + WEIGHTS["copresence"] * cop
                + WEIGHTS["temporal"] * tmp
                + WEIGHTS["cross_side"] * xsd
                + WEIGHTS["behavior_sim"] * bsm
                + WEIGHTS["embedding_sim"] * esm
            )
            all_scores.append(link_score)
            if link_score == 0.0:
                zero_score_pairs += 1
            elif link_score >= LINK_EMIT_THRESHOLD - 0.10 and link_score < LINK_EMIT_THRESHOLD:
                near_miss_count += 1
            if link_score < LINK_EMIT_THRESHOLD:
                continue

            evidence = json_dumps_safe({
                "weights": WEIGHTS,
                "components": {"org_history": org, "copresence": cop, "temporal": tmp,
                               "cross_side": xsd, "behavior_sim": bsm, "embedding_sim": esm},
                "battle_count": ctx.get("battle_count", 0),
                "lp_confidence": ctx.get("lp_confidence"),
            })
            link_rows.append((
                a, b, round(link_score, 6), _tier(link_score), "all_time",
                round(org, 6), round(cop, 6), round(tmp, 6),
                round(xsd, 6), round(bsm, 6), round(esm, 6),
                evidence, computed_at, run_id,
            ))

        # ── 4. Persist links ─────────────────────────────────────────
        for i in range(0, len(link_rows), BATCH_SIZE):
            batch = link_rows[i : i + BATCH_SIZE]
            db.execute_many(
                """
                INSERT INTO character_identity_links
                    (character_id_a, character_id_b, link_score, confidence_tier,
                     window_label, org_history_score, copresence_score, temporal_score,
                     cross_side_score, behavior_sim_score, embedding_sim_score,
                     evidence_json, computed_at, source_run_id)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE
                    link_score = VALUES(link_score),
                    confidence_tier = VALUES(confidence_tier),
                    org_history_score = VALUES(org_history_score),
                    copresence_score = VALUES(copresence_score),
                    temporal_score = VALUES(temporal_score),
                    cross_side_score = VALUES(cross_side_score),
                    behavior_sim_score = VALUES(behavior_sim_score),
                    embedding_sim_score = VALUES(embedding_sim_score),
                    evidence_json = VALUES(evidence_json),
                    computed_at = VALUES(computed_at),
                    source_run_id = VALUES(source_run_id)
                """,
                batch,
            )
            rows_written += len(batch)

        # ── 5. Cluster high-confidence links ─────────────────────────
        uf = _UnionFind()
        link_lookup: dict[tuple[int, int], float] = {}
        for row in link_rows:
            a, b, score = int(row[0]), int(row[1]), float(row[2])
            link_lookup[(a, b)] = score
            if score >= CLUSTER_EDGE_THRESHOLD:
                uf.union(a, b)

        components = uf.components()

        # Delete old clusters and rewrite (cascades to members)
        db.execute("DELETE FROM character_identity_clusters WHERE 1=1")

        clusters_written = 0
        for members in components.values():
            if len(members) < 2 or len(members) > MAX_CLUSTER_SIZE:
                continue
            # Compute internal density
            pair_scores: list[float] = []
            for i, ma in enumerate(members):
                for mb in members[i + 1:]:
                    key = (min(ma, mb), max(ma, mb))
                    pair_scores.append(link_lookup.get(key, 0.0))
            density = sum(pair_scores) / len(pair_scores) if pair_scores else 0.0
            confidence = min(1.0, density)
            top_evidence = json_dumps_safe({
                "member_count": len(members),
                "pair_scores_summary": {"min": round(min(pair_scores), 4) if pair_scores else 0,
                                         "max": round(max(pair_scores), 4) if pair_scores else 0,
                                         "mean": round(density, 4)},
            })

            db.execute(
                """
                INSERT INTO character_identity_clusters
                    (member_count, cluster_confidence, internal_density,
                     top_evidence_json, computed_at, source_run_id)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (len(members), round(confidence, 6), round(density, 6),
                 top_evidence, computed_at, run_id),
            )
            # Get the auto-increment cluster_id
            cid_rows = db.fetch_all("SELECT LAST_INSERT_ID() AS cid")
            cluster_id = int(cid_rows[0]["cid"])

            member_rows = []
            for m in members:
                # membership_score = mean of this member's link scores to other cluster members
                m_scores = []
                for other in members:
                    if other == m:
                        continue
                    key = (min(m, other), max(m, other))
                    m_scores.append(link_lookup.get(key, 0.0))
                ms = sum(m_scores) / len(m_scores) if m_scores else 0.0
                member_rows.append((cluster_id, m, round(ms, 6), computed_at))

            if member_rows:
                db.execute_many(
                    """
                    INSERT INTO character_identity_cluster_members
                        (cluster_id, character_id, membership_score, computed_at)
                    VALUES (%s, %s, %s, %s)
                    """,
                    member_rows,
                )
            clusters_written += 1

        # ── 6. Finalize ──────────────────────────────────────────────
        # Score distribution over ALL candidate pairs (not just emitted
        # links).  When no pair crosses the emit threshold, this is the
        # only way to see whether scores were uniformly low or just below
        # the cut.
        score_dist: dict[str, Any] = {}
        if all_scores:
            s = sorted(all_scores)
            score_dist = {
                "candidate_count": len(all_scores),
                "p50": round(s[len(s) // 2], 4),
                "p90": round(s[int(len(s) * 0.9)], 4),
                "p99": round(s[int(len(s) * 0.99)], 4),
                "min": round(s[0], 4),
                "max": round(s[-1], 4),
                "zero_score_pairs": zero_score_pairs,
                "near_miss_count": near_miss_count,
                "emit_threshold": LINK_EMIT_THRESHOLD,
            }

        # Per-component diagnostics: mean contribution + how many pairs had
        # a non-zero signal for each component.  A component with 0% non-zero
        # almost certainly means its cache table is empty.
        denom = max(1, len(all_scores))
        component_means = {k: round(v / denom, 4) for k, v in component_sums.items()}
        component_hit_rates = {k: round(component_nonzero[k] / denom, 4) for k in component_nonzero}

        # Cache coverage over the deduplicated character list.  char_list is
        # the set of character IDs that appear in at least one candidate pair.
        char_count = max(1, len(char_list))
        cache_coverage = {
            "char_pool_size": len(char_list),
            "org_history_hit_rate": round(len(org_cache) / char_count, 4),
            "temporal_hit_rate": round(len(temporal_cache) / char_count, 4),
            "snapshot_hit_rate": round(len(snapshot_cache) / char_count, 4),
            "typed_pair_count": len(typed_cache),
        }

        logger.info(
            "identity-resolution scoring: candidates=%d links=%d clusters=%d "
            "score_p50=%s score_p99=%s zero_pairs=%d near_miss=%d "
            "cache_hits=%s component_hit_rates=%s",
            len(candidates), rows_written, clusters_written,
            score_dist.get("p50"), score_dist.get("p99"),
            zero_score_pairs, near_miss_count,
            cache_coverage, component_hit_rates,
        )

        _finish_run(db, run_id, "success", len(candidates), rows_written, clusters_written,
                     score_dist=score_dist)

        duration_ms = int((time.perf_counter() - started) * 1000)
        result = JobResult.success(
            job_key=lock_key,
            summary=f"Scored {len(candidates)} pairs → {rows_written} links, {clusters_written} clusters.",
            rows_processed=rows_processed, rows_written=rows_written,
            duration_ms=duration_ms,
            meta={"run_id": run_id, "candidate_pairs": len(candidates),
                  "links_written": rows_written, "clusters_written": clusters_written,
                  "score_distribution": score_dist,
                  "component_means": component_means,
                  "component_hit_rates": component_hit_rates,
                  "cache_coverage": cache_coverage},
        ).to_dict()
        finish_job_run(db, job, status="success", rows_processed=rows_processed,
                       rows_written=rows_written, meta=result)
        return result

    except Exception as exc:
        _finish_run(db, run_id, "failed", 0, 0, 0, error=str(exc))
        finish_job_run(db, job, status="failed", rows_processed=rows_processed,
                       rows_written=rows_written, error_text=str(exc))
        raise


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _bulk_load(
    db: SupplyCoreDb, table: str, key_col: str, char_ids: list[int],
    extra_where: str = "",
) -> dict[int, dict[str, Any]]:
    out: dict[int, dict[str, Any]] = {}
    for i in range(0, len(char_ids), BATCH_SIZE):
        batch = char_ids[i : i + BATCH_SIZE]
        ph = ",".join(["%s"] * len(batch))
        rows = db.fetch_all(
            f"SELECT * FROM {table} WHERE {key_col} IN ({ph}) {extra_where}",
            tuple(batch),
        )
        for r in rows:
            cid = r.get(key_col)
            if cid is not None:
                out[int(cid)] = r
    return out


def _bulk_load_snapshots(db: SupplyCoreDb, char_ids: list[int]) -> dict[int, dict[str, Any]]:
    """Load latest spy_v1 snapshot per character."""
    out: dict[int, dict[str, Any]] = {}
    for i in range(0, len(char_ids), BATCH_SIZE):
        batch = char_ids[i : i + BATCH_SIZE]
        ph = ",".join(["%s"] * len(batch))
        rows = db.fetch_all(
            f"""
            SELECT cfs.character_id, cfs.feature_vector_json
            FROM character_feature_snapshots cfs
            INNER JOIN (
                SELECT character_id, MAX(computed_at) AS max_at
                FROM character_feature_snapshots
                WHERE feature_set = 'spy_v1' AND character_id IN ({ph})
                GROUP BY character_id
            ) latest ON latest.character_id = cfs.character_id AND latest.max_at = cfs.computed_at
            WHERE cfs.feature_set = 'spy_v1'
            """,
            tuple(batch),
        )
        for r in rows:
            cid = r.get("character_id")
            if cid is not None:
                out[int(cid)] = r
    return out


def _bulk_load_cross_side(db: SupplyCoreDb, char_ids: list[int]) -> dict[tuple[int, int], int]:
    """Load cross-side interaction counts for candidate pairs."""
    out: dict[tuple[int, int], int] = {}
    for i in range(0, len(char_ids), BATCH_SIZE):
        batch = char_ids[i : i + BATCH_SIZE]
        ph = ",".join(["%s"] * len(batch))
        rows = db.fetch_all(
            f"""
            SELECT character_a_id, character_b_id, interaction_count
            FROM character_typed_interactions
            WHERE interaction_type = 'cross_side'
              AND character_a_id IN ({ph})
            """,
            tuple(batch),
        )
        for r in rows:
            a, b = int(r["character_a_id"]), int(r["character_b_id"])
            key = (min(a, b), max(a, b))
            out[key] = out.get(key, 0) + int(r.get("interaction_count") or 0)
    return out


def _finish_run(
    db: SupplyCoreDb, run_id: str, status: str,
    candidate_pairs: int, links: int, clusters: int,
    score_dist: dict[str, Any] | None = None, error: str | None = None,
) -> None:
    db.execute(
        """
        UPDATE identity_resolution_runs
        SET finished_at = %s, status = %s, candidate_pairs = %s,
            links_written = %s, clusters_written = %s,
            score_distribution_json = %s, error_text = %s
        WHERE run_id = %s
        """,
        (_now_sql(), status, candidate_pairs, links, clusters,
         json_dumps_safe(score_dist) if score_dist else None,
         error, run_id),
    )
