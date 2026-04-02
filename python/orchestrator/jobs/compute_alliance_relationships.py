"""Compute alliance relationship graph from ALL killmail co-occurrence data.

Builds two relationship types from killmail co-occurrence:

* **allied** — two alliances whose members appear as co-attackers on the
  same killmail (they're fighting on the same side).
* **hostile** — two alliances where one attacks the other (attacker vs victim).

Additionally detects **temporary ceasefires**: pairs of alliances that are
normally hostile but cooperate in specific regions or against specific enemies.

Strategy:
  1. MariaDB extracts raw killmail data (efficient sequential scan).
  2. Python computes pairwise edge accumulators with time-decay + region context.
  3. Neo4j stores the graph and does the heavy lifting for:
     - Transitive alliance inference (friend-of-friend)
     - Regional relationship overlays
     - Ceasefire anomaly detection (hostile pair co-attacking in a region)
  4. Results materialized to ``alliance_relationships`` (MariaDB) for fast reads.

Uses ALL killmails including ``mail_type='untracked'`` — this is the whole
point of ingesting everything from zKill.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from typing import Any

from ..db import SupplyCoreDb
from ..job_result import JobResult
from ..job_utils import finish_job_run, start_job_run

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SYNC_STATE_KEY = "compute_alliance_relationships_cursor"
MAX_KILLMAILS_PER_RUN = 50000

# Minimum co-occurrences to create an edge
MIN_ALLIED_SHARED_KILLMAILS = 3
MIN_HOSTILE_ENGAGEMENTS = 3
MIN_CEASEFIRE_CO_OCCURRENCES = 2

# Time windows for rolling weights
WINDOW_7D = timedelta(days=7)
WINDOW_30D = timedelta(days=30)
WINDOW_90D = timedelta(days=90)


def _now_sql() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")


# ---------------------------------------------------------------------------
# Sync-state helpers
# ---------------------------------------------------------------------------

def _sync_state_get(db: SupplyCoreDb, dataset_key: str) -> int | None:
    row = db.fetch_one(
        "SELECT last_cursor FROM sync_state WHERE dataset_key = %s LIMIT 1",
        (dataset_key,),
    )
    if row and row.get("last_cursor"):
        cursor = str(row["last_cursor"]).strip()
        return int(cursor) if cursor.isdigit() else None
    return None


def _sync_state_upsert(db: SupplyCoreDb, dataset_key: str, cursor: int, row_count: int) -> None:
    db.execute(
        """
        INSERT INTO sync_state (dataset_key, sync_mode, status, last_success_at, last_cursor, last_row_count, last_error_message)
        VALUES (%s, 'incremental', 'success', UTC_TIMESTAMP(), %s, %s, NULL)
        ON DUPLICATE KEY UPDATE
            status = 'success',
            last_success_at = VALUES(last_success_at),
            last_cursor = VALUES(last_cursor),
            last_row_count = VALUES(last_row_count),
            last_error_message = NULL,
            updated_at = CURRENT_TIMESTAMP
        """,
        (dataset_key, str(cursor), max(0, int(row_count))),
    )


# ---------------------------------------------------------------------------
# Data collection from MariaDB (sequential scan — what SQL is good at)
# ---------------------------------------------------------------------------

def _fetch_killmail_attacker_alliances(
    db: SupplyCoreDb, min_sequence: int, max_rows: int
) -> tuple[list[dict[str, Any]], int]:
    """Fetch killmails with attacker alliance IDs + region context for graph building.

    Each row includes region_id for regional relationship detection.
    """
    rows = db.fetch_all(
        """
        SELECT
            ke.sequence_id,
            ke.killmail_time,
            ke.victim_alliance_id,
            ke.region_id,
            ke.solar_system_id,
            GROUP_CONCAT(DISTINCT ka.alliance_id ORDER BY ka.alliance_id SEPARATOR ',') AS attacker_alliance_ids
        FROM killmail_events ke
        INNER JOIN killmail_attackers ka ON ka.sequence_id = ke.sequence_id
        WHERE ke.sequence_id > %s
          AND ka.alliance_id IS NOT NULL AND ka.alliance_id > 0
        GROUP BY ke.sequence_id
        ORDER BY ke.sequence_id ASC
        LIMIT %s
        """,
        (min_sequence, max_rows),
    )
    max_seq = max((int(r["sequence_id"]) for r in rows), default=min_sequence)
    return rows, max_seq


# ---------------------------------------------------------------------------
# Edge computation (Python — pairwise accumulation)
# ---------------------------------------------------------------------------

def _parse_km_time(km_time: Any) -> datetime | None:
    if km_time is None:
        return None
    if isinstance(km_time, str):
        try:
            return datetime.fromisoformat(km_time).replace(tzinfo=UTC)
        except (ValueError, TypeError):
            return None
    if isinstance(km_time, datetime):
        return km_time.replace(tzinfo=UTC) if km_time.tzinfo is None else km_time
    return None


def _compute_edges(
    rows: list[dict[str, Any]],
) -> tuple[
    dict[tuple[int, int], dict],
    dict[tuple[int, int], dict],
    dict[tuple[int, int, int], dict],
]:
    """Compute allied, hostile, and ceasefire edge accumulators.

    Returns:
        allied_edges: {(a1, a2): {...}} — co-attacking alliances (same side)
        hostile_edges: {(attacker, victim): {...}} — attacker vs victim
        ceasefire_candidates: {(a1, a2, region_id): {...}} — hostile pairs co-attacking
            in a specific region (potential ceasefire/temporary cooperation)
    """
    now = datetime.now(UTC)

    def _new_edge() -> dict:
        return {
            "count": 0, "w7": 0.0, "w30": 0.0, "w90": 0.0,
            "first_seen": None, "last_seen": None,
            "regions": defaultdict(int),  # region_id → count
        }

    allied_edges: dict[tuple[int, int], dict] = defaultdict(_new_edge)
    hostile_edges: dict[tuple[int, int], dict] = defaultdict(_new_edge)

    for row in rows:
        km_time = _parse_km_time(row.get("killmail_time"))
        if km_time is None:
            continue

        age = now - km_time
        w7 = 1.0 if age <= WINDOW_7D else 0.0
        w30 = 1.0 if age <= WINDOW_30D else 0.0
        w90 = 1.0 if age <= WINDOW_90D else 0.0

        region_id = int(row.get("region_id") or 0)

        # Parse attacker alliances
        raw_attackers = str(row.get("attacker_alliance_ids") or "")
        attacker_ids = []
        for a in raw_attackers.split(","):
            a = a.strip()
            if a.isdigit() and int(a) > 0:
                attacker_ids.append(int(a))

        if not attacker_ids:
            continue

        unique_attackers = sorted(set(attacker_ids))

        # Allied edges: all pairs of co-attacking alliances
        for i in range(len(unique_attackers)):
            for j in range(i + 1, len(unique_attackers)):
                a1, a2 = unique_attackers[i], unique_attackers[j]
                key = (a1, a2) if a1 < a2 else (a2, a1)
                edge = allied_edges[key]
                edge["count"] += 1
                edge["w7"] += w7
                edge["w30"] += w30
                edge["w90"] += w90
                if region_id > 0:
                    edge["regions"][region_id] += 1
                if edge["first_seen"] is None or km_time < edge["first_seen"]:
                    edge["first_seen"] = km_time
                if edge["last_seen"] is None or km_time > edge["last_seen"]:
                    edge["last_seen"] = km_time

        # Hostile edges: each attacker alliance vs victim alliance
        victim_alliance_id = int(row.get("victim_alliance_id") or 0)
        if victim_alliance_id > 0:
            for att_id in unique_attackers:
                if att_id == victim_alliance_id:
                    continue  # Skip awox
                key = (att_id, victim_alliance_id)
                edge = hostile_edges[key]
                edge["count"] += 1
                edge["w7"] += w7
                edge["w30"] += w30
                edge["w90"] += w90
                if region_id > 0:
                    edge["regions"][region_id] += 1
                if edge["first_seen"] is None or km_time < edge["first_seen"]:
                    edge["first_seen"] = km_time
                if edge["last_seen"] is None or km_time > edge["last_seen"]:
                    edge["last_seen"] = km_time

    # Detect ceasefire candidates: pairs that are hostile but ALSO co-attack,
    # potentially in specific regions only.
    hostile_pairs: set[tuple[int, int]] = set()
    for (att, vic) in hostile_edges:
        canonical = (att, vic) if att < vic else (vic, att)
        hostile_pairs.add(canonical)

    ceasefire_candidates: dict[tuple[int, int, int], dict] = {}
    for (a1, a2), edge in allied_edges.items():
        canonical = (a1, a2) if a1 < a2 else (a2, a1)
        if canonical not in hostile_pairs:
            continue
        # This pair is both hostile AND allied — potential ceasefire
        # Break down by region to see where the cooperation happens
        for region_id, region_count in edge["regions"].items():
            if region_count >= MIN_CEASEFIRE_CO_OCCURRENCES:
                ceasefire_candidates[(a1, a2, region_id)] = {
                    "co_attacks_in_region": region_count,
                    "total_co_attacks": edge["count"],
                    "first_seen": edge["first_seen"],
                    "last_seen": edge["last_seen"],
                    "w7": edge["w7"],
                    "w30": edge["w30"],
                    "w90": edge["w90"],
                }

    return dict(allied_edges), dict(hostile_edges), ceasefire_candidates


def _compute_confidence(edge: dict, max_count: int) -> float:
    """Compute confidence score [0.0, 1.0] from volume and recency."""
    if max_count <= 0:
        return 0.0
    volume = min(1.0, edge["count"] / max(1, max_count * 0.3))
    recency = 0.0
    if edge["w7"] > 0:
        recency = 1.0
    elif edge["w30"] > 0:
        recency = 0.7
    elif edge["w90"] > 0:
        recency = 0.4
    else:
        recency = 0.1
    return round(min(1.0, volume * 0.6 + recency * 0.4), 4)


# ---------------------------------------------------------------------------
# Persistence: MariaDB (materialized view for fast PHP reads)
# ---------------------------------------------------------------------------

def _flush_edges_to_mariadb(
    db: SupplyCoreDb,
    allied_edges: dict[tuple[int, int], dict],
    hostile_edges: dict[tuple[int, int], dict],
    computed_at: str,
) -> int:
    """Upsert alliance relationships to MariaDB."""
    rows_written = 0

    max_allied = max((e["count"] for e in allied_edges.values()), default=1)
    max_hostile = max((e["count"] for e in hostile_edges.values()), default=1)

    def _fmt_dt(dt: datetime | None) -> str | None:
        return dt.strftime("%Y-%m-%d %H:%M:%S") if dt else None

    # Allied edges (symmetric)
    allied_batch = []
    for (a1, a2), edge in allied_edges.items():
        if edge["count"] < MIN_ALLIED_SHARED_KILLMAILS:
            continue
        confidence = _compute_confidence(edge, max_allied)
        row = (
            a1, a2, "allied", edge["count"], 0, confidence,
            edge["w7"], edge["w30"], edge["w90"],
            _fmt_dt(edge["first_seen"]), _fmt_dt(edge["last_seen"]), computed_at,
        )
        allied_batch.append(row)
        allied_batch.append((
            a2, a1, "allied", edge["count"], 0, confidence,
            edge["w7"], edge["w30"], edge["w90"],
            _fmt_dt(edge["first_seen"]), _fmt_dt(edge["last_seen"]), computed_at,
        ))

    # Hostile edges (directional)
    hostile_batch = []
    for (attacker, victim), edge in hostile_edges.items():
        if edge["count"] < MIN_HOSTILE_ENGAGEMENTS:
            continue
        confidence = _compute_confidence(edge, max_hostile)
        hostile_batch.append((
            attacker, victim, "hostile", edge["count"], 0, confidence,
            edge["w7"], edge["w30"], edge["w90"],
            _fmt_dt(edge["first_seen"]), _fmt_dt(edge["last_seen"]), computed_at,
        ))

    all_rows = allied_batch + hostile_batch
    if not all_rows:
        return 0

    for batch_start in range(0, len(all_rows), 500):
        chunk = all_rows[batch_start:batch_start + 500]
        with db.transaction() as (_, cursor):
            for row in chunk:
                cursor.execute(
                    """
                    INSERT INTO alliance_relationships (
                        source_alliance_id, target_alliance_id, relationship_type,
                        shared_killmails, shared_pilots, confidence,
                        weight_7d, weight_30d, weight_90d,
                        first_seen_at, last_seen_at, computed_at
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE
                        shared_killmails = VALUES(shared_killmails),
                        shared_pilots = VALUES(shared_pilots),
                        confidence = VALUES(confidence),
                        weight_7d = VALUES(weight_7d),
                        weight_30d = VALUES(weight_30d),
                        weight_90d = VALUES(weight_90d),
                        first_seen_at = COALESCE(
                            LEAST(first_seen_at, VALUES(first_seen_at)),
                            VALUES(first_seen_at)
                        ),
                        last_seen_at = COALESCE(
                            GREATEST(last_seen_at, VALUES(last_seen_at)),
                            VALUES(last_seen_at)
                        ),
                        computed_at = VALUES(computed_at)
                    """,
                    row,
                )
                rows_written += max(0, int(cursor.rowcount or 0))

    return rows_written


def _flush_ceasefires_to_mariadb(
    db: SupplyCoreDb,
    ceasefire_candidates: dict[tuple[int, int, int], dict],
    computed_at: str,
) -> int:
    """Write ceasefire detections to alliance_ceasefires table."""
    if not ceasefire_candidates:
        return 0

    rows_written = 0
    batch = list(ceasefire_candidates.items())

    for batch_start in range(0, len(batch), 500):
        chunk = batch[batch_start:batch_start + 500]
        with db.transaction() as (_, cursor):
            for (a1, a2, region_id), data in chunk:
                def _fmt(dt: datetime | None) -> str | None:
                    return dt.strftime("%Y-%m-%d %H:%M:%S") if dt else None

                cursor.execute(
                    """
                    INSERT INTO alliance_ceasefires (
                        alliance_id_a, alliance_id_b, region_id,
                        co_attacks_in_region, total_co_attacks,
                        weight_7d, weight_30d, weight_90d,
                        first_seen_at, last_seen_at, computed_at
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE
                        co_attacks_in_region = VALUES(co_attacks_in_region),
                        total_co_attacks = VALUES(total_co_attacks),
                        weight_7d = VALUES(weight_7d),
                        weight_30d = VALUES(weight_30d),
                        weight_90d = VALUES(weight_90d),
                        first_seen_at = COALESCE(
                            LEAST(first_seen_at, VALUES(first_seen_at)),
                            VALUES(first_seen_at)
                        ),
                        last_seen_at = COALESCE(
                            GREATEST(last_seen_at, VALUES(last_seen_at)),
                            VALUES(last_seen_at)
                        ),
                        computed_at = VALUES(computed_at)
                    """,
                    (
                        a1, a2, region_id,
                        data["co_attacks_in_region"], data["total_co_attacks"],
                        data["w7"], data["w30"], data["w90"],
                        _fmt(data["first_seen"]), _fmt(data["last_seen"]),
                        computed_at,
                    ),
                )
                rows_written += max(0, int(cursor.rowcount or 0))

    return rows_written


# ---------------------------------------------------------------------------
# Neo4j: push edges and let graph do the heavy lifting
# ---------------------------------------------------------------------------

def _push_to_neo4j(
    neo4j_client: Any,
    allied_edges: dict[tuple[int, int], dict],
    hostile_edges: dict[tuple[int, int], dict],
    ceasefire_candidates: dict[tuple[int, int, int], dict],
) -> int:
    """Push alliance relationship edges to Neo4j.

    Neo4j then enables:
    - Transitive inference: A allied-with B, B allied-with C → A likely allied-with C
    - Community detection: identify coalition clusters via graph algorithms
    - Ceasefire anomaly: HOSTILE_TO + ALLIED_WITH between same pair in specific region
    """
    if neo4j_client is None:
        return 0

    edges_pushed = 0
    computed_at = datetime.now(UTC).isoformat()

    # ── Allied edges ──────────────────────────────────────────────────────
    allied_params = []
    for (a1, a2), edge in allied_edges.items():
        if edge["count"] < MIN_ALLIED_SHARED_KILLMAILS:
            continue
        # Top 3 regions for the relationship
        top_regions = sorted(edge["regions"].items(), key=lambda x: x[1], reverse=True)[:3]
        allied_params.append({
            "source": a1, "target": a2,
            "shared_killmails": edge["count"],
            "weight_7d": edge["w7"], "weight_30d": edge["w30"], "weight_90d": edge["w90"],
            "top_region_1": top_regions[0][0] if len(top_regions) > 0 else 0,
            "top_region_2": top_regions[1][0] if len(top_regions) > 1 else 0,
            "top_region_3": top_regions[2][0] if len(top_regions) > 2 else 0,
            "computed_at": computed_at,
        })

    if allied_params:
        try:
            for batch_start in range(0, len(allied_params), 200):
                chunk = allied_params[batch_start:batch_start + 200]
                neo4j_client.query(
                    """
                    UNWIND $edges AS e
                    MERGE (a1:Alliance {alliance_id: e.source})
                    MERGE (a2:Alliance {alliance_id: e.target})
                    MERGE (a1)-[r:ALLIED_WITH]-(a2)
                    SET r.shared_killmails = e.shared_killmails,
                        r.weight_7d = e.weight_7d,
                        r.weight_30d = e.weight_30d,
                        r.weight_90d = e.weight_90d,
                        r.top_region_1 = e.top_region_1,
                        r.top_region_2 = e.top_region_2,
                        r.top_region_3 = e.top_region_3,
                        r.computed_at = e.computed_at
                    """,
                    {"edges": chunk},
                )
                edges_pushed += len(chunk)
        except Exception:
            logger.warning("Failed to push allied edges to Neo4j", exc_info=True)

    # ── Hostile edges ─────────────────────────────────────────────────────
    hostile_params = []
    for (attacker, victim), edge in hostile_edges.items():
        if edge["count"] < MIN_HOSTILE_ENGAGEMENTS:
            continue
        top_regions = sorted(edge["regions"].items(), key=lambda x: x[1], reverse=True)[:3]
        hostile_params.append({
            "source": attacker, "target": victim,
            "engagements": edge["count"],
            "weight_7d": edge["w7"], "weight_30d": edge["w30"], "weight_90d": edge["w90"],
            "top_region_1": top_regions[0][0] if len(top_regions) > 0 else 0,
            "top_region_2": top_regions[1][0] if len(top_regions) > 1 else 0,
            "top_region_3": top_regions[2][0] if len(top_regions) > 2 else 0,
            "computed_at": computed_at,
        })

    if hostile_params:
        try:
            for batch_start in range(0, len(hostile_params), 200):
                chunk = hostile_params[batch_start:batch_start + 200]
                neo4j_client.query(
                    """
                    UNWIND $edges AS e
                    MERGE (a1:Alliance {alliance_id: e.source})
                    MERGE (a2:Alliance {alliance_id: e.target})
                    MERGE (a1)-[r:HOSTILE_TO]->(a2)
                    SET r.engagements = e.engagements,
                        r.weight_7d = e.weight_7d,
                        r.weight_30d = e.weight_30d,
                        r.weight_90d = e.weight_90d,
                        r.top_region_1 = e.top_region_1,
                        r.top_region_2 = e.top_region_2,
                        r.top_region_3 = e.top_region_3,
                        r.computed_at = e.computed_at
                    """,
                    {"edges": chunk},
                )
                edges_pushed += len(chunk)
        except Exception:
            logger.warning("Failed to push hostile edges to Neo4j", exc_info=True)

    # ── Ceasefire edges (regional cooperation between hostile pairs) ──────
    ceasefire_params = []
    for (a1, a2, region_id), data in ceasefire_candidates.items():
        ceasefire_params.append({
            "source": a1, "target": a2, "region_id": region_id,
            "co_attacks": data["co_attacks_in_region"],
            "weight_30d": data["w30"],
            "computed_at": computed_at,
        })

    if ceasefire_params:
        try:
            for batch_start in range(0, len(ceasefire_params), 200):
                chunk = ceasefire_params[batch_start:batch_start + 200]
                neo4j_client.query(
                    """
                    UNWIND $edges AS e
                    MATCH (a1:Alliance {alliance_id: e.source})
                    MATCH (a2:Alliance {alliance_id: e.target})
                    MATCH (r:Region {region_id: e.region_id})
                    MERGE (a1)-[cf:CEASEFIRE_WITH {region_id: e.region_id}]-(a2)
                    SET cf.co_attacks = e.co_attacks,
                        cf.weight_30d = e.weight_30d,
                        cf.computed_at = e.computed_at
                    """,
                    {"edges": chunk},
                )
                edges_pushed += len(chunk)
        except Exception:
            logger.warning("Failed to push ceasefire edges to Neo4j", exc_info=True)

    return edges_pushed


def _neo4j_compute_transitive_allies(neo4j_client: Any) -> list[dict]:
    """Let Neo4j find transitive alliance relationships (friend-of-friend).

    Returns alliance pairs that are not directly connected but share strong
    mutual allies — these are likely in the same coalition.
    """
    if neo4j_client is None:
        return []

    try:
        return neo4j_client.query(
            """
            MATCH (a1:Alliance)-[r1:ALLIED_WITH]-(bridge:Alliance)-[r2:ALLIED_WITH]-(a2:Alliance)
            WHERE a1.alliance_id < a2.alliance_id
              AND NOT (a1)-[:ALLIED_WITH]-(a2)
              AND r1.weight_30d > 0 AND r2.weight_30d > 0
            WITH a1, a2,
                 COUNT(DISTINCT bridge) AS shared_allies,
                 AVG(r1.weight_30d + r2.weight_30d) / 2 AS avg_bridge_weight
            WHERE shared_allies >= 2
            RETURN a1.alliance_id AS source_id,
                   a2.alliance_id AS target_id,
                   shared_allies,
                   avg_bridge_weight
            ORDER BY shared_allies DESC, avg_bridge_weight DESC
            LIMIT 100
            """
        )
    except Exception:
        logger.warning("Failed to compute transitive allies in Neo4j", exc_info=True)
        return []


def _neo4j_detect_ceasefires(neo4j_client: Any) -> list[dict]:
    """Let Neo4j detect ceasefire anomalies from the graph.

    Finds alliance pairs that have BOTH HOSTILE_TO and ALLIED_WITH edges,
    which indicates they're enemies in general but cooperate in certain contexts.
    Enriches with regional context from the CEASEFIRE_WITH edges.
    """
    if neo4j_client is None:
        return []

    try:
        return neo4j_client.query(
            """
            MATCH (a1:Alliance)-[h:HOSTILE_TO]->(a2:Alliance)
            WHERE (a1)-[:ALLIED_WITH]-(a2)
            OPTIONAL MATCH (a1)-[cf:CEASEFIRE_WITH]-(a2)
            WITH a1, a2, h,
                 COLLECT(DISTINCT {region_id: cf.region_id, co_attacks: cf.co_attacks}) AS ceasefire_regions
            RETURN a1.alliance_id AS alliance_a,
                   a2.alliance_id AS alliance_b,
                   a1.name AS name_a,
                   a2.name AS name_b,
                   h.engagements AS hostile_engagements,
                   ceasefire_regions
            ORDER BY h.engagements DESC
            LIMIT 50
            """
        )
    except Exception:
        logger.warning("Failed to detect ceasefires in Neo4j", exc_info=True)
        return []


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_compute_alliance_relationships(
    db: SupplyCoreDb,
    runtime: dict[str, Any] | None = None,
    neo4j_raw: dict[str, Any] | None = None,
    *,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Compute alliance relationship graph from killmail co-occurrence."""
    job_key = "compute_alliance_relationships"
    job = start_job_run(db, job_key)
    started = datetime.now(UTC)
    computed_at = _now_sql()

    try:
        # Get cursor
        last_cursor = _sync_state_get(db, SYNC_STATE_KEY)
        min_sequence = last_cursor if last_cursor is not None else 0

        # Initialize Neo4j client if available
        neo4j_client = None
        try:
            from ..neo4j import Neo4jClient, Neo4jConfig
            config = Neo4jConfig.from_runtime(neo4j_raw or {})
            if config.enabled:
                neo4j_client = Neo4jClient(config)
        except Exception:
            neo4j_client = None

        # Step 1: MariaDB fetches raw killmail data (sequential scan)
        rows, max_sequence = _fetch_killmail_attacker_alliances(
            db, min_sequence, MAX_KILLMAILS_PER_RUN,
        )

        if not rows:
            finish_job_run(db, job, status="success", rows_processed=0, rows_written=0)
            return JobResult.success(
                job_key=job_key,
                summary="No new killmails to process for alliance relationships.",
                rows_processed=0, rows_written=0, duration_ms=0,
            ).to_dict()

        # Step 2: Python computes pairwise edge accumulators
        allied_edges, hostile_edges, ceasefire_candidates = _compute_edges(rows)

        allied_count = sum(1 for e in allied_edges.values() if e["count"] >= MIN_ALLIED_SHARED_KILLMAILS)
        hostile_count = sum(1 for e in hostile_edges.values() if e["count"] >= MIN_HOSTILE_ENGAGEMENTS)
        ceasefire_count = len(ceasefire_candidates)

        logger.info(
            "Alliance relationships: %d killmails -> %d allied, %d hostile, %d ceasefire candidates",
            len(rows), allied_count, hostile_count, ceasefire_count,
        )

        rows_written = 0
        neo4j_pushed = 0
        transitive_allies = []
        detected_ceasefires = []

        if not dry_run:
            # Step 3a: Materialize to MariaDB for fast PHP reads
            rows_written = _flush_edges_to_mariadb(db, allied_edges, hostile_edges, computed_at)
            ceasefire_rows = _flush_ceasefires_to_mariadb(db, ceasefire_candidates, computed_at)
            rows_written += ceasefire_rows

            # Step 3b: Push to Neo4j for graph-powered queries
            neo4j_pushed = _push_to_neo4j(neo4j_client, allied_edges, hostile_edges, ceasefire_candidates)

            # Step 4: Let Neo4j do the heavy lifting — transitive inference
            transitive_allies = _neo4j_compute_transitive_allies(neo4j_client)
            if transitive_allies:
                logger.info("Neo4j found %d transitive alliance pairs", len(transitive_allies))

            # Step 5: Let Neo4j detect ceasefire anomalies
            detected_ceasefires = _neo4j_detect_ceasefires(neo4j_client)
            if detected_ceasefires:
                logger.info("Neo4j detected %d ceasefire situations", len(detected_ceasefires))

            # Update cursor
            _sync_state_upsert(db, SYNC_STATE_KEY, max_sequence, rows_written)

        duration_ms = int((datetime.now(UTC) - started).total_seconds() * 1000)
        finish_job_run(db, job, status="success", rows_processed=len(rows), rows_written=rows_written)

        return JobResult.success(
            job_key=job_key,
            summary=(
                f"Processed {len(rows)} killmails -> "
                f"{allied_count} allied, {hostile_count} hostile, "
                f"{ceasefire_count} ceasefire candidates "
                f"({rows_written} MariaDB, {neo4j_pushed} Neo4j). "
                f"Neo4j: {len(transitive_allies)} transitive allies, "
                f"{len(detected_ceasefires)} ceasefires detected."
            ),
            rows_processed=len(rows),
            rows_written=rows_written,
            duration_ms=duration_ms,
            meta={
                "min_sequence": min_sequence,
                "max_sequence": max_sequence,
                "killmails_scanned": len(rows),
                "allied_edges": allied_count,
                "hostile_edges": hostile_count,
                "ceasefire_candidates": ceasefire_count,
                "neo4j_edges_pushed": neo4j_pushed,
                "neo4j_transitive_allies": len(transitive_allies),
                "neo4j_ceasefires_detected": len(detected_ceasefires),
                "dry_run": dry_run,
            },
        ).to_dict()

    except Exception as exc:
        duration_ms = int((datetime.now(UTC) - started).total_seconds() * 1000)
        finish_job_run(db, job, status="failed", rows_processed=0, rows_written=0)
        return JobResult.error(
            job_key=job_key,
            error_text=str(exc),
            duration_ms=duration_ms,
        ).to_dict()
