"""Graph Universe Sync — populate Neo4j with EVE universe topology.

Reads ``ref_systems``, ``ref_constellations``, ``ref_regions``, and
``ref_stargates`` from SQL and creates/updates the corresponding nodes
and ``CONNECTS_TO`` relationships in Neo4j.

Idempotent via Cypher ``MERGE``.  Runs daily or whenever the SDE is
re-imported.
"""

from __future__ import annotations

import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

if __package__ in (None, ""):
    package_root = str(Path(__file__).resolve().parents[2])
    if package_root not in sys.path:
        sys.path.insert(0, package_root)
    from orchestrator.db import SupplyCoreDb
    from orchestrator.job_result import JobResult
    from orchestrator.neo4j import Neo4jClient, Neo4jConfig
else:
    from ..db import SupplyCoreDb
    from ..job_result import JobResult
    from ..neo4j import Neo4jClient, Neo4jConfig

BATCH_SIZE = 500


def run_graph_universe_sync(
    db: SupplyCoreDb,
    neo4j_raw: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Sync universe topology (systems, constellations, regions, stargates) into Neo4j."""
    started = time.perf_counter()
    job_name = "graph_universe_sync"

    config = Neo4jConfig.from_runtime(neo4j_raw or {})
    if not config.enabled:
        return JobResult.skipped(job_key=job_name, reason="neo4j disabled").to_dict()

    client = Neo4jClient(config)
    rows_processed = 0
    rows_written = 0

    # ── Ensure constraints and indexes ──────────────────────────────────
    client.query("CREATE CONSTRAINT constellation_id IF NOT EXISTS FOR (n:Constellation) REQUIRE n.constellation_id IS UNIQUE")
    client.query("CREATE CONSTRAINT region_region_id IF NOT EXISTS FOR (n:Region) REQUIRE n.region_id IS UNIQUE")
    client.query("CREATE INDEX system_security IF NOT EXISTS FOR (n:System) ON (n.security)")
    client.query("CREATE INDEX system_constellation IF NOT EXISTS FOR (n:System) ON (n.constellation_id)")

    # ── Sync regions ────────────────────────────────────────────────────
    regions = db.fetch_all("SELECT region_id, region_name FROM ref_regions")
    rows_processed += len(regions)
    for offset in range(0, len(regions), BATCH_SIZE):
        chunk = regions[offset:offset + BATCH_SIZE]
        client.query(
            """
            UNWIND $rows AS row
            MERGE (r:Region {region_id: toInteger(row.region_id)})
              SET r.name = row.region_name
            """,
            {"rows": chunk},
        )
        rows_written += len(chunk)

    # ── Sync constellations ─────────────────────────────────────────────
    constellations = db.fetch_all(
        "SELECT constellation_id, region_id, constellation_name FROM ref_constellations"
    )
    rows_processed += len(constellations)
    for offset in range(0, len(constellations), BATCH_SIZE):
        chunk = constellations[offset:offset + BATCH_SIZE]
        client.query(
            """
            UNWIND $rows AS row
            MERGE (c:Constellation {constellation_id: toInteger(row.constellation_id)})
              SET c.name = row.constellation_name
            WITH c, row
            MERGE (r:Region {region_id: toInteger(row.region_id)})
            MERGE (c)-[:IN_REGION]->(r)
            """,
            {"rows": chunk},
        )
        rows_written += len(chunk)

    # ── Sync systems ────────────────────────────────────────────────────
    systems = db.fetch_all(
        "SELECT system_id, constellation_id, region_id, system_name, security FROM ref_systems"
    )
    rows_processed += len(systems)
    for offset in range(0, len(systems), BATCH_SIZE):
        chunk = systems[offset:offset + BATCH_SIZE]
        client.query(
            """
            UNWIND $rows AS row
            MERGE (s:System {system_id: toInteger(row.system_id)})
              SET s.name = row.system_name,
                  s.security = toFloat(row.security),
                  s.constellation_id = toInteger(row.constellation_id),
                  s.region_id = toInteger(row.region_id)
            WITH s, row
            MERGE (c:Constellation {constellation_id: toInteger(row.constellation_id)})
            MERGE (s)-[:IN_CONSTELLATION]->(c)
            """,
            {"rows": chunk},
        )
        rows_written += len(chunk)

    # ── Sync stargate connections ───────────────────────────────────────
    stargates = db.fetch_all(
        "SELECT system_id, dest_system_id FROM ref_stargates"
    )
    rows_processed += len(stargates)
    for offset in range(0, len(stargates), BATCH_SIZE):
        chunk = stargates[offset:offset + BATCH_SIZE]
        client.query(
            """
            UNWIND $rows AS row
            MATCH (a:System {system_id: toInteger(row.system_id)})
            MATCH (b:System {system_id: toInteger(row.dest_system_id)})
            MERGE (a)-[:CONNECTS_TO]->(b)
            """,
            {"rows": chunk},
        )
        rows_written += len(chunk)

    # ── Record sync state ───────────────────────────────────────────────
    now_sql = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
    db.execute(
        """
        INSERT INTO graph_sync_state (sync_key, last_synced_at)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE last_synced_at = VALUES(last_synced_at)
        """,
        ("graph_universe_sync", now_sql),
    )

    duration_ms = int((time.perf_counter() - started) * 1000)
    return JobResult.success(
        job_key=job_name,
        summary=f"Synced {len(regions)} regions, {len(constellations)} constellations, {len(systems)} systems, {len(stargates)} stargates.",
        rows_processed=rows_processed,
        rows_written=rows_written,
        duration_ms=duration_ms,
    ).to_dict()
