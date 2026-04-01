"""Jump bridge sync — import and maintain alliance jump bridge network.

Reads jump bridge data from the ``jump_bridges`` table (populated by the
PHP import UI or CLI) and projects ``JUMP_BRIDGE`` relationships into Neo4j
between System nodes.  These relationships supplement ``CONNECTS_TO``
(stargate) edges and enable JB-aware routing for theater intelligence,
staging system detection, and threat corridor analysis.

Also provides a parser for the common Dotlan/clipboard JB list format
that the PHP side can call to populate the table.
"""

from __future__ import annotations

import re
import time
from datetime import UTC, datetime
from typing import Any

from ..db import SupplyCoreDb
from ..job_result import JobResult
from ..job_utils import finish_job_run, start_job_run
from ..json_utils import json_dumps_safe
from ..neo4j import Neo4jClient, Neo4jConfig

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

NEO4J_BATCH_SIZE = 200

# Known alliance ticker → alliance_id mapping (extend as needed)
_ALLIANCE_TICKERS: dict[str, int] = {}


def _now_sql() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")


# ---------------------------------------------------------------------------
# JB list parser — handles the Dotlan/clipboard format
# ---------------------------------------------------------------------------

# Each row in the pasted list looks like:
#   MC6O-F  II - 1  Vale of the...  FRT (5)  UH-9ZG  I - 1  Vale of the...  FRT (5)
# We need to extract: from_system_name, to_system_name, owner_name

_JB_LINE_RE = re.compile(
    r"^"
    r"(?P<from_sys>[A-Za-z0-9-]+)"       # from system name
    r"\s+.*?"                              # moon info + region (skip)
    r"(?P<from_owner>\S+)\s+\(\d+\)"      # owner ticker (N)
    r"\s+"
    r"(?P<to_sys>[A-Za-z0-9-]+)"          # to system name
    r"\s+.*?"                              # moon info + region (skip)
    r"(?P<to_owner>\S+)\s+\(\d+\)"        # owner ticker (N)
    r"\s*$"
)


def parse_jb_list(raw_text: str) -> list[dict[str, str]]:
    """Parse a pasted JB list into structured rows.

    Returns list of dicts with keys: from_system, to_system, owner.
    Skips header lines and unparseable rows.
    """
    results = []
    for line in raw_text.strip().splitlines():
        line = line.strip()
        if not line or line.startswith("From") or line.startswith("System"):
            continue

        m = _JB_LINE_RE.match(line)
        if m:
            results.append({
                "from_system": m.group("from_sys"),
                "to_system": m.group("to_sys"),
                "owner": m.group("from_owner"),
            })
        else:
            # Fallback: try splitting by tabs
            parts = re.split(r"\t+", line)
            if len(parts) >= 5:
                from_sys = parts[0].strip()
                # Find the to_system — it's the first column-like entry after owner
                to_sys = parts[4].strip() if len(parts) > 4 else ""
                owner = ""
                for p in parts:
                    p = p.strip()
                    om = re.match(r"^(\S+)\s+\(\d+\)$", p)
                    if om:
                        owner = om.group(1)
                        break
                if from_sys and to_sys:
                    results.append({
                        "from_system": from_sys,
                        "to_system": to_sys,
                        "owner": owner,
                    })

    return results


def import_jb_list_to_db(
    db: SupplyCoreDb,
    raw_text: str,
    *,
    source: str = "manual",
    deactivate_missing: bool = True,
) -> dict[str, int]:
    """Parse a JB list and upsert into the jump_bridges table.

    If ``deactivate_missing`` is True, any existing JB rows from the same
    ``source`` that are NOT in the new list will be marked ``is_active = 0``.

    Returns counts: {parsed, resolved, inserted, deactivated}.
    """
    parsed = parse_jb_list(raw_text)
    if not parsed:
        return {"parsed": 0, "resolved": 0, "inserted": 0, "deactivated": 0}

    # Resolve system names to IDs
    all_names = list({r["from_system"] for r in parsed} | {r["to_system"] for r in parsed})
    placeholders = ",".join(["%s"] * len(all_names))
    sys_rows = db.fetch_all(
        f"SELECT system_id, system_name FROM ref_systems WHERE system_name IN ({placeholders})",
        all_names,
    )
    name_to_id = {r["system_name"]: int(r["system_id"]) for r in sys_rows}

    imported_at = _now_sql()
    insert_rows = []
    seen_pairs: set[tuple[int, int]] = set()

    for r in parsed:
        from_id = name_to_id.get(r["from_system"])
        to_id = name_to_id.get(r["to_system"])
        if not from_id or not to_id or from_id == to_id:
            continue

        # Normalize direction (always store smaller ID first for dedup)
        pair = (min(from_id, to_id), max(from_id, to_id))
        if pair in seen_pairs:
            continue
        seen_pairs.add(pair)

        owner = r.get("owner", "")
        insert_rows.append((
            from_id, to_id, None, owner, source, 1, imported_at,
            None, owner, source, 1, imported_at,
        ))

    if insert_rows:
        db.execute_many(
            """
            INSERT INTO jump_bridges
                (from_system_id, to_system_id, owner_alliance_id,
                 owner_name, source, is_active, imported_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                owner_alliance_id = %s, owner_name = %s,
                source = %s, is_active = %s, imported_at = %s
            """,
            insert_rows,
        )

    deactivated = 0
    if deactivate_missing and seen_pairs:
        # Build list of active from/to pairs
        active_pairs_sql = " OR ".join(
            [f"(from_system_id = {p[0]} AND to_system_id = {p[1]})" for p in seen_pairs]
        )
        result = db.execute(
            f"""
            UPDATE jump_bridges
            SET is_active = 0
            WHERE source = %s
              AND is_active = 1
              AND NOT ({active_pairs_sql})
            """,
            (source,),
        )
        deactivated = getattr(result, "rowcount", 0) if result else 0

    return {
        "parsed": len(parsed),
        "resolved": len(insert_rows),
        "inserted": len(insert_rows),
        "deactivated": deactivated,
    }


# ---------------------------------------------------------------------------
# Neo4j sync — project JUMP_BRIDGE relationships
# ---------------------------------------------------------------------------

def run_jump_bridge_sync(
    db: SupplyCoreDb,
    neo4j_raw: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Sync active jump bridges from MariaDB to Neo4j as JUMP_BRIDGE relationships."""
    lock_key = "jump_bridge_sync"
    job = start_job_run(db, lock_key)
    started = time.perf_counter()
    rows_processed = 0
    rows_written = 0

    config = Neo4jConfig.from_runtime(neo4j_raw or {})
    if not config.enabled:
        finish_job_run(db, job, status="skipped",
                       rows_processed=0, rows_written=0)
        return {"status": "skipped", "reason": "neo4j disabled"}

    client = Neo4jClient(config)

    try:
        # Fetch all active jump bridges
        bridges = db.fetch_all(
            """
            SELECT from_system_id, to_system_id, owner_name
            FROM jump_bridges
            WHERE is_active = 1
            """
        )

        rows_processed = len(bridges)

        if not bridges:
            # Clear any stale JB relationships
            client.query("MATCH ()-[r:JUMP_BRIDGE]->() DELETE r")
            finish_job_run(db, job, status="success",
                           rows_processed=0, rows_written=0)
            return {"status": "success", "rows_processed": 0, "rows_written": 0,
                    "duration_ms": int((time.perf_counter() - started) * 1000)}

        # Remove all existing JUMP_BRIDGE relationships and rebuild
        # (simpler than diffing, and JB count is typically small)
        client.query("MATCH ()-[r:JUMP_BRIDGE]->() DELETE r")

        # Create JUMP_BRIDGE relationships in batches
        for i in range(0, len(bridges), NEO4J_BATCH_SIZE):
            batch = bridges[i:i + NEO4J_BATCH_SIZE]
            neo4j_rows = [
                {
                    "from_id": int(b["from_system_id"]),
                    "to_id": int(b["to_system_id"]),
                    "owner": str(b.get("owner_name") or ""),
                }
                for b in batch
            ]

            # Create bidirectional JUMP_BRIDGE relationships
            client.query(
                """
                UNWIND $rows AS row
                MATCH (a:System {system_id: row.from_id})
                MATCH (b:System {system_id: row.to_id})
                MERGE (a)-[:JUMP_BRIDGE {owner: row.owner}]->(b)
                MERGE (b)-[:JUMP_BRIDGE {owner: row.owner}]->(a)
                """,
                {"rows": neo4j_rows},
            )
            rows_written += len(neo4j_rows) * 2  # bidirectional

        finish_job_run(db, job, status="success",
                       rows_processed=rows_processed, rows_written=rows_written)
        return {
            "status": "success",
            "rows_processed": rows_processed,
            "rows_written": rows_written,
            "bridges_synced": len(bridges),
            "duration_ms": int((time.perf_counter() - started) * 1000),
        }
    except Exception as exc:
        finish_job_run(db, job, status="failed",
                       rows_processed=rows_processed, rows_written=rows_written,
                       error_text=str(exc))
        return {"status": "failed", "error_text": str(exc),
                "rows_processed": rows_processed, "rows_written": rows_written}
