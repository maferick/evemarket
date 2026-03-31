#!/usr/bin/env python3
"""Delete duplicate Neo4j relationships in batches.

Usage:
    python scripts/cleanup_duplicate_relationships.py [--batch-size 5000] [--dry-run]

Connects using the same Neo4j config as the orchestrator (NEO4J_URI, etc.).
"""
from __future__ import annotations

import argparse
import sys
import time

from neo4j import GraphDatabase

# ── Config from env / defaults ────────────────────────────────────────────
DEFAULT_URI = "bolt://localhost:7687"
DEFAULT_USER = "neo4j"
DEFAULT_PASSWORD = "neo4j"
DEFAULT_DB = "neo4j"


def _connect(args: argparse.Namespace) -> GraphDatabase.driver:
    import os
    uri = os.getenv("NEO4J_URI", args.uri or DEFAULT_URI)
    user = os.getenv("NEO4J_USER", args.user or DEFAULT_USER)
    password = os.getenv("NEO4J_PASSWORD", args.password or DEFAULT_PASSWORD)
    return GraphDatabase.driver(uri, auth=(user, password))


def count_duplicates(session, db: str) -> int:
    result = session.run(
        "MATCH (a)-[r1]->(b), (a)-[r2]->(b) "
        "WHERE type(r1) = type(r2) AND elementId(r1) < elementId(r2) "
        "RETURN count(r2) AS cnt",
        database_=db,
    )
    return result.single()["cnt"]


def delete_batch(session, db: str, batch_size: int) -> int:
    result = session.run(
        "MATCH (a)-[r1]->(b), (a)-[r2]->(b) "
        "WHERE type(r1) = type(r2) AND elementId(r1) < elementId(r2) "
        "WITH r2 LIMIT $limit "
        "DELETE r2 "
        "RETURN count(*) AS deleted",
        limit=batch_size,
        database_=db,
    )
    return result.single()["deleted"]


def main() -> None:
    parser = argparse.ArgumentParser(description="Clean duplicate Neo4j relationships")
    parser.add_argument("--uri", default=None, help=f"Neo4j URI (default: env NEO4J_URI or {DEFAULT_URI})")
    parser.add_argument("--user", default=None, help=f"Neo4j user (default: env NEO4J_USER or {DEFAULT_USER})")
    parser.add_argument("--password", default=None, help=f"Neo4j password (default: env NEO4J_PASSWORD)")
    parser.add_argument("--database", default=DEFAULT_DB, help=f"Neo4j database (default: {DEFAULT_DB})")
    parser.add_argument("--batch-size", type=int, default=5000, help="Relationships to delete per batch (default: 5000)")
    parser.add_argument("--dry-run", action="store_true", help="Only count duplicates, don't delete")
    args = parser.parse_args()

    driver = _connect(args)
    db = args.database

    with driver.session() as session:
        total = count_duplicates(session, db)
        print(f"Found {total:,} duplicate relationships")

        if args.dry_run or total == 0:
            driver.close()
            return

        deleted_total = 0
        batch_num = 0
        t0 = time.time()

        while True:
            batch_num += 1
            deleted = delete_batch(session, db, args.batch_size)
            deleted_total += deleted
            elapsed = time.time() - t0
            rate = deleted_total / elapsed if elapsed > 0 else 0
            remaining = total - deleted_total
            eta = remaining / rate if rate > 0 else 0

            print(
                f"  Batch {batch_num}: deleted {deleted:,} | "
                f"total deleted {deleted_total:,}/{total:,} | "
                f"{rate:,.0f}/s | ETA {eta / 60:.1f}m"
            )

            if deleted == 0:
                break

        elapsed = time.time() - t0
        print(f"\nDone. Deleted {deleted_total:,} duplicates in {elapsed:.1f}s")

    driver.close()


if __name__ == "__main__":
    main()
