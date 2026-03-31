#!/usr/bin/env python3
"""Delete duplicate Neo4j relationships in batches.

Usage:
    python scripts/cleanup_duplicate_relationships.py [--uri bolt://localhost:7687] [--user neo4j] [--password secret]
    python scripts/cleanup_duplicate_relationships.py --batch-size 10000
    python scripts/cleanup_duplicate_relationships.py --dry-run

Targets specific relationship types first (PART_OF, MEMBER_OF, CURRENT_CORP),
then sweeps for any remaining duplicates across all types.
"""
from __future__ import annotations

import argparse
import time

from neo4j import GraphDatabase


def _delete_by_type(session, db: str, rel_type: str, batch_size: int) -> int:
    """Delete duplicates for a specific relationship type. Returns total deleted."""
    count_row = session.run(
        f"MATCH (a)-[r1:{rel_type}]->(b), (a)-[r2:{rel_type}]->(b) "
        "WHERE elementId(r1) < elementId(r2) "
        "RETURN count(r2) AS cnt",
        database_=db,
    ).single()
    total = count_row["cnt"]
    if total == 0:
        return 0

    print(f"\n  {rel_type}: {total:,} duplicates")
    deleted_total = 0
    batch = 0
    t0 = time.time()

    while True:
        batch += 1
        d = session.run(
            f"MATCH (a)-[r1:{rel_type}]->(b), (a)-[r2:{rel_type}]->(b) "
            "WHERE elementId(r1) < elementId(r2) "
            "WITH r2 LIMIT $limit DELETE r2 RETURN count(*) AS deleted",
            limit=batch_size,
            database_=db,
        ).single()["deleted"]
        deleted_total += d
        elapsed = time.time() - t0
        rate = deleted_total / elapsed if elapsed > 0 else 0
        eta = (total - deleted_total) / rate / 60 if rate > 0 else 0
        print(f"    Batch {batch}: {deleted_total:,}/{total:,} | {rate:,.0f}/s | ETA {eta:.1f}m")
        if d == 0:
            break

    return deleted_total


def main() -> None:
    parser = argparse.ArgumentParser(description="Clean duplicate Neo4j relationships")
    parser.add_argument("--uri", default="bolt://localhost:7687")
    parser.add_argument("--user", default="neo4j")
    parser.add_argument("--password", default="neo4j")
    parser.add_argument("--database", default="neo4j")
    parser.add_argument("--batch-size", type=int, default=10000)
    parser.add_argument("--dry-run", action="store_true", help="Only count, don't delete")
    args = parser.parse_args()

    driver = GraphDatabase.driver(args.uri, auth=(args.user, args.password))

    with driver.session() as s:
        # Discover all duplicate relationship types
        rows = s.run(
            "MATCH (a)-[r1]->(b), (a)-[r2]->(b) "
            "WHERE type(r1) = type(r2) AND elementId(r1) < elementId(r2) "
            "RETURN type(r1) AS rel_type, count(r2) AS cnt "
            "ORDER BY cnt DESC",
            database_=args.database,
        )
        types = [(r["rel_type"], r["cnt"]) for r in rows]

        if not types:
            print("No duplicate relationships found.")
            driver.close()
            return

        grand_total = sum(cnt for _, cnt in types)
        print(f"Found {grand_total:,} total duplicates across {len(types)} relationship type(s):")
        for rt, cnt in types:
            print(f"  {rt}: {cnt:,}")

        if args.dry_run:
            driver.close()
            return

        deleted_grand = 0
        t0 = time.time()
        for rel_type, _ in types:
            deleted_grand += _delete_by_type(s, args.database, rel_type, args.batch_size)

        elapsed = time.time() - t0
        print(f"\nDone. Deleted {deleted_grand:,} duplicates in {elapsed:.1f}s")

    driver.close()


if __name__ == "__main__":
    main()
