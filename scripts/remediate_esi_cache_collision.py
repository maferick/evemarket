#!/usr/bin/env python3
"""Remediate ESI gateway endpoint-key collision bug.

Background
----------
Several orchestrator call-sites were passing a parameterised ``route_template``
(e.g. ``/latest/killmails/{killmail_id}/{killmail_hash}/``) to the ESI
compliance gateway while leaving ``identity`` at the default ``"anonymous"``.
Because the gateway's cache key is built from
``method + route_template + params_sig + identity + page``, every call for
those route templates collapsed onto a single shared key. The first
successful fetch populated Redis, MariaDB ``esi_cache_entries`` and
``esi_endpoint_state``; every subsequent call for a different resource was
Expires-gated and served the SAME cached body — silent data corruption.

Affected route templates (all had ``identity=anonymous`` and variables in
the URL path):

  1. ``/latest/killmails/{killmail_id}/{killmail_hash}/``
     (character_killmail_sync, killmail_history_backfill)
  2. ``/latest/universe/structures/{structure_id}/``
     (esi_market_adapter.fetch_structure_metadata)
  3. ``/latest/markets/{region_id}/orders/``
     (esi_market_adapter._fetch_region_orders_via_gateway)
  4. ``/latest/markets/structures/{structure_id}/``
     (esi_market_adapter._fetch_structure_orders_via_gateway)
  5. ``/latest/characters/{character_id}/search/``
     (esi_entity_resolver._search_via_gateway)

What this script does
---------------------
1. Flushes poisoned Redis keys (``esi:payload:v1:*`` and ``esi:meta:v1:*``)
   matching the five buggy route templates with ``identity=anonymous``.
2. Deletes poisoned rows from MariaDB ``esi_cache_entries``
   (namespace_key='esi.payload').
3. Deletes poisoned rows from MariaDB ``esi_endpoint_state``.
4. Deletes killmail rows written by the two affected jobs while the bug
   was live (2026-04-08 16:51:22 UTC onward). Cascades through
   ``killmail_event_payloads`` (FK) and explicitly cleans up
   ``killmail_attackers`` + ``killmail_items`` by sequence_id.
5. Prints a summary so the next ``character_killmail_sync`` and
   ``killmail_history_backfill`` runs can re-fetch fresh, correct data.

Usage
-----
  # Preview (no changes):
  python scripts/remediate_esi_cache_collision.py

  # Execute everything (interactive confirmation):
  python scripts/remediate_esi_cache_collision.py --apply

  # Execute without prompting (CI / one-shot):
  python scripts/remediate_esi_cache_collision.py --apply --yes

  # Skip killmail row deletion (only flush caches):
  python scripts/remediate_esi_cache_collision.py --apply --yes --keep-killmails
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Any

# Resolve orchestrator package so we can reuse its db + redis clients.
_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT / "python"))

logger = logging.getLogger("remediate_esi_cache_collision")

# The five buggy route templates. We match with LIKE/SCAN wildcards on
# the ``{sig}`` portion so we catch every param signature that was ever
# stored against ``identity=anonymous`` for that template.
BUGGY_TEMPLATES: list[tuple[str, str]] = [
    (
        "/latest/killmails/{killmail_id}/{killmail_hash}/",
        "killmails (character_killmail_sync + killmail_history_backfill)",
    ),
    (
        "/latest/universe/structures/{structure_id}/",
        "structure metadata (esi_market_adapter)",
    ),
    (
        "/latest/markets/{region_id}/orders/",
        "market region orders (esi_market_adapter)",
    ),
    (
        "/latest/markets/structures/{structure_id}/",
        "market structure orders (esi_market_adapter)",
    ),
    (
        "/latest/characters/{character_id}/search/",
        "character search (esi_entity_resolver)",
    ),
]

# When the bug became live. Choose the earliest of the two PRs that
# introduced the gateway path for killmails:
#   PR #815 (killmail_history_backfill)  2026-04-08 16:51:22 UTC
#   PR #933 (character_killmail_sync)    2026-04-10 09:34:37 UTC
BUG_WINDOW_UTC_START = "2026-04-08 16:51:22"


# ---------------------------------------------------------------------------
# Redis flush
# ---------------------------------------------------------------------------

def flush_redis(redis: Any, *, apply: bool) -> dict[str, int]:
    """Delete poisoned Redis payload/meta/suppress/lock keys.

    Uses SCAN rather than KEYS to avoid blocking the server.
    """
    stats = {"payload": 0, "meta": 0, "suppress": 0, "lock": 0, "scanned": 0}
    if not redis.available:
        logger.warning("Redis not available — skipping Redis flush")
        return stats

    # We need the underlying raw client for SCAN/DELETE with prefix handling.
    raw = getattr(redis, "_client", None)
    prefix = getattr(redis, "_prefix", "supplycore")
    if raw is None:
        logger.warning("RedisClient has no underlying client — skipping")
        return stats

    # Build MATCH patterns for each buggy template.
    # We match BOTH exact param sigs (where known) AND the wildcard `*` sig
    # (for templates whose params vary at runtime, like search & market orders).
    # Key layout:  {prefix}:esi:{payload|meta|fetch_lock|suppress}:v1:GET:{tpl}:{sig}:anonymous:p{page}
    match_patterns: list[str] = []
    for template, _label in BUGGY_TEMPLATES:
        # Wildcard match: any sig, identity=anonymous, any page.
        for namespace in ("payload", "meta", "fetch_lock", "suppress"):
            match_patterns.append(
                f"{prefix}:esi:{namespace}:v1:GET:{template}:*:anonymous:*"
            )

    for pattern in match_patterns:
        logger.info("Scanning Redis pattern: %s", pattern)
        cursor = 0
        batch: list[bytes | str] = []
        while True:
            try:
                cursor, keys = raw.scan(cursor=cursor, match=pattern, count=500)
            except Exception as exc:
                logger.error("Redis SCAN failed for %s: %s", pattern, exc)
                break
            for key in keys:
                stats["scanned"] += 1
                batch.append(key)
                key_str = key.decode() if isinstance(key, bytes) else str(key)
                if ":esi:payload:" in key_str:
                    stats["payload"] += 1
                elif ":esi:meta:" in key_str:
                    stats["meta"] += 1
                elif ":esi:suppress:" in key_str:
                    stats["suppress"] += 1
                elif ":esi:fetch_lock:" in key_str:
                    stats["lock"] += 1
            if len(batch) >= 500 or (cursor == 0 and batch):
                if apply:
                    try:
                        raw.delete(*batch)
                    except Exception as exc:
                        logger.error("Redis DELETE failed: %s", exc)
                batch = []
            if cursor == 0:
                break
    return stats


# ---------------------------------------------------------------------------
# MariaDB flush
# ---------------------------------------------------------------------------

def flush_mariadb_cache(db: Any, *, apply: bool) -> dict[str, int]:
    """Delete poisoned ``esi_cache_entries`` and ``esi_endpoint_state`` rows."""
    stats = {"esi_cache_entries": 0, "esi_endpoint_state": 0}

    for template, label in BUGGY_TEMPLATES:
        # cache_key / endpoint_key looks like:
        #   GET:{template}:{sig}:anonymous:p{N}
        # Use LIKE with the template + :%:anonymous:% suffix so we catch
        # every param signature and every page.
        like_pattern = f"GET:{template}:%:anonymous:%"

        # Count what will be touched.
        try:
            row = db.fetch_one(
                """SELECT COUNT(*) AS n
                   FROM esi_cache_entries
                   WHERE namespace_key = 'esi.payload'
                     AND cache_key LIKE %s""",
                (like_pattern,),
            )
            count_cache = int((row or {}).get("n") or 0)
        except Exception as exc:
            logger.error("COUNT esi_cache_entries failed for %s: %s", label, exc)
            count_cache = 0

        try:
            row = db.fetch_one(
                """SELECT COUNT(*) AS n
                   FROM esi_endpoint_state
                   WHERE endpoint_key LIKE %s""",
                (like_pattern,),
            )
            count_state = int((row or {}).get("n") or 0)
        except Exception as exc:
            logger.error("COUNT esi_endpoint_state failed for %s: %s", label, exc)
            count_state = 0

        logger.info(
            "  %s:  %d cache_entries,  %d endpoint_state rows (pattern %s)",
            label, count_cache, count_state, like_pattern,
        )
        stats["esi_cache_entries"] += count_cache
        stats["esi_endpoint_state"] += count_state

        if apply:
            try:
                db.execute(
                    """DELETE FROM esi_cache_entries
                       WHERE namespace_key = 'esi.payload'
                         AND cache_key LIKE %s""",
                    (like_pattern,),
                )
            except Exception as exc:
                logger.error("DELETE esi_cache_entries failed: %s", exc)
            try:
                db.execute(
                    """DELETE FROM esi_endpoint_state
                       WHERE endpoint_key LIKE %s""",
                    (like_pattern,),
                )
            except Exception as exc:
                logger.error("DELETE esi_endpoint_state failed: %s", exc)

    return stats


# ---------------------------------------------------------------------------
# Killmail row deletion
# ---------------------------------------------------------------------------

def delete_corrupted_killmails(db: Any, *, apply: bool) -> dict[str, int]:
    """Delete ``killmail_events`` rows written while the bug was live.

    The FK on ``killmail_event_payloads.sequence_id`` has ``ON DELETE
    CASCADE`` so payload rows go automatically. ``killmail_attackers`` and
    ``killmail_items`` have no FK so we clean them up by sequence_id
    explicitly before deleting the parent row.

    Scope: any ``killmail_events`` row with ``created_at >= BUG_WINDOW_UTC_START``.
    These rows have the correct ``killmail_id`` / ``killmail_hash`` but
    their embedded ESI body (victim, attackers, solar_system, time) likely
    belongs to a different killmail.
    """
    stats = {"events": 0, "attackers": 0, "items": 0, "payloads": 0}

    # Collect the sequence_id set first so we can purge the derived tables
    # before letting the FK cascade fire.
    try:
        sample = db.fetch_one(
            """SELECT COUNT(*) AS n,
                      MIN(created_at) AS first_seen,
                      MAX(created_at) AS last_seen
               FROM killmail_events
               WHERE created_at >= %s""",
            (BUG_WINDOW_UTC_START,),
        ) or {}
    except Exception as exc:
        logger.error("COUNT killmail_events failed: %s", exc)
        return stats

    stats["events"] = int(sample.get("n") or 0)
    first_seen = sample.get("first_seen")
    last_seen = sample.get("last_seen")
    logger.info(
        "  killmail_events written since %s UTC: %d (window %s .. %s)",
        BUG_WINDOW_UTC_START, stats["events"], first_seen, last_seen,
    )

    if stats["events"] == 0:
        return stats

    # Derived-table counts (informational).
    try:
        row = db.fetch_one(
            """SELECT COUNT(*) AS n
               FROM killmail_attackers a
               INNER JOIN killmail_events e ON e.sequence_id = a.sequence_id
               WHERE e.created_at >= %s""",
            (BUG_WINDOW_UTC_START,),
        ) or {}
        stats["attackers"] = int(row.get("n") or 0)
    except Exception as exc:
        logger.error("COUNT killmail_attackers failed: %s", exc)

    try:
        row = db.fetch_one(
            """SELECT COUNT(*) AS n
               FROM killmail_items i
               INNER JOIN killmail_events e ON e.sequence_id = i.sequence_id
               WHERE e.created_at >= %s""",
            (BUG_WINDOW_UTC_START,),
        ) or {}
        stats["items"] = int(row.get("n") or 0)
    except Exception as exc:
        logger.error("COUNT killmail_items failed: %s", exc)

    try:
        row = db.fetch_one(
            """SELECT COUNT(*) AS n
               FROM killmail_event_payloads p
               INNER JOIN killmail_events e ON e.sequence_id = p.sequence_id
               WHERE e.created_at >= %s""",
            (BUG_WINDOW_UTC_START,),
        ) or {}
        stats["payloads"] = int(row.get("n") or 0)
    except Exception as exc:
        logger.error("COUNT killmail_event_payloads failed: %s", exc)

    logger.info(
        "  cascaded rows: %d attackers, %d items, %d payloads",
        stats["attackers"], stats["items"], stats["payloads"],
    )

    if not apply:
        return stats

    # Delete in dependency order: derived tables with no FK first, then
    # killmail_events (which cascades to killmail_event_payloads).
    try:
        db.execute(
            """DELETE a FROM killmail_attackers a
               INNER JOIN killmail_events e ON e.sequence_id = a.sequence_id
               WHERE e.created_at >= %s""",
            (BUG_WINDOW_UTC_START,),
        )
    except Exception as exc:
        logger.error("DELETE killmail_attackers failed: %s", exc)

    try:
        db.execute(
            """DELETE i FROM killmail_items i
               INNER JOIN killmail_events e ON e.sequence_id = i.sequence_id
               WHERE e.created_at >= %s""",
            (BUG_WINDOW_UTC_START,),
        )
    except Exception as exc:
        logger.error("DELETE killmail_items failed: %s", exc)

    try:
        db.execute(
            """DELETE FROM killmail_events
               WHERE created_at >= %s""",
            (BUG_WINDOW_UTC_START,),
        )
    except Exception as exc:
        logger.error("DELETE killmail_events failed: %s", exc)

    return stats


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--apply", action="store_true", help="Execute changes. Without this flag, the script only previews what it would do.")
    parser.add_argument("--yes", action="store_true", help="Skip interactive confirmation.")
    parser.add_argument("--keep-killmails", action="store_true", help="Only flush caches; do not delete suspect killmail_events rows.")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-7s  %(message)s",
        datefmt="%H:%M:%S",
    )

    mode = "APPLY" if args.apply else "DRY-RUN"
    logger.info("=" * 72)
    logger.info("ESI endpoint-key collision remediation  [%s]", mode)
    logger.info("=" * 72)

    # Deferred imports so that ``--help`` works without the orchestrator's
    # runtime dependencies (pymysql, redis) being installed.
    from orchestrator.config import load_php_runtime_config
    from orchestrator.db import SupplyCoreDb
    from orchestrator.redis_client import RedisClient

    # Load config + clients.
    app_root = _REPO_ROOT
    config = load_php_runtime_config(app_root)
    db = SupplyCoreDb(config.raw.get("db", {}))
    redis_cfg = dict(config.raw.get("redis") or {})
    redis = RedisClient(redis_cfg)

    logger.info(
        "DB target:    %s@%s:%s/%s",
        config.raw.get("db", {}).get("username"),
        config.raw.get("db", {}).get("host"),
        config.raw.get("db", {}).get("port"),
        config.raw.get("db", {}).get("database"),
    )
    logger.info(
        "Redis target: %s:%s/%s  prefix=%s  available=%s",
        redis_cfg.get("host"), redis_cfg.get("port"),
        redis_cfg.get("database"), redis_cfg.get("prefix"),
        redis.available,
    )
    logger.info("Bug window:   %s UTC .. now", BUG_WINDOW_UTC_START)
    logger.info("")

    if args.apply and not args.yes:
        sys.stderr.write(
            "\nThis will DELETE poisoned ESI cache entries and (unless "
            "--keep-killmails) every killmail_events row written since "
            f"{BUG_WINDOW_UTC_START} UTC, cascading to killmail_attackers, "
            "killmail_items, killmail_event_payloads.\n\n"
            "Type 'yes' to proceed: "
        )
        sys.stderr.flush()
        reply = sys.stdin.readline().strip().lower()
        if reply != "yes":
            logger.info("Aborted by operator.")
            return 1

    # Phase 1: Redis.
    logger.info("[1/3] Flushing poisoned Redis keys ...")
    redis_stats = flush_redis(redis, apply=args.apply)
    logger.info(
        "      scanned=%d  payload=%d  meta=%d  suppress=%d  lock=%d",
        redis_stats["scanned"], redis_stats["payload"], redis_stats["meta"],
        redis_stats["suppress"], redis_stats["lock"],
    )

    # Phase 2: MariaDB cache.
    logger.info("")
    logger.info("[2/3] Flushing poisoned MariaDB cache rows ...")
    db_stats = flush_mariadb_cache(db, apply=args.apply)
    logger.info(
        "      esi_cache_entries=%d  esi_endpoint_state=%d",
        db_stats["esi_cache_entries"], db_stats["esi_endpoint_state"],
    )

    # Phase 3: killmail data.
    logger.info("")
    if args.keep_killmails:
        logger.info("[3/3] Skipping killmail deletion (--keep-killmails)")
        logger.info(
            "      WARNING: %s will skip corrupted rows via uniq_killmail_identity",
            "character_killmail_sync",
        )
        logger.info("      and never re-fetch them. Run without --keep-killmails to remediate.")
        km_stats = {"events": 0, "attackers": 0, "items": 0, "payloads": 0}
    else:
        logger.info("[3/3] Deleting corrupted killmail data ...")
        km_stats = delete_corrupted_killmails(db, apply=args.apply)
        logger.info(
            "      events=%d  attackers=%d  items=%d  payloads=%d",
            km_stats["events"], km_stats["attackers"], km_stats["items"], km_stats["payloads"],
        )

    logger.info("")
    logger.info("=" * 72)
    if args.apply:
        logger.info("DONE. Next steps:")
        logger.info("  1. Verify the identity-collision fix is deployed (commit d1cf67d).")
        logger.info("  2. Let character_killmail_sync and killmail_history_backfill run;")
        logger.info("     they will re-fetch fresh bodies for the deleted killmails.")
        logger.info("  3. Spot-check telemetry: ESI cache hit rate should drop from 100%%.")
    else:
        logger.info("DRY-RUN complete. Re-run with --apply to execute.")
    logger.info("=" * 72)
    return 0


if __name__ == "__main__":
    sys.exit(main())
