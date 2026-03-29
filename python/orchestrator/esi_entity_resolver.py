"""Native Python ESI entity resolution.

Resolves entity IDs to names (and vice versa) via ESI POST endpoints,
writing results to the ``entity_metadata_cache`` table in MariaDB.
Replaces the PHP bridge entity resolution that previously called ESI
directly from PHP.

Also provides authenticated search via ``/characters/{id}/search/``
for the ``esi-search`` bridge command.
"""

from __future__ import annotations

import json
import logging
import time
from datetime import UTC, datetime
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from .db import SupplyCoreDb
    from .esi_gateway import EsiGateway

logger = logging.getLogger("supplycore.esi_entity_resolver")

# ESI /universe/names/ accepts up to 1000 IDs per request.
_UNIVERSE_NAMES_BATCH_SIZE = 1000
# Default cache TTL for resolved entities (24 hours).
_DEFAULT_EXPIRES_HOURS = 24


class EsiEntityResolver:
    """Resolve entity IDs to names via ESI, write results to entity_metadata_cache."""

    def __init__(self, gateway: EsiGateway, db: SupplyCoreDb) -> None:
        self._gateway = gateway
        self._db = db

    # -- Bulk resolution ---------------------------------------------------

    def resolve_names(self, ids: list[int]) -> list[dict[str, Any]]:
        """POST ``/latest/universe/names/`` — bulk ID→name resolution.

        Chunks into batches of 1000 (ESI limit).  Returns a list of
        ``{"id": int, "name": str, "category": str}`` dicts.
        """
        unique_ids = sorted(set(i for i in ids if i > 0))
        if not unique_ids:
            return []

        all_results: list[dict[str, Any]] = []
        for offset in range(0, len(unique_ids), _UNIVERSE_NAMES_BATCH_SIZE):
            chunk = unique_ids[offset:offset + _UNIVERSE_NAMES_BATCH_SIZE]
            resp = self._gateway.post(
                "/latest/universe/names/",
                body=chunk,
                params={"datasource": "tranquility"},
            )
            if resp.status_code == 200 and isinstance(resp.body, list):
                all_results.extend(resp.body)
            elif resp.status_code == 404:
                # 404 means some IDs are invalid — try individually
                pass
            else:
                logger.warning("universe/names/ returned status %d for %d IDs", resp.status_code, len(chunk))

        return all_results

    def resolve_ids(self, names: list[str]) -> dict[str, Any]:
        """POST ``/latest/universe/ids/`` — bulk name→ID resolution.

        Returns the raw ESI response dict with keys like
        ``alliances``, ``corporations``, ``characters``, etc.
        """
        clean_names = sorted(set(n.strip() for n in names if n.strip()))
        if not clean_names:
            return {}

        resp = self._gateway.post(
            "/latest/universe/ids/",
            body=clean_names,
            params={"datasource": "tranquility", "language": "en"},
        )
        if resp.status_code == 200 and isinstance(resp.body, dict):
            return resp.body
        logger.warning("universe/ids/ returned status %d", resp.status_code)
        return {}

    # -- Entity cache resolution -------------------------------------------

    def resolve_pending_entities(
        self,
        batch_size: int = 500,
        retry_after_minutes: int = 30,
    ) -> dict[str, Any]:
        """Fetch pending entities from ``entity_metadata_cache``, resolve via ESI.

        This replaces the PHP bridge call to ``resolve-pending-entities``.
        Returns a result dict compatible with the sync job framework.
        """
        # Fetch pending entity IDs grouped by type.
        pending = self._fetch_pending(batch_size, retry_after_minutes)
        total_pending = sum(len(ids) for ids in pending.values())

        if total_pending == 0:
            return {
                "rows_processed": 0,
                "rows_written": 0,
                "rows_failed": 0,
                "warnings": [],
                "summary": "No pending entities to resolve.",
                "meta": {"total_pending": 0, "resolved": 0, "failed": 0, "remaining_pending": 0},
            }

        # Collect all IDs for bulk resolution.
        all_ids: list[int] = []
        for ids in pending.values():
            all_ids.extend(ids)

        # Resolve via ESI.
        name_results = self.resolve_names(all_ids)

        # Build lookup: id → {name, category}
        resolved_map: dict[int, dict[str, str]] = {}
        for row in name_results:
            if isinstance(row, dict):
                eid = int(row.get("id", 0))
                name = str(row.get("name", "")).strip()
                category = str(row.get("category", "")).lower().strip()
                if eid > 0 and name:
                    resolved_map[eid] = {"name": name, "category": category}

        # Write results to entity_metadata_cache.
        now_utc = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
        expires_at = datetime.fromtimestamp(
            time.time() + _DEFAULT_EXPIRES_HOURS * 3600, tz=UTC
        ).strftime("%Y-%m-%d %H:%M:%S")

        resolved_count = 0
        failed_count = 0

        for entity_type, ids in pending.items():
            for eid in ids:
                info = resolved_map.get(eid)
                if info:
                    self._upsert_resolved(eid, info["category"] or entity_type, info["name"], now_utc, expires_at)
                    resolved_count += 1
                else:
                    self._mark_failed(eid, entity_type, now_utc)
                    failed_count += 1

        # Count remaining pending.
        remaining = self._db.fetch_scalar(
            "SELECT COUNT(*) FROM entity_metadata_cache "
            "WHERE resolution_status IN ('pending', 'failed') "
            "AND entity_type IN ('alliance', 'corporation', 'character')"
        )

        return {
            "rows_processed": total_pending,
            "rows_written": resolved_count,
            "rows_failed": failed_count,
            "warnings": [],
            "summary": (
                f"Resolved {resolved_count}/{total_pending} pending entities "
                f"({failed_count} failed, {remaining} still pending)."
            ),
            "meta": {
                "total_pending": total_pending,
                "resolved": resolved_count,
                "failed": failed_count,
                "remaining_pending": remaining,
            },
        }

    # -- Authenticated search ----------------------------------------------

    def search_entities(
        self,
        query: str,
        categories: list[str],
        character_id: int,
        access_token: str,
    ) -> dict[str, Any]:
        """Search ESI via ``/characters/{id}/search/`` through the gateway.

        Returns the raw ESI search response (keys per category).
        """
        if not query.strip() or character_id <= 0:
            return {}

        resp = self._gateway.get(
            f"/latest/characters/{character_id}/search/",
            params={
                "categories": ",".join(categories),
                "strict": "false",
                "search": query.strip(),
            },
            access_token=access_token,
            route_template="/latest/characters/{character_id}/search/",
        )
        if resp.from_cache or resp.not_modified:
            return resp.body if isinstance(resp.body, dict) else {}
        if 200 <= resp.status_code < 300 and isinstance(resp.body, dict):
            return resp.body
        return {}

    # -- Internal helpers --------------------------------------------------

    def _fetch_pending(self, limit: int, retry_after_minutes: int) -> dict[str, list[int]]:
        """Fetch pending entity IDs from entity_metadata_cache."""
        now_utc = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
        retry_threshold = datetime.fromtimestamp(
            time.time() - retry_after_minutes * 60, tz=UTC
        ).strftime("%Y-%m-%d %H:%M:%S")

        rows = self._db.fetch_all(
            """SELECT entity_type, entity_id
               FROM entity_metadata_cache
               WHERE (
                   resolution_status = 'pending'
                   OR (resolution_status = 'failed' AND updated_at < %s)
                   OR (resolution_status = 'resolved' AND expires_at IS NOT NULL AND expires_at < %s)
               )
               AND entity_type IN ('alliance', 'corporation', 'character')
               ORDER BY
                   CASE resolution_status WHEN 'pending' THEN 0 WHEN 'failed' THEN 1 ELSE 2 END,
                   updated_at ASC
               LIMIT %s""",
            (retry_threshold, now_utc, limit),
        )

        result: dict[str, list[int]] = {}
        for row in rows:
            entity_type = str(row["entity_type"])
            entity_id = int(row["entity_id"])
            result.setdefault(entity_type, []).append(entity_id)
        return result

    def _upsert_resolved(self, entity_id: int, entity_type: str, name: str, now: str, expires: str) -> None:
        """Update entity_metadata_cache with resolved data."""
        try:
            self._db.execute(
                """INSERT INTO entity_metadata_cache
                   (entity_type, entity_id, entity_name, source_system, resolution_status,
                    expires_at, last_requested_at, resolved_at)
                   VALUES (%s, %s, %s, 'esi', 'resolved', %s, %s, %s)
                   ON DUPLICATE KEY UPDATE
                    entity_name = VALUES(entity_name),
                    source_system = 'esi',
                    resolution_status = 'resolved',
                    expires_at = VALUES(expires_at),
                    last_requested_at = VALUES(last_requested_at),
                    resolved_at = VALUES(resolved_at),
                    last_error_message = NULL""",
                (entity_type, entity_id, name, expires, now, now),
            )
        except Exception as exc:
            logger.warning("Failed to upsert resolved entity %s:%d: %s", entity_type, entity_id, exc)

    def _mark_failed(self, entity_id: int, entity_type: str, now: str) -> None:
        """Mark an entity as failed in entity_metadata_cache."""
        try:
            self._db.execute(
                """UPDATE entity_metadata_cache
                   SET resolution_status = 'failed',
                       last_error_message = 'ESI lookup returned no result',
                       updated_at = %s
                   WHERE entity_type = %s AND entity_id = %s""",
                (now, entity_type, entity_id),
            )
        except Exception as exc:
            logger.warning("Failed to mark entity %s:%d as failed: %s", entity_type, entity_id, exc)
