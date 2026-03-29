"""Resolve pending/failed/expired entities in entity_metadata_cache.

Uses the native Python :class:`~orchestrator.esi_entity_resolver.EsiEntityResolver`
to resolve entities via ESI through the gateway.  This ensures all ESI calls
go through the Python rate limiter and compliance lifecycle — PHP never calls
ESI directly for entity resolution.
"""
from __future__ import annotations

from typing import Any

from ..db import SupplyCoreDb
from .sync_runtime import run_sync_phase_job


BATCH_SIZE = 500
RETRY_AFTER_MINUTES = 30


def _processor(db: SupplyCoreDb, raw_config: dict[str, Any] | None = None) -> dict[str, object]:
    from ..esi_entity_resolver import EsiEntityResolver
    from ..esi_gateway import build_gateway

    redis_cfg = dict((raw_config or {}).get("redis") or {})
    gateway = build_gateway(db=db, redis_config=redis_cfg)
    resolver = EsiEntityResolver(gateway=gateway, db=db)
    return resolver.resolve_pending_entities(
        batch_size=BATCH_SIZE,
        retry_after_minutes=RETRY_AFTER_MINUTES,
    )


def run_entity_metadata_resolve_sync(db: SupplyCoreDb, raw_config: dict[str, Any] | None = None) -> dict[str, object]:
    return run_sync_phase_job(
        db,
        job_key="entity_metadata_resolve_sync",
        phase="A",
        objective="Resolve pending entity metadata via ESI",
        processor=lambda d: _processor(d, raw_config),
    )
