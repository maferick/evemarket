"""Resolve pending/failed/expired entities in entity_metadata_cache.

Uses the native Python :class:`~orchestrator.esi_entity_resolver.EsiEntityResolver`
to resolve entities via ESI through the gateway.  This ensures all ESI calls
go through the Python rate limiter and compliance lifecycle — PHP never calls
ESI directly for entity resolution.

Per run, the processor drains as many batches as it can within
``DRAIN_TIME_BUDGET_SECONDS``.  This is necessary because killmail backfill
(see ``functions.php:16410``) inserts new ``pending`` rows at a high rate
whenever unseen alliances/corps/characters appear in ingested killmails, and
a single 500-row batch per orchestrator tick cannot keep up.
"""
from __future__ import annotations

import time
from typing import Any

from ..db import SupplyCoreDb
from .sync_runtime import run_sync_phase_job


# 1000 matches the ESI ``/universe/names/`` max-per-POST limit, so each
# batch maps to exactly one upstream HTTP call.
BATCH_SIZE = 1000
RETRY_AFTER_MINUTES = 30
# Max wall time the processor will spend draining batches within a single
# run.  Stays comfortably under the 180s ``timeout_seconds`` configured for
# this job in ``worker_registry.py``.
DRAIN_TIME_BUDGET_SECONDS = 120


def _processor(db: SupplyCoreDb, raw_config: dict[str, Any] | None = None) -> dict[str, object]:
    from ..esi_entity_resolver import EsiEntityResolver
    from ..esi_gateway import build_gateway

    redis_cfg = dict((raw_config or {}).get("redis") or {})
    gateway = build_gateway(db=db, redis_config=redis_cfg)
    resolver = EsiEntityResolver(gateway=gateway, db=db)

    deadline = time.monotonic() + DRAIN_TIME_BUDGET_SECONDS
    total_processed = 0
    total_resolved = 0
    total_failed = 0
    batches_completed = 0
    last_result: dict[str, Any] = {}

    while True:
        result = resolver.resolve_pending_entities(
            batch_size=BATCH_SIZE,
            retry_after_minutes=RETRY_AFTER_MINUTES,
        )
        last_result = result
        batches_completed += 1
        processed = int(result.get("rows_processed") or 0)
        meta = dict(result.get("meta") or {})
        total_processed += processed
        total_resolved += int(meta.get("resolved") or 0)
        total_failed += int(meta.get("failed") or 0)

        if processed == 0:
            # No eligible rows — queue is drained for now.
            break
        if time.monotonic() >= deadline:
            # Time budget exhausted; the next orchestrator tick will continue.
            break

    remaining = int((last_result.get("meta") or {}).get("remaining_pending") or 0)

    return {
        "rows_processed": total_processed,
        "rows_written": total_resolved,
        "rows_failed": total_failed,
        "warnings": [],
        "summary": (
            f"Resolved {total_resolved}/{total_processed} pending entities "
            f"across {batches_completed} batches "
            f"({total_failed} failed, {remaining} still pending)."
        ),
        "meta": {
            "total_pending": total_processed,
            "resolved": total_resolved,
            "failed": total_failed,
            "remaining_pending": remaining,
            "batches_completed": batches_completed,
            "batch_size": BATCH_SIZE,
        },
    }


def run_entity_metadata_resolve_sync(db: SupplyCoreDb, raw_config: dict[str, Any] | None = None) -> dict[str, object]:
    return run_sync_phase_job(
        db,
        job_key="entity_metadata_resolve_sync",
        phase="A",
        objective="Resolve pending entity metadata via ESI",
        processor=lambda d: _processor(d, raw_config),
    )
