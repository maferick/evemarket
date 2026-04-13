from __future__ import annotations

from ..db import SupplyCoreDb
from ..job_result import JobResult
from .graph_pipeline import (
    run_compute_graph_derived_relationships,
    run_compute_graph_prune,
    run_compute_graph_sync_battle_intelligence,
    run_compute_graph_topology_metrics,
)


def run_compute_graph_sync(db: SupplyCoreDb, neo4j_raw: dict[str, object] | None = None) -> dict[str, object]:
    battle = run_compute_graph_sync_battle_intelligence(db, neo4j_raw)
    derived = run_compute_graph_derived_relationships(db, neo4j_raw)
    prune = run_compute_graph_prune(db, neo4j_raw)
    topology = run_compute_graph_topology_metrics(db, neo4j_raw)

    status = "success"
    all_results = (battle, derived, prune, topology)
    if any(str(result.get("status")) == "failed" for result in all_results):
        status = "failed"
    elif all(str(result.get("status")) == "skipped" for result in all_results):
        status = "skipped"

    total_processed = sum(int(result.get("rows_processed") or 0) for result in all_results)
    total_written = sum(int(result.get("rows_written") or 0) for result in all_results)
    return JobResult(
        status=status,
        summary=f"Graph sync pipeline (sync+derived+prune+topology) finished with status {status}.",
        rows_seen=total_processed,
        rows_processed=total_processed,
        rows_written=total_written,
        meta={
            "job_key": "compute_graph_sync",
            "compute_graph_sync_battle_intelligence": battle,
            "compute_graph_derived_relationships": derived,
            "compute_graph_prune": prune,
            "compute_graph_topology_metrics": topology,
            "execution_mode": "python",
        },
    ).to_dict()
