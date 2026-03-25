from __future__ import annotations

from ..db import SupplyCoreDb
from .graph_pipeline import (
    run_compute_graph_derived_relationships,
    run_compute_graph_prune,
    run_compute_graph_sync_battle_intelligence,
    run_compute_graph_sync_doctrine_dependency,
    run_compute_graph_topology_metrics,
)


def run_compute_graph_sync(db: SupplyCoreDb, neo4j_raw: dict[str, object] | None = None) -> dict[str, object]:
    doctrine = run_compute_graph_sync_doctrine_dependency(db, neo4j_raw)
    battle = run_compute_graph_sync_battle_intelligence(db, neo4j_raw)
    derived = run_compute_graph_derived_relationships(db, neo4j_raw)
    prune = run_compute_graph_prune(db, neo4j_raw)
    topology = run_compute_graph_topology_metrics(db, neo4j_raw)

    status = "success"
    all_results = (doctrine, battle, derived, prune, topology)
    if any(str(result.get("status")) == "failed" for result in all_results):
        status = "failed"
    elif all(str(result.get("status")) == "skipped" for result in all_results):
        status = "skipped"

    return {
        "status": status,
        "rows_processed": sum(int(result.get("rows_processed") or 0) for result in all_results),
        "rows_written": sum(int(result.get("rows_written") or 0) for result in all_results),
        "meta": {
            "compute_graph_sync_doctrine_dependency": doctrine,
            "compute_graph_sync_battle_intelligence": battle,
            "compute_graph_derived_relationships": derived,
            "compute_graph_prune": prune,
            "compute_graph_topology_metrics": topology,
            "execution_mode": "python",
        },
        "summary": f"Graph sync pipeline (sync+derived+prune+topology) finished with status {status}.",
    }
