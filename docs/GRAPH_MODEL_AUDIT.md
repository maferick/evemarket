# Graph Model Audit Summary (v2 scaling + behavioral pass)

## Scope audited

- Neo4j anchor and derived relationship density.
- Python graph sync/derived/query jobs and worker registration.
- MariaDB graph read-model outputs used by website/scoring.

## Findings before this pass

1. Derived edges could grow without strict threshold/top-k controls.
2. Relationship semantics existed but needed stronger temporal metadata (`first_seen`, `last_seen`, recency weighting).
3. Graph health snapshots were not persisted for regular operator review.
4. Suspicion scoring leaned on first-order metrics and lacked explicit baseline/negative modeling.
5. Cluster/group-level intelligence outputs were limited.

## Changes applied in this pass

- Added thresholding + top-k retention + time-window-bounded derivation.
- Added dedicated prune job (`compute_graph_prune`) for stale/weak edge cleanup.
- Added topology metrics job (`compute_graph_topology_metrics`) with persisted graph intelligence and cluster rollups.
- Added baseline modeling job (`compute_behavioral_baselines`).
- Added suspicion scoring v2 (`compute_suspicion_scores_v2`) with recency/all-time/momentum and component evidence.
- Added graph health snapshot persistence and scheduler coverage for all new jobs.

## Ongoing operator checks

- Monitor `graph_health_snapshots.max_character_degree` and notes for density warnings.
- Validate row volumes and score distribution in `character_suspicion_scores` after each upgrade run.
- Tune thresholds/top-k and v2 weights only via centralized constants.
