# Graph Intelligence Model (Production v2)

## Architecture invariants

- Python computes, Neo4j analyzes, MariaDB serves website read models.
- PHP does not query Neo4j/Influx directly.
- Graph/scoring jobs stay Python-native (no PHP fallback in compute lane).

## Query-first model

### Anchor nodes

- `Character`, `Battle`, `BattleSide`, `Doctrine`, `Fit`, `Item`
- Optional context: `Alliance`, `Corporation`, `ShipType`, `System`

### Core relationships

- `Doctrine-[:USES]->Fit`
- `Fit-[:CONTAINS]->Item`
- `Battle-[:HAS_SIDE]->BattleSide`
- `Character-[:PARTICIPATED_IN]->Battle`
- `Character-[:ON_SIDE]->BattleSide`
- `Character-[:FLEW]->ShipType`

### Derived relationships (time-aware)

Derived edges now include bounded-time semantics and recurrence metadata where applicable:

- `CO_OCCURS_WITH`
- `SHARES_ITEM_WITH`
- `ASSOCIATED_WITH_ANOMALY`
- `CROSSED_SIDES`
- `USES_CRITICAL_ITEM`

Relationship metadata includes:

- `first_seen`
- `last_seen`
- `occurrence_count`
- `recent_occurrence_count`
- `recent_weight`
- `all_time_weight`

## Graph compression and pruning

To control density and reduce noise:

1. Thresholded creation:
   - co-occurrence threshold
   - fit overlap threshold
   - anomaly association threshold
2. Top-K retention:
   - top co-occurrence edges per character
   - top fit overlap edges per fit
3. Bounded time windows for derived edges (default 30d + recent 7d counters)
4. Dedicated prune job removes stale/low-signal edges and writes graph health snapshots.

## Topology metrics

`compute_graph_topology_metrics` materializes practical topology signals into MariaDB:

- `pagerank_score` (degree-based approximation fallback)
- `bridge_score`
- `community_id` (lightweight side-transition proxy)
- `anomalous_neighbor_density`
- `suspicious_cluster_density`
- `bridge_between_clusters_score`

## Behavioral baseline model

`compute_behavioral_baselines` computes negative-model features to reduce false positives:

- `normal_battle_frequency`
- `normal_co_occurrence_density`
- `low_sustain_participation_frequency`
- `expected_enemy_efficiency`
- `role_adjusted_baseline`
- `anomaly_delta_score`

## Suspicion scoring v2

`compute_suspicion_scores_v2` combines:

- battle-local behavior features
- graph topology/recurrence signals
- baseline-adjusted deltas
- recency signal (`suspicion_score_recent`)

Persisted score outputs include:

- `suspicion_score` / `suspicion_score_all_time`
- `suspicion_score_recent`
- `suspicion_momentum`
- `support_evidence_count`
- `community_id`
- `top_supporting_battles_json`
- `top_graph_neighbors_json`
- explainable `explanation_json`

## MariaDB read-model outputs

- `item_dependency_score`
- `doctrine_dependency_depth`
- `fit_overlap_score`
- `battle_actor_graph_metrics`
- `character_graph_intelligence`
- `character_behavioral_baselines`
- `character_suspicion_scores`
- `graph_health_snapshots`
- `suspicious_actor_clusters`
- `suspicious_cluster_membership`

## CLI runbook

```bash
python -m orchestrator.main compute-graph-sync-doctrine-dependency --app-root /workspace/SupplyCore
python -m orchestrator.main compute-graph-sync-battle-intelligence --app-root /workspace/SupplyCore
python -m orchestrator.main compute-graph-derived-relationships --app-root /workspace/SupplyCore
python -m orchestrator.main compute-graph-prune --app-root /workspace/SupplyCore
python -m orchestrator.main compute-graph-topology-metrics --app-root /workspace/SupplyCore
python -m orchestrator.main compute-graph-insights --app-root /workspace/SupplyCore
python -m orchestrator.main compute-behavioral-baselines --app-root /workspace/SupplyCore
python -m orchestrator.main compute-suspicion-scores-v2 --app-root /workspace/SupplyCore
```

## Validation queries

### Neo4j

```cypher
MATCH (n) UNWIND labels(n) AS label RETURN label, count(*) AS c ORDER BY c DESC;
MATCH ()-[r]->() RETURN type(r) AS rel_type, count(*) AS c ORDER BY c DESC;
MATCH (c:Character)-[r:CO_OCCURS_WITH]->() RETURN avg(r.weight), max(r.weight), count(*) LIMIT 1;
MATCH (f:Fit)-[r:SHARES_ITEM_WITH]->() RETURN avg(r.overlap_score), max(r.overlap_score), count(*) LIMIT 1;
MATCH (c:Character)-[r:CO_OCCURS_WITH]->() RETURN c.character_id, count(*) AS rels ORDER BY rels DESC LIMIT 20;
```

### MariaDB

```sql
SELECT COUNT(*) FROM graph_health_snapshots;
SELECT COUNT(*) FROM character_graph_intelligence;
SELECT COUNT(*) FROM character_behavioral_baselines;
SELECT COUNT(*) FROM character_suspicion_scores;

SELECT character_id, suspicion_score, suspicion_score_recent, suspicion_momentum
FROM character_suspicion_scores
ORDER BY suspicion_score DESC
LIMIT 50;

SELECT id, snapshot_ts, max_character_degree, avg_character_degree, notes
FROM graph_health_snapshots
ORDER BY id DESC
LIMIT 20;
```

## Troubleshooting

- Graph too dense: increase thresholds, reduce top-K, run prune job.
- Scores too flat: tune v2 weights and scaling bounds in suspicion v2 job.
- Scores too noisy: increase minimum sample count and baseline weight share.
- Empty communities: inspect `cross_side_cluster_score` and topology job source data.
- No clusters found: verify battle participant cardinality and anomaly labeling jobs.
- Pruning too aggressive: raise stale days and lower thresholds carefully.
- Expensive metrics: run topology less frequently and limit to eligible battle windows.
