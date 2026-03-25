# Battle Intelligence Validation SQL

Run these after the battle job chain in order:

1. `compute-battle-rollups`
2. `compute-battle-target-metrics`
3. `compute-battle-anomalies`
4. `compute-battle-actor-features`
5. `compute-suspicion-scores`

```sql
-- 1) Clustered battle count
SELECT COUNT(*) AS battle_count FROM battle_rollups;

-- 2) Eligible battle count (>= 100 distinct participants)
SELECT COUNT(*) AS eligible_battle_count
FROM battle_rollups
WHERE eligible_for_suspicion = 1;

-- 3) Anomaly class counts
SELECT anomaly_class, COUNT(*) AS side_count
FROM battle_anomalies
GROUP BY anomaly_class
ORDER BY side_count DESC;

-- 4) Top 20 anomalous battle sides
SELECT ba.battle_id, ba.side_key, ba.anomaly_class, ba.z_efficiency_score, ba.percentile_rank,
       br.system_id, rs.system_name, br.participant_count, br.started_at, br.ended_at
FROM battle_anomalies ba
INNER JOIN battle_rollups br ON br.battle_id = ba.battle_id
LEFT JOIN ref_systems rs ON rs.system_id = br.system_id
ORDER BY ba.z_efficiency_score DESC
LIMIT 20;

-- 5) Top 20 suspicious characters
SELECT css.character_id,
       COALESCE(emc.entity_name, CONCAT('Character #', css.character_id)) AS character_name,
       css.suspicion_score,
       css.percentile_rank,
       css.high_sustain_frequency,
       css.low_sustain_frequency,
       css.cross_side_rate,
       css.enemy_efficiency_uplift,
       css.supporting_battle_count
FROM character_suspicion_scores css
LEFT JOIN entity_metadata_cache emc ON emc.entity_type = 'character' AND emc.entity_id = css.character_id
ORDER BY css.suspicion_score DESC
LIMIT 20;

-- 6) Character drilldown sample (replace ? with a character_id)
SELECT css.character_id,
       css.suspicion_score,
       css.explanation_json,
       css.top_supporting_battles_json,
       cbi.total_battle_count,
       cbi.eligible_battle_count,
       cbi.high_sustain_frequency,
       cbi.low_sustain_frequency,
       cbi.cross_side_rate,
       cbi.enemy_efficiency_uplift,
       cbi.ally_efficiency_uplift
FROM character_suspicion_scores css
INNER JOIN character_battle_intelligence cbi ON cbi.character_id = css.character_id
WHERE css.character_id = ?;

-- 7) Battle drilldown sample (replace ? with a battle_id)
SELECT br.battle_id, br.system_id, rs.system_name, br.started_at, br.ended_at,
       br.duration_seconds, br.participant_count, br.eligible_for_suspicion, br.battle_size_class
FROM battle_rollups br
LEFT JOIN ref_systems rs ON rs.system_id = br.system_id
WHERE br.battle_id = ?;

SELECT bsm.battle_id, bsm.side_key, bsm.participant_count, bsm.logi_count, bsm.command_count,
       bsm.capital_count, bsm.total_kills, bsm.kill_rate_per_minute,
       bsm.median_sustain_factor, bsm.average_sustain_factor,
       bsm.efficiency_score, bsm.z_efficiency_score,
       ba.anomaly_class, ba.explanation_json
FROM battle_side_metrics bsm
LEFT JOIN battle_anomalies ba ON ba.battle_id = bsm.battle_id AND ba.side_key = bsm.side_key
WHERE bsm.battle_id = ?
ORDER BY bsm.z_efficiency_score DESC;

-- 8) Recent job_runs for battle jobs
SELECT job_name, status, duration_ms, rows_processed, rows_written, error_text, started_at, finished_at
FROM job_runs
WHERE job_name IN (
    'compute_battle_rollups',
    'compute_battle_target_metrics',
    'compute_battle_anomalies',
    'compute_battle_actor_features',
    'compute_suspicion_scores'
)
ORDER BY started_at DESC
LIMIT 50;
```
