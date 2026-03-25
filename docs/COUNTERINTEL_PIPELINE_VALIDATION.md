# Counter-Intelligence Pipeline Validation

## SQL checks

```sql
-- raw killmail fact completeness
SELECT
  COUNT(*) AS killmail_rows,
  SUM(victim_damage_taken IS NOT NULL) AS with_victim_damage,
  SUM(battle_id IS NOT NULL) AS with_battle_assignment
FROM killmail_events;
```

```sql
-- attacker damage availability
SELECT
  COUNT(*) AS attacker_rows,
  SUM(damage_done IS NOT NULL) AS with_damage_done,
  SUM(final_blow = 1) AS final_blow_rows
FROM killmail_attackers;
```

```sql
-- battle overperformance output and control split
SELECT anomaly_class, COUNT(*) AS side_rows, AVG(overperformance_score) AS avg_score
FROM battle_enemy_overperformance_scores
GROUP BY anomaly_class
ORDER BY side_rows DESC;
```

```sql
-- leaderboard shape
SELECT
  s.character_id,
  s.review_priority_score,
  s.percentile_rank,
  s.confidence_score,
  s.evidence_count,
  f.anomalous_presence_rate,
  f.enemy_sustain_lift,
  f.graph_bridge_score
FROM character_counterintel_scores s
INNER JOIN character_counterintel_features f ON f.character_id = s.character_id
ORDER BY s.review_priority_score DESC
LIMIT 25;
```

```sql
-- evidence payload check
SELECT character_id, evidence_key, evidence_value, evidence_text
FROM character_counterintel_evidence
ORDER BY computed_at DESC, character_id ASC
LIMIT 50;
```

## Cypher checks

```cypher
MATCH (c:Character)-[r:PRESENT_IN_ANOMALOUS_BATTLE]->(b:Battle)
RETURN c.character_id AS character_id, COUNT(*) AS anomalous_battles, AVG(r.review_priority_score) AS avg_score
ORDER BY anomalous_battles DESC
LIMIT 25;
```

```cypher
MATCH (s:BattleSide)-[:BELONGS_TO]->(b:Battle)
WHERE s.anomaly_class = 'high_enemy_overperformance'
RETURN b.battle_id AS battle_id, s.side_uid AS side_uid, s.overperformance_score AS score
ORDER BY score DESC
LIMIT 50;
```

## Example leaderboard row

```json
{
  "character_id": 93827123,
  "review_priority_score": 0.8125,
  "percentile_rank": 0.992,
  "confidence_score": 0.76,
  "evidence_count": 3,
  "anomalous_presence_rate": 0.67,
  "enemy_sustain_lift": 1.41,
  "graph_bridge_score": 2.87
}
```

## Example evidence rows

```json
[
  {
    "character_id": 93827123,
    "evidence_key": "anomalous_battle_presence_count",
    "evidence_value": 4,
    "evidence_text": "present in 4 anomalous large battles"
  },
  {
    "character_id": 93827123,
    "evidence_key": "anomalous_presence_rate",
    "evidence_value": 0.667,
    "evidence_text": "anomalous presence rate 0.667 vs control 0.333"
  }
]
```
