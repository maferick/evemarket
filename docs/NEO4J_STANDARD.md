# Neo4j Data Model Standard

> **Single source of truth** for all Neo4j node labels, properties, relationships,
> constraints, indexes, and datetime conventions in SupplyCore.
> Every Python job that touches Neo4j **must** conform to this document.

---

## 1. Node Labels & Primary Keys

Every node type uses a **descriptive primary key** (never bare `.id`).

| Label | Primary Key Property | Type | Example |
|---|---|---|---|
| `Character` | `character_id` | int | `2114794365` |
| `Battle` | `battle_id` | string | `30002537:1711720800` |
| `BattleSide` | `side_uid` | string | `30002537:1711720800\|A` |
| `Alliance` | `alliance_id` | int | `99003214` |
| `Corporation` | `corporation_id` | int | `98000001` |
| `ShipType` | `type_id` | int | `587` |
| `Killmail` | `killmail_id` | int | `119432876` |
| `ShipClass` | `ship_class_id` | int | `25` |
| `Doctrine` | `doctrine_id` | int | `1` |
| `Fit` | `fit_id` | int | `42` |
| `Item` | `type_id` | int | `2488` |
| `System` | `system_id` | int | `30002537` |
| `Constellation` | `constellation_id` | int | `20000370` |
| `Region` | `region_id` | int | `10000042` |
| `ComputeCheckpoint` | `run_id` | string | `20260331_143000` |
| `Cohort` | *(generated)* | string | computed |

### Naming rule

- **Primary key** = `<entity_singular>_id` (e.g. `character_id`, `battle_id`)
- **Exception**: `ShipClass` uses `ship_class_id` (not bare `id`)
- **Never** use a bare `.id` property for any node

---

## 2. Common Node Properties

### Character
| Property | Type | Set by |
|---|---|---|
| `character_id` | int | graph_pipeline, evewho_alliance_member_sync |
| `name` | string | evewho_alliance_member_sync, graph_pipeline |
| `alliance_id` | int? | graph_pipeline |
| `corporation_id` | int? | graph_pipeline |
| `tracked` | bool | intelligence_pipeline |
| `suspicion_score` | float? | intelligence_pipeline |
| `computed_at` | string (ISO 8601) | intelligence_pipeline |
| `org_synced_at` | string (ISO 8601) | evewho_alliance_member_sync |
| `community_id` | string? | graph_community_detection |
| `community_label` | int? | graph_community_detection |
| `betweenness_approx` | float? | graph_community_detection |
| `pr` | float? | graph_community_detection |

### Battle
| Property | Type | Set by |
|---|---|---|
| `battle_id` | string | graph_pipeline, counterintel_pipeline |
| `started_at` | string (ISO 8601) | graph_pipeline |
| `ended_at` | string (ISO 8601) | graph_pipeline |
| `system_id` | int | graph_pipeline |
| `region_id` | int? | graph_pipeline |
| `constellation_id` | int? | graph_pipeline |
| `participant_count` | int | graph_pipeline |
| `duration_seconds` | int? | derived |
| `battle_size_class` | string? | derived |

### Killmail
| Property | Type | Set by |
|---|---|---|
| `killmail_id` | int | graph_pipeline, intelligence_pipeline |
| `battle_id` | string? | graph_pipeline, intelligence_pipeline |
| `killed_at` | string (ISO 8601) | graph_pipeline |
| `total_value` | float? | graph_pipeline |
| `victim_ship_type_id` | int? | graph_pipeline |
| `damage` | float? | intelligence_pipeline |
| `occurred_at` | string (ISO 8601) | intelligence_pipeline |

### Alliance / Corporation
| Property | Type |
|---|---|
| `alliance_id` / `corporation_id` | int |
| `name` | string |
| `is_npc` | bool? (Corporation only) |

### System / Constellation / Region
| Property | Type |
|---|---|
| `system_id` / `constellation_id` / `region_id` | int |
| `name` | string |
| `security` | float? (System only) |

---

## 3. Relationship Types (canonical names)

### Organization Hierarchy
| Relationship | Direction | Properties | Notes |
|---|---|---|---|
| `MEMBER_OF` | Character → Corporation | `from` (ISO), `to` (ISO, nullable) | Historical corp membership with date range |
| `CURRENT_CORP` | Character → Corporation | `as_of` (ISO) | Current corp, updated on each sync |
| `PART_OF` | Corporation → Alliance | `as_of` (ISO) | Current corp-alliance link |
| `WAS_MEMBER_OF` | Character → Alliance | `started_at` (ISO), `ended_at` (ISO) | Historical alliance membership |
| `MEMBER_OF_ALLIANCE` | Character → Alliance | *(none)* | Current alliance membership |
| `MEMBER_OF_CORPORATION` | Character → Corporation | *(none)* | Current corp membership |
| `HISTORICALLY_IN` | Character → Corporation | `start` (ISO), `end` (ISO?), `source` | ESI history |

### Deprecated / Renamed
| Old Name | Replaced By | Action |
|---|---|---|
| `IN_ALLIANCE` | `PART_OF` | **Renamed** — counterintel_pipeline must use `PART_OF` |

### Battle Participation
| Relationship | Direction | Properties |
|---|---|---|
| `PARTICIPATED_IN` | Character → Battle | `side_key`, `centrality`, `visibility`, `alliance_id`, `corporation_id` |
| `ON_SIDE` | Character → BattleSide | *(none)* |
| `HAS_SIDE` | Battle → BattleSide | *(none)* |
| `BELONGS_TO` | BattleSide → Battle | *(none)* |
| `PRESENT_IN_ANOMALOUS_BATTLE` | Character → Battle | `review_priority_score`, `computed_at` |
| `IN_SYSTEM` | Battle → System | *(none)* |
| `LOCATED_IN` | Battle → System | *(none)* |
| `REPRESENTED_BY_ALLIANCE` | BattleSide → Alliance | *(none)* |
| `REPRESENTED_BY_CORPORATION` | BattleSide → Corporation | *(none)* |

### Combat Interactions
| Relationship | Direction | Properties |
|---|---|---|
| `ATTACKED_ON` | Character → Killmail | `damage`, `final_blow` |
| `VICTIM_OF` | Character → Killmail | `ship_type_id` |
| `OCCURRED_IN` | Killmail → System | *(none)* |
| `PART_OF_BATTLE` | Killmail → Battle | *(none)* |
| `DIRECT_COMBAT` | Character → Character | `count`, `last_at` (ISO), `updated_at` (ISO) |
| `ASSISTED_KILL` | Character → Character | `count`, `last_at` (ISO), `updated_at` (ISO) |
| `SAME_FLEET` | Character → Character | `count`, `updated_at` (ISO) |
| `USED_SHIP` | Character → ShipType | *(none)* |
| `ENGAGED_ALLIANCE` | Character → Alliance | engagement metrics |

### Derived / Computed
| Relationship | Direction | Properties |
|---|---|---|
| `CO_OCCURS_WITH` | Character → Character | `first_seen`, `last_seen`, `occurrence_count`, `recent_occurrence_count`, `high_sustain_battle_count`, `weight`, `recent_weight`, `all_time_weight` |
| `CO_PRESENT_WITH` | Character → Character | `count`, `anomalous_count`, `source`, `computed_at` |
| `CO_PRESENT_CLUSTER` | Character → Character | `co_battles`, `last_at` |
| `CROSSED_SIDES` | Character → Character (self) | `first_seen`, `last_seen`, `occurrence_count`, `side_count`, `side_transition_count`, `weight`, `recent_weight`, `all_time_weight` |
| `ASSOCIATED_WITH_ANOMALY` | Character → Battle | `first_seen`, `last_seen`, `occurrence_count`, `recent_occurrence_count`, `avg_z_score`, `count`, `weight`, `recent_weight`, `all_time_weight` |
| `SHARED_ALLIANCE_WITH` | Character → Character | `overlap_start`, `overlap_end`, `overlap_score` |

### Doctrine / Fit / Item Graph
| Relationship | Direction | Properties |
|---|---|---|
| `USES` | Doctrine → Fit | *(none)* |
| `CONTAINS` | Fit → Item | *(none)* |
| `SHARES_ITEM_WITH` | Fit → Fit | `shared_item_count`, `overlap_score`, `weight`, `first_seen`, `last_seen` |
| `USES_CRITICAL_ITEM` | Fit → Item | `criticality_score`, `doctrine_count`, `fit_count`, `last_seen` |

### Universe Topology
| Relationship | Direction |
|---|---|
| `CONNECTS_TO` | System → System |
| `IN_CONSTELLATION` | System → Constellation |
| `IN_REGION` | Constellation → Region |

### Cohort
| Relationship | Direction | Properties |
|---|---|---|
| `BELONGS_TO_COHORT` | Character → Cohort | `computed_at` |

---

## 4. Datetime Convention

### Rule: ISO 8601 with Z suffix for Neo4j, SQL format for MariaDB

| Context | Format | Example |
|---|---|---|
| **Neo4j node/relationship properties** | `YYYY-MM-DDTHH:MM:SSZ` | `2026-03-31T14:30:00Z` |
| **Neo4j Cypher `datetime()` calls** | `toString(datetime())` | Always wrap in `toString()` |
| **MariaDB columns** | `YYYY-MM-DD HH:MM:SS` | `2026-03-31 14:30:00` |
| **Python → Neo4j parameters** | `.isoformat()` or `strftime("%Y-%m-%dT%H:%M:%SZ")` | Always include Z |
| **Log files (JSON)** | `.isoformat()` | `2026-03-31T14:30:00+00:00` |

### Helper functions (Python side)

```python
# For Neo4j property values passed as parameters
def _utc_now_iso() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")

# For MariaDB columns
def _now_sql() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
```

### In Cypher queries

```cypher
-- CORRECT: always wrap in toString()
SET r.updated_at = toString(datetime())
SET r.computed_at = toString(datetime())

-- WRONG: raw datetime() stored as native datetime object (breaks string comparisons)
SET r.updated_at = datetime()
```

---

## 5. Constraints (canonical names)

All constraint names follow the pattern `<label_lower>_<property>`.

```cypher
CREATE CONSTRAINT character_character_id IF NOT EXISTS FOR (n:Character) REQUIRE n.character_id IS UNIQUE;
CREATE CONSTRAINT battle_battle_id IF NOT EXISTS FOR (n:Battle) REQUIRE n.battle_id IS UNIQUE;
CREATE CONSTRAINT killmail_killmail_id IF NOT EXISTS FOR (n:Killmail) REQUIRE n.killmail_id IS UNIQUE;
CREATE CONSTRAINT alliance_alliance_id IF NOT EXISTS FOR (n:Alliance) REQUIRE n.alliance_id IS UNIQUE;
CREATE CONSTRAINT corp_corporation_id IF NOT EXISTS FOR (n:Corporation) REQUIRE n.corporation_id IS UNIQUE;
CREATE CONSTRAINT doctrine_doctrine_id IF NOT EXISTS FOR (n:Doctrine) REQUIRE n.doctrine_id IS UNIQUE;
CREATE CONSTRAINT fit_fit_id IF NOT EXISTS FOR (n:Fit) REQUIRE n.fit_id IS UNIQUE;
CREATE CONSTRAINT item_type_id IF NOT EXISTS FOR (n:Item) REQUIRE n.type_id IS UNIQUE;
CREATE CONSTRAINT side_side_uid IF NOT EXISTS FOR (n:BattleSide) REQUIRE n.side_uid IS UNIQUE;
CREATE CONSTRAINT ship_type_id IF NOT EXISTS FOR (n:ShipType) REQUIRE n.type_id IS UNIQUE;
CREATE CONSTRAINT system_system_id IF NOT EXISTS FOR (n:System) REQUIRE n.system_id IS UNIQUE;
CREATE CONSTRAINT constellation_constellation_id IF NOT EXISTS FOR (n:Constellation) REQUIRE n.constellation_id IS UNIQUE;
CREATE CONSTRAINT region_region_id IF NOT EXISTS FOR (n:Region) REQUIRE n.region_id IS UNIQUE;
CREATE CONSTRAINT shipclass_ship_class_id IF NOT EXISTS FOR (n:ShipClass) REQUIRE n.ship_class_id IS UNIQUE;
CREATE CONSTRAINT checkpoint_run_id IF NOT EXISTS FOR (n:ComputeCheckpoint) REQUIRE n.run_id IS UNIQUE;
```

---

## 6. Indexes

```cypher
CREATE INDEX character_lookup IF NOT EXISTS FOR (n:Character) ON (n.character_id);
CREATE INDEX character_tracked IF NOT EXISTS FOR (n:Character) ON (n.tracked);
CREATE INDEX battle_lookup IF NOT EXISTS FOR (n:Battle) ON (n.battle_id);
CREATE INDEX battle_started_lookup IF NOT EXISTS FOR (n:Battle) ON (n.started_at);
CREATE INDEX doctrine_lookup IF NOT EXISTS FOR (n:Doctrine) ON (n.doctrine_id);
CREATE INDEX fit_lookup IF NOT EXISTS FOR (n:Fit) ON (n.fit_id);
CREATE INDEX item_lookup IF NOT EXISTS FOR (n:Item) ON (n.type_id);
CREATE INDEX side_key_lookup IF NOT EXISTS FOR (n:BattleSide) ON (n.side_key);
CREATE INDEX system_security IF NOT EXISTS FOR (n:System) ON (n.security);
CREATE INDEX system_constellation IF NOT EXISTS FOR (n:System) ON (n.constellation_id);
CREATE INDEX killmail_battle_id IF NOT EXISTS FOR (n:Killmail) ON (n.battle_id);
CREATE INDEX corp_id IF NOT EXISTS FOR (n:Corporation) ON (n.corporation_id);
CREATE INDEX alliance_id IF NOT EXISTS FOR (n:Alliance) ON (n.alliance_id);

-- Relationship indexes
CREATE INDEX member_from IF NOT EXISTS FOR ()-[r:MEMBER_OF]-() ON (r.from);
CREATE INDEX part_of_as_of IF NOT EXISTS FOR ()-[r:PART_OF]-() ON (r.as_of);
CREATE INDEX current_corp_as_of IF NOT EXISTS FOR ()-[r:CURRENT_CORP]-() ON (r.as_of);
```

---

## 7. Naming Convention for `name` Properties

All node types store their display name in a property called **`name`** (never `character_name`, `alliance_name`, etc.).

```cypher
-- CORRECT
COALESCE(n.name, 'Unknown')

-- WRONG (legacy, must not be used)
COALESCE(n.name, n.character_name, n.alliance_name, ...)
```

---

## 8. Changes Applied

### Files modified in this standardization pass

| File | Changes |
|---|---|
| `battle_intelligence.py` | `.id` → `.battle_id`/`.character_id` in constraints and MERGE; constraint names aligned |
| `intelligence_pipeline.py` | `k.id` → `k.killmail_id`, `sc.id` → `sc.ship_class_id`; `toString(datetime())` for computed_at |
| `theater_graph_integration.py` | `{id: cid}` → `{character_id: cid}`, `c.id` → `c.character_id` |
| `counterintel_pipeline.py` | `IN_ALLIANCE` → `PART_OF` |
| `graph_typed_interactions.py` | `datetime()` → `toString(datetime())` in SET clauses |
| `graph_evidence_paths.py` | Removed `n.character_name`, `n.alliance_name` fallbacks |
| `neo4j_indexes.cypher` | Updated to match canonical constraint/index list |

---

## 9. Migration Notes

After applying these code changes, the Neo4j database needs a **full reset** (drop all nodes/relationships) because:

1. Old nodes may have `.id` properties instead of `.character_id` / `.battle_id` / `.killmail_id`
2. Old `IN_ALLIANCE` relationships need to become `PART_OF`
3. Datetime properties stored as native `datetime` objects need to become ISO strings
4. Old constraints with wrong names need to be dropped before new ones can be created

The reset script should:
- Drop all constraints and indexes
- Delete all nodes and relationships (`MATCH (n) DETACH DELETE n` in batches)
- Re-run `_ensure_schema()` from graph_pipeline.py
- Re-run all sync jobs to repopulate
