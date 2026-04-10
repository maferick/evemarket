# Bloom Intelligence — Cypher Cheat-Sheet for Neo4j Browser

> **Who this is for**: operators running **Neo4j Community Edition** who
> cannot use the Bloom UI (Bloom is Enterprise-only). Every phrase from
> `setup/neo4j_bloom_perspective.json` is reproduced here as a pasteable
> Cypher snippet for the free **Neo4j Browser**.
>
> This gives you ~90% of the Bloom analyst workflow — you lose the
> click-to-expand visual interaction, but all the curated queries still
> work unchanged. The companion runbook is
> `docs/BLOOM_OPERATIONAL_INTELLIGENCE.md`.

---

## 0. Setup

### Open Neo4j Browser

Neo4j Browser is the free web UI that ships with every Neo4j install
(Community and Enterprise). It's almost certainly already running:

    http://<your-neo4j-host>:7474/browser/

Log in with the same credentials the orchestrator uses (the
`neo4j.username` / `neo4j.password` values from `app_settings`, or from
the `NEO4J_USERNAME` / `NEO4J_PASSWORD` env vars).

### Setting query parameters

Most of the phrases below use `$name` / `$system` / `$alliance`
placeholders. In Neo4j Browser you set those with the `:params` command
**once per session**, then every subsequent `MATCH` picks them up
automatically:

```
:params {name: "Pilot Name Fragment"}
```

To set multiple at once:

```
:params {from: "Jita", to: "Amarr"}
```

To clear: `:params {}`.

### Make sure the entry-point labels exist

The "Show …" phrases in section 1 depend on the four additive labels
maintained by the `compute_bloom_entry_points` job. Run this once to
confirm they are populated:

```cypher
MATCH (n:HotBattle)       RETURN 'HotBattle'       AS tag, count(n) AS c
UNION ALL
MATCH (n:HighRiskPilot)   RETURN 'HighRiskPilot'   AS tag, count(n) AS c
UNION ALL
MATCH (n:StrategicSystem) RETURN 'StrategicSystem' AS tag, count(n) AS c
UNION ALL
MATCH (n:HotAlliance)     RETURN 'HotAlliance'     AS tag, count(n) AS c;
```

If any tier is zero, run the job manually:

```bash
python -m orchestrator run-job \
  --app-root /var/www/SupplyCore \
  --job-key compute_bloom_entry_points
```

---

## 1. Discovery phrases

Fast look-ups for the first click of any investigation.

### Find pilot

```
:params {name: "pilot fragment"}
```

```cypher
MATCH (p:Character)
WHERE toLower(p.name) CONTAINS toLower($name)
RETURN p
ORDER BY COALESCE(p.suspicion_score, 0) DESC
LIMIT 50;
```

### Find alliance

```
:params {name: "alliance fragment"}
```

```cypher
MATCH (a:Alliance)
WHERE toLower(a.name) CONTAINS toLower($name)
RETURN a
LIMIT 50;
```

### Show hot engagements

No parameters — opens the `:HotBattle` tier ordered by heat score.

```cypher
MATCH (b:HotBattle)
WITH b ORDER BY b.bloom_hot_score DESC LIMIT 50
OPTIONAL MATCH (b)-[:IN_SYSTEM|LOCATED_IN]->(s:System)
RETURN b, s;
```

### Show high-risk pilots

No parameters — opens the `:HighRiskPilot` tier ordered by recent
suspicion score.

```cypher
MATCH (p:HighRiskPilot)
WITH p
ORDER BY COALESCE(p.suspicion_score_recent, p.suspicion_score, 0) DESC
LIMIT 50
OPTIONAL MATCH (p)-[:MEMBER_OF_ALLIANCE]->(a:Alliance)
RETURN p, a;
```

### Show strategic systems

No parameters — opens the `:StrategicSystem` tier ordered by recent
battle density.

```cypher
MATCH (s:StrategicSystem)
WITH s ORDER BY s.bloom_recent_battle_count DESC LIMIT 50
RETURN s;
```

---

## 2. Locational phrases

Where is the fighting happening right now?

> **APOC required.** These three phrases use `apoc.date.format` for a
> clean "last N days" window. If APOC is not installed, replace the
> `apoc.date.format(...)` call with a literal ISO timestamp like
> `'2026-04-03T00:00:00Z'` and re-run.

### Show battles in a system (last 30 days)

```
:params {system: "Jita"}
```

```cypher
MATCH (s:System)
WHERE toLower(s.name) CONTAINS toLower($system)
WITH s
MATCH (b:Battle)-[:IN_SYSTEM|LOCATED_IN]->(s)
WHERE b.started_at >= apoc.date.format(
    datetime() - duration('P30D'),
    'ms',
    'yyyy-MM-dd\'T\'HH:mm:ss\'Z\''
)
RETURN b, s
ORDER BY b.started_at DESC
LIMIT 100;
```

### Show battles in a region (last 14 days)

```
:params {region: "Delve"}
```

```cypher
MATCH (r:Region)
WHERE toLower(r.name) CONTAINS toLower($region)
WITH r
MATCH (b:Battle)-[:IN_SYSTEM|LOCATED_IN]->(:System)
      -[:IN_CONSTELLATION]->(:Constellation)
      -[:IN_REGION]->(r)
WHERE b.started_at >= apoc.date.format(
    datetime() - duration('P14D'),
    'ms',
    'yyyy-MM-dd\'T\'HH:mm:ss\'Z\''
)
RETURN b, r
ORDER BY b.started_at DESC
LIMIT 100;
```

### Find alliances active in a system (last 30 days)

```
:params {system: "Jita"}
```

```cypher
MATCH (s:System)
WHERE toLower(s.name) CONTAINS toLower($system)
WITH s
MATCH (b:Battle)-[:IN_SYSTEM|LOCATED_IN]->(s)
WHERE b.started_at >= apoc.date.format(
    datetime() - duration('P30D'),
    'ms',
    'yyyy-MM-dd\'T\'HH:mm:ss\'Z\''
)
MATCH (p:Character)-[:PARTICIPATED_IN]->(b)
MATCH (p)-[:MEMBER_OF_ALLIANCE]->(a:Alliance)
RETURN DISTINCT a, count(DISTINCT b) AS battles
ORDER BY battles DESC
LIMIT 50;
```

---

## 3. Investigation phrases

Pivot from a suspect / alliance / doctrine and follow the evidence.

### Pilots who flew with a target (top 25 co-occurrence peers)

```
:params {name: "target pilot name"}
```

```cypher
MATCH (p:Character)
WHERE toLower(p.name) CONTAINS toLower($name)
WITH p LIMIT 1
MATCH (p)-[r:CO_OCCURS_WITH]-(other:Character)
WHERE COALESCE(r.recent_weight, r.weight, 0) > 0
RETURN p, r, other
ORDER BY COALESCE(r.recent_weight, r.weight, 0) DESC
LIMIT 25;
```

### Find pilots who flew with members of a hostile alliance

```
:params {alliance: "hostile alliance name"}
```

```cypher
MATCH (a:Alliance)
WHERE toLower(a.name) CONTAINS toLower($alliance)
WITH a LIMIT 1
MATCH (p:Character)-[:CO_OCCURS_WITH]-(hostile:Character)
      -[:MEMBER_OF_ALLIANCE]->(a)
WITH p, count(DISTINCT hostile) AS overlap
ORDER BY overlap DESC
LIMIT 50
RETURN p, overlap;
```

### Doctrine usage — fits and items

```
:params {name: "doctrine name"}
```

```cypher
MATCH (d:Doctrine)
WHERE toLower(d.name) CONTAINS toLower($name)
WITH d LIMIT 1
MATCH (d)-[:USES]->(f:Fit)
OPTIONAL MATCH (f)-[:CONTAINS]->(i:Item)
RETURN d, f, i
LIMIT 250;
```

### Alliance relationships (ally / hostile graph)

```
:params {name: "alliance name"}
```

```cypher
MATCH (a:Alliance)
WHERE toLower(a.name) CONTAINS toLower($name)
WITH a LIMIT 1
OPTIONAL MATCH (a)-[r1:ALLIED_WITH]-(ally:Alliance)
OPTIONAL MATCH (a)-[r2:HOSTILE_TO]-(enemy:Alliance)
RETURN a, r1, ally, r2, enemy
LIMIT 100;
```

### Threat corridor — shortest gate path (≤ 10 jumps)

```
:params {from: "Jita", to: "Amarr"}
```

```cypher
MATCH (a:System), (b:System)
WHERE toLower(a.name) = toLower($from)
  AND toLower(b.name) = toLower($to)
MATCH path = shortestPath((a)-[:CONNECTS_TO*..10]-(b))
RETURN path
LIMIT 1;
```

### Explain suspicion — the visual evidence trace

Pulls co-occurrence peers, anomaly associations, and side-crossings for
a target. This is the human validation loop that closes the feedback
between the scoring pipeline and analyst trust.

```
:params {name: "suspect pilot name"}
```

```cypher
MATCH (p:Character)
WHERE toLower(p.name) CONTAINS toLower($name)
WITH p LIMIT 1
OPTIONAL MATCH (p)-[r1:CO_OCCURS_WITH]-(peer:Character)
WITH p, r1, peer
ORDER BY COALESCE(r1.recent_weight, 0) DESC LIMIT 15
OPTIONAL MATCH (p)-[r2:ASSOCIATED_WITH_ANOMALY]->(b:Battle)
OPTIONAL MATCH (p)-[r3:CROSSED_SIDES]-(other:Character)
RETURN p, r1, peer, r2, b, r3, other;
```

---

## 4. Validation & freshness

### Tag freshness — when was each tier last refreshed?

```cypher
MATCH (n)
WHERE n.bloom_tagged_at IS NOT NULL
RETURN labels(n) AS labels, n.bloom_tagged_at AS tagged_at
ORDER BY tagged_at DESC
LIMIT 25;
```

### Top 10 of each tier (quick sanity check)

```cypher
MATCH (b:HotBattle)
RETURN 'HotBattle' AS tier, b.battle_id AS id, b.bloom_hot_score AS score
ORDER BY score DESC LIMIT 10;
```

```cypher
MATCH (p:HighRiskPilot)
RETURN 'HighRiskPilot' AS tier, p.name AS name,
       COALESCE(p.suspicion_score_recent, p.suspicion_score, 0) AS score
ORDER BY score DESC LIMIT 10;
```

```cypher
MATCH (s:StrategicSystem)
RETURN 'StrategicSystem' AS tier, s.name AS name,
       s.bloom_recent_battle_count AS battles
ORDER BY battles DESC LIMIT 10;
```

```cypher
MATCH (a:HotAlliance)
RETURN 'HotAlliance' AS tier, a.name AS name,
       a.bloom_recent_engagement_count AS engagements
ORDER BY engagements DESC LIMIT 10;
```

---

## 5. Operator notes

- **Favourite the Browser tab**. Neo4j Browser lets you save any query
  as a favourite (the star icon in the editor). Paste each phrase once,
  star it, and your sidebar becomes the equivalent of the Bloom search
  phrase list.
- **Dense edges (`CO_OCCURS_WITH`, `SAME_FLEET`) are unbounded by
  design.** Never write `MATCH (p)-[:CO_OCCURS_WITH*1..3]-(x)` — always
  filter by `recent_weight` / `weight` and `LIMIT`.
- **The PHP dashboard also surfaces the four tiers.** See the
  *Intelligence Anchors* panel on the main dashboard, populated from
  `bloom_entry_points_materialized` (maintained by the same job that
  maintains the Neo4j labels). That gives you the top-10 view without
  leaving the operator cockpit.
- **Related docs:** `docs/BLOOM_OPERATIONAL_INTELLIGENCE.md` (full
  runbook), `setup/neo4j_bloom_perspective.json` (the perspective JSON
  these phrases are extracted from), `setup/neo4j_bloom_entry_points.cypher`
  (the indexes backing the four tiers).
