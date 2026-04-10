# Neo4j Bloom — Operational Intelligence Interface

> **Positioning**: Bloom is not a visualization toy. It is a
> **business-facing query layer on top of the SupplyCore Neo4j intelligence
> graph** — an interactive investigation surface that sits *beside* the PHP
> dashboard, not behind it.

| Layer          | Purpose                                                  |
|----------------|----------------------------------------------------------|
| PHP Dashboard  | Curated, fixed operator cockpit (control plane).         |
| **Bloom**      | **Exploratory, investigative analyst surface.**          |
| Neo4j          | Knowledge graph (nodes, relationships, GDS outputs).     |
| Orchestrator   | Python ingestion + enrichment + scoring pipelines.       |

Bloom fills the missing "interactive intelligence exploration" layer: the
place where an analyst starts from a known hostile alliance and follows
relationship chains — co-occurrence, shared battles, recurring fleets — to
surface hidden structure that static dashboards cannot.

---

## What Bloom gives us

1. **Natural-language exploration** of the graph. Non-technical users issue
   phrases like *"Show battles in Vale of the Silent"* and Bloom translates
   them into bounded Cypher.
2. **Pivot-driven investigation workflows**: click → expand → refine →
   follow relationships.
3. **A curated domain language** that hides plumbing labels (`BattleSide`,
   `Cohort`, `ComputeCheckpoint`) and renames raw labels into operator
   vocabulary (`Character → Pilot`, `Battle → Engagement`, etc.).

Out-of-the-box Bloom is **not enough**. Usability requires a productized
perspective, curated search phrases, exploration limits, and precomputed
entry points — all of which are now shipped in this repository.

---

## Artifacts shipped in this repo

| Artifact                                                | Purpose                                                                 |
|---------------------------------------------------------|-------------------------------------------------------------------------|
| `setup/neo4j_bloom_perspective.json`                    | Importable Bloom perspective: labels, rel styles, hidden types, search phrases. |
| `setup/neo4j_bloom_entry_points.cypher`                 | Indexes backing the smart entry-point labels.                           |
| `python/orchestrator/jobs/compute_bloom_entry_points.py`| Python worker that maintains the entry-point labels (batch, idempotent).|
| `database/migrations/20260424_bloom_entry_points.sql`   | Registers the job on the Python scheduler (15 minute cadence).          |

Registration points covered:

- `python/orchestrator/jobs/__init__.py` — export
- `python/orchestrator/processor_registry.py` — dispatch
- `python/orchestrator/worker_registry.py` — scheduling + deps
- `src/functions.php` — authoritative job registry + dashboard grouping

---

## Smart entry-point labels

Bloom explorations collapse when analysts start from "all pilots" or
"all battles". The `compute_bloom_entry_points` job maintains four
additive labels on top of the canonical graph so analysts always start
from meaningful anchors:

| Label              | Base node   | Tagging rule (default)                                       |
|--------------------|-------------|--------------------------------------------------------------|
| `:HotBattle`       | `:Battle`   | Started in last 7d with `participant_count ≥ 25`.            |
| `:HighRiskPilot`   | `:Character`| `suspicion_score_recent` (or `suspicion_score`) `≥ 0.65`.    |
| `:StrategicSystem` | `:System`   | Hosted `≥ 10` battles in the last 14 days.                   |
| `:HotAlliance`     | `:Alliance` | Members participated in `≥ 15` distinct battles in last 7d.  |

The job is **incremental**: each run tags new qualifiers and untags
nodes that no longer meet the criteria. Writes are capped per label
(`bloom_entry_point_max_tags_per_label`, default 500) so one run can
never produce an unbounded Neo4j transaction.

All thresholds are runtime-overridable via the `neo4j` runtime section
without code changes:

```
bloom_hot_battle_window_days
bloom_hot_battle_min_participants
bloom_high_risk_pilot_min_score
bloom_strategic_system_window_days
bloom_strategic_system_min_battles
bloom_hot_alliance_window_days
bloom_hot_alliance_min_engagements
bloom_entry_point_max_tags_per_label
```

Tagged nodes carry auxiliary properties the Bloom perspective surfaces
directly in the caption:

- `bloom_tagged_at` — ISO timestamp of the last refresh
- `bloom_hot_score` — HotBattle: participant count
- `bloom_recent_battle_count` — StrategicSystem: battles in window
- `bloom_recent_engagement_count` — HotAlliance: distinct engagements

---

## Installation / bring-up

Prerequisite: Neo4j 5.x with Bloom, and the canonical schema already
applied from `setup/neo4j_indexes.cypher`.

### 1. Apply the entry-point indexes

```bash
cat setup/neo4j_bloom_entry_points.cypher \
  | cypher-shell -u "$NEO4J_USERNAME" -p "$NEO4J_PASSWORD"
```

Idempotent — safe to re-run.

### 2. Apply the migration so the scheduler picks up the job

```bash
php bin/run-migrations.php
```

### 3. Run the entry-point job once manually to seed the graph

```bash
python -m orchestrator.main run-job \
    --job-key compute_bloom_entry_points \
    --app-root /opt/supplycore
```

Expected output (verbose summary):

```
Bloom entry points refreshed: +412 tagged, -0 untagged
(HotBattle +61/-0, HighRiskPilot +187/-0, StrategicSystem +94/-0, HotAlliance +70/-0).
```

The job then runs every 15 minutes on the Python worker pool alongside
the rest of the graph pipeline.

### 4. Import the Bloom perspective

In the Bloom UI:

1. Open **Perspectives → Import**.
2. Select `setup/neo4j_bloom_perspective.json`.
3. Set it as the active perspective for your workspace.

The perspective will immediately show:

- Domain-language captions (`Pilot`, `Engagement`, `Solar System`…)
- Hidden plumbing labels (`BattleSide`, `Cohort`, `ComputeCheckpoint`)
- Colored / weighted relationship styles
- 14 prewired search phrases in the sidebar

---

## Search phrases

The shipped perspective includes 14 phrases covering the high-value
workflows. Each one compiles to a bounded Cypher query — no full scans,
no unbounded expansions.

### Discovery phrases

| Phrase                         | Purpose                                          |
|--------------------------------|--------------------------------------------------|
| `Find pilot $name`             | Locate pilots by name fragment.                  |
| `Find alliance $name`          | Locate alliances by name fragment.               |
| `Show hot engagements`         | Open the `:HotBattle` entry-point tier.          |
| `Show high-risk pilots`        | Open the `:HighRiskPilot` entry-point tier.      |
| `Show strategic systems`       | Open the `:StrategicSystem` entry-point tier.    |

### Locational phrases

| Phrase                            | Purpose                                          |
|-----------------------------------|--------------------------------------------------|
| `Show battles in $system`         | Battles in a system, last 30 days.               |
| `Show battles in region $region`  | Battles across a region, last 14 days.           |
| `Find alliances active in $system`| Alliances with members fighting in a system.     |

### Investigation phrases

| Phrase                                         | Purpose                                          |
|------------------------------------------------|--------------------------------------------------|
| `Show pilots who flew with $name`              | Top 25 co-occurrence peers by recent weight.     |
| `Find pilots who flew with alliance $alliance` | Pilots overlapping with members of a hostile alliance. |
| `Show doctrine $name usage`                    | Doctrine → fits → items tree.                    |
| `Show relationships for alliance $name`        | Ally / hostile alliance graph around a target.   |
| `Find route from $from to $to`                 | Gate-graph shortestPath (≤ 10 jumps).            |
| `Explain suspicion for $name`                  | Pull co-occurrence, anomaly and crossed-sides evidence for a suspect. |

> **APOC dependency**: the locational phrases use `apoc.date.format` to
> template a "last N days" window cleanly. If APOC is unavailable, replace
> those calls with a literal ISO timestamp in the phrase body.

---

## High-leverage use cases

### 1. Threat actor discovery

Start from a known hostile alliance → expand to `HOSTILE_TO` / `ALLIED_WITH`
neighbours → pivot into pilot co-occurrence. Replaces manual log scanning
and static reports.

### 2. Battle decomposition

Click a `:HotBattle` → expand to `PARTICIPATED_IN` pilots → pivot into
`USED_SHIP` and `CO_OCCURS_WITH` to see "what else did these pilots do".
Much richer than a precomputed summary row.

### 3. Suspicion graph explanation

Run `Explain suspicion for <pilot>` to visually trace co-occurrence +
anomaly association + side-crossings. This is the human validation loop
that closes the feedback loop between the scoring pipeline and analyst
trust.

### 4. Doctrine / fleet pattern mapping

Start from a doctrine node → walk `USES` / `CONTAINS` / `SHARES_ITEM_WITH`
to find meta shifts and adoption curves.

### 5. Strategic corridor / theater analysis

Walk `System-[:CONNECTS_TO]->System` paths anchored on `:StrategicSystem`
nodes to see active corridors. Precursor to the SVG map intelligence
overlay.

---

## What Bloom is NOT

- **Not the main frontend.** The PHP dashboard remains the operator cockpit.
- **Not a replacement for dashboards.** Fixed views stay in `public/`.
- **Not a high-scale public UI.** Expensive, dense, licensed per seat.

Bloom is an **internal intelligence tool** for analysts. Treat it the same
way you treat a SQL console: powerful, expert-oriented, and kept inside
the control plane.

---

## Operational safeguards

- **Max expand depth in the perspective is 2.** Do NOT raise without first
  filtering by weight or time window — `CO_OCCURS_WITH` / `SAME_FLEET` are
  dense and will produce thousand-node explosions otherwise.
- **Max neighbour count is 250** per expansion. Matches the practical
  rendering budget of a typical analyst session.
- **Max tags per label is 500** (overridable). Entry-point tiers are
  intentionally kept small to force ordering by relevance rather than
  dumping the whole graph.
- The `compute_bloom_entry_points` job depends on `compute_graph_sync`
  and `compute_suspicion_scores_v2`. The worker registry encodes this —
  Bloom tier is only refreshed once the base graph and scores are fresh.

---

## Troubleshooting

| Symptom                                              | Likely cause / fix                                          |
|------------------------------------------------------|-------------------------------------------------------------|
| `Show hot engagements` returns nothing               | Run `compute_bloom_entry_points` manually; no battles meet the threshold. |
| Expand into `CO_OCCURS_WITH` is slow / freezes       | Expansion depth > 2. Reduce depth and filter by weight.     |
| Search phrase using `apoc.date.format` errors        | APOC plugin not installed on Neo4j. Replace with literal ISO timestamp. |
| `HighRiskPilot` tier is always empty                 | `compute_suspicion_scores_v2` hasn't populated scores yet, or threshold too high. Lower `bloom_high_risk_pilot_min_score` or wait for the scoring pipeline to run. |
| Perspective imports but captions still show raw labels | Bloom session is holding the old perspective. Reload the perspective or refresh the page. |
| Entry-point indexes missing after Neo4j reset        | Re-apply `setup/neo4j_bloom_entry_points.cypher`.           |

### Validation queries

Count current entry points:

```cypher
MATCH (n:HotBattle)       RETURN 'HotBattle'       AS tag, count(n) AS c
UNION ALL
MATCH (n:HighRiskPilot)   RETURN 'HighRiskPilot'   AS tag, count(n) AS c
UNION ALL
MATCH (n:StrategicSystem) RETURN 'StrategicSystem' AS tag, count(n) AS c
UNION ALL
MATCH (n:HotAlliance)     RETURN 'HotAlliance'     AS tag, count(n) AS c;
```

Inspect tag freshness:

```cypher
MATCH (n)
WHERE n.bloom_tagged_at IS NOT NULL
RETURN labels(n) AS labels, n.bloom_tagged_at AS tagged_at
ORDER BY tagged_at DESC
LIMIT 25;
```

---

## Where to go from here

Advanced plays once the basics are stable:

1. **Feed GDS outputs into the perspective.** Community IDs, betweenness
   and PageRank are already on `Character` — add a phrase like
   *"Show central pilots in community $id"*.
2. **Tighten the loop with the scheduler.** Auto-tag emerging threats
   (e.g. a `:EmergingWarzone` label fed by `compute_threat_corridors`).
3. **Use Bloom for validation.** When the counterintel pipeline flags a
   pilot, analysts open Bloom, run `Explain suspicion for …`, and confirm
   or dismiss. Verdicts feed back into `graph_analyst_recalibration`.

Bloom is a force multiplier — but only if it is productized. This runbook
is the productization contract.
