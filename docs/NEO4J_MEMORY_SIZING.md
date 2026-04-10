# Neo4j Memory Sizing for SupplyCore

> Concrete heap / page-cache / GDS sizing for the SupplyCore Neo4j instance
> running on a **128 GB, 32-thread shared host** alongside MariaDB, PHP-FPM,
> Redis, and the Python worker lanes.
>
> Reference:
> [Neo4j GDS — Memory Estimation](https://neo4j.com/docs/graph-data-science/current/common-usage/memory-estimation/),
> [Neo4j Ops Manual — Memory Configuration](https://neo4j.com/docs/operations-manual/current/performance/memory-configuration/),
> [GDS Configuration Settings](https://neo4j.com/docs/graph-data-science/current/production-deployment/configuration-settings/).
>
> Companion config: [`setup/neo4j_memory.conf`](../setup/neo4j_memory.conf).

---

## 1. TL;DR

- Set `server.memory.heap.max_size=31g` (stay under the 32 GB compressed-oops
  boundary — 32 GB is strictly worse than 31 GB).
- Set `server.memory.pagecache.size=32g`.
- Turn on `gds.validate_using_max_memory_estimation=true`.
- MariaDB `innodb_buffer_pool_size` should be cut to **≤ 40 GB** on this host
  when Neo4j runs in the aggressive profile — otherwise the two fight for
  page cache and the kernel starts swapping.
- Never call GDS with `sudo: true` in our jobs. It bypasses the memory guard.
- Re-run `neo4j-admin server memory-recommendation` after any bulk rebuild to
  validate the page-cache sizing against the real store size.

---

## 2. Why the Neo4j GDS docs matter for us

The "interesting" things from the upstream
[memory estimation page](https://neo4j.com/docs/graph-data-science/current/common-usage/memory-estimation/)
that are **not obvious** and apply directly to SupplyCore:

1. **GDS projections live entirely on the JVM heap, not in the page cache.**
   Every `CALL gds.graph.project(...)` we issue — for `character_combat_*` in
   `neo4j_ml_exploration.py`, `character_community` in
   `graph_community_detection.py`, and the system topology projection in
   `compute_map_intelligence.py` — consumes heap. Growing the page cache does
   not help GDS. Growing the heap does.
2. **Undirected projection doubles relationship memory.** All three of our
   projections use `orientation: 'UNDIRECTED'`, so every edge is stored twice
   in GDS working memory. Keep this in mind when reading the estimator's
   `bytesMax` — it already accounts for the duplication, but our instinct is
   usually to halve the number.
3. **Relationship properties are stored alongside relationship pairs.** Our
   character combat projection projects a `weight` property
   (`COALESCE(r.weight, COALESCE(r.count, 1.0))`). That adds 8 bytes × edges ×
   2 (undirected) to the heap footprint.
4. **The memory guard uses `bytesMin` by default.** A job can estimate at
   "fits in heap" under the optimistic estimate and still OOM at runtime.
   Set `gds.validate_using_max_memory_estimation=true` so the guard uses
   `bytesMax` — this is the single most important safety flag on a shared host.
5. **`sudo: true` disables the memory guard entirely.** Grep-check every
   GDS call in `python/orchestrator/jobs/*.py` before merging; none of the
   current calls set it, and none should.
6. **32 GB heap is *worse* than 31 GB heap** because the JVM stops using
   compressed object pointers at 32 GB, roughly reducing effective heap size.
   The comfortable sweet spot is 28–31 GB.
7. **`neo4j-admin server memory-recommendation` reads the actual store files**
   and tells you the minimum page cache to hold data + native indexes. Run it
   after every full rebuild; do not pick page-cache sizes blind.

---

## 3. Host memory budget (128 GB, shared)

The host also runs MariaDB (`market_orders_history` is 107M rows),
Apache/PHP-FPM, Redis, and the Python worker lanes. Two profiles:

| Component                                | Conservative | Aggressive (GDS-heavy) |
| ---------------------------------------- | -----------: | ---------------------: |
| Linux + filesystem cache headroom        |         4 GB |                   4 GB |
| MariaDB `innodb_buffer_pool_size`        |        56 GB |                  40 GB |
| MariaDB overhead (log buffer, tmp, etc.) |         4 GB |                   4 GB |
| Apache + PHP-FPM pool                    |         4 GB |                   4 GB |
| Redis                                    |         2 GB |                   2 GB |
| Python worker lanes (realtime/ingestion/compute/maintenance, systemd `MemoryMax` summed) |  8 GB |                   8 GB |
| **Neo4j heap** (`server.memory.heap.max_size`) |      16 GB |              **31 GB** |
| **Neo4j page cache** (`server.memory.pagecache.size`) |      24 GB |       **32 GB** |
| Neo4j off-heap / native / direct buffers |         4 GB |                   4 GB |
| GDS working headroom (counted in heap)   |    *(in heap)* |           *(in heap)* |
| Kernel slab / misc                       |         6 GB |                   1 GB |
| **Total**                                |   **128 GB** |             **128 GB** |

Defaults in [`setup/neo4j_memory.conf`](../setup/neo4j_memory.conf) track the
**aggressive** column. Switch to the conservative column when the operator
dashboard shows MariaDB `Buffer pool hit rate < 99.0%` on market scan queries,
or when GDS jobs are idle most of the day.

### Cross-service guardrails

- MariaDB `innodb_buffer_pool_size` in
  [`setup/mariadb_performance.cnf`](../setup/mariadb_performance.cnf) defaults
  to `8G`. On a 128 GB host this is wildly undersized. When you apply the
  Neo4j config, also raise MariaDB to `40G` (aggressive) or `56G`
  (conservative) — see the table above.
- Python worker lanes are capped in the systemd unit files with `MemoryMax`
  (compute 3 G, ingestion 1 G, realtime 2 G, maintenance 512 M). Those caps
  are already reflected in the table; do not double-count them in Neo4j's
  budget.
- Do not enable transparent huge pages and do not set `vm.swappiness=60`.
  Recommend `vm.swappiness=1` and `transparent_hugepage=never` at the kernel
  cmdline, matching Neo4j's operations guide.

---

## 4. Estimating what GDS actually needs

Before touching heap size, ask the cluster what the real workload costs.

### 4.1 Estimate the biggest projection we create

`neo4j_ml_exploration.py` runs three windows (`30d`, `90d`, `lifetime`) over
`Character` nodes connected by
`CO_OCCURS_WITH | DIRECT_COMBAT | ASSISTED_KILL | SAME_FLEET`, projected
undirected with a `weight` relationship property. The lifetime window is the
worst case. Estimate it directly against the live data:

```cypher
CALL gds.graph.project.estimate(
  'Character',
  {
    CO_OCCURS_WITH: { orientation: 'UNDIRECTED', properties: 'weight' },
    DIRECT_COMBAT:  { orientation: 'UNDIRECTED', properties: 'count'  },
    ASSISTED_KILL:  { orientation: 'UNDIRECTED', properties: 'count'  },
    SAME_FLEET:     { orientation: 'UNDIRECTED', properties: 'count'  }
  }
)
YIELD requiredMemory, bytesMin, bytesMax,
      heapPercentageMin, heapPercentageMax,
      nodeCount, relationshipCount;
```

Read `heapPercentageMax` against the current `server.memory.heap.max_size`. If
it exceeds **40 %**, reduce the time window, prune derived edges harder in
`compute_graph_prune`, or grow the heap — in that order.

### 4.2 Estimate the heaviest algorithm on that projection

FastRP and Betweenness are the two algorithms that stress heap the most in
our pipeline. Estimate them on an *already-created* projection:

```cypher
CALL gds.fastRP.stream.estimate('character_combat_lifetime', {
  embeddingDimension: 64,
  iterationWeights: [0.0, 1.0, 1.0, 0.8, 0.4]
})
YIELD requiredMemory, bytesMin, bytesMax,
      heapPercentageMin, heapPercentageMax;

CALL gds.betweenness.stream.estimate('character_combat_lifetime', {})
YIELD requiredMemory, bytesMin, bytesMax,
      heapPercentageMin, heapPercentageMax;
```

Add the larger of the two to the projection cost from §4.1. That sum is the
lower bound on `server.memory.heap.max_size` — double it for comfort.

### 4.3 Estimate a hypothetical future size (capacity planning)

Once the graph grows past what's in the database today, use a fictive
estimate so we know when to grow the box before the jobs actually start
OOMing:

```cypher
CALL gds.graph.project.estimate('*', '*', {
  nodeCount: 500000,
  relationshipCount: 20000000,
  nodeProperties: 'ignored',
  relationshipProperties: 'weight'
})
YIELD requiredMemory, bytesMin, bytesMax,
      heapPercentageMin, heapPercentageMax;
```

This is how we reason about "if we double tracked alliances, can the current
box take it?" without needing to actually project 20 M edges.

### 4.4 Ask the CLI for the page-cache target

```bash
sudo -u neo4j /usr/bin/neo4j-admin server memory-recommendation
```

The output includes `Total size of data and native indexes`. `server.memory.pagecache.size`
should be **≥** that number. If the CLI recommendation drifts above 32 GB we
need to either grow the box or prune older Killmail / ComputeCheckpoint nodes.

---

## 5. Config reference (applied in `setup/neo4j_memory.conf`)

| Key                                            | Value | Why |
| ---------------------------------------------- | ----- | --- |
| `server.memory.heap.initial_size`              | `31g` | Same as max to avoid resize STW pauses. |
| `server.memory.heap.max_size`                  | `31g` | Max heap that keeps compressed oops. |
| `server.memory.pagecache.size`                 | `32g` | Holds store + native indexes with headroom. |
| `dbms.memory.transaction.total.max`            | `2g`  | Cap across all concurrent writes. |
| `db.memory.transaction.total.max`              | `2g`  | Per-database cap. |
| `db.memory.transaction.max`                    | `512m`| Per-transaction cap — batches are small anyway. |
| `gds.validate_using_max_memory_estimation`     | `true`| Memory guard uses `bytesMax`, not `bytesMin`. |
| `gds.progress_tracking_enabled`                | `true`| Lets us read live progress from `gds.listProgress()`. |
| `dbms.security.procedures.unrestricted`        | `gds.*,apoc.*` | Required for GDS to function. |
| `dbms.security.procedures.allowlist`           | `gds.*,apoc.*` | Required when allowlist is enabled. |
| `db.transaction.timeout`                       | `600s` | Above the longest per-query timeout our jobs pass. |
| `server.bolt.thread_pool_max_size`             | `16`  | Leaves cores for MariaDB + PHP on 32-thread CPU. |
| `-XX:+UseG1GC`                                 | —     | G1 handles GDS humongous allocations. |
| `-XX:G1HeapRegionSize=32m`                     | —     | Reduces humongous-allocation fragmentation. |
| `-XX:+AlwaysPreTouch`                          | —     | Commit heap at start; no mid-run faulting pauses. |

---

## 6. Things our jobs should *not* do

- **Never pass `sudo: true`** to a GDS procedure in
  `python/orchestrator/jobs/*.py`. It disables the pre-flight memory guard
  and turns an estimator overshoot into an OOM kill.
- **Do not keep GDS projections alive between jobs.** All three of our
  projection sites already drop-then-recreate; keep it that way. A long-lived
  `character_combat_lifetime` projection pins tens of GB of heap even when
  no algorithm is running.
- **Do not run graph_community_detection and neo4j_ml_exploration
  concurrently.** Both project an undirected `Character` graph over
  overlapping rel types; running them in parallel roughly doubles the heap
  high-water mark. Place them in the same lane or add a dependency in the
  scheduler.
- **Do not widen FastRP's `embeddingDimension` past 128** on this host
  without re-running §4.2. Memory scales linearly with dimension.

---

## 7. Monitoring and alerting hooks

- `graph_health_snapshots.max_character_degree` is already persisted by
  `compute_graph_prune`. When it trends above 2 000, the downstream
  projection cost in §4.1 will start climbing faster than linear — that is
  our leading indicator for resizing heap.
- Add a recurring probe in `graph_data_quality_check` that runs the §4.1
  estimator and writes `heapPercentageMax` into
  `graph_health_snapshots.notes` so the operator dashboard can alert on
  sustained `> 50 %`.
- Tail `/var/log/neo4j/gc.log` (path set in `setup/neo4j_memory.conf`) for
  `Full GC` entries. Any `Full GC` during GDS execution means the heap is
  undersized — do not dismiss it.

---

## 8. Rollout checklist

1. Apply [`setup/neo4j_memory.conf`](../setup/neo4j_memory.conf) into
   `/etc/neo4j/neo4j.conf` (merge keys; do not blindly overwrite).
2. Update MariaDB `innodb_buffer_pool_size` to match §3.
3. `sudo systemctl restart neo4j` then `sudo systemctl restart mariadb`.
4. Run `neo4j-admin server memory-recommendation` and confirm the reported
   data+index total is ≤ `server.memory.pagecache.size`.
5. From `cypher-shell`, run the three estimator queries from §4.
6. Run `neo4j_ml_exploration` once manually and confirm
   `heap_percentage_max < 50 %` in the job log.
7. Re-check `graph_health_snapshots` for any new warnings before re-enabling
   the compute lane schedule.
