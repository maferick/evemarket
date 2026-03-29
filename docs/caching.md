# ESI Caching Architecture

> How SupplyCore fetches, caches, and coordinates EVE ESI data across
> Python workers, Redis, and MariaDB.

---

## Overview

Python is the **sole ESI caller** for all automated data flows. Every ESI
request goes through the `EsiGateway`, which enforces the full compliance
lifecycle before any HTTP call leaves the process:

```
Job (market sync, entity resolve, killmail enrichment, ...)
  │
  ▼
EsiGateway
  ├─ 1. Load cached metadata   (Redis → MariaDB → local memory)
  ├─ 2. Expires-gate            skip if data is still fresh
  ├─ 3. Distributed fetch lock  prevent duplicate requests (Redis)
  ├─ 4. Conditional headers     If-None-Match / If-Modified-Since
  ├─ 5. Rate-limit check        shared budget (Redis → MariaDB → local)
  ├─ 6. HTTP request to ESI     only if all checks pass
  ├─ 7. Process response        update metadata, handle 304/429
  ├─ 8. Persist state           write to Redis + MariaDB + local
  └─ 9. Release fetch lock
```

PHP never calls ESI for automated sync. It reads results from MariaDB
(and optionally Redis) that Python produced.

---

## The Three-Tier Cache

Every piece of ESI-related state lives in **three places**, in order of
speed and volatility:

```
┌─────────────────────────────────────────────────────────┐
│  Tier 1: Redis            (fast, ephemeral, cross-process) │
│  ─────────────────────────────────────────────────────── │
│  • ESI endpoint metadata   esi:meta:v1:{endpoint_key}   │
│  • Rate-limit buckets      esi:ratelimit:v1:{group}     │
│  • Retry-after deadlines   esi:retry_after:v1:{group}   │
│  • Fetch locks             lock:esi_fetch:v1:{key}      │
│  • Request suppression     esi:suppress:v1:{key}        │
│                                                         │
│  TTL: driven by ESI Expires header (60s–3600s)          │
│  Loss on restart: acceptable — Tier 2 rebuilds it       │
├─────────────────────────────────────────────────────────┤
│  Tier 2: MariaDB          (durable, authoritative)      │
│  ─────────────────────────────────────────────────────── │
│  • esi_endpoint_state      etag, last_modified,         │
│                            expires_at, status counts    │
│  • esi_rate_limit_observations  append-only audit log   │
│  • esi_pagination_consistency_events                    │
│  • entity_metadata_cache   resolved entity names/IDs    │
│  • alliance_structure_metadata  structure names          │
│  • market_orders_current   ingested order data          │
│                                                         │
│  TTL: none (permanent), prunable by age for audit logs  │
│  Loss: unacceptable — this is the source of truth       │
├─────────────────────────────────────────────────────────┤
│  Tier 3: Process memory   (fast, single-process only)   │
│  ─────────────────────────────────────────────────────── │
│  • _local_meta dict        endpoint metadata fallback   │
│  • _GroupBucket instances  per-group rate-limit state   │
│  • _corp_alliance_cache    session-local entity cache   │
│                                                         │
│  TTL: process lifetime                                  │
│  Loss: expected on restart — Tiers 1+2 rebuild it       │
└─────────────────────────────────────────────────────────┘
```

### Read path (load metadata)

```
_load_meta(endpoint_key):
  1. Redis GET esi:meta:v1:{key}        → HIT? return it
  2. MariaDB SELECT esi_endpoint_state  → HIT? return it, repopulate Redis
  3. Process-local dict                 → HIT? return it
  4. None                               → endpoint has never been fetched
```

### Write path (save metadata)

```
_save_meta(meta) + _persist_endpoint_state(meta):
  1. Process-local dict ← meta          (always, instant)
  2. Redis SET with TTL ← meta          (if available)
  3. MariaDB UPSERT esi_endpoint_state  (always, durable)
```

Both writes happen on every successful ESI response, 304 Not Modified,
and rate-limit event. If Redis is down, steps 1 and 3 still execute.

---

## ESI Compliance Lifecycle

### Expires-gating

Every ESI response includes an `Expires` header. The gateway parses it
to a Unix timestamp and stores it in `EndpointMeta.expires_at`.

Before making any request, the gateway checks:

```python
if meta.expires_at > time.time():
    return GatewayResponse(from_cache=True)  # skip the HTTP call
```

This is the single most impactful optimization: it prevents redundant
ESI calls when the data hasn't changed. ESI market endpoints typically
expire every 5 minutes; character endpoints every 30–60 minutes.

When no `Expires` header is present, a conservative fallback of 60
seconds is used.

### Conditional requests (ETag / 304)

When cached metadata includes an `ETag` or `Last-Modified` value, the
gateway adds conditional headers to the request:

```
If-None-Match: "abc123"
If-Modified-Since: Thu, 01 Jan 2026 00:00:00 GMT
```

If ESI returns **304 Not Modified**, the gateway:
- Updates `last_checked_at` and `expires_at` timestamps
- Increments `not_modified_count`
- Returns `GatewayResponse(not_modified=True)` with no body
- Does **not** re-fetch or re-write the payload

This saves bandwidth and reduces ESI token cost (304 costs 1 token vs
2 for a full 200 response).

### Paginated Last-Modified consistency

Market order endpoints return paginated results (up to 20 pages). If
CCP updates the data mid-pagination, different pages may reflect
different snapshots. The gateway enforces consistency:

1. Fetch page 1, record its `Last-Modified` as the reference timestamp
2. Fetch pages 2..N, compare each page's `Last-Modified` to the reference
3. If any page differs: **abort the entire cycle** and retry from page 1
4. Up to 2 retries (3 total attempts)
5. On final failure: log to `esi_pagination_consistency_events` and return
   partial data with a warning

This prevents ingesting a mix of old and new orders that could create
phantom price spikes or disappearing inventory.

---

## Rate-Limit Coordination

ESI uses a token-bucket rate limiter per group. Each request costs tokens
(2xx = 2 tokens, 4xx = 5 tokens, 304 = 1 token). When tokens are
exhausted, ESI returns 429 with a `Retry-After` header.

### Shared state across workers

The rate limiter tracks per-group budgets and shares them via Redis:

```
Read:  Redis esi:ratelimit:v1:{group} → MariaDB esi_rate_limit_observations → local bucket
Write: local bucket → Redis → MariaDB (observation audit log)
```

The **most-conservative-wins** strategy: if Redis reports lower
`remaining` than the local bucket, the local bucket adopts the lower
value. This prevents workers from independently exhausting the budget.

### Retry-after suppression

When any worker receives a 429:
1. The `Retry-After` seconds are stored in Redis: `esi:retry_after:v1:{group}`
2. All workers check this key before making requests
3. A suppression key `esi:suppress:v1:{endpoint}` is set to block that
   specific endpoint
4. The observation is recorded in MariaDB for audit

When Redis is down, the limiter falls back to:
- Process-local `_retry_after_deadline` (global per process)
- Most recent MariaDB observation (last 15 minutes) for the group

---

## Distributed Fetch Locks

To prevent two workers from fetching the same endpoint simultaneously:

```
lock:esi_fetch:v1:{endpoint_key}  TTL=30s
```

The lock uses Redis `SET NX EX` with a random token and Lua
compare-and-delete for safe release (same pattern as PHP's
`supplycore_redis_lock_release`).

If the lock is held by another worker:
- Wait up to 5 seconds, polling every 0.5 seconds
- If still locked: proceed without the lock (degraded mode, avoids deadlock)
- The lock TTL (30s) ensures automatic cleanup if the holder crashes

When Redis is down, fetch locks are skipped entirely. Workers may
duplicate requests, but the data is idempotent — duplicate fetches
produce the same result.

---

## Freshness-Driven Scheduling

The scheduler (`queue_due_recurring_jobs`) integrates ESI freshness
into its scheduling decisions:

```
For each ESI job:
  1. Check esi_endpoint_state for the endpoints this job depends on
  2. If ALL endpoints still have expires_at > NOW:
     → defer the job (decision: "rolling_deferred_esi_fresh")
  3. If any endpoint has expired:
     → proceed with normal scheduling (urgency based on staleness)
```

This means jobs only run when there's actually new data to fetch.
The mapping from job to endpoints is defined in `ESI_JOB_ENDPOINT_MAP`
in `scheduler_pressure.py`.

The scheduler also computes **system pressure** (healthy / loaded /
critical) from running/queued/failed job counts and scales urgency
accordingly. Under critical pressure, non-essential jobs are deferred.

---

## Redis Key Schema

All keys are version-prefixed (`v1`) and use the configured prefix
(default `supplycore`). Full key format: `{prefix}:{namespace}`.

| Key pattern | Purpose | TTL |
|-------------|---------|-----|
| `esi:meta:v1:{endpoint_key}` | Endpoint metadata (etag, expires, etc.) | Expires-driven (60s–3600s) |
| `esi:payload:v1:{endpoint_key}` | Cached response body | Expires-driven |
| `esi:ratelimit:v1:{group}:{identity}` | Rate-limit bucket state | Window + 60s |
| `esi:retry_after:v1:{group}:{identity}` | Retry-after deadline (Unix timestamp) | Retry-After + 5s |
| `esi:suppress:v1:{endpoint_or_group}` | Request suppression (429 storm protection) | Retry-After + 5s |
| `lock:esi_fetch:v1:{endpoint_key}` | Distributed fetch lock | 30s |
| `lock:esi_queue:v1:{queue_scope}` | Queue claim lock | Operation TTL |

### Endpoint key format

```
GET:/latest/markets/{region_id}/orders/:a3f2c1b9e4d7:anonymous:p1
 │            │                          │          │        │
 method    route_template         SHA-1 of params  identity  page
```

### Serialization

All Redis values use JSON envelopes for cross-language compatibility
with the PHP Redis layer (`src/cache.php`):

```json
{
  "v": 1,
  "ts": "2026-03-29T18:00:00Z",
  "data": { ... },
  "meta": { ... }
}
```

No PHP `serialize()` or Python `pickle` — always JSON.

---

## Redis Failure Modes

| Scenario | Behavior |
|----------|----------|
| Redis down at startup | Gateway works with MariaDB + local memory. Logs warning once. |
| Redis goes down mid-operation | Current operation completes. Next operation falls back. |
| Redis payload evicted/missing | Gateway falls through to conditional ESI request (304 if unchanged). |
| Redis restarts (empty) | MariaDB metadata repopulates Redis on first read per endpoint. |
| Redis slow (>5s timeout) | Operation fails gracefully, marked unavailable until next success. |
| Redis data corruption | JSON parse failure returns None, endpoint re-fetched from ESI. |

**Invariant:** no data exists _only_ in Redis. MariaDB always has the
durable copy. Redis loss causes temporary re-fetching from ESI (extra
network calls) but never data loss or incorrect behavior.

---

## Retry and Circuit Breaker

### Exponential backoff

When a job fails, the retry delay scales with the attempt number:

```
Attempt 1: base_delay × 1  (e.g. 30s)
Attempt 2: base_delay × 2  (e.g. 60s)
Attempt 3: base_delay × 3  (e.g. 90s)
...capped at 300s
```

### Circuit breaker

If a job_key has 3+ consecutive failures in the last 10 minutes, the
circuit breaker activates: retry delay is tripled (up to 600s). This
prevents a failing endpoint from consuming worker capacity in a tight
retry loop.

### Lock-group enforcement

Jobs in the same `lock_group` (e.g. `market_sync`) are serialized at
claim time via a SQL WHERE clause. If a `market_sync` job is running,
no other `market_sync` job can be claimed. This prevents concurrent
writes to the same market order tables.

---

## PHP and ESI

PHP **never calls ESI** for automated data flows. It reads from:

- `entity_metadata_cache` — resolved entity names (alliance, corp, character)
- `alliance_structure_metadata` — structure names (populated by Python or first-time setup)
- `ref_npc_stations` — NPC station names (static reference data)
- `market_orders_current` — market order data (populated by Python sync jobs)

### Accepted PHP ESI exceptions

Four authenticated callsites remain in PHP for synchronous UI interactions
that require an OAuth token and cannot be deferred to async resolution:

| Function | Endpoint | Why it stays |
|----------|----------|-------------|
| `esi_alliance_structure_metadata()` | `/universe/structures/{id}/` | First-time structure setup — ESI only returns data for structures where the character has docking rights |
| `esi_structure_search()` | `/characters/{id}/search/?categories=structure` | Settings UI structure search — only returns dockable structures |
| `esi_alliance_and_corporation_search()` | `/characters/{id}/search/?categories=alliance,corporation` | Settings UI entity autocomplete |
| `doctrine_search_inventory_type_esi()` | `/characters/{id}/search/?categories=inventory_type` | Doctrine editor item type autocomplete |

All four are user-initiated (settings pages only), not automated sync.
They use `http_get_json()` directly with an OAuth token.

### Async entity resolution

When PHP encounters an unknown entity ID (e.g. during killmail display):

1. PHP checks `entity_metadata_cache` — if resolved, use it
2. If missing: call `db_entity_metadata_cache_mark_pending()` to queue it
3. Return placeholder text ("Unknown Alliance")
4. Python's `entity_metadata_resolve_sync` job picks up pending entries
5. Resolves via ESI `/universe/names/` through the gateway
6. Writes result to `entity_metadata_cache`
7. Next page load shows the resolved name

---

## Datastore Responsibilities

| Data | Authoritative store | Redis role | Writer |
|------|-------------------|------------|--------|
| Market orders | MariaDB `market_orders_current` | none | Python sync jobs |
| ESI endpoint metadata | MariaDB `esi_endpoint_state` | fast read cache | Python gateway |
| Rate-limit observations | MariaDB `esi_rate_limit_observations` | live bucket state | Python gateway + limiter |
| Entity names | MariaDB `entity_metadata_cache` | none | Python entity resolver |
| Structure names | MariaDB `alliance_structure_metadata` | none | Python sync + PHP first-time setup |
| NPC station names | MariaDB `ref_npc_stations` | none | Static reference data |
| Pagination events | MariaDB `esi_pagination_consistency_events` | none | Python gateway |
| Fetch locks | Redis (ephemeral) | authoritative | Python gateway |
| Suppression keys | Redis (ephemeral) | authoritative | Python gateway |
| Graph relationships | Neo4j | none | Python graph pipeline |
| Time-series telemetry | InfluxDB | none | Python export jobs |

**Rule:** if losing the data would cause incorrect behavior, it must be
in MariaDB. Redis holds only ephemeral coordination state that can be
rebuilt from ESI responses or MariaDB records.
