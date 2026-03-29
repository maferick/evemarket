# ESI + Redis + Multi-Store Audit (Phase 1, revised)

Date: 2026-03-29 (UTC)

> Revision note: this version narrows remediation scope to **ESI correctness + rate limiting + queueing/coordination + ESI metadata caching only**. It intentionally excludes generic “hot page”/dashboard caching work.

---

## 1. Executive summary

### What was audited
Repository-wide ESI behavior across Python and PHP, specifically:
- ESI request paths
- ESI header handling (`Expires`, `ETag`, `Last-Modified`, `X-Ratelimit-*`, `Retry-After`)
- pagination behavior and consistency checks
- retry and backoff behavior
- scheduler/worker queueing and overlap control
- Redis usage for rate-limit coordination, fetch suppression, lock/claim coordination, and ESI metadata cache
- durable metadata/state in MariaDB
- datastore responsibility boundaries across MariaDB, Redis, Neo4j, InfluxDB

### Top findings
1. **CRITICAL**: PHP still issues direct ESI requests from `src/functions.php`, violating the requested Python-authoritative ESI model.
2. **CRITICAL**: Paginated ESI fetch flows do not enforce snapshot consistency via Last-Modified across all pages.
3. **HIGH**: End-to-end conditional-request lifecycle is incomplete (missing uniform Expires-gating + If-None-Match + 304 refresh path).
4. **HIGH**: Python limiter is process-local; no shared Redis coordination across workers/processes.
5. **HIGH**: Redis exists in PHP, but Python has config only and no runtime Redis implementation.

### Direction
- Python becomes sole ESI caller and sole writer of ESI-related Redis state.
- Redis is used only for ESI operational concerns in this phase:
  - ESI metadata cache
  - shared rate-limit state
  - retry-after suppression
  - distributed fetch/queue locks
- MariaDB remains authoritative for durable business + durable ESI audit/state.

---

## 2. Current-state findings by subsystem/file

### 2.1 ESI access paths

#### Python
- `python/orchestrator/esi_client.py`
  - Shared ESI client exists with header capture and status helpers.
  - Missing full conditional lifecycle (persisted Expires/ETag/Last-Modified and request revalidation headers).
- `python/orchestrator/esi_rate_limiter.py`
  - Parses ratelimit headers and Retry-After.
  - Missing distributed/shared state and per-identity persisted bucket state.
- `python/orchestrator/esi_market_adapter.py`
  - Paginated endpoint adapter exists.
  - Missing Last-Modified cross-page consistency enforcement.
- ESI-using jobs:
  - `python/orchestrator/jobs/esi_alliance_history_sync.py`
  - `python/orchestrator/jobs/killmail.py`
  - `python/orchestrator/jobs/killmail_history_backfill.py`
  - `python/orchestrator/jobs/alliance_current_sync.py`
  - `python/orchestrator/jobs/market_hub_current_sync.py`

#### PHP
- `src/functions.php` still performs direct ESI requests in multiple workflows:
  - market sync (`sync_alliance_structure_orders`, `sync_market_hub_current_orders`)
  - universe/entity lookups (`esi_universe_names_lookup`, `esi_npc_station_metadata`, etc.)
  - ESI search helpers and metadata lookups
- Generic HTTP helpers (`http_get_json*`, `http_get_json_multi*`) retry/backoff but are not ESI-compliance aware.

### 2.2 Caching/compliance gaps (ESI-specific)

Observed gaps:
- Expires not used as strict pre-request gate across all ESI call sites.
- ETag not consistently persisted and reused with `If-None-Match`.
- 304 handling not normalized as first-class control path.
- Last-Modified captured inconsistently and not enforced for paginated snapshot integrity.
- No unified per-endpoint metadata record with page granularity.

### 2.3 Rate-limit gaps

- Python limiter is local-memory only (no cross-process visibility).
- PHP ESI paths are not token-aware by group/bucket.
- No shared retry suppression keys for 429 storms.
- No durable rate-limit observation store for audit.
- No explicit protective throttle strategy before remaining tokens get low.

### 2.4 Queueing/concurrency findings

- Python worker framework has lock groups and interval/staleness metadata (`python/orchestrator/worker_registry.py`, `python/orchestrator/db.py`).
- But ESI fetch dedupe is not centralized around endpoint-signature claims.
- PHP and Python dual-path ESI execution creates queueing ambiguity and overlap risk.

### 2.5 Redis findings (ESI scope)

- `src/cache.php`: usable Redis primitives + lock functions (PHP).
- `python/orchestrator/config.py`: Redis config present, but no Python runtime Redis client.
- No shared key schema governing ESI metadata/ratelimit/queue keys across languages.

### 2.6 Datastore usage sanity (non-ESI domains)

- Neo4j usage is primarily Python-side graph compute (acceptable).
- InfluxDB usage is mainly operational telemetry/export tooling (acceptable).
- For this remediation scope, Neo4j and InfluxDB remain unchanged except for ESI telemetry extension recommendations.

---

## 3. Redis opportunity map (ESI-only scope)

## 3.1 ESI metadata cache
- **Cache**: `etag`, `last_modified`, `expires_at`, `last_status_code`, `x_pages`, `last_checked_at` per endpoint signature/page.
- **Why safe**: metadata is rebuildable from future ESI responses and non-authoritative for business truth.
- **TTL**: derived from `expires_at` (fallback conservative TTL when missing).
- **Invalidation**: overwrite on response; key-version bump for schema changes.
- **Pattern**: cache-aside metadata + optional short-lived payload cache.

## 3.2 Rate-limit coordination
- **Cache**: per-group/per-identity `remaining`, `limit_window`, `retry_after_deadline`, last observation timestamp.
- **Why safe**: operational coordination only.
- **TTL**: window duration (+ small buffer).
- **Invalidation**: natural expiry + overwrite on each response.
- **Pattern**: ephemeral coordination state.

## 3.3 Request suppression / anti-storm
- **Cache**: temporary suppression keys for known `429` or heavy error conditions.
- **Why safe**: protects upstream and avoids retry storms.
- **TTL**: `Retry-After` bounded.
- **Invalidation**: TTL expiry.
- **Pattern**: ephemeral control key.

## 3.4 Distributed locks for ESI fetch and queue claims
- **Cache**: endpoint fetch lock, queue claim lock, per-job lock.
- **Why safe**: lock state is reconstructable and bounded by TTL.
- **TTL**: short operation-bound TTL with token-safe release.
- **Invalidation**: token release script or expiry.
- **Pattern**: distributed lock.

## 3.5 Negative cache for ESI misses/errors (optional)
- **Cache**: short-lived “not found/unavailable” markers for repeated misses.
- **Why safe**: very short TTL and non-authoritative.
- **TTL**: 15–120s.
- **Pattern**: negative cache.

---

## 4. Risk matrix

| Severity | Issue | Evidence | Consequence |
|---|---|---|---|
| CRITICAL | PHP direct ESI calls | `src/functions.php` ESI callsites | Violates Python-only ESI model; duplicate logic |
| CRITICAL | Paginated snapshot inconsistency risk | PHP paginated market fetch | Partial refresh drift and inconsistent ingestion |
| HIGH | Missing strict Expires + ETag/304 lifecycle | Python+PHP ESI paths | Unnecessary calls; potential cache-circumvention behavior |
| HIGH | No shared ratelimit coordination | Python local limiter + PHP unaware | Bucket exhaustion and 429/420 risk |
| HIGH | Python lacks Redis runtime usage | config-only Redis in Python | Cannot enforce shared queue/rate/metadata coordination |
| MEDIUM | Incomplete durable ESI/rate observability | current schema | Hard postmortem and compliance auditability |

---

## 5. Target architecture

### 5.1 Shared ESI client model

#### Python authoritative `EsiGateway` (new)
Behavior contract:
1. Normalize endpoint key (method, route template, params signature, identity context, page).
2. Read Redis metadata before fetch.
3. If now < Expires: skip outbound call.
4. Else send conditional headers (`If-None-Match`, `If-Modified-Since` when present).
5. Handle response:
   - 2xx: persist headers + payload metadata
   - 304: refresh check timestamps without full payload rewrite
   - 429: honor Retry-After and set suppression key
6. For paginated reads: enforce identical Last-Modified across pages in one retrieval cycle.

#### Payload caching (implemented)
The gateway stores response bodies in both Redis (`esi:payload:v1:{endpoint_key}`)
and MariaDB (`esi_cache_entries` with `namespace_key='esi.payload'`), following
the same three-tier pattern as metadata:

```
_load_payload(endpoint_key):
  1. Redis GET esi:payload:v1:{key}              → HIT? return it
  2. MariaDB SELECT esi_cache_entries             → HIT? return it, repopulate Redis
  3. None                                         → fall through to conditional ESI request
```

On Expires-gate hits with a cached body, the `GatewayResponse` includes the
payload so callers get data without an ESI request. If neither Redis nor MariaDB
has the payload (evicted or first run), the gateway falls through to a
conditional ESI request (304 if unchanged, 200 with fresh data).

For corporation → alliance lookups, `entity_metadata_cache` in MariaDB provides
an additional durable cache tier that survives Redis restarts and is bulk-loaded
at job start for fast in-memory access.

#### PHP role
- No ESI requests.
- Read-only consumer of Redis/MariaDB results prepared by Python.
- For missing freshness, signal queue/refresh intent (no direct upstream calls).

### 5.2 Redis architecture (ESI scope only)

#### Namespaces
- `esi:meta:v1:{endpoint_key}`
- `esi:payload:v1:{endpoint_key}` (optional, short-lived)
- `esi:ratelimit:v1:{group}:{identity}`
- `esi:retry_after:v1:{group}:{identity}`
- `esi:suppress:v1:{endpoint_or_group}`
- `lock:esi_fetch:v1:{endpoint_key}`
- `lock:esi_queue:v1:{queue_scope}`
- `queue:esi:v1:{job_or_scope}` (if Redis-backed queue aids coordination)

#### Serialization
- JSON envelope `{ "v": 1, "ts": "...", "data": ..., "meta": ... }`.
- Cross-language compatible (no PHP serialization; no Python pickle).

#### TTL policy
- Metadata/payload: driven by Expires (fallback conservative floor/ceiling).
- Ratelimit keys: window length + buffer.
- Retry/suppress keys: Retry-After + buffer.
- Locks: operation TTL with safe release.

#### Failure mode
- Redis down:
  - Python continues with local safeguards and durable MariaDB writes, logs degraded mode.
  - PHP falls back to MariaDB read paths only.
  - No durable correctness relies solely on Redis.

### 5.3 Scheduler/worker model (queue/rate/freshness aware)

- ESI tasks scheduled by freshness due-time (Expires), not fixed burst cron.
- Shared lock before endpoint fetch to prevent duplicate concurrent requests.
- Shared ratelimit state consulted before request dispatch.
- Retry-after suppression prevents herd retries.
- Jittered dispatch and “next run after completion” semantics.

---

## 6. MariaDB + Redis data ownership model

| Data domain | Authoritative store | Redis role | Owner process |
|---|---|---|---|
| Durable business/sync data | MariaDB | none/optional read optimization | Python writes, PHP reads |
| ESI metadata audit (durable) | MariaDB | live coordination mirror | Python |
| Live ESI freshness/rate/suppression | Redis | authoritative operational coordination | Python |
| Queue claims/locks | Redis (ephemeral) + MariaDB job records | distributed coordination | Python |
| Graph relationships | Neo4j | none in this phase | Python |
| Time-series telemetry | InfluxDB | none in this phase | Python |

Classification:
- Authoritative: MariaDB business + durable ESI audit.
- Ephemeral operational: Redis ESI metadata mirror/rate/locks/suppression.
- Graph: Neo4j.
- Telemetry: InfluxDB.

---

## 7. Schema changes (durable ESI/rate/queue audit)

### 7.1 `esi_endpoint_state` (durable latest state)
Columns:
- endpoint_key (PK)
- method
- route_template
- param_signature
- identity_context
- page_number
- etag
- last_modified
- expires_at
- last_checked_at
- last_success_at
- last_status_code
- not_modified_count
- success_count
- error_count
- inconsistency_flag

### 7.2 `esi_rate_limit_observations`
Columns:
- id
- observed_at
- ratelimit_group
- identity_context
- x_ratelimit_limit
- x_ratelimit_remaining
- x_ratelimit_used
- retry_after_seconds
- status_code

### 7.3 `esi_pagination_consistency_events`
Columns:
- id
- endpoint_key
- retrieval_window_id
- expected_last_modified
- inconsistent_page_numbers_json
- detected_at
- resolution_state

### 7.4 Optional `esi_queue_claim_events`
Columns:
- id
- queue_scope
- claimer_id
- claimed_at
- released_at
- outcome

Redis-only (non-durable):
- locks, suppression keys, transient ratelimit state mirrors.

---

## 8. Implementation roadmap

### Phase 1 — quick, safe ESI controls
1. Add Python Redis runtime module and shared ESI key helpers.
2. Add `EsiGateway` wrapper around current `EsiClient` for Expires/ETag/304 lifecycle.
3. Add shared Redis ratelimit + retry-after state updates.
4. Add endpoint-level distributed fetch lock + queue claim lock.
5. Add structured ESI compliance telemetry logs/events.

Target files:
- add: `python/orchestrator/redis_client.py`
- add: `python/orchestrator/redis_keys.py`
- add: `python/orchestrator/esi_gateway.py`
- update: `python/orchestrator/esi_client.py`
- update: `python/orchestrator/esi_rate_limiter.py`
- update: ESI jobs/adapters to use gateway

### Phase 2 — remove PHP ESI execution
1. Decommission PHP direct ESI callsites in `src/functions.php`.
2. Route PHP to Python-produced data only (Redis first, MariaDB fallback).
3. Add schema migrations for durable ESI endpoint/rate/pagination audit tables.

### Phase 3 — queue/rate hardening
1. Freshness-driven scheduling (Expires-aware next due).
2. Group/identity-aware dispatch pacing from shared Redis state.
3. Retry storm protection policies and circuit suppression.

### Phase 4 — cleanup
1. Remove duplicate PHP HTTP/ESI helpers no longer needed.
2. Consolidate docs/runbooks around Python-only ESI model.

Rollback strategy:
- Feature flags for gateway enablement, Redis coordination, and suppression behavior.

---

## 9. Validation and test plan

### A. ESI correctness
- Verify no request occurs before Expires.
- Verify If-None-Match emitted when ETag exists.
- Verify 304 path updates state and does not treat as failure.
- Verify Last-Modified persisted and compared for paginated resources.
- Verify inconsistency event logged when page headers mismatch.

### B. Rate-limit + queue coordination
- Validate per-group/per-identity Redis state updates.
- Validate Retry-After suppression blocks immediate retries.
- Validate concurrent workers do not double-fetch same endpoint while lock held.
- Validate queue claims are unique and safely released/expired.

### C. Redis resilience
- Validate degraded behavior when Redis unavailable (no correctness loss).
- Validate key TTL behavior for metadata, ratelimit, suppression, and locks.
- Validate cross-language JSON envelope compatibility.

### D. Observability
Track and alert on:
- request count by status class
- 304 ratio
- skipped-due-to-expires count
- 429/420 incidents
- retry-after suppressions
- lock contention rate
- queue claim collisions

---

## 10. Implementation status

All four phases have been implemented:

### Phase 1 — Python ESI gateway with Redis coordination ✓
- `redis_client.py`, `redis_keys.py` — Redis client with graceful degradation
- `esi_gateway.py` — Expires-gating, ETag/304, paginated Last-Modified consistency, distributed fetch locks
- `esi_rate_limiter.py` — Cross-process Redis-backed rate-limit coordination
- `esi_endpoint_state`, `esi_rate_limit_observations`, `esi_pagination_consistency_events` audit tables
- All ESI jobs updated to use gateway when Redis is enabled

### Phase 2 — PHP ESI call removal ✓
- `esi_entity_resolver.py` — Native Python entity resolution (replaces PHP bridge ESI calls)
- `EsiClient.post()` / `EsiGateway.post()` — POST support for /universe/names/, /universe/ids/
- 8 PHP ESI functions replaced with cache reads + async queue
- NPC station lookups use local `ref_npc_stations` table
- ~590 lines of PHP ESI market sync code removed

### Phase 3 — Queue/rate hardening ✓
- `scheduler_pressure.py` — Pressure state + ESI freshness calculations
- Freshness-driven scheduling (skip jobs when ESI data still fresh)
- Lock-group enforcement at claim time (SQL WHERE clause)
- Exponential retry backoff + circuit breaker for repeated failures

### Phase 4 — Cleanup ✓
- Removed dead PHP functions: `canonicalize_esi_market_order`, `esi_market_request_headers`,
  `sync_alliance_structure_orders`, `sync_market_hub_current_orders`,
  `market_order_page_canonical_rows`, `killmail_entity_public_endpoint`

### Accepted PHP ESI exceptions (permanent)
Three authenticated ESI callsites remain in PHP because they require synchronous
user feedback with OAuth tokens for structure docking rights verification:
- `esi_alliance_structure_metadata()` — first-time structure setup
- `esi_structure_search()` — settings UI structure search (docking rights)
- `esi_alliance_and_corporation_search()` — settings UI entity autocomplete
