# Batching-by-Default for Compute/Graph/Sync Jobs

## Rule

For compute, graph, and sync workloads, the default implementation strategy is **batched + resumable**. Giant all-at-once passes are disallowed unless the data size is permanently tiny and explicitly documented.

## Required design traits

1. Batch by one or more bounded dimensions:
   - primary key range
   - timestamp window
   - sequence/cursor
   - limited relationship/anchor set
   - fixed chunk/page size
2. Persist checkpoint/cursor state in durable storage so work resumes safely after timeout/restart/failure.
3. Keep each batch runtime and memory bounded and predictable.
4. Commit writes incrementally (transaction per batch or safe grouped batches).
5. Make batch size configurable.
6. Emit per-batch telemetry:
   - `batch_start`
   - `batch_end`
   - `rows_processed`
   - `rows_written`
   - `duration_ms`
   - `checkpoint_after`
   - `errors`

## Database and graph specifics

### MariaDB

- Prefer chunked inserts/upserts.
- Avoid reading all rows into memory before writing.
- Avoid giant write transactions.

### Neo4j

- Avoid unbounded full-graph sweeps in one pass.
- Use bounded anchor sets/subqueries.
- Checkpoint anchor cursor/progress.

## Timeouts

If a job times out, redesign into batches first. Increase timeout only after batching is in place and still insufficient.

## Scope guidance

Apply this to all multi-day/high-volume jobs, including graph sync/derived/insights and battle intelligence compute pipelines.
