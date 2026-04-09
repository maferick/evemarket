# Airflow DAG Adoption Assessment (SupplyCore)

## Executive Summary

Yes — we can adopt Apache Airflow as the DAG orchestrator for SupplyCore **if we treat Airflow as an orchestration layer, not as a replacement runtime**.

The current architecture already has strong DAG semantics (dependencies, lanes, priorities, retries), but they are implemented in our Python loop runner + worker registry. Airflow can absorb that orchestration responsibility while preserving our hard rule that PHP remains metadata/control-plane and Python performs all compute execution.

## What We Have Today (Baseline)

SupplyCore already models recurring jobs as a dependency graph with execution tiers, lane isolation, and concurrency controls:

- Job dependency edges are stored in Python worker definitions (`depends_on`) and resolved into topological tiers via `scheduling_graph.py`.
- Lane-aware execution (`realtime`, `ingestion`, `compute`, `maintenance`) is enforced in `loop_runner.py`.
- The PHP authoritative registry (`supplycore_authoritative_job_registry()`) remains the source of truth for schedulable job metadata and execution mode.
- Worker definitions in Python capture resource controls (timeouts, memory hints, retry delays, concurrency groups).

In other words, we already have a DAG scheduler design; Airflow would be a control-plane upgrade for scheduling and observability rather than a greenfield workflow system.

## Fit Analysis: Airflow vs SupplyCore Requirements

## 1) Architectural compatibility

**Compatible** if we keep this boundary:

- PHP stays metadata-only (job catalog, settings, UI).
- Python stays execution runtime (actual compute code in `python/orchestrator/jobs/*`).
- Airflow only triggers Python job processors (e.g., one Airflow task per `job_key`).

This matches SupplyCore’s dual-runtime rules and avoids reintroducing PHP compute behavior.

## 2) DAG semantics parity

Airflow can represent our current DAG primitives:

- `depends_on` -> Airflow task dependencies.
- `concurrency_group` -> Airflow pools (or serialized task groups).
- lane separation -> separate DAGs per lane, or lane-tagged task groups.
- retries/timeouts -> native Airflow task settings.

## 3) Runtime parity requirement (worker/scheduler/manual)

This is the hardest requirement and must be designed explicitly.

To preserve parity, Airflow tasks should invoke the same entrypoint used by manual CLI (`python -m orchestrator run-job --job-key ...`) so behavior remains identical.

## 4) Operational compatibility

Airflow introduces additional services and operational burden:

- scheduler
- triggerer (if using deferrable operators)
- webserver/api
- executor backend (Celery/Kubernetes/Local)
- metadata DB + broker (executor-dependent)

This is manageable but is a non-trivial platform expansion.

## Primary Gaps / Risks

## A) Metadata DB split and MariaDB posture

SupplyCore currently centers on MariaDB for platform state. Airflow has its own metadata DB constraints and operational expectations.

Recommendation: keep Airflow metadata in a dedicated Postgres/MySQL instance and leave SupplyCore application data in existing MariaDB. Do **not** merge Airflow metadata tables into `database/schema.sql`.

## B) Two sources of scheduling truth

Today, scheduling cadence/state is in SupplyCore tables (`sync_schedules`, etc.). If Airflow is introduced, schedule ownership must be explicit.

Preferred model:

- Airflow owns timing + DAG dependency orchestration.
- SupplyCore keeps job definitions and business metadata.
- SupplyCore tables continue to store job run telemetry/results for product UI.

## C) Duplicate orchestration risk

Running both loop-runner and Airflow concurrently without strict partitioning will cause duplicate job execution.

Migration must include a **single scheduler authority switch** per lane/job set.

## D) Adapter and batching guardrails

Airflow does not remove current job constraints:

- all jobs stay batched and resumable,
- external APIs still go through adapter modules,
- graph workloads remain incremental.

Airflow should orchestrate these jobs, not rewrite their internals.

## Recommended Target Architecture

## Control plane

- PHP remains the authoritative job registry/UI.
- Add a small export path that emits Airflow DAG config from registry + worker metadata.

## Orchestration plane

- Airflow DAG files generated from SupplyCore registry (initially one DAG per lane).
- Tasks call `python -m orchestrator run-job --job-key <key>` (or equivalent stable wrapper).
- Airflow pools map to existing concurrency groups.

## Execution plane

- Existing Python job processors unchanged.
- Existing adapter boundaries unchanged.
- Existing result contract (`status`, `rows_processed`, `summary`, etc.) unchanged.

## What It Would Take (Phased Plan)

## Phase 0 — Discovery/contract freeze (2–4 days)

- Freeze and document canonical job contract (inputs, outputs, exit codes).
- Finalize mapping table: `job_key` -> `dag_id/task_id`, lane, pool, retries, timeout.
- Decide scheduler authority strategy (full cutover vs lane-by-lane).

## Phase 1 — Airflow foundation (3–6 days)

- Deploy Airflow stack (likely Docker Compose or K8s) with dedicated metadata DB.
- Configure executor (CeleryExecutor recommended for parity with distributed worker behavior).
- Set up secrets/env for SupplyCore DB + service credentials.

## Phase 2 — DAG bridge MVP (4–7 days)

- Implement generated DAG module(s) from SupplyCore registry/worker definitions.
- Implement a hardened `run_job(job_key)` wrapper operator that:
  - executes existing orchestrator CLI,
  - captures structured result payload,
  - pushes telemetry back to SupplyCore tables/log stream.
- Implement Airflow pools for current concurrency groups.

## Phase 3 — Shadow mode validation (4–10 days)

- Run Airflow in dry-run/shadow mode on a subset of lanes (no writes or isolated writes).
- Validate runtime parity against current loop-runner outputs.
- Validate failure/retry semantics and idempotency under forced restarts.

## Phase 4 — Incremental cutover (3–8 days)

- Cut over one lane at a time (maintenance -> ingestion -> compute -> realtime).
- Disable loop-runner scheduling for migrated lane(s).
- Keep manual CLI path as break-glass.

## Phase 5 — UX + observability alignment (2–5 days)

- Link SupplyCore job pages to Airflow run IDs/log URLs.
- Keep product-facing status in SupplyCore UI backed by existing tables.
- Add runbook updates for on-call and rollback.

## Estimated Total Effort

Roughly **3–6 weeks** for a production-grade migration with parity validation, depending on infra readiness and how much DAG generation automation we build.

## Minimal Viable Path (if we want low risk)

1. Start with **maintenance lane only** in Airflow.
2. Keep existing Python job code unchanged.
3. Use generated DAG from current registry metadata.
4. Require parity sign-off before each additional lane.

This gives us a reversible path with clear blast-radius control.

## Go / No-Go Recommendation

**Go, with guardrails.**

Adopt Airflow only as orchestration infrastructure. Keep SupplyCore’s current control-plane/runtime split and existing Python processors as the execution contract. Do not attempt a big-bang rewrite.

