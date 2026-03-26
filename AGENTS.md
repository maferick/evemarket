# AGENTS.md

## Mission

Maintain a **modular, scalable, production-grade data platform** for EveMarket / SupplyCore.

Prioritize:
- correctness over shortcuts
- consistency over one-off fixes
- scalability over single-run success

---

## System Architecture (Authoritative)

The system is a **dual-runtime platform**:

### Control Plane (PHP)
Responsible for:
- UI (dashboard, settings, navigation)
- configuration and metadata
- job definitions (metadata only)
- user interaction

### Execution Plane (Python)
Responsible for:
- all compute jobs
- ingestion pipelines (zkill, ESI, EveWho, etc.)
- analytics and scoring
- graph processing (Neo4j)
- batching and data pipelines

### Hard Rules

- PHP must **never execute compute workloads**
- Python is the **only runtime for background processing**
- Scheduler definitions in PHP are **metadata-only**
- No logic duplication between PHP and Python

---

## Job Execution Contract (Non-Negotiable)

All compute jobs must follow:

- `execution_mode = 'python'`
- `execution_language = 'python'`
- Must have a registered Python processor (worker pool)
- Must NOT have a PHP handler

### Forbidden

- PHP fallback execution
- PHP bridges for compute jobs
- Scheduler-only execution paths
- “temporary” dual implementations

### Runtime Parity Requirement

Every job must run identically via:

- worker pool (primary)
- scheduler dispatch
- manual CLI execution

If behavior differs → implementation is invalid

---

## Job Registry (Authoritative Inventory)

- `supplycore_authoritative_job_registry()` in `src/functions.php` is the **single source of truth**
- Do not infer jobs from:
  - database tables
  - scheduler state
  - file scanning

### Rules

- All jobs must be defined in the registry
- Internal/helper jobs must not appear as user-manageable jobs
- Job keys must be stable and unique

---

## Database Architecture

### Authority

- `database/schema.sql` is the **only source of truth**

### Rules

- No code may assume columns not defined in schema.sql
- All INSERT/UPDATE statements must match schema exactly
- No implicit schema evolution in code

### Required Workflow

If a job needs new fields:

1. Update `schema.sql`
2. Apply migration
3. Update Python job
4. Validate queries

### Forbidden

- “Unknown column” runtime fixes
- Hardcoding fields not in schema
- Silent schema drift

---

## Database Access

### PHP

- All DB logic must live in:
  - `src/db.php`
- Reusable query helpers must be centralized
- No inline SQL scattered across pages

### Python

- DB access must be:
  - consistent
  - reusable
  - abstracted (no ad-hoc connection logic per job)

---

## Batch Processing (Mandatory)

All compute jobs must be **batch-based**.

### Requirements

- Process data in bounded chunks
- Use:
  - cursor-based iteration OR
  - time-window batching
- Support resumability (checkpointing)

### Each job must:

- define batch size
- track progress (cursor / timestamp / ID)
- be safely retryable

### Forbidden

- full-table scans in one run
- “process everything” jobs
- unbounded memory usage

---

## Graph Processing (Neo4j)

### Constraints

- All graph jobs must be **incremental and batched**
- Transactions must stay within memory limits

### Query Rules

- Do NOT use deprecated patterns:
  - `size((n)--())` ❌
- Use:
  - `COUNT { (n)--() }` ✅

### Design Requirements

- support partial rebuilds
- avoid large fan-out queries
- avoid full graph traversals per run

---

## External Data Integration

All external APIs must go through **adapter layers**.

### Examples

- zKill → `python/orchestrator/zkill_adapter.py`
- EveWho → `python/orchestrator/evewho_adapter.py`

### Adapter Responsibilities

- rate limiting
- retries
- normalization
- error handling

### Forbidden

- direct API calls inside compute jobs
- mixing API logic with business logic

---

## Observability & Logging

All jobs must emit **structured logs**.

### Required Fields

- job_key
- batch size
- rows processed
- duration
- outcome (success/failure)
- error details (if any)

### Rules

- logs must be actionable
- logs must allow debugging without DB access
- silent failures are not allowed

---

## Settings Architecture

- Settings must be **modular and section-based**
- No monolithic settings pages
- Each feature must define its own settings group

### UI Rules

- Settings must:
  - be enable/disable aware
  - include clear descriptions
  - reflect actual runtime behavior

---

## Navigation

- Navigation must be defined centrally via:
  - `nav_items()` in `src/functions.php`
- No duplicated or hardcoded menus

---

## Code Organization

### PHP

- DB → `src/db.php`
- shared logic → `src/functions.php`
- pages → `public/` (lean, no heavy logic)

### Python

- jobs → `python/orchestrator/`
- adapters → dedicated adapter modules
- shared utilities → reusable helpers (no duplication)

---

## UI / Design

- Clean, minimal, **shadcn/ui-inspired**
- Tailwind utility-first approach
- Avoid visual inconsistency or custom one-offs

---

## Change Workflow (Required)

1. Update schema (if needed)
2. Update DB layer (if needed)
3. Implement Python job / logic
4. Register job in authoritative registry
5. Validate batch behavior
6. Validate runtime parity
7. Update UI/settings if needed
8. Update README if behavior changes

---

## Quality Standards

- Prefer small, composable functions
- Avoid hidden side effects
- Validate all inputs
- Maintain CSRF protections
- Use PHP 8+ strict typing
- Keep Python logic explicit and testable

---

## Anti-Patterns (Strictly Forbidden)

- PHP executing compute jobs
- jobs without batching
- schema assumptions not in schema.sql
- direct API calls inside jobs
- duplicated logic across runtimes
- scheduler-only execution hacks
- memory-heavy graph queries
- “quick fixes” that bypass architecture

---

## Guiding Principle

> This is not a PHP application with background scripts.  
> This is a **data platform with a PHP control plane and a Python execution engine**.

All contributions must reinforce that model.
