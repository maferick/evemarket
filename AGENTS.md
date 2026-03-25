# AGENTS.md

Guidance for coding agents working in this repository.

## Mission

Preserve a modular, production-oriented PHP architecture for EveMarket. Favor maintainability and consistency over quick one-off solutions.

## Core Rules

1. **Do not scatter DB code.**
   - All database connection/query behavior belongs in `src/db.php`.
   - If new query primitives are needed, add reusable wrappers there.

2. **Do not scatter helper/business logic.**
   - Shared helper functions and app-level reusable logic belong in `src/functions.php`.
   - Keep controllers/pages lean by calling centralized helpers.

3. **Keep settings modular.**
   - Settings are section-based and expandable.
   - New settings modules must follow existing section architecture (not a flat, monolithic page).

4. **Keep navigation centralized.**
   - Main and nested submenu definitions belong in `nav_items()` in `src/functions.php`.
   - Avoid hardcoding duplicate menu structures across pages.

5. **Respect deployment target.**
   - Stack assumptions: PHP + MySQL + Apache2.
   - Keep rewrite/routing conventions compatible with Apache `mod_rewrite`.

6. **Design consistency.**
   - UI should stay clean, minimal, and shadcn/ui-inspired.
   - Use Tailwind utility patterns consistently.

7. **Worker mode consistency.**
   - Compute workers must use the standard Python worker-pool units.
   - Do not reintroduce dedicated `supplycore-php-compute-worker*` units.
   - Keep compute workers Python-only and route non-native/fallback jobs to sync workers.

8. **Python recurring job runtime parity (anti-regression).**
   - Python jobs must be implemented as Python-native processors (no PHP bridge or PHP subprocess fallback for compute jobs).
   - If `execution_mode='python'`, runtime execution must stay in Python with `execution_language='python'` and `subprocess_invoked=false`.
   - Do not add compute jobs to `PHP_BRIDGED_JOB_KEYS` or any equivalent bridge allowlist.
   - Every Python recurring job must run through all intended launcher paths with one logic path:
     - worker pool (`python/orchestrator/worker_pool.py`)
     - scheduler-dispatched Python runtime (`python/orchestrator/job_runner.py`)
     - manual Python CLI (`python -m orchestrator ...`)
   - Do not add scheduler-runtime-only guards unless absolutely required. If unavoidable, document the reason and provide a Python-native context adapter for non-scheduler launchers.
   - Normalize runtime dependencies (config, DB access, logger/log sinks, timestamps/metadata) through reusable Python context helpers instead of launcher-specific assumptions.
   - New or changed Python jobs are not complete until parity is validated across worker, scheduler-dispatched, and manual CLI execution paths.
   - Never “fix” Python job execution failures by routing the job through PHP when the target architecture is Python-only.
   - Runtime safety audit is mandatory: enabled `execution_mode='python'` compute jobs must have a worker-pool processor binding and must not depend on scheduler-only PHP handlers.
   - Python jobs must never be represented in `scheduler_job_definitions()` as PHP executable handler closures that throw "must run in the Python scheduler runtime"; keep them metadata-only in PHP and executable only via Python runtimes.

9. **Authoritative job inventory (required).**
   - Treat `supplycore_authoritative_job_registry()` in `src/functions.php` as the canonical job inventory.
   - Do not infer inventory from database contents or ad-hoc code scanning.
   - Internal/helper entries are never normal user-manageable jobs.
   - External integrations (for example zKill) must be normalized through explicit adapter boundaries (for example `python/orchestrator/zkill_adapter.py`) instead of blind rewrites.

## Preferred Workflow for Changes

1. Update schema (`database/schema.sql`) if persistence needs change.
2. Add/adjust database wrappers in `src/db.php` if needed.
3. Add/adjust shared logic in `src/functions.php`.
4. Update pages in `public/` using shared helpers only.
5. Update README if structure, setup, or workflows change.

## Quality Expectations

- Favor small, composable functions.
- Validate user input and preserve CSRF protections for form submissions.
- Avoid introducing framework-level complexity unless requested.
- Keep code style straightforward PHP 8+ with strict types.
