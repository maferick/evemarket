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
