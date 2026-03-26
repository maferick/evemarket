<?php

declare(strict_types=1);

session_start();

require_once __DIR__ . '/functions.php';

// Auto-apply pending database migrations on every page load.
// The check is cheap (single SELECT on schema_migrations) and only runs
// actual SQL when new or updated migration files are detected.
try {
    supplycore_auto_run_migrations_if_pending();
} catch (Throwable) {
    // Silently ignore — migrations will be retried on next request.
    // The settings page shows migration status for manual intervention.
}
