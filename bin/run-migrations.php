#!/usr/bin/env php
<?php

declare(strict_types=1);

/**
 * Database migration runner.
 *
 * Reads SQL files from database/migrations/, tracks applied state in the
 * schema_migrations table, and applies any new or updated files.
 *
 * Usage:
 *   php bin/run-migrations.php              # Run all pending migrations
 *   php bin/run-migrations.php --dry-run    # Show what would run
 *   php bin/run-migrations.php --status     # Show migration status
 */

// Load functions directly — skip bootstrap.php which auto-runs migrations
// and starts a session (neither needed in CLI context).
require_once __DIR__ . '/../src/functions.php';

$dryRun = in_array('--dry-run', $argv, true);
$statusOnly = in_array('--status', $argv, true);

$result = supplycore_run_migrations($dryRun, $statusOnly);

if ($statusOnly) {
    $applied = (array) ($result['applied'] ?? []);
    $pending = (array) ($result['pending'] ?? []);

    fwrite(STDOUT, sprintf("Applied: %d  Pending: %d\n", count($applied), count($pending)));
    foreach ($pending as $file) {
        fwrite(STDOUT, sprintf("  PENDING  %s\n", $file));
    }
    foreach ($applied as $file => $info) {
        fwrite(STDOUT, sprintf("  APPLIED  %s  (at %s)\n", $file, (string) ($info['applied_at'] ?? 'unknown')));
    }
    exit(0);
}

$ran = (int) ($result['migrations_run'] ?? 0);
$errors = (array) ($result['errors'] ?? []);

if (count($errors) > 0) {
    fwrite(STDERR, sprintf("Migration completed with %d error(s):\n", count($errors)));
    foreach ($errors as $error) {
        fwrite(STDERR, sprintf("  [%s] %s\n", (string) ($error['file'] ?? '?'), (string) ($error['message'] ?? 'Unknown error')));
    }
    exit(1);
}

fwrite(STDOUT, sprintf("Migrations complete: %d applied.\n", $ran));
