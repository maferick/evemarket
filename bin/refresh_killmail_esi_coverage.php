#!/usr/bin/env php
<?php

declare(strict_types=1);

// Materialize the killmail-intelligence ESI coverage snapshot into page_cache.
//
// db_killmail_esi_coverage_snapshot() runs several UNION queries over
// killmail_events + killmail_attackers that take ~2 minutes against a multi-
// million-row dataset. Running it inline on every page request made
// /killmail-intelligence time out, so the overview page now reads a cached
// snapshot from the page_cache table. This script computes the snapshot and
// writes it to the cache.
//
// Suggested schedule: every 5 minutes via system cron or equivalent.
//   */5 * * * * /usr/bin/php /path/to/SupplyCore/bin/refresh_killmail_esi_coverage.php >> /var/log/supplycore/esi_coverage.log 2>&1
//
// Exit codes:
//   0  success
//   1  refresh failed (snapshot computed returned no participants)
//   2  unhandled exception

require_once __DIR__ . '/../src/bootstrap.php';

$startDate = '2024-01-01';
$startedAt = microtime(true);

try {
    $snapshot = killmail_esi_coverage_cache_refresh($startDate);
} catch (Throwable $exception) {
    fwrite(STDERR, json_encode([
        'event' => 'killmail_esi_coverage.refresh.error',
        'ts' => gmdate(DATE_ATOM),
        'start_date' => $startDate,
        'error' => $exception->getMessage(),
        'file' => $exception->getFile(),
        'line' => $exception->getLine(),
    ], JSON_UNESCAPED_SLASHES) . PHP_EOL);
    exit(2);
}

$durationMs = (int) round((microtime(true) - $startedAt) * 1000);

fwrite(STDOUT, json_encode([
    'event' => 'killmail_esi_coverage.refresh.ok',
    'ts' => gmdate(DATE_ATOM),
    'start_date' => $startDate,
    'duration_ms' => $durationMs,
    'participant_characters' => (int) ($snapshot['participant_characters'] ?? 0),
    'with_current_affiliation' => (int) ($snapshot['with_current_affiliation'] ?? 0),
    'history_refresh_completed' => (int) ($snapshot['history_refresh_completed'] ?? 0),
    'enrolled_for_killmail_backfill' => (int) ($snapshot['enrolled_for_killmail_backfill'] ?? 0),
    'killmail_backfill_done' => (int) ($snapshot['killmail_backfill_done'] ?? 0),
], JSON_UNESCAPED_SLASHES) . PHP_EOL);

exit(0);
