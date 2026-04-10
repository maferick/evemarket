<?php

declare(strict_types=1);

/**
 * bin/horizon-rewind <dataset_key> <cursor>
 *
 * Manually rewind a dataset's forward cursor. Used when out-of-window
 * late data lands (older than repair_window_seconds) and we need to
 * re-process a range of source events from further back than the
 * rolling repair window covers.
 *
 * Bypasses the monotonic guard in db_horizon_advance_cursor() and
 * resets stall tracking. The next refresh run will read from the given
 * cursor (plus any rolling-window offset).
 *
 * The cursor must be in the usual "timestamp|id" format, e.g.
 *   bin/horizon-rewind.php compute_battle_rollups "2026-04-01 00:00:00|0"
 */

require_once __DIR__ . '/../src/bootstrap.php';

$datasetKey = trim((string) ($argv[1] ?? ''));
$cursor = trim((string) ($argv[2] ?? ''));

if ($datasetKey === '' || $cursor === '') {
    fwrite(STDERR, "Usage: php bin/horizon-rewind.php <dataset_key> <cursor>\n");
    fwrite(STDERR, "  cursor format: \"YYYY-MM-DD HH:MM:SS|id\"\n");
    exit(2);
}

$parsed = db_time_series_parse_cursor($cursor);
if (($parsed['timestamp'] ?? '') === '') {
    fwrite(STDERR, sprintf("Invalid cursor '%s'. Expected 'YYYY-MM-DD HH:MM:SS|id'.\n", $cursor));
    exit(2);
}

$state = db_horizon_state_get($datasetKey);
if ($state === null) {
    fwrite(STDERR, sprintf("No sync_state row for dataset '%s'.\n", $datasetKey));
    exit(1);
}

$previousCursor = (string) ($state['last_cursor'] ?? '');

$ok = db_horizon_rewind_cursor($datasetKey, $cursor);
if (!$ok) {
    fwrite(STDERR, sprintf("Failed to rewind dataset '%s'.\n", $datasetKey));
    exit(1);
}

fwrite(
    STDOUT,
    sprintf(
        "Rewound dataset '%s' from '%s' to '%s'. Stall tracking cleared.\n",
        $datasetKey,
        $previousCursor,
        $cursor
    )
);
exit(0);
