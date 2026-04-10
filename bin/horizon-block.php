<?php

declare(strict_types=1);

/**
 * bin/horizon-block <dataset_key>
 *
 * Opt a dataset out of the detect_backfill_complete auto-approval loop.
 * Sets sync_state.auto_approve_blocked = 1.
 *
 * When blocked, detect_backfill_complete will still *propose* the
 * dataset for backfill_complete (so it shows up in the admin review
 * queue) but will never auto-flip the gate regardless of how long the
 * proposal has been soaking. A human must run bin/horizon-approve.php
 * to flip the gate, or bin/horizon-unblock.php to re-enable
 * auto-approval.
 *
 * Use this for datasets that need a human eye on the cutover -- e.g.
 * a new compute job whose correctness hasn't been validated against a
 * shadow run yet.
 */

require_once __DIR__ . '/../src/bootstrap.php';

$datasetKey = trim((string) ($argv[1] ?? ''));
if ($datasetKey === '') {
    fwrite(STDERR, "Usage: php bin/horizon-block.php <dataset_key>\n");
    exit(2);
}

$state = db_horizon_state_get($datasetKey);
if ($state === null) {
    fwrite(STDERR, sprintf("No sync_state row for dataset '%s'.\n", $datasetKey));
    exit(1);
}

if (!empty($state['auto_approve_blocked'])) {
    fwrite(
        STDOUT,
        sprintf("Dataset '%s' already has auto_approve_blocked = 1. Nothing to do.\n", $datasetKey)
    );
    exit(0);
}

$ok = db_horizon_block_auto_approve($datasetKey);
if (!$ok) {
    fwrite(STDERR, sprintf("Failed to block auto-approval for dataset '%s'.\n", $datasetKey));
    exit(1);
}

fwrite(
    STDOUT,
    sprintf(
        "Blocked auto-approval for dataset '%s'. detect_backfill_complete will still propose but never auto-approve.\n",
        $datasetKey
    )
);
exit(0);
