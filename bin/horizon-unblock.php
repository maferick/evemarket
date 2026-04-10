<?php

declare(strict_types=1);

/**
 * bin/horizon-unblock <dataset_key>
 *
 * Clear a previously-set auto-approval block. Sets
 * sync_state.auto_approve_blocked = 0.
 *
 * The dataset becomes eligible for auto-approval again on the next
 * detect_backfill_complete run (subject to the usual soak and health
 * criteria -- see python/orchestrator/jobs/detect_backfill_complete.py
 * for the full timeline).
 */

require_once __DIR__ . '/../src/bootstrap.php';

$datasetKey = trim((string) ($argv[1] ?? ''));
if ($datasetKey === '') {
    fwrite(STDERR, "Usage: php bin/horizon-unblock.php <dataset_key>\n");
    exit(2);
}

$state = db_horizon_state_get($datasetKey);
if ($state === null) {
    fwrite(STDERR, sprintf("No sync_state row for dataset '%s'.\n", $datasetKey));
    exit(1);
}

if (empty($state['auto_approve_blocked'])) {
    fwrite(
        STDOUT,
        sprintf("Dataset '%s' already has auto_approve_blocked = 0. Nothing to do.\n", $datasetKey)
    );
    exit(0);
}

$ok = db_horizon_unblock_auto_approve($datasetKey);
if (!$ok) {
    fwrite(STDERR, sprintf("Failed to unblock auto-approval for dataset '%s'.\n", $datasetKey));
    exit(1);
}

fwrite(
    STDOUT,
    sprintf(
        "Unblocked auto-approval for dataset '%s'. Next detect_backfill_complete run may auto-approve.\n",
        $datasetKey
    )
);
exit(0);
