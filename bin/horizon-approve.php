<?php

declare(strict_types=1);

/**
 * bin/horizon-approve <dataset_key>
 *
 * Approve a dataset for incremental-only horizon mode. Flips
 * sync_state.backfill_complete to 1 and clears any pending proposal.
 *
 * After approval, the next refresh run for this dataset is allowed to
 * switch to the rolling-repair-window read pattern. Revert with
 * bin/horizon-reset.php.
 */

require_once __DIR__ . '/../src/bootstrap.php';

$datasetKey = trim((string) ($argv[1] ?? ''));
if ($datasetKey === '') {
    fwrite(STDERR, "Usage: php bin/horizon-approve.php <dataset_key>\n");
    exit(2);
}

$state = db_horizon_state_get($datasetKey);
if ($state === null) {
    fwrite(STDERR, sprintf("No sync_state row for dataset '%s'.\n", $datasetKey));
    exit(1);
}

if ((int) ($state['backfill_complete'] ?? 0) === 1) {
    fwrite(STDOUT, sprintf("Dataset '%s' already has backfill_complete = 1. Nothing to do.\n", $datasetKey));
    exit(0);
}

$ok = db_horizon_approve_backfill_complete($datasetKey);
if (!$ok) {
    fwrite(STDERR, sprintf("Failed to approve dataset '%s'.\n", $datasetKey));
    exit(1);
}

fwrite(
    STDOUT,
    sprintf(
        "Approved dataset '%s'. backfill_complete = 1. Next run may use incremental-only mode.\n",
        $datasetKey
    )
);
exit(0);
