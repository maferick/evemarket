<?php

declare(strict_types=1);

/**
 * bin/horizon-reject <dataset_key>
 *
 * Clear a pending backfill_complete proposal without approving it. Used
 * when the detector proposes a dataset that an admin has decided is not
 * actually ready for incremental-only mode.
 *
 * Does NOT change backfill_complete itself — safe to run on any dataset.
 */

require_once __DIR__ . '/../src/bootstrap.php';

$datasetKey = trim((string) ($argv[1] ?? ''));
if ($datasetKey === '') {
    fwrite(STDERR, "Usage: php bin/horizon-reject.php <dataset_key>\n");
    exit(2);
}

$state = db_horizon_state_get($datasetKey);
if ($state === null) {
    fwrite(STDERR, sprintf("No sync_state row for dataset '%s'.\n", $datasetKey));
    exit(1);
}

if (($state['backfill_proposed_at'] ?? null) === null) {
    fwrite(STDOUT, sprintf("Dataset '%s' has no pending proposal. Nothing to do.\n", $datasetKey));
    exit(0);
}

$ok = db_horizon_reject_backfill_complete($datasetKey);
if (!$ok) {
    fwrite(STDERR, sprintf("Failed to reject dataset '%s'.\n", $datasetKey));
    exit(1);
}

fwrite(
    STDOUT,
    sprintf("Rejected pending proposal for dataset '%s'. backfill_complete left unchanged.\n", $datasetKey)
);
exit(0);
