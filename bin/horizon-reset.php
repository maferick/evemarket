<?php

declare(strict_types=1);

/**
 * bin/horizon-reset <dataset_key>
 *
 * Revert a dataset from incremental-only horizon mode back to its
 * original full/backfill + incremental behavior. Flips
 * sync_state.backfill_complete to 0 and clears any pending proposal.
 *
 * Use this as the rollback procedure when a horizon-mode pilot shows
 * drift or correctness issues. The next refresh run will resume the
 * pre-horizon path.
 */

require_once __DIR__ . '/../src/bootstrap.php';

$datasetKey = trim((string) ($argv[1] ?? ''));
if ($datasetKey === '') {
    fwrite(STDERR, "Usage: php bin/horizon-reset.php <dataset_key>\n");
    exit(2);
}

$state = db_horizon_state_get($datasetKey);
if ($state === null) {
    fwrite(STDERR, sprintf("No sync_state row for dataset '%s'.\n", $datasetKey));
    exit(1);
}

$ok = db_horizon_reset_backfill_complete($datasetKey);
if (!$ok) {
    fwrite(STDERR, sprintf("Failed to reset dataset '%s'.\n", $datasetKey));
    exit(1);
}

fwrite(
    STDOUT,
    sprintf(
        "Reset dataset '%s'. backfill_complete = 0. Next run will resume full/backfill + incremental mode.\n",
        $datasetKey
    )
);
exit(0);
