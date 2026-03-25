<?php

declare(strict_types=1);

require_once __DIR__ . '/../src/bootstrap.php';

$filters = [
    'page' => 1,
    'page_size' => 250,
    'search' => '',
    'alliance_id' => 0,
    'corporation_id' => 0,
    'tracked_only' => false,
];

$storageDuplicates = db_killmail_duplicate_identities(100);
$resultDuplicates = db_killmail_overview_duplicate_identity_check($filters);

echo "Killmail storage duplicate identity count: " . count($storageDuplicates) . PHP_EOL;
foreach ($storageDuplicates as $row) {
    echo " - {$row['esi_killmail_key']} count={$row['duplicate_count']} sequences={$row['sequence_ids']}" . PHP_EOL;
}

echo "Killmail overview result duplicate identity count: " . count($resultDuplicates) . PHP_EOL;
foreach ($resultDuplicates as $row) {
    echo " - {$row['esi_killmail_key']} row_count={$row['row_count']}" . PHP_EOL;
}
