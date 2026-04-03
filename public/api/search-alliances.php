<?php

declare(strict_types=1);

require_once __DIR__ . '/../../src/bootstrap.php';

header('Content-Type: application/json; charset=utf-8');
header('Cache-Control: private, max-age=60');

$q = trim((string) ($_GET['q'] ?? ''));

if (mb_strlen($q) < 1) {
    echo json_encode(['results' => []]);
    exit;
}

$latestSequencesSql = db_killmail_latest_sequences_sql();

// Use sargable numeric range instead of CAST(id AS CHAR) LIKE for index-friendly ID prefix search
$numericPrefix = preg_replace('/[^0-9]/', '', $q);
$params = [$q];
if ($numericPrefix !== '' && $numericPrefix !== '0') {
    $lowerBound = (int) $numericPrefix;
    $upperBound = (int) ($numericPrefix . str_repeat('9', max(0, 15 - strlen($numericPrefix))));
    $idCondition = 'OR (e.victim_alliance_id BETWEEN ? AND ?)';
    $params[] = $lowerBound;
    $params[] = $upperBound;
} else {
    $idCondition = '';
}

$rows = db_select(
    "SELECT DISTINCT
        e.victim_alliance_id AS entity_id,
        COALESCE(emc.entity_name, CONCAT('Alliance #', e.victim_alliance_id)) AS entity_label
     FROM {$latestSequencesSql} latest
     INNER JOIN killmail_events e ON e.sequence_id = latest.sequence_id
     LEFT JOIN entity_metadata_cache emc
       ON emc.entity_type = 'alliance'
      AND emc.entity_id = e.victim_alliance_id
     WHERE e.victim_alliance_id IS NOT NULL
       AND e.victim_alliance_id > 0
       AND (
           emc.entity_name LIKE CONCAT('%', ?, '%')
           {$idCondition}
       )
     ORDER BY entity_label ASC
     LIMIT 25",
    $params
);

$results = [];
foreach ($rows as $row) {
    $id = (int) ($row['entity_id'] ?? 0);
    if ($id > 0) {
        $results[] = ['id' => $id, 'label' => (string) ($row['entity_label'] ?? 'Alliance #' . $id)];
    }
}

echo json_encode(['results' => $results]);
