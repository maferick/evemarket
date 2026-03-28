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
$rows = db_select(
    "SELECT DISTINCT
        e.victim_alliance_id AS entity_id,
        COALESCE(NULLIF(ta.label, ''), CONCAT('Alliance #', e.victim_alliance_id)) AS entity_label
     FROM {$latestSequencesSql} latest
     INNER JOIN killmail_events e ON e.sequence_id = latest.sequence_id
     LEFT JOIN killmail_tracked_alliances ta
       ON ta.alliance_id = e.victim_alliance_id
      AND ta.is_active = 1
     WHERE e.victim_alliance_id IS NOT NULL
       AND e.victim_alliance_id > 0
       AND (
           ta.label LIKE CONCAT('%', ?, '%')
           OR CAST(e.victim_alliance_id AS CHAR) LIKE CONCAT(?, '%')
       )
     ORDER BY entity_label ASC
     LIMIT 25",
    [$q, $q]
);

$results = [];
foreach ($rows as $row) {
    $id = (int) ($row['entity_id'] ?? 0);
    if ($id > 0) {
        $results[] = ['id' => $id, 'label' => (string) ($row['entity_label'] ?? 'Alliance #' . $id)];
    }
}

echo json_encode(['results' => $results]);
