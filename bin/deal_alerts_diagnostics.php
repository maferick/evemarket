<?php

declare(strict_types=1);

require_once __DIR__ . '/../src/bootstrap.php';

$persist = in_array('--persist', $argv, true);
$result = deal_alert_refresh_diagnostic_result($persist ? 'diagnostic-persist' : 'diagnostic-dry-run', $persist);
$meta = is_array($result['meta'] ?? null) ? $result['meta'] : [];

$summary = [
    'verification_mode' => (string) ($meta['verification_mode'] ?? ($persist ? 'persisted' : 'dry_run')),
    'rows_seen' => (int) ($result['rows_seen'] ?? 0),
    'rows_written' => (int) ($result['rows_written'] ?? 0),
    'input_row_count' => (int) ($meta['input_row_count'] ?? 0),
    'history_row_count' => (int) ($meta['history_row_count'] ?? 0),
    'candidate_row_count' => (int) ($meta['candidate_row_count'] ?? 0),
    'output_row_count' => (int) ($meta['output_row_count'] ?? 0),
    'persisted_row_count' => (int) ($meta['persisted_row_count'] ?? 0),
    'inactive_row_count' => (int) ($meta['inactive_row_count'] ?? 0),
    'sources_scanned' => (int) ($meta['sources_scanned'] ?? 0),
    'materialization_write_timestamp' => $meta['materialization_write_timestamp'] ?? null,
    'last_calculation_duration_ms' => $meta['last_calculation_duration_ms'] ?? null,
    'last_zero_output_reason' => $meta['last_zero_output_reason'] ?? null,
    'source_input_counts' => $meta['source_input_counts'] ?? [],
    'source_candidate_counts' => $meta['source_candidate_counts'] ?? [],
    'source_output_counts' => $meta['source_output_counts'] ?? [],
];

fwrite(STDOUT, json_encode($summary, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES) . PHP_EOL);
