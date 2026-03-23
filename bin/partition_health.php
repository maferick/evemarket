<?php

declare(strict_types=1);

require_once __DIR__ . '/../src/bootstrap.php';

try {
    $diagnostics = db_market_history_partition_diagnostics();
} catch (Throwable $exception) {
    fwrite(STDERR, 'Partition diagnostics unavailable: ' . $exception->getMessage() . PHP_EOL);
    exit(1);
}

$partitionedTables = array_values((array) ($diagnostics['partitioned_tables'] ?? []));
$evaluationTables = array_values((array) ($diagnostics['evaluation_tables'] ?? []));

foreach ($partitionedTables as $table) {
    $logicalTable = (string) ($table['logical_table'] ?? '');
    $partitionedTable = (string) ($table['partitioned_table'] ?? '');
    $readMode = (string) ($table['read_mode'] ?? 'legacy');
    $writeMode = (string) ($table['write_mode'] ?? 'legacy');
    $retentionCutoff = (string) ($table['retention_cutoff'] ?? '');
    $retentionDays = (int) ($table['retention_days'] ?? 0);
    $oldestPartition = is_array($table['oldest_partition'] ?? null) ? $table['oldest_partition'] : [];
    $newestPartition = is_array($table['newest_partition'] ?? null) ? $table['newest_partition'] : [];
    $missingFuturePartitions = array_values((array) ($table['missing_future_partitions'] ?? []));

    echo $logicalTable . PHP_EOL;
    echo '  partitioned_table: ' . $partitionedTable . PHP_EOL;
    echo '  read_mode: ' . $readMode . PHP_EOL;
    echo '  write_mode: ' . $writeMode . PHP_EOL;
    echo '  retention: ' . $retentionDays . ' days (cutoff ' . $retentionCutoff . ')' . PHP_EOL;
    echo '  oldest_partition: ' . ((string) ($oldestPartition['partition_name'] ?? '-') ?: '-') . PHP_EOL;
    echo '  newest_partition: ' . ((string) ($newestPartition['partition_name'] ?? '-') ?: '-') . PHP_EOL;
    echo '  future_partitions: ' . ($missingFuturePartitions === [] ? 'ready' : 'missing ' . implode(', ', $missingFuturePartitions)) . PHP_EOL;
    echo '  partitions:' . PHP_EOL;
    foreach (array_values((array) ($table['partitions'] ?? [])) as $partition) {
        echo '    - ' . (string) ($partition['partition_name'] ?? '-') . ' -> ' . (string) (($partition['boundary_exclusive'] ?? 'MAXVALUE') ?? 'MAXVALUE') . PHP_EOL;
    }
}

if ($evaluationTables !== []) {
    echo 'deferred_candidates:' . PHP_EOL;
    foreach ($evaluationTables as $table) {
        echo '  - ' . (string) ($table['table'] ?? '') . ': ' . (string) ($table['recommended_action'] ?? 'keep_unpartitioned') . ' (' . (string) ($table['reason'] ?? '') . ')' . PHP_EOL;
    }
}
