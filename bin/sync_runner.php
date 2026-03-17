#!/usr/bin/env php
<?php

declare(strict_types=1);

require_once __DIR__ . '/../src/bootstrap.php';

const SYNC_RUNNER_JOB_SEQUENCE = [
    'alliance-current',
    'alliance-history',
    'hub-history',
];

function sync_runner_main(array $argv): int
{
    try {
        $options = sync_runner_parse_arguments($argv);
    } catch (InvalidArgumentException $exception) {
        sync_runner_log_stderr('sync_runner.invalid_arguments', [
            'error' => $exception->getMessage(),
            'usage' => 'php bin/sync_runner.php --job=alliance-current|alliance-history|hub-history|all --source-id=<id> --mode=incremental|full [--since=<ISO8601>]',
        ]);

        return 2;
    }

    $requestedJobs = $options['job'] === 'all' ? SYNC_RUNNER_JOB_SEQUENCE : [$options['job']];
    $runMode = $options['mode'];
    $sourceId = (int) $options['source_id'];
    $since = $options['since'];

    $hasFailures = false;

    foreach ($requestedJobs as $jobKey) {
        $jobResult = sync_runner_execute_job($jobKey, $sourceId, $runMode, $since);
        if (($jobResult['ok'] ?? false) !== true) {
            $hasFailures = true;
        }
    }

    return $hasFailures ? 1 : 0;
}

function sync_runner_parse_arguments(array $argv): array
{
    $options = getopt('', ['job:', 'source-id:', 'mode:', 'since::']);

    $job = trim((string) ($options['job'] ?? ''));
    $sourceIdValue = trim((string) ($options['source-id'] ?? ''));
    $mode = trim((string) ($options['mode'] ?? ''));
    $sinceRaw = isset($options['since']) ? trim((string) $options['since']) : null;

    $allowedJobs = ['alliance-current', 'alliance-history', 'hub-history', 'all'];
    if ($job === '' || !in_array($job, $allowedJobs, true)) {
        throw new InvalidArgumentException('Argument --job must be one of: ' . implode(', ', $allowedJobs) . '.');
    }

    if ($sourceIdValue === '' || preg_match('/^[1-9][0-9]*$/', $sourceIdValue) !== 1) {
        throw new InvalidArgumentException('Argument --source-id must be a positive integer.');
    }

    $allowedModes = ['incremental', 'full'];
    if ($mode === '' || !in_array($mode, $allowedModes, true)) {
        throw new InvalidArgumentException('Argument --mode must be one of: ' . implode(', ', $allowedModes) . '.');
    }

    $since = null;
    if ($sinceRaw !== null && $sinceRaw !== '') {
        $timestamp = strtotime($sinceRaw);
        if ($timestamp === false) {
            throw new InvalidArgumentException('Argument --since must be a valid ISO8601 datetime.');
        }

        $since = gmdate(DATE_ATOM, $timestamp);
    }

    return [
        'job' => $job,
        'source_id' => (int) $sourceIdValue,
        'mode' => $mode,
        'since' => $since,
    ];
}

function sync_runner_execute_job(string $jobKey, int $sourceId, string $runMode, ?string $since): array
{
    $startedAt = gmdate(DATE_ATOM);

    try {
        $result = sync_runner_dispatch_job($jobKey, $sourceId, $runMode, $since);
        $datasetKey = (string) ($result['dataset_key'] ?? '');
        $runId = sync_runner_latest_run_id_safe($datasetKey);
        $sourceRows = (int) ($result['rows_seen'] ?? 0);
        $writtenRows = (int) ($result['rows_written'] ?? 0);
        $warnings = $result['warnings'] ?? [];

        if ($warnings !== []) {
            sync_runner_log_stderr('sync_runner.job_warning', [
                'job' => $jobKey,
                'dataset_key' => $datasetKey,
                'run_id' => $runId,
                'source_rows' => $sourceRows,
                'written_rows' => $writtenRows,
                'warnings' => array_values($warnings),
                'mode' => $runMode,
                'source_id' => $sourceId,
                'since' => $since,
                'started_at' => $startedAt,
                'finished_at' => gmdate(DATE_ATOM),
            ]);

            return ['ok' => false];
        }

        sync_runner_log_stdout('sync_runner.job_success', [
            'job' => $jobKey,
            'dataset_key' => $datasetKey,
            'run_id' => $runId,
            'source_rows' => $sourceRows,
            'written_rows' => $writtenRows,
            'mode' => $runMode,
            'source_id' => $sourceId,
            'since' => $since,
            'started_at' => $startedAt,
            'finished_at' => gmdate(DATE_ATOM),
        ]);

        return ['ok' => true];
    } catch (Throwable $exception) {
        $datasetKey = sync_runner_dataset_key_for_job($jobKey, $sourceId);
        $runId = sync_runner_latest_run_id_safe($datasetKey);

        sync_runner_log_stderr('sync_runner.job_error', [
            'job' => $jobKey,
            'dataset_key' => $datasetKey,
            'run_id' => $runId,
            'source_rows' => 0,
            'written_rows' => 0,
            'mode' => $runMode,
            'source_id' => $sourceId,
            'since' => $since,
            'started_at' => $startedAt,
            'finished_at' => gmdate(DATE_ATOM),
            'error' => $exception->getMessage(),
        ]);

        return ['ok' => false];
    }
}

function sync_runner_dispatch_job(string $jobKey, int $sourceId, string $runMode, ?string $since): array
{
    if ($since !== null) {
        putenv('SYNC_SINCE=' . $since);
    }

    if ($jobKey === 'alliance-current') {
        $datasetKey = sync_dataset_key_alliance_structure_orders_current($sourceId);
        $result = sync_alliance_structure_orders($sourceId, $runMode);

        return $result + ['dataset_key' => $datasetKey];
    }

    if ($jobKey === 'alliance-history') {
        $datasetKey = sync_dataset_key_alliance_structure_orders_history($sourceId);
        $result = sync_alliance_structure_orders($sourceId, $runMode);

        return $result + ['dataset_key' => $datasetKey];
    }

    $datasetKey = sync_dataset_key_market_hub_history_daily((string) $sourceId);
    $result = sync_market_hub_history((string) $sourceId, $runMode);

    return $result + ['dataset_key' => $datasetKey];
}

function sync_runner_dataset_key_for_job(string $jobKey, int $sourceId): string
{
    if ($jobKey === 'alliance-current') {
        return sync_dataset_key_alliance_structure_orders_current($sourceId);
    }

    if ($jobKey === 'alliance-history') {
        return sync_dataset_key_alliance_structure_orders_history($sourceId);
    }

    return sync_dataset_key_market_hub_history_daily((string) $sourceId);
}

function sync_runner_latest_run_id_safe(string $datasetKey): ?int
{
    if ($datasetKey === '') {
        return null;
    }

    try {
        $latest = db_sync_run_latest_by_dataset($datasetKey);
    } catch (Throwable) {
        return null;
    }

    return $latest === null ? null : (int) ($latest['id'] ?? 0);
}

function sync_runner_log_stdout(string $event, array $payload): void
{
    sync_runner_write_log(STDOUT, $event, $payload);
}

function sync_runner_log_stderr(string $event, array $payload): void
{
    sync_runner_write_log(STDERR, $event, $payload);
}

function sync_runner_write_log($stream, string $event, array $payload): void
{
    $line = [
        'event' => $event,
        'ts' => gmdate(DATE_ATOM),
    ] + $payload;

    fwrite($stream, json_encode($line, JSON_UNESCAPED_SLASHES) . PHP_EOL);
}

exit(sync_runner_main($argv));
