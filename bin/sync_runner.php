#!/usr/bin/env php
<?php

declare(strict_types=1);

require_once __DIR__ . '/../src/bootstrap.php';

const SYNC_RUNNER_JOB_SEQUENCE = [
    'alliance-current',
    'alliance-history',
    'hub-current',
    'market-hub-local-history',
    'hub-history',
    'maintenance-prune',
    'killmail-r2z2',
];

function sync_runner_main(array $argv): int
{
    try {
        $options = sync_runner_parse_arguments($argv);
    } catch (InvalidArgumentException $exception) {
        sync_runner_log_stderr('sync_runner.invalid_arguments', [
            'error' => $exception->getMessage(),
            'usage' => 'php bin/sync_runner.php --job=alliance-current|alliance-history|hub-current|market-hub-local-history|hub-history|maintenance-prune|killmail-r2z2|all --source-id=<id> --mode=incremental|full [--since=<ISO8601>] [--window-days=<days>]',
        ]);

        return 2;
    }

    $requestedJobs = $options['job'] === 'all' ? SYNC_RUNNER_JOB_SEQUENCE : [$options['job']];
    $runMode = $options['mode'];
    $sourceId = (int) $options['source_id'];
    $since = $options['since'];
    $windowDays = $options['window_days'];

    $hasFailures = false;

    foreach ($requestedJobs as $jobKey) {
        $jobResult = sync_runner_execute_job($jobKey, $sourceId, $runMode, $since, $windowDays);
        if (($jobResult['ok'] ?? false) !== true) {
            $hasFailures = true;
        }
    }

    return $hasFailures ? 1 : 0;
}

function sync_runner_parse_arguments(array $argv): array
{
    $options = getopt('', ['job:', 'source-id:', 'mode:', 'since::', 'window-days::']);

    $job = trim((string) ($options['job'] ?? ''));
    $sourceIdValue = trim((string) ($options['source-id'] ?? ''));
    $mode = trim((string) ($options['mode'] ?? ''));
    $sinceRaw = isset($options['since']) ? trim((string) $options['since']) : null;
    $windowDaysRaw = isset($options['window-days']) ? trim((string) $options['window-days']) : null;

    $allowedJobs = ['alliance-current', 'alliance-history', 'hub-current', 'market-hub-local-history', 'hub-history', 'maintenance-prune', 'killmail-r2z2', 'all'];
    if ($job === '' || !in_array($job, $allowedJobs, true)) {
        throw new InvalidArgumentException('Argument --job must be one of: ' . implode(', ', $allowedJobs) . '.');
    }

    $jobRequiresSourceId = in_array($job, ['alliance-current', 'alliance-history', 'all'], true);
    if ($jobRequiresSourceId && ($sourceIdValue === '' || preg_match('/^[1-9][0-9]*$/', $sourceIdValue) !== 1)) {
        throw new InvalidArgumentException('Argument --source-id must be a positive integer for the selected job.');
    }

    if ($sourceIdValue !== '' && preg_match('/^[1-9][0-9]*$/', $sourceIdValue) !== 1) {
        throw new InvalidArgumentException('Argument --source-id must be a positive integer when provided.');
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

    $windowDays = null;
    if ($windowDaysRaw !== null && $windowDaysRaw !== '') {
        if (preg_match('/^[1-9][0-9]{0,2}$/', $windowDaysRaw) !== 1) {
            throw new InvalidArgumentException('Argument --window-days must be a positive integer between 1 and 999 when provided.');
        }

        $windowDays = (int) $windowDaysRaw;
    }

    return [
        'job' => $job,
        'source_id' => $sourceIdValue === '' ? 0 : (int) $sourceIdValue,
        'mode' => $mode,
        'since' => $since,
        'window_days' => $windowDays,
    ];
}

function sync_runner_execute_job(string $jobKey, int $sourceId, string $runMode, ?string $since, ?int $windowDays = null): array
{
    $startedAt = gmdate(DATE_ATOM);

    try {
        $result = sync_runner_dispatch_job($jobKey, $sourceId, $runMode, $since, $windowDays);
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
                'window_days' => $windowDays,
                'started_at' => $startedAt,
                'finished_at' => gmdate(DATE_ATOM),
            ]);
            sync_runner_log_cron_file('sync_runner.job_warning_summary', [
                'job' => $jobKey,
                'dataset_key' => $datasetKey,
                'run_id' => $runId,
                'source_rows' => $sourceRows,
                'written_rows' => $writtenRows,
                'warnings' => array_values($warnings),
                'mode' => $runMode,
                'source_id' => $sourceId,
                'window_days' => $windowDays,
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
            'window_days' => $windowDays,
            'started_at' => $startedAt,
            'finished_at' => gmdate(DATE_ATOM),
        ]);
        sync_runner_log_cron_file('sync_runner.job_success_summary', [
            'job' => $jobKey,
            'dataset_key' => $datasetKey,
            'run_id' => $runId,
            'source_rows' => $sourceRows,
            'written_rows' => $writtenRows,
            'mode' => $runMode,
            'source_id' => $sourceId,
            'window_days' => $windowDays,
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
            'window_days' => $windowDays,
            'started_at' => $startedAt,
            'finished_at' => gmdate(DATE_ATOM),
            'error' => $exception->getMessage(),
        ]);
        sync_runner_log_cron_file('sync_runner.job_error_summary', [
            'job' => $jobKey,
            'dataset_key' => $datasetKey,
            'run_id' => $runId,
            'mode' => $runMode,
            'source_id' => $sourceId,
            'window_days' => $windowDays,
            'error' => $exception->getMessage(),
        ]);

        return ['ok' => false];
    }
}

function sync_runner_dispatch_job(string $jobKey, int $sourceId, string $runMode, ?string $since, ?int $windowDays = null): array
{
    $effectiveSince = $since ?? sync_runner_backfill_start_for_job($jobKey);
    if ($effectiveSince !== null) {
        putenv('SYNC_SINCE=' . $effectiveSince);
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

    if ($jobKey === 'killmail-r2z2') {
        $datasetKey = sync_dataset_key_killmail_r2z2_stream();
        $result = sync_killmail_r2z2_stream($runMode);

        return $result + ['dataset_key' => $datasetKey];
    }

    if ($jobKey === 'hub-current') {
        $hubRef = market_hub_setting_reference();
        if ($hubRef === '') {
            throw new RuntimeException('Hub current sync skipped: configure a reference market hub in Trading Stations settings.');
        }

        $datasetKey = sync_dataset_key_market_hub_current_orders($hubRef);
        $result = sync_market_hub_current_orders($hubRef, $runMode);

        return $result + ['dataset_key' => $datasetKey];
    }

    if ($jobKey === 'market-hub-local-history') {
        $hubRef = market_hub_setting_reference();
        if ($hubRef === '') {
            throw new RuntimeException('Hub local history sync skipped: configure a reference market hub in Trading Stations settings.');
        }

        $datasetKey = sync_dataset_key_market_hub_local_history_daily($hubRef);
        $result = sync_market_hub_local_history($hubRef, $runMode, $windowDays);

        return $result + ['dataset_key' => $datasetKey];
    }

    if ($jobKey === 'maintenance-prune') {
        $datasetKey = sync_dataset_key_maintenance_history_prune();
        $retentionDays = (int) get_setting('raw_order_snapshot_retention_days', '30');
        $result = sync_market_orders_history_prune($retentionDays, $runMode);

        return $result + ['dataset_key' => $datasetKey];
    }

    $hubRef = market_hub_setting_reference();
    if ($hubRef === '') {
        throw new RuntimeException('Hub history sync skipped: configure a reference market hub in Trading Stations settings.');
    }

    $datasetKey = sync_dataset_key_market_hub_history_daily($hubRef);
    $result = sync_market_hub_history($hubRef, $runMode);

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

    if ($jobKey === 'hub-current') {
        $hubRef = market_hub_setting_reference();

        return sync_dataset_key_market_hub_current_orders($hubRef !== '' ? $hubRef : ((string) $sourceId));
    }

    if ($jobKey === 'market-hub-local-history') {
        $hubRef = market_hub_setting_reference();

        return sync_dataset_key_market_hub_local_history_daily($hubRef !== '' ? $hubRef : ((string) $sourceId));
    }

    if ($jobKey === 'maintenance-prune') {
        return sync_dataset_key_maintenance_history_prune();
    }

    if ($jobKey === 'killmail-r2z2') {
        return sync_dataset_key_killmail_r2z2_stream();
    }

    $hubRef = market_hub_setting_reference();

    return sync_dataset_key_market_hub_history_daily($hubRef !== '' ? $hubRef : ((string) $sourceId));
}

function sync_runner_backfill_start_for_job(string $jobKey): ?string
{
    $settingKey = match ($jobKey) {
        'alliance-current' => 'alliance_current_backfill_start_date',
        'alliance-history' => 'alliance_history_backfill_start_date',
        'hub-current' => 'hub_history_backfill_start_date',
        'hub-history' => 'hub_history_backfill_start_date',
        default => '',
    };

    if ($settingKey === '') {
        return null;
    }

    $raw = trim((string) get_setting($settingKey, ''));
    if ($raw === '') {
        return null;
    }

    $timestamp = strtotime($raw . ' 00:00:00 UTC');
    if ($timestamp === false) {
        return null;
    }

    return gmdate(DATE_ATOM, $timestamp);
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

function sync_runner_log_cron_file(string $event, array $payload): void
{
    $line = sync_runner_format_log_line($event, $payload);
    $logPath = dirname(__DIR__) . '/storage/logs/cron.log';
    $logDir = dirname($logPath);

    if (!is_dir($logDir)) {
        mkdir($logDir, 0775, true);
    }

    file_put_contents($logPath, $line, FILE_APPEND | LOCK_EX);
}

function sync_runner_format_log_line(string $event, array $payload): string
{
    return json_encode([
        'event' => $event,
        'ts' => gmdate(DATE_ATOM),
    ] + $payload, JSON_UNESCAPED_SLASHES) . PHP_EOL;
}

function sync_runner_write_log($stream, string $event, array $payload): void
{
    fwrite($stream, sync_runner_format_log_line($event, $payload));
}

exit(sync_runner_main($argv));
