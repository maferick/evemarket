#!/usr/bin/env php
<?php

declare(strict_types=1);

require_once __DIR__ . '/../src/bootstrap.php';

const CRON_TICK_RUNNER_LOCK = 'cron_tick_runner';
const CRON_TICK_JOB_MAP = [
    'alliance_current_sync' => ['job' => 'alliance-current', 'source_env' => 'EVEMARKET_ALLIANCE_SOURCE_ID'],
    'alliance_historical_sync' => ['job' => 'alliance-history', 'source_env' => 'EVEMARKET_ALLIANCE_SOURCE_ID'],
    'market_hub_historical_sync' => ['job' => 'hub-history', 'source_env' => 'EVEMARKET_HUB_SOURCE_ID'],
];

function cron_tick_main(): int
{
    if (!runner_lock_acquire(CRON_TICK_RUNNER_LOCK)) {
        cron_tick_log(STDOUT, 'cron_tick.skipped_locked', []);

        return 0;
    }

    try {
        return cron_tick_run_due_jobs();
    } finally {
        runner_lock_release(CRON_TICK_RUNNER_LOCK);
    }
}

function cron_tick_run_due_jobs(): int
{
    $hasFailures = false;

    foreach (db_sync_schedule_fetch_due_jobs() as $schedule) {
        $scheduleId = (int) ($schedule['id'] ?? 0);
        $jobKey = (string) ($schedule['job_key'] ?? '');
        if ($scheduleId < 1 || $jobKey === '') {
            continue;
        }

        $definition = CRON_TICK_JOB_MAP[$jobKey] ?? null;
        if ($definition === null) {
            cron_tick_log(STDERR, 'cron_tick.job_skipped_unknown', ['job_key' => $jobKey]);
            $hasFailures = true;
            continue;
        }

        $claimed = db_transaction(static fn (): ?array => db_sync_schedule_claim_job($scheduleId));
        if ($claimed === null) {
            continue;
        }

        $job = (string) $definition['job'];
        $sourceId = cron_tick_source_id_from_env($definition['source_env']);

        if ($definition['source_env'] !== null && $sourceId === null) {
            db_transaction(static fn (): bool => db_sync_schedule_mark_failure($scheduleId, 'Invalid or missing source id environment variable.'));
            cron_tick_log(STDERR, 'cron_tick.job_skipped_invalid_source', [
                'job' => $job,
                'job_key' => $jobKey,
                'source_env' => (string) $definition['source_env'],
            ]);
            $hasFailures = true;
            continue;
        }

        $exitCode = cron_tick_run_sync_runner($job, $sourceId);
        if ($exitCode === 0) {
            db_transaction(static fn (): bool => db_sync_schedule_mark_success($scheduleId));
            continue;
        }

        db_transaction(static fn (): bool => db_sync_schedule_mark_failure($scheduleId, sprintf('Sync runner exited with status %d.', $exitCode)));
        $hasFailures = true;
    }

    return $hasFailures ? 1 : 0;
}

function cron_tick_source_id_from_env(?string $envKey): ?int
{
    if ($envKey === null || $envKey === '') {
        return null;
    }

    $raw = trim((string) getenv($envKey));
    if ($raw === '' || preg_match('/^[1-9][0-9]*$/', $raw) !== 1) {
        return null;
    }

    return (int) $raw;
}

function cron_tick_run_sync_runner(string $job, ?int $sourceId): int
{
    $parts = [
        PHP_BINARY,
        __DIR__ . '/sync_runner.php',
        '--job=' . $job,
        '--mode=incremental',
    ];

    if ($sourceId !== null) {
        $parts[] = '--source-id=' . $sourceId;
    }

    $command = implode(' ', array_map('escapeshellarg', $parts));

    $descriptorSpec = [
        0 => ['pipe', 'r'],
        1 => ['pipe', 'w'],
        2 => ['pipe', 'w'],
    ];

    $process = proc_open($command, $descriptorSpec, $pipes, dirname(__DIR__));
    if (!is_resource($process)) {
        cron_tick_log(STDERR, 'cron_tick.job_spawn_failed', ['job' => $job]);

        return 1;
    }

    fclose($pipes[0]);
    $stdout = stream_get_contents($pipes[1]) ?: '';
    fclose($pipes[1]);
    $stderr = stream_get_contents($pipes[2]) ?: '';
    fclose($pipes[2]);

    $exitCode = proc_close($process);

    if ($stdout !== '') {
        fwrite(STDOUT, trim($stdout) . PHP_EOL);
    }

    if ($stderr !== '') {
        fwrite(STDERR, trim($stderr) . PHP_EOL);
    }

    cron_tick_log($exitCode === 0 ? STDOUT : STDERR, 'cron_tick.job_finished', [
        'job' => $job,
        'source_id' => $sourceId,
        'exit_code' => $exitCode,
    ]);

    return $exitCode;
}

function cron_tick_log($stream, string $event, array $payload): void
{
    $line = ['event' => $event, 'ts' => gmdate(DATE_ATOM)] + $payload;
    fwrite($stream, json_encode($line, JSON_UNESCAPED_SLASHES) . PHP_EOL);
}

exit(cron_tick_main());
