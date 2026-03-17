#!/usr/bin/env php
<?php

declare(strict_types=1);

require_once __DIR__ . '/../src/bootstrap.php';

const CRON_TICK_RUNNER_LOCK = 'cron_tick_runner';

function cron_tick_output(string $event, array $payload = [], $stream = STDOUT): void
{
    $line = ['event' => $event, 'ts' => gmdate(DATE_ATOM)] + $payload;
    fwrite($stream, json_encode($line, JSON_UNESCAPED_SLASHES) . PHP_EOL);
}

function cron_tick_exit_code(array $summary): int
{
    return !empty($summary['scheduler_failed']) ? 1 : 0;
}

function cron_tick_main(): int
{
    $tickStartedAt = microtime(true);
    $lockAcquired = false;

    cron_tick_output('cron_tick.started', [
        'scheduler_status' => 'starting',
    ]);

    try {
        $lockAcquired = runner_lock_acquire(CRON_TICK_RUNNER_LOCK);
        if (!$lockAcquired) {
            $durationMs = (int) round((microtime(true) - $tickStartedAt) * 1000);
            cron_tick_output('cron_tick.lock_skipped', [
                'scheduler_status' => 'skipped_locked',
                'duration_ms' => $durationMs,
            ]);

            return 0;
        }

        cron_tick_output('cron_tick.lock_acquired', [
            'scheduler_status' => 'running',
        ]);

        $summary = cron_tick_run('cron_tick_output');
        $summary['scheduler_failed'] = false;
        $summary['duration_ms'] = (int) round((microtime(true) - $tickStartedAt) * 1000);

        cron_tick_output('cron_tick.summary', [
            'jobs_due' => (int) ($summary['jobs_due'] ?? 0),
            'jobs_processed' => (int) ($summary['jobs_processed'] ?? 0),
            'jobs_succeeded' => (int) ($summary['jobs_succeeded'] ?? 0),
            'jobs_failed' => (int) ($summary['jobs_failed'] ?? 0),
            'duration_ms' => (int) ($summary['duration_ms'] ?? 0),
            'scheduler_failed' => false,
            'scheduler_status' => 'ok',
        ]);

        cron_tick_output('cron_tick.completed', [
            'scheduler_status' => 'ok',
            'duration_ms' => (int) ($summary['duration_ms'] ?? 0),
        ]);

        return cron_tick_exit_code($summary);
    } catch (Throwable $exception) {
        $durationMs = (int) round((microtime(true) - $tickStartedAt) * 1000);
        cron_tick_output('cron_tick.scheduler_error', [
            'scheduler_failed' => true,
            'scheduler_status' => 'failed',
            'duration_ms' => $durationMs,
            'error' => $exception->getMessage(),
        ], STDERR);

        return 1;
    } finally {
        if ($lockAcquired) {
            runner_lock_release(CRON_TICK_RUNNER_LOCK);
        }
    }
}

exit(cron_tick_main());
