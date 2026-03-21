#!/usr/bin/env php
<?php

declare(strict_types=1);

require_once __DIR__ . '/../src/bootstrap.php';

function scheduler_job_runner_output(string $event, array $payload = [], $stream = STDOUT): void
{
    $line = ['event' => $event, 'ts' => gmdate(DATE_ATOM)] + $payload;
    fwrite($stream, json_encode($line, JSON_UNESCAPED_SLASHES) . PHP_EOL);
}

function scheduler_job_runner_parse_options(): int
{
    $options = getopt('', ['schedule-id:']);
    $scheduleId = (int) ($options['schedule-id'] ?? 0);
    if ($scheduleId <= 0) {
        throw new InvalidArgumentException('Argument --schedule-id must be a positive integer.');
    }

    return $scheduleId;
}

function scheduler_job_runner_main(): int
{
    $scheduleId = scheduler_job_runner_parse_options();
    $job = db_sync_schedule_fetch_by_id($scheduleId);
    if ($job === null) {
        throw new RuntimeException('No scheduler job found for schedule ID ' . $scheduleId . '.');
    }

    $jobKey = trim((string) ($job['job_key'] ?? 'unknown_job'));
    $jobType = scheduler_job_type($jobKey);
    $status = trim((string) ($job['last_status'] ?? ''));
    $lockedUntil = trim((string) ($job['locked_until'] ?? ''));

    if ($status !== 'running' || $lockedUntil === '') {
        throw new RuntimeException('Scheduler job "' . $jobKey . '" is no longer claimed for background execution.');
    }

    scheduler_job_runner_output('job.background.started', [
        'job_id' => $scheduleId,
        'job' => $jobKey,
        'job_type' => $jobType,
        'scheduled_for' => (string) ($job['next_due_at'] ?? $job['next_run_at'] ?? ''),
        'locked_until' => $lockedUntil,
    ]);

    $result = scheduler_run_job($job);
    $resultStatus = (string) ($result['status'] ?? 'failed');

    scheduler_job_runner_output(
        $resultStatus === 'failed' ? 'job.background.failed' : 'job.background.completed',
        [
            'job_id' => (int) ($result['job_id'] ?? $scheduleId),
            'job' => (string) ($result['job_key'] ?? $jobKey),
            'job_type' => (string) ($result['job_type'] ?? $jobType),
            'scheduled_for' => (string) ($result['scheduled_for'] ?? ''),
            'actual_start_time' => (string) ($result['started_at'] ?? ''),
            'finish_time' => (string) ($result['finished_at'] ?? ''),
            'duration_ms' => (int) ($result['duration_ms'] ?? 0),
            'status' => $resultStatus,
            'summary' => (string) ($result['summary'] ?? ''),
            'error' => $resultStatus === 'failed' ? (string) ($result['error'] ?? '') : null,
        ]
    );

    $meta = is_array($result['meta'] ?? null) ? $result['meta'] : [];
    if ($meta !== []) {
        scheduler_job_runner_output('job.background.outcome', [
            'job_id' => (int) ($result['job_id'] ?? $scheduleId),
            'job' => (string) ($result['job_key'] ?? $jobKey),
            'status' => $resultStatus,
        ] + $meta);
    }

    db_scheduler_daemon_request_wake(scheduler_daemon_key());

    return $resultStatus === 'failed' ? 1 : 0;
}

try {
    exit(scheduler_job_runner_main());
} catch (Throwable $exception) {
    scheduler_job_runner_output('job.background.scheduler_error', [
        'error' => $exception->getMessage(),
    ], STDERR);

    exit(1);
}
