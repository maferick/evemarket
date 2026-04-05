#!/usr/bin/env php
<?php

declare(strict_types=1);

/**
 * SupplyCore AI briefing worker.
 *
 * Drains the ai_jobs queue and executes AI generation work that used to run
 * inline on web requests (theater battle summaries, opposition daily
 * briefings). Designed to be managed by systemd — see
 * ops/systemd/supplycore-ai-worker.service.
 *
 * Usage:
 *   php bin/ai_briefing_worker.php --drain                    # long-running loop (systemd)
 *   php bin/ai_briefing_worker.php --once                     # claim and run a single job, then exit
 *   php bin/ai_briefing_worker.php --job=<id>                 # run a specific job by id (debug)
 *   php bin/ai_briefing_worker.php --sweep                    # reclaim stale leases, then exit
 *
 * Options:
 *   --worker-id=<tag>       Identifier written to ai_jobs.worker_id (default: hostname-pid)
 *   --lease=<seconds>       Lease length per claimed job (default: 900)
 *   --poll=<seconds>        Idle sleep between queue polls (default: 5)
 *   --max-jobs=<n>          Exit cleanly after processing n jobs (0 = unlimited)
 *   --max-runtime=<seconds> Exit cleanly after n seconds of wall time (0 = unlimited)
 */

require_once __DIR__ . '/../src/bootstrap.php';

// CLI hygiene: no PHP timeout, unbuffered output, journal-friendly logging.
@set_time_limit(0);
@ini_set('max_execution_time', '0');
@ini_set('memory_limit', '512M');
if (function_exists('ob_implicit_flush')) {
    @ob_implicit_flush(true);
}

$options = getopt('', [
    'drain',
    'once',
    'sweep',
    'job::',
    'worker-id::',
    'lease::',
    'poll::',
    'max-jobs::',
    'max-runtime::',
    'help',
]);

if (isset($options['help'])) {
    fwrite(STDOUT, "Usage: php bin/ai_briefing_worker.php --drain|--once|--sweep|--job=<id>\n");
    exit(0);
}

$workerId = (string) ($options['worker-id'] ?? (gethostname() ?: 'worker') . '-' . getmypid());
$leaseSeconds = max(60, (int) ($options['lease'] ?? 900));
$pollSeconds = max(1, (int) ($options['poll'] ?? 5));
$maxJobs = max(0, (int) ($options['max-jobs'] ?? 0));
$maxRuntime = max(0, (int) ($options['max-runtime'] ?? 0));

function ai_worker_log(string $level, string $message, array $context = []): void
{
    $line = sprintf(
        '[ai-worker] [%s] %s%s',
        $level,
        $message,
        $context === [] ? '' : ' ' . json_encode($context, JSON_UNESCAPED_SLASHES | JSON_INVALID_UTF8_SUBSTITUTE)
    );
    fwrite($level === 'error' || $level === 'warn' ? STDERR : STDOUT, $line . PHP_EOL);
}

function ai_worker_is_transient(Throwable $e): bool
{
    $message = $e->getMessage();
    // Network / HTTP 5xx / timeout / cold-start style errors are retried.
    // JSON validation, 4xx, missing-endpoint errors are terminal.
    if (preg_match('/HTTP\s+5\d\d/i', $message) === 1) {
        return true;
    }
    if (stripos($message, 'timed out') !== false
        || stripos($message, 'did not finish before timeout') !== false
        || stripos($message, 'connection') !== false
        || stripos($message, 'network') !== false
        || stripos($message, 'cold start') !== false
        || stripos($message, 'IN_QUEUE') !== false
        || stripos($message, 'IN_PROGRESS') !== false) {
        return true;
    }
    return false;
}

function ai_worker_run_job(array $job, string $workerId): void
{
    $id = (int) ($job['id'] ?? 0);
    $feature = (string) ($job['feature'] ?? '');
    $target = (string) ($job['target_key'] ?? '');
    $provider = (string) ($job['provider'] ?? '');
    $attempts = (int) ($job['attempts'] ?? 0);

    ai_worker_log('info', 'job.start', [
        'id' => $id,
        'feature' => $feature,
        'target_key' => $target,
        'provider' => $provider,
        'attempt' => $attempts,
        'worker' => $workerId,
    ]);

    $startedAt = microtime(true);
    try {
        $result = supplycore_ai_jobs_execute($job);
        $durationMs = (int) round((microtime(true) - $startedAt) * 1000);
        supplycore_ai_jobs_complete($id, $result, $durationMs);
        ai_worker_log('info', 'job.done', [
            'id' => $id,
            'feature' => $feature,
            'duration_ms' => $durationMs,
        ]);
    } catch (Throwable $e) {
        $durationMs = (int) round((microtime(true) - $startedAt) * 1000);
        $transient = ai_worker_is_transient($e);
        supplycore_ai_jobs_fail($id, $e->getMessage(), $transient);
        ai_worker_log($transient ? 'warn' : 'error', 'job.fail', [
            'id' => $id,
            'feature' => $feature,
            'duration_ms' => $durationMs,
            'transient' => $transient,
            'error' => $e->getMessage(),
        ]);
    }
}

// --sweep: reclaim any stale leases and exit. Useful as a one-shot tool or
// as part of a systemd OnCalendar timer.
if (isset($options['sweep'])) {
    $swept = supplycore_ai_jobs_sweep_expired_leases();
    ai_worker_log('info', 'sweep.done', ['reclaimed' => $swept]);
    exit(0);
}

// --job=<id>: run one specific row (debug hook, bypasses the claim loop).
if (isset($options['job']) && $options['job'] !== false) {
    $jobId = (int) $options['job'];
    $job = supplycore_ai_jobs_get($jobId);
    if ($job === null) {
        ai_worker_log('error', 'job.not_found', ['id' => $jobId]);
        exit(2);
    }
    ai_worker_run_job($job, $workerId);
    exit(0);
}

// --once: claim and run a single job (if any) then exit.
if (isset($options['once'])) {
    supplycore_ai_jobs_sweep_expired_leases();
    $job = supplycore_ai_jobs_claim($workerId, $leaseSeconds);
    if ($job === null) {
        ai_worker_log('info', 'queue.empty', []);
        exit(0);
    }
    ai_worker_run_job($job, $workerId);
    exit(0);
}

// --drain: long-running loop. Default mode when invoked by systemd.
if (!isset($options['drain'])) {
    fwrite(STDERR, "One of --drain / --once / --sweep / --job=<id> is required.\n");
    exit(64);
}

$shouldStop = false;
$handleSignal = static function (int $signal) use (&$shouldStop): void {
    $shouldStop = true;
    ai_worker_log('info', 'signal.stop', ['signal' => $signal]);
};
if (function_exists('pcntl_async_signals') && function_exists('pcntl_signal')) {
    pcntl_async_signals(true);
    pcntl_signal(SIGTERM, $handleSignal);
    pcntl_signal(SIGINT, $handleSignal);
    pcntl_signal(SIGHUP, $handleSignal);
}

ai_worker_log('info', 'drain.start', [
    'worker_id' => $workerId,
    'lease' => $leaseSeconds,
    'poll' => $pollSeconds,
    'max_jobs' => $maxJobs,
    'max_runtime' => $maxRuntime,
]);

$processed = 0;
$deadline = $maxRuntime > 0 ? (time() + $maxRuntime) : 0;
$lastSweep = 0;

while (!$shouldStop) {
    // Cheap sweep for expired leases once a minute so one crashed worker
    // doesn't permanently wedge its claimed rows.
    if ((time() - $lastSweep) >= 60) {
        try {
            supplycore_ai_jobs_sweep_expired_leases();
        } catch (Throwable $e) {
            ai_worker_log('warn', 'sweep.error', ['error' => $e->getMessage()]);
        }
        $lastSweep = time();
    }

    try {
        $job = supplycore_ai_jobs_claim($workerId, $leaseSeconds);
    } catch (Throwable $e) {
        ai_worker_log('error', 'claim.error', ['error' => $e->getMessage()]);
        sleep($pollSeconds);
        continue;
    }

    if ($job === null) {
        if ($deadline > 0 && time() >= $deadline) {
            break;
        }
        sleep($pollSeconds);
        continue;
    }

    ai_worker_run_job($job, $workerId);
    $processed++;

    if ($maxJobs > 0 && $processed >= $maxJobs) {
        break;
    }
    if ($deadline > 0 && time() >= $deadline) {
        break;
    }
}

ai_worker_log('info', 'drain.stop', ['processed' => $processed]);
exit(0);
