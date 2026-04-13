<?php

declare(strict_types=1);

require_once __DIR__ . '/../../src/bootstrap.php';

$title = 'Log Viewer';
$pageHeaderBadge = 'System health at a glance';
$pageHeaderSummary = 'Monitor job runs, failures, timeouts, and external service connectivity. Fix issues before they cascade.';

$liveRefreshConfig = supplycore_live_refresh_page_config('log_viewer');

$pageData = log_viewer_page_data();
$externalHealth = log_viewer_external_health();

$jobs = $pageData['jobs'];
$failedRuns = $pageData['failed_runs'];
$stuckRuns = $pageData['stuck_runs'];
$neverRan = $pageData['never_ran'];
$logFiles = $pageData['log_files'];
$schedulerCycles = $pageData['scheduler_cycles'] ?? [];
$kpi = $pageData['kpi'];
$recentRuns = $pageData['recent_runs'];
$backlog = $pageData['backlog'];

// Determine page freshness from data
$pageFreshness = [];

include __DIR__ . '/../../src/views/partials/header.php';

if (function_exists('ob_flush')) { @ob_flush(); }
@flush();

// ── Filter state ──────────────────────────────────────────────────────────────
$filter = trim((string) ($_GET['filter'] ?? 'all'));
$validFilters = ['all', 'failed', 'timeout', 'never_ran', 'overdue', 'healthy', 'disabled'];
if (!in_array($filter, $validFilters, true)) {
    $filter = 'all';
}

$filteredJobs = match ($filter) {
    'failed' => array_filter($jobs, fn (array $j) => $j['health'] === 'failed' || $j['health'] === 'stuck'),
    'timeout' => array_filter($jobs, fn (array $j) => $j['health'] === 'timeout'),
    'never_ran' => array_filter($jobs, fn (array $j) => $j['health'] === 'never_ran'),
    'overdue' => array_filter($jobs, fn (array $j) => $j['overdue']),
    'healthy' => array_filter($jobs, fn (array $j) => $j['health'] === 'healthy'),
    'disabled' => array_filter($jobs, fn (array $j) => $j['health'] === 'disabled'),
    default => $jobs,
};

// Needs-attention count
$attentionCount = $kpi['total_failed'] + $kpi['total_timeout'] + $kpi['total_overdue'];
?>

<!-- ═══════════════════════════════════════════════════════════════════════════
     KPI Cards
     ══════════════════════════════════════════════════════════════════════════ -->
<!-- ui-section:log-viewer-kpi:start -->
<section class="grid gap-4 sm:grid-cols-2 xl:grid-cols-4" data-ui-section="log-viewer-kpi">
    <article class="kpi-card">
        <p class="eyebrow">Needs attention</p>
        <p class="mt-3 metric-value text-[2.35rem] <?= $attentionCount > 0 ? 'text-rose-100' : 'text-emerald-100' ?>"><?= $attentionCount ?></p>
        <p class="mt-2 text-sm text-slate-300">
            <?php if ($attentionCount === 0): ?>
                All systems operational.
            <?php else: ?>
                <?= $kpi['total_failed'] ?> failed · <?= $kpi['total_timeout'] ?> timed out · <?= $kpi['total_overdue'] ?> overdue
            <?php endif; ?>
        </p>
    </article>
    <article class="kpi-card">
        <p class="eyebrow">Healthy jobs</p>
        <p class="mt-3 metric-value text-[2.35rem] text-emerald-100"><?= $kpi['total_healthy'] ?></p>
        <p class="mt-2 text-sm text-slate-300">Jobs that completed their last run successfully.</p>
    </article>
    <article class="kpi-card">
        <p class="eyebrow">Never ran</p>
        <p class="mt-3 metric-value text-[2.35rem] <?= $kpi['total_never_ran'] > 0 ? 'text-violet-100' : 'text-slate-300' ?>"><?= $kpi['total_never_ran'] ?></p>
        <p class="mt-2 text-sm text-slate-300">Enabled jobs that have never executed.</p>
    </article>
    <article class="kpi-card">
        <p class="eyebrow">Enabled / Total</p>
        <p class="mt-3 metric-value text-[2.35rem] text-sky-100"><?= $kpi['total_enabled'] ?> <span class="text-lg text-slate-400">/ <?= count($jobs) ?></span></p>
        <p class="mt-2 text-sm text-slate-300">Active scheduled jobs out of total registered.</p>
    </article>
</section>
<!-- ui-section:log-viewer-kpi:end -->

<!-- ═══════════════════════════════════════════════════════════════════════════
     Backlog Overview — real-time queue depth and overdue jobs
     ══════════════════════════════════════════════════════════════════════════ -->
<!-- ui-section:log-viewer-backlog:start -->
<section class="mt-8" data-ui-section="log-viewer-backlog">
    <h2 class="section-title mb-4">Backlog Overview</h2>
    <div class="grid gap-4 sm:grid-cols-3 mb-6">
        <article class="kpi-card">
            <p class="eyebrow">Queued</p>
            <p class="mt-3 metric-value text-[2rem] <?= $backlog['queue']['queued'] > 0 ? 'text-sky-100' : 'text-slate-400' ?>"><?= $backlog['queue']['queued'] ?></p>
            <p class="mt-2 text-sm text-slate-300">Jobs waiting to be picked up by a worker.</p>
        </article>
        <article class="kpi-card">
            <p class="eyebrow">Running</p>
            <p class="mt-3 metric-value text-[2rem] <?= $backlog['queue']['running'] > 0 ? 'text-emerald-100' : 'text-slate-400' ?>"><?= $backlog['queue']['running'] ?></p>
            <p class="mt-2 text-sm text-slate-300">Jobs currently executing across all workers.</p>
        </article>
        <article class="kpi-card">
            <p class="eyebrow">Retry</p>
            <p class="mt-3 metric-value text-[2rem] <?= $backlog['queue']['retry'] > 0 ? 'text-amber-100' : 'text-slate-400' ?>"><?= $backlog['queue']['retry'] ?></p>
            <p class="mt-2 text-sm text-slate-300">Jobs waiting to retry after a transient failure.</p>
        </article>
    </div>
    <?php if ($backlog['overdue_jobs'] !== []): ?>
    <div class="rounded-2xl border border-amber-400/20 bg-amber-500/6 p-1">
        <div class="table-shell overflow-x-auto">
            <table class="table-ui w-full">
                <thead>
                    <tr>
                        <th class="text-left">Job</th>
                        <th class="text-left">Last run</th>
                        <th class="text-right">Overdue by</th>
                        <th class="text-left">Interval</th>
                        <th class="text-left">Status</th>
                    </tr>
                </thead>
                <tbody>
                    <?php foreach ($backlog['overdue_jobs'] as $oj): ?>
                        <?php
                        $overdueBy = $oj['last_run_at']
                            ? max(0, time() - strtotime((string) $oj['last_run_at']) - (int) $oj['interval_seconds'])
                            : null;
                        ?>
                        <tr>
                            <td>
                                <p class="font-medium text-white"><?= htmlspecialchars($oj['label'], ENT_QUOTES) ?></p>
                                <p class="mt-0.5 text-xs text-slate-400"><?= htmlspecialchars($oj['job_key'], ENT_QUOTES) ?></p>
                            </td>
                            <td class="text-slate-300"><?= htmlspecialchars($oj['last_run_relative'], ENT_QUOTES) ?></td>
                            <td class="text-right font-semibold text-amber-200"><?= $overdueBy !== null ? htmlspecialchars(human_duration_seconds((float) $overdueBy), ENT_QUOTES) : 'Never ran' ?></td>
                            <td class="text-slate-300"><?= $oj['interval_seconds'] > 0 ? htmlspecialchars(human_duration_seconds((float) $oj['interval_seconds']), ENT_QUOTES) : '-' ?></td>
                            <td>
                                <span class="badge <?= htmlspecialchars($oj['health_tone'], ENT_QUOTES) ?>"><?= htmlspecialchars($oj['health_label'], ENT_QUOTES) ?></span>
                            </td>
                        </tr>
                    <?php endforeach; ?>
                </tbody>
            </table>
        </div>
    </div>
    <?php endif; ?>
</section>

<!-- ═══════════════════════════════════════════════════════════════════════════
     External Services — compact inline cards
     ══════════════════════════════════════════════════════════════════════════ -->
<section class="mt-8">
    <h2 class="section-title mb-4">External Services</h2>
    <div class="grid gap-4 sm:grid-cols-2">
        <?php foreach ($externalHealth as $svcKey => $svc): ?>
            <article class="rounded-2xl border p-4 <?= htmlspecialchars($svc['tone'], ENT_QUOTES) ?>">
                <div class="flex items-center justify-between gap-3">
                    <div class="flex items-center gap-3">
                        <span class="badge <?= htmlspecialchars($svc['tone'], ENT_QUOTES) ?>"><?= htmlspecialchars($svc['label'], ENT_QUOTES) ?></span>
                        <span class="font-medium text-white"><?= htmlspecialchars($svc['name'], ENT_QUOTES) ?></span>
                    </div>
                    <span class="text-sm text-slate-300"><?= $svc['latency_ms'] ?>ms<?php if ($svc['version']): ?> · <?= htmlspecialchars((string) $svc['version'], ENT_QUOTES) ?><?php endif; ?></span>
                </div>
            </article>
        <?php endforeach; ?>
    </div>
</section>

<!-- ═══════════════════════════════════════════════════════════════════════════
     Stuck / Timed-out Runs — grouped by job
     ══════════════════════════════════════════════════════════════════════════ -->
<?php if ($stuckRuns !== []): ?>
<section class="mt-8">
    <h2 class="section-title mb-4 text-orange-200">Stuck / Timed-out Runs</h2>
    <div class="rounded-2xl border border-orange-400/20 bg-orange-500/6 p-1">
        <div class="table-shell overflow-x-auto">
            <table class="table-ui w-full">
                <thead>
                    <tr>
                        <th class="text-left">Job</th>
                        <th class="text-right">Stuck instances</th>
                        <th class="text-left">Oldest</th>
                        <th class="text-left">Newest</th>
                        <th class="text-right">Running for</th>
                    </tr>
                </thead>
                <tbody>
                    <?php foreach ($stuckRuns as $run): ?>
                        <tr>
                            <td class="font-medium text-white"><?= htmlspecialchars($run['dataset_key'], ENT_QUOTES) ?></td>
                            <td class="text-right">
                                <span class="inline-flex items-center rounded-full border border-orange-400/25 bg-orange-500/12 px-2.5 py-0.5 text-xs font-semibold text-orange-100">
                                    <?= $run['count'] ?>
                                </span>
                            </td>
                            <td class="text-slate-300"><?= htmlspecialchars(supplycore_format_datetime($run['oldest_started_at']), ENT_QUOTES) ?></td>
                            <td class="text-slate-300"><?= htmlspecialchars(supplycore_format_datetime($run['newest_started_at']), ENT_QUOTES) ?></td>
                            <td class="text-right font-semibold text-orange-200"><?= htmlspecialchars(human_duration_seconds((float) $run['max_running_seconds']), ENT_QUOTES) ?></td>
                        </tr>
                    <?php endforeach; ?>
                </tbody>
            </table>
        </div>
    </div>
</section>
<?php endif; ?>

<!-- ═══════════════════════════════════════════════════════════════════════════
     Recent Failures — grouped by error pattern
     ══════════════════════════════════════════════════════════════════════════ -->
<?php if ($failedRuns !== []): ?>
<section class="mt-8">
    <h2 class="section-title mb-4 text-rose-200">Recent Failures <span class="text-sm font-normal text-slate-400">(last 24 h)</span></h2>
    <div class="rounded-2xl border border-rose-400/20 bg-rose-500/6 p-1">
        <div class="table-shell overflow-x-auto">
            <table class="table-ui w-full">
                <thead>
                    <tr>
                        <th class="text-left">Job</th>
                        <th class="text-right">Occurrences</th>
                        <th class="text-left">Last seen</th>
                        <th class="text-left">First seen</th>
                        <th class="text-left">Error</th>
                    </tr>
                </thead>
                <tbody>
                    <?php foreach ($failedRuns as $run): ?>
                        <tr>
                            <td class="font-medium text-white"><?= htmlspecialchars($run['dataset_key'], ENT_QUOTES) ?></td>
                            <td class="text-right">
                                <span class="inline-flex items-center rounded-full border <?= $run['count'] > 3 ? 'border-rose-400/25 bg-rose-500/12 text-rose-100' : 'border-amber-400/25 bg-amber-500/12 text-amber-100' ?> px-2.5 py-0.5 text-xs font-semibold">
                                    <?= $run['count'] ?>&times;
                                </span>
                            </td>
                            <td class="text-slate-300"><?= htmlspecialchars(supplycore_relative_datetime($run['latest_started_at']), ENT_QUOTES) ?></td>
                            <td class="text-slate-300"><?= htmlspecialchars(supplycore_relative_datetime($run['oldest_started_at']), ENT_QUOTES) ?></td>
                            <td class="max-w-sm">
                                <p class="truncate text-xs text-rose-200" title="<?= htmlspecialchars((string) ($run['error_message'] ?? ''), ENT_QUOTES) ?>"><?= htmlspecialchars((string) ($run['error_message'] ?? 'No message'), ENT_QUOTES) ?></p>
                            </td>
                        </tr>
                    <?php endforeach; ?>
                </tbody>
            </table>
        </div>
    </div>
</section>
<?php endif; ?>
<!-- ui-section:log-viewer-backlog:end -->

<!-- ═══════════════════════════════════════════════════════════════════════════
     Job Status Table — grouped by pipeline tier
     ══════════════════════════════════════════════════════════════════════════ -->
<!-- ui-section:log-viewer-jobs:start -->
<section class="mt-8" data-ui-section="log-viewer-jobs">
    <div class="mb-4 flex flex-wrap items-center justify-between gap-3">
        <h2 class="section-title">All Jobs</h2>
        <div class="flex flex-wrap gap-2">
            <?php
            $filterOptions = [
                'all' => ['label' => 'All (' . count($jobs) . ')', 'tone' => ''],
                'failed' => ['label' => 'Failed (' . $kpi['total_failed'] . ')', 'tone' => $kpi['total_failed'] > 0 ? 'text-rose-200' : ''],
                'timeout' => ['label' => 'Timeout (' . $kpi['total_timeout'] . ')', 'tone' => $kpi['total_timeout'] > 0 ? 'text-orange-200' : ''],
                'overdue' => ['label' => 'Overdue (' . $kpi['total_overdue'] . ')', 'tone' => $kpi['total_overdue'] > 0 ? 'text-amber-200' : ''],
                'never_ran' => ['label' => 'Never ran (' . $kpi['total_never_ran'] . ')', 'tone' => $kpi['total_never_ran'] > 0 ? 'text-violet-200' : ''],
                'healthy' => ['label' => 'Healthy (' . $kpi['total_healthy'] . ')', 'tone' => ''],
                'disabled' => ['label' => 'Disabled', 'tone' => ''],
            ];
            foreach ($filterOptions as $fKey => $fOpt): ?>
                <a href="?filter=<?= $fKey ?>"
                   class="rounded-lg border px-3 py-1.5 text-xs font-medium transition <?= $filter === $fKey ? 'border-sky-400/40 bg-sky-500/15 text-sky-100' : 'border-white/10 bg-white/5 text-slate-300 hover:bg-white/10' ?> <?= $fOpt['tone'] ?>">
                    <?= htmlspecialchars($fOpt['label'], ENT_QUOTES) ?>
                </a>
            <?php endforeach; ?>
        </div>
    </div>

    <?php
    // Group filtered jobs by tier, sorted by tier number
    $jobsByTier = [];
    foreach ($filteredJobs as $job) {
        $tier = $job['tier'] ?? 5;
        $jobsByTier[$tier][] = $job;
    }
    ksort($jobsByTier);

    $tierTones = [
        1 => 'border-sky-400/30 text-sky-200',
        2 => 'border-teal-400/30 text-teal-200',
        3 => 'border-indigo-400/30 text-indigo-200',
        4 => 'border-purple-400/30 text-purple-200',
        5 => 'border-amber-400/30 text-amber-200',
        6 => 'border-slate-400/30 text-slate-300',
    ];
    ?>

    <div class="rounded-2xl border border-white/8 bg-white/[0.02] p-1">
        <div class="table-shell overflow-x-auto">
            <table class="table-ui w-full">
                <thead>
                    <tr>
                        <th class="text-left">Job</th>
                        <th class="text-left">Status</th>
                        <th class="text-left">Last run</th>
                        <th class="text-left">Last success</th>
                        <th class="text-right" title="Duration of the most recent run (elapsed so far if still running)">Last duration</th>
                        <th class="text-left">Interval</th>
                        <th class="text-left">Pressure</th>
                        <th class="text-left">Issue</th>
                    </tr>
                </thead>
                <tbody>
                    <?php if ($filteredJobs === []): ?>
                        <tr><td colspan="8" class="py-8 text-center text-slate-400">No jobs match this filter.</td></tr>
                    <?php endif; ?>
                    <?php foreach ($jobsByTier as $tierNum => $tierJobs):
                        $tierLabel = $tierJobs[0]['tier_label'] ?? "Tier {$tierNum}";
                        $tierTone = $tierTones[$tierNum] ?? $tierTones[6];
                        $tierHealthy = count(array_filter($tierJobs, fn ($j) => $j['health'] === 'healthy'));
                        $tierTotal = count($tierJobs);
                        $tierFailed = count(array_filter($tierJobs, fn ($j) => $j['health'] === 'failed' || $j['health'] === 'stuck'));
                    ?>
                        <tr class="bg-white/[0.03]">
                            <td colspan="8" class="py-2.5 px-4">
                                <div class="flex items-center gap-3">
                                    <span class="inline-flex items-center rounded-md border px-2 py-0.5 text-xs font-bold <?= $tierTone ?>">T<?= $tierNum ?></span>
                                    <span class="font-semibold text-white text-sm"><?= htmlspecialchars($tierLabel, ENT_QUOTES) ?></span>
                                    <span class="text-xs text-slate-400"><?= $tierHealthy ?>/<?= $tierTotal ?> healthy<?php if ($tierFailed > 0): ?> · <span class="text-rose-300"><?= $tierFailed ?> failed</span><?php endif; ?></span>
                                </div>
                            </td>
                        </tr>
                        <?php foreach ($tierJobs as $job): ?>
                        <tr class="<?= $job['overdue'] ? 'bg-amber-500/5' : '' ?>">
                            <td>
                                <p class="font-medium text-white"><?= htmlspecialchars($job['label'], ENT_QUOTES) ?></p>
                                <p class="mt-0.5 text-xs text-slate-400"><?= htmlspecialchars($job['job_key'], ENT_QUOTES) ?></p>
                            </td>
                            <td>
                                <span class="badge <?= htmlspecialchars($job['health_tone'], ENT_QUOTES) ?>">
                                    <?= htmlspecialchars($job['health_label'], ENT_QUOTES) ?>
                                </span>
                                <?php if ($job['overdue']): ?>
                                    <span class="badge border-amber-400/20 bg-amber-500/10 text-amber-100 ml-1">Overdue</span>
                                <?php endif; ?>
                            </td>
                            <td class="text-slate-300"><?= htmlspecialchars($job['last_run_relative'], ENT_QUOTES) ?></td>
                            <td class="text-slate-300"><?= htmlspecialchars($job['last_success_relative'], ENT_QUOTES) ?></td>
                            <?php
                                $lastDur = $job['last_duration_seconds'] ?? null;
                                $interval = (int) $job['interval_seconds'];
                                // Tone: amber if duration exceeds 80% of interval
                                // (too slow for its schedule), rose if it exceeds
                                // the interval entirely, slate otherwise.
                                $durTone = 'text-slate-300';
                                if ($lastDur !== null && $interval > 0) {
                                    if ($lastDur >= $interval) {
                                        $durTone = 'text-rose-300';
                                    } elseif ($lastDur >= (int) ($interval * 0.8)) {
                                        $durTone = 'text-amber-300';
                                    }
                                }
                            ?>
                            <td class="text-right tabular-nums <?= $durTone ?>"><?= $lastDur !== null ? htmlspecialchars(human_duration_seconds((float) $lastDur), ENT_QUOTES) : '<span class="text-slate-500">-</span>' ?></td>
                            <td class="text-slate-300"><?= $job['interval_seconds'] > 0 ? htmlspecialchars(human_duration_seconds((float) $job['interval_seconds']), ENT_QUOTES) : '-' ?></td>
                            <td>
                                <?php
                                $pressureTone = match ($job['pressure_state']) {
                                    'critical' => 'text-rose-300',
                                    'elevated' => 'text-amber-300',
                                    default => 'text-emerald-300',
                                };
                                ?>
                                <span class="text-sm <?= $pressureTone ?>"><?= htmlspecialchars(ucfirst($job['pressure_state']), ENT_QUOTES) ?></span>
                            </td>
                            <td class="max-w-xs">
                                <?php if ($job['last_failure_message']): ?>
                                    <p class="truncate text-xs text-rose-200" title="<?= htmlspecialchars($job['last_failure_message'], ENT_QUOTES) ?>"><?= htmlspecialchars($job['last_failure_message'], ENT_QUOTES) ?></p>
                                <?php elseif ($job['recent_timeout_count'] > 0): ?>
                                    <p class="text-xs text-orange-200"><?= $job['recent_timeout_count'] ?> recent timeout(s)</p>
                                <?php elseif ($job['recent_deferral_count'] > 0): ?>
                                    <p class="text-xs text-slate-400"><?= $job['recent_deferral_count'] ?> deferral(s)</p>
                                <?php elseif ($job['last_planner_reason']): ?>
                                    <p class="truncate text-xs text-slate-400" title="<?= htmlspecialchars($job['last_planner_reason'], ENT_QUOTES) ?>"><?= htmlspecialchars($job['last_planner_reason'], ENT_QUOTES) ?></p>
                                <?php elseif ($job['last_run_summary']): ?>
                                    <p class="truncate text-xs text-slate-400" title="<?= htmlspecialchars($job['last_run_summary'], ENT_QUOTES) ?>"><?= htmlspecialchars($job['last_run_summary'], ENT_QUOTES) ?></p>
                                <?php else: ?>
                                    <span class="text-xs text-slate-500">-</span>
                                <?php endif; ?>
                            </td>
                        </tr>
                        <?php endforeach; ?>
                    <?php endforeach; ?>
                </tbody>
            </table>
        </div>
    </div>
</section>
<!-- ui-section:log-viewer-jobs:end -->

<!-- ═══════════════════════════════════════════════════════════════════════════
     Scheduler Cycle Report — parsed from storage/logs/scheduler-report.jsonl
     ══════════════════════════════════════════════════════════════════════════ -->
<?php if ($schedulerCycles !== []): ?>
<section class="mt-8" data-ui-section="log-viewer-scheduler-cycles">
    <h2 class="section-title mb-4">Scheduler Cycles</h2>
    <p class="mb-3 text-xs text-slate-400">
        Last <?= count($schedulerCycles) ?> cycles parsed from
        <code class="text-slate-300">storage/logs/scheduler-report.jsonl</code> (one row per lane per cycle, newest first).
    </p>
    <div class="rounded-2xl border border-white/8 bg-white/[0.02] p-1">
        <div class="table-shell overflow-x-auto">
            <table class="table-ui w-full">
                <thead>
                    <tr>
                        <th class="text-left">Lane</th>
                        <th class="text-right">#</th>
                        <th class="text-left">Started</th>
                        <th class="text-right">Duration</th>
                        <th class="text-right" title="ran / due / total">Ran/Due/Total</th>
                        <th class="text-right">OK</th>
                        <th class="text-right">Failed</th>
                        <th class="text-right" title="due jobs whose deps weren't satisfied this cycle">Blocked</th>
                        <th class="text-left">Slowest job</th>
                        <th class="text-right">Memory</th>
                        <th class="text-left">Failures</th>
                    </tr>
                </thead>
                <tbody>
                    <?php foreach ($schedulerCycles as $sc):
                        $rowTone = $sc['has_failures']
                            ? 'bg-rose-500/5'
                            : ($sc['jobs_ran'] > 0 ? '' : 'opacity-60');
                        $startedTitle = $sc['started_at'] !== null
                            ? $sc['started_at'] . ' (UTC)'
                            : '';
                        $finishedTitle = $sc['finished_at'] !== null
                            ? 'finished ' . $sc['finished_at'] . ' (UTC)'
                            : '';
                        $memMb = $sc['memory_bytes'] > 0
                            ? number_format($sc['memory_bytes'] / 1048576, 0) . ' MB'
                            : '-';
                    ?>
                        <tr class="<?= $rowTone ?>">
                            <td class="font-medium text-white">
                                <?= htmlspecialchars($sc['lane'], ENT_QUOTES) ?>
                            </td>
                            <td class="text-right text-slate-400"><?= $sc['cycle'] ?></td>
                            <td class="text-slate-300" title="<?= htmlspecialchars($startedTitle . ($finishedTitle !== '' ? ' — ' . $finishedTitle : ''), ENT_QUOTES) ?>">
                                <?= htmlspecialchars(supplycore_relative_datetime($sc['started_at']), ENT_QUOTES) ?>
                            </td>
                            <td class="text-right text-slate-300">
                                <?= htmlspecialchars(human_duration_seconds($sc['duration_seconds']), ENT_QUOTES) ?>
                            </td>
                            <td class="text-right text-slate-400">
                                <?= $sc['jobs_ran'] ?> / <?= $sc['jobs_due'] ?> / <?= $sc['jobs_total'] ?>
                            </td>
                            <td class="text-right">
                                <?php if ($sc['jobs_succeeded'] > 0): ?>
                                    <span class="text-emerald-300"><?= $sc['jobs_succeeded'] ?></span>
                                <?php else: ?>
                                    <span class="text-slate-500">-</span>
                                <?php endif; ?>
                            </td>
                            <td class="text-right">
                                <?php if ($sc['jobs_failed'] > 0): ?>
                                    <span class="text-rose-300"><?= $sc['jobs_failed'] ?></span>
                                <?php else: ?>
                                    <span class="text-slate-500">-</span>
                                <?php endif; ?>
                            </td>
                            <td class="text-right">
                                <?php if ($sc['jobs_blocked_by_deps'] > 0): ?>
                                    <span class="text-amber-300"><?= $sc['jobs_blocked_by_deps'] ?></span>
                                <?php else: ?>
                                    <span class="text-slate-500">-</span>
                                <?php endif; ?>
                            </td>
                            <td class="max-w-xs truncate text-xs text-slate-300" title="<?= htmlspecialchars($sc['slowest_job'], ENT_QUOTES) ?>">
                                <?php if ($sc['slowest_job'] !== ''): ?>
                                    <?= htmlspecialchars($sc['slowest_job'], ENT_QUOTES) ?>
                                    <span class="text-slate-500">(<?= htmlspecialchars(human_duration_seconds($sc['slowest_seconds']), ENT_QUOTES) ?>)</span>
                                <?php else: ?>
                                    <span class="text-slate-500">-</span>
                                <?php endif; ?>
                            </td>
                            <td class="text-right text-xs text-slate-400"><?= $memMb ?></td>
                            <td class="max-w-md">
                                <?php if ($sc['failures'] !== []): ?>
                                    <ul class="space-y-0.5">
                                        <?php foreach (array_slice($sc['failures'], 0, 4) as $f):
                                            $jobKey = (string) ($f['job_key'] ?? '?');
                                            $err = (string) ($f['error'] ?? '');
                                        ?>
                                            <li class="truncate text-xs text-rose-200" title="<?= htmlspecialchars($jobKey . ': ' . $err, ENT_QUOTES) ?>">
                                                <span class="font-medium"><?= htmlspecialchars($jobKey, ENT_QUOTES) ?>:</span>
                                                <?= htmlspecialchars($err, ENT_QUOTES) ?>
                                            </li>
                                        <?php endforeach; ?>
                                        <?php if (count($sc['failures']) > 4): ?>
                                            <li class="text-xs text-rose-300">+<?= count($sc['failures']) - 4 ?> more</li>
                                        <?php endif; ?>
                                    </ul>
                                <?php else: ?>
                                    <span class="text-xs text-slate-500">-</span>
                                <?php endif; ?>
                            </td>
                        </tr>
                    <?php endforeach; ?>
                </tbody>
            </table>
        </div>
    </div>
</section>
<?php endif; ?>

<!-- ═══════════════════════════════════════════════════════════════════════════
     Log Files on Disk — collapsible; each file also links to the structured
     viewer (/log-viewer/file) which pretty-prints JSONL lane-*.log output.
     ══════════════════════════════════════════════════════════════════════════ -->
<?php if ($logFiles !== []): ?>
<section class="mt-8">
    <h2 class="section-title mb-4">Log Files</h2>
    <p class="mb-3 text-xs text-slate-400">
        Click <span class="text-sky-300">Open</span> on any lane-*.log or *.jsonl file
        to load it in the structured viewer (filterable by level, search, tail length).
    </p>
    <div class="space-y-2">
        <?php foreach ($logFiles as $lf):
            $viewerHref = '/log-viewer/file?' . http_build_query(['name' => $lf['filename']]);
        ?>
            <details class="group rounded-2xl border border-white/8 bg-white/[0.02]">
                <summary class="flex cursor-pointer items-center justify-between gap-3 px-5 py-3 text-sm">
                    <div class="flex items-center gap-3">
                        <span class="font-medium text-white"><?= htmlspecialchars($lf['filename'], ENT_QUOTES) ?></span>
                        <span class="text-xs text-slate-400"><?= htmlspecialchars($lf['size_human'], ENT_QUOTES) ?></span>
                    </div>
                    <div class="flex items-center gap-3">
                        <span class="text-xs text-slate-500">modified <?= htmlspecialchars($lf['modified_relative'], ENT_QUOTES) ?></span>
                        <a href="<?= htmlspecialchars($viewerHref, ENT_QUOTES) ?>"
                           class="rounded-md border border-sky-400/30 bg-sky-500/15 px-2.5 py-1 text-[0.7rem] font-medium text-sky-100 hover:bg-sky-500/25"
                           onclick="event.stopPropagation();">
                            Open
                        </a>
                    </div>
                </summary>
                <?php if ($lf['tail_lines'] !== []): ?>
                    <div class="border-t border-white/8 px-5 py-3">
                        <pre class="max-h-48 overflow-auto rounded-lg bg-black/30 p-3 text-[0.7rem] leading-relaxed text-slate-300"><?php
                            foreach ($lf['tail_lines'] as $line) {
                                echo htmlspecialchars($line, ENT_QUOTES) . "\n";
                            }
                        ?></pre>
                    </div>
                <?php endif; ?>
            </details>
        <?php endforeach; ?>
    </div>
</section>
<?php endif; ?>

<!-- ═══════════════════════════════════════════════════════════════════════════
     Recent Runs — deduplicated, one row per job with run count
     ══════════════════════════════════════════════════════════════════════════ -->
<!-- ui-section:log-viewer-runs:start -->
<section class="mt-8" data-ui-section="log-viewer-runs">
    <h2 class="section-title mb-4">Recent Runs</h2>
    <div class="rounded-2xl border border-white/8 bg-white/[0.02] p-1">
        <div class="table-shell overflow-x-auto">
            <table class="table-ui w-full">
                <thead>
                    <tr>
                        <th class="text-left">Job</th>
                        <th class="text-left">Status</th>
                        <th class="text-left">Last run</th>
                        <th class="text-right">Duration</th>
                        <th class="text-right" title="written rows / source rows read">Written / Read</th>
                        <th class="text-right">Recent OK</th>
                        <th class="text-left">Summary / Error</th>
                    </tr>
                </thead>
                <tbody>
                    <?php foreach ($recentRuns as $run):
                        $runTone = match ($run['run_status']) {
                            'success' => '',
                            'failed' => 'bg-rose-500/5',
                            'running' => 'bg-sky-500/5',
                            default => '',
                        };
                        $statusBadge = match ($run['run_status']) {
                            'success' => 'border-emerald-400/20 bg-emerald-500/10 text-emerald-100',
                            'failed' => 'border-rose-400/20 bg-rose-500/10 text-rose-100',
                            'running' => 'border-sky-400/20 bg-sky-500/10 text-sky-100',
                            default => 'border-slate-400/20 bg-slate-500/10 text-slate-200',
                        };
                        $recentSuccessCount = (int) ($run['recent_success_count'] ?? 0);
                    ?>
                        <tr class="<?= $runTone ?>">
                            <td class="font-medium text-white"><?= htmlspecialchars($run['dataset_key'], ENT_QUOTES) ?></td>
                            <td><span class="badge <?= $statusBadge ?>"><?= htmlspecialchars(ucfirst($run['run_status']), ENT_QUOTES) ?></span></td>
                            <td class="text-slate-300"><?= htmlspecialchars(supplycore_relative_datetime($run['started_at']), ENT_QUOTES) ?></td>
                            <td class="text-right text-slate-300"><?= htmlspecialchars(human_duration_seconds((float) ($run['duration_seconds'] ?? 0)), ENT_QUOTES) ?></td>
                            <?php
                                $written = (int) $run['written_rows'];
                                $source  = (int) $run['source_rows'];
                                if ($written === 0 && $source === 0) {
                                    $rowsTone = 'text-slate-500';
                                    $rowsTitle = 'Nothing to process';
                                } elseif ($written === 0 && $source > 0) {
                                    $rowsTone = 'text-slate-400';
                                    $rowsTitle = 'All source rows already up to date — nothing written';
                                } elseif ($written > $source) {
                                    $rowsTone = 'text-sky-300';
                                    $rowsTitle = 'Fanout: one source row produced multiple output rows';
                                } else {
                                    $rowsTone = 'text-slate-300';
                                    $rowsTitle = 'Normal write';
                                }
                            ?>
                            <td class="text-right <?= $rowsTone ?>" title="<?= $rowsTitle ?>">
                                <?= number_format($written) ?> / <?= number_format($source) ?>
                            </td>
                            <td class="text-right">
                                <?php if ($recentSuccessCount > 1): ?>
                                    <span class="text-xs text-emerald-300"><?= $recentSuccessCount ?> runs</span>
                                <?php else: ?>
                                    <span class="text-xs text-slate-500">-</span>
                                <?php endif; ?>
                            </td>
                            <td class="max-w-xs">
                                <?php if (!empty($run['error_message'])): ?>
                                    <p class="truncate text-xs text-rose-200" title="<?= htmlspecialchars((string) $run['error_message'], ENT_QUOTES) ?>"><?= htmlspecialchars((string) $run['error_message'], ENT_QUOTES) ?></p>
                                <?php elseif (!empty($run['summary'])): ?>
                                    <p class="truncate text-xs text-slate-400" title="<?= htmlspecialchars((string) $run['summary'], ENT_QUOTES) ?>"><?= htmlspecialchars((string) $run['summary'], ENT_QUOTES) ?></p>
                                <?php endif; ?>
                            </td>
                        </tr>
                    <?php endforeach; ?>
                </tbody>
            </table>
        </div>
    </div>
</section>
<!-- ui-section:log-viewer-runs:end -->

<?php include __DIR__ . '/../../src/views/partials/footer.php'; ?>
