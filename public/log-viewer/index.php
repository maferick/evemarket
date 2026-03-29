<?php

declare(strict_types=1);

require_once __DIR__ . '/../../src/bootstrap.php';

$title = 'Log Viewer';
$pageHeaderBadge = 'System health at a glance';
$pageHeaderSummary = 'Monitor job runs, failures, timeouts, and external service connectivity. Fix issues before they cascade.';

$pageData = log_viewer_page_data();
$externalHealth = log_viewer_external_health();

$jobs = $pageData['jobs'];
$failedRuns = $pageData['failed_runs'];
$stuckRuns = $pageData['stuck_runs'];
$neverRan = $pageData['never_ran'];
$logFiles = $pageData['log_files'];
$kpi = $pageData['kpi'];
$recentRuns = $pageData['recent_runs'];

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
    'failed' => array_filter($jobs, fn (array $j) => $j['health'] === 'failed'),
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
<section class="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
    <article class="kpi-card">
        <p class="eyebrow">Needs attention</p>
        <p class="mt-3 metric-value text-[2.35rem] <?= $attentionCount > 0 ? 'text-rose-100' : 'text-emerald-100' ?>"><?= $attentionCount ?></p>
        <p class="mt-2 text-sm text-slate-300">Failed, timed-out, or overdue jobs requiring action.</p>
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

<!-- ═══════════════════════════════════════════════════════════════════════════
     External Services
     ══════════════════════════════════════════════════════════════════════════ -->
<section class="mt-8">
    <h2 class="section-title mb-4">External Services</h2>
    <div class="grid gap-4 sm:grid-cols-2">
        <?php foreach ($externalHealth as $svcKey => $svc): ?>
            <article class="rounded-2xl border p-5 <?= htmlspecialchars($svc['tone'], ENT_QUOTES) ?>">
                <div class="flex items-start justify-between gap-3">
                    <div>
                        <p class="text-xs font-semibold uppercase tracking-[0.16em] text-current/70"><?= htmlspecialchars($svc['name'], ENT_QUOTES) ?></p>
                        <h3 class="mt-2 text-lg font-semibold text-white"><?= htmlspecialchars($svc['label'], ENT_QUOTES) ?></h3>
                        <p class="mt-2 text-sm text-slate-200"><?= htmlspecialchars($svc['detail'], ENT_QUOTES) ?></p>
                    </div>
                    <span class="badge <?= htmlspecialchars($svc['tone'], ENT_QUOTES) ?>"><?= htmlspecialchars($svc['label'], ENT_QUOTES) ?></span>
                </div>
                <div class="mt-4 grid gap-3 text-sm text-slate-200 sm:grid-cols-3">
                    <div class="rounded-lg border border-white/10 bg-black/20 p-3">
                        <p class="text-xs uppercase tracking-[0.16em] text-slate-400">Latency</p>
                        <p class="mt-2 font-semibold text-white"><?= $svc['latency_ms'] ?>ms</p>
                    </div>
                    <div class="rounded-lg border border-white/10 bg-black/20 p-3">
                        <p class="text-xs uppercase tracking-[0.16em] text-slate-400">Version</p>
                        <p class="mt-2 font-semibold text-white"><?= htmlspecialchars((string) ($svc['version'] ?? 'N/A'), ENT_QUOTES) ?></p>
                    </div>
                    <div class="rounded-lg border border-white/10 bg-black/20 p-3">
                        <p class="text-xs uppercase tracking-[0.16em] text-slate-400">Endpoint</p>
                        <p class="mt-2 font-semibold text-white truncate" title="<?= htmlspecialchars($svc['url'], ENT_QUOTES) ?>"><?= htmlspecialchars($svc['url'], ENT_QUOTES) ?></p>
                    </div>
                </div>
            </article>
        <?php endforeach; ?>
    </div>
</section>

<!-- ═══════════════════════════════════════════════════════════════════════════
     Stuck / Timed-out Runs (if any)
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
                        <th class="text-left">Started</th>
                        <th class="text-right">Running for</th>
                    </tr>
                </thead>
                <tbody>
                    <?php foreach ($stuckRuns as $run): ?>
                        <tr>
                            <td class="font-medium text-white"><?= htmlspecialchars($run['dataset_key'], ENT_QUOTES) ?></td>
                            <td class="text-slate-300"><?= htmlspecialchars(supplycore_format_datetime($run['started_at']), ENT_QUOTES) ?></td>
                            <td class="text-right font-semibold text-orange-200"><?= htmlspecialchars(human_duration_seconds((float) $run['running_seconds']), ENT_QUOTES) ?></td>
                        </tr>
                    <?php endforeach; ?>
                </tbody>
            </table>
        </div>
    </div>
</section>
<?php endif; ?>

<!-- ═══════════════════════════════════════════════════════════════════════════
     Recent Failures (24 h)
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
                        <th class="text-left">Started</th>
                        <th class="text-right">Duration</th>
                        <th class="text-left">Error</th>
                    </tr>
                </thead>
                <tbody>
                    <?php foreach ($failedRuns as $run): ?>
                        <tr>
                            <td class="font-medium text-white"><?= htmlspecialchars($run['dataset_key'], ENT_QUOTES) ?></td>
                            <td class="text-slate-300"><?= htmlspecialchars(supplycore_relative_datetime($run['started_at']), ENT_QUOTES) ?></td>
                            <td class="text-right text-slate-300"><?= htmlspecialchars(human_duration_seconds((float) $run['duration_seconds']), ENT_QUOTES) ?></td>
                            <td class="max-w-xs truncate text-rose-200" title="<?= htmlspecialchars((string) ($run['error_message'] ?? ''), ENT_QUOTES) ?>"><?= htmlspecialchars((string) ($run['error_message'] ?? 'No message'), ENT_QUOTES) ?></td>
                        </tr>
                    <?php endforeach; ?>
                </tbody>
            </table>
        </div>
    </div>
</section>
<?php endif; ?>

<!-- ═══════════════════════════════════════════════════════════════════════════
     Job Status Table
     ══════════════════════════════════════════════════════════════════════════ -->
<section class="mt-8">
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

    <div class="rounded-2xl border border-white/8 bg-white/[0.02] p-1">
        <div class="table-shell overflow-x-auto">
            <table class="table-ui w-full">
                <thead>
                    <tr>
                        <th class="text-left">Job</th>
                        <th class="text-left">Status</th>
                        <th class="text-left">Last run</th>
                        <th class="text-left">Last success</th>
                        <th class="text-left">Interval</th>
                        <th class="text-left">Pressure</th>
                        <th class="text-left">Issue</th>
                    </tr>
                </thead>
                <tbody>
                    <?php if ($filteredJobs === []): ?>
                        <tr><td colspan="7" class="py-8 text-center text-slate-400">No jobs match this filter.</td></tr>
                    <?php endif; ?>
                    <?php foreach ($filteredJobs as $job): ?>
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

<!-- ═══════════════════════════════════════════════════════════════════════════
     Log Files on Disk
     ══════════════════════════════════════════════════════════════════════════ -->
<?php if ($logFiles !== []): ?>
<section class="mt-8">
    <h2 class="section-title mb-4">Log Files</h2>
    <div class="grid gap-4 sm:grid-cols-2 xl:grid-cols-3">
        <?php foreach ($logFiles as $lf): ?>
            <article class="surface-secondary rounded-2xl p-4">
                <div class="flex items-start justify-between gap-2">
                    <div>
                        <p class="font-medium text-white"><?= htmlspecialchars($lf['filename'], ENT_QUOTES) ?></p>
                        <p class="mt-1 text-xs text-slate-400"><?= htmlspecialchars($lf['size_human'], ENT_QUOTES) ?> · modified <?= htmlspecialchars($lf['modified_relative'], ENT_QUOTES) ?></p>
                    </div>
                    <span class="badge border-white/10 bg-white/5 text-slate-300"><?= htmlspecialchars($lf['size_human'], ENT_QUOTES) ?></span>
                </div>
                <?php if ($lf['tail_lines'] !== []): ?>
                    <div class="mt-3 rounded-lg border border-white/8 bg-black/30 p-3">
                        <p class="mb-2 text-[0.65rem] font-semibold uppercase tracking-[0.2em] text-slate-500">Last <?= count($lf['tail_lines']) ?> lines</p>
                        <pre class="max-h-32 overflow-auto text-[0.7rem] leading-relaxed text-slate-300"><?php
                            foreach ($lf['tail_lines'] as $line) {
                                echo htmlspecialchars($line, ENT_QUOTES) . "\n";
                            }
                        ?></pre>
                    </div>
                <?php endif; ?>
            </article>
        <?php endforeach; ?>
    </div>
</section>
<?php endif; ?>

<!-- ═══════════════════════════════════════════════════════════════════════════
     Recent Runs Timeline
     ══════════════════════════════════════════════════════════════════════════ -->
<section class="mt-8">
    <h2 class="section-title mb-4">Recent Runs <span class="text-sm font-normal text-slate-400">(last 200)</span></h2>
    <div class="rounded-2xl border border-white/8 bg-white/[0.02] p-1">
        <div class="table-shell overflow-x-auto">
            <table class="table-ui w-full">
                <thead>
                    <tr>
                        <th class="text-left">Job</th>
                        <th class="text-left">Status</th>
                        <th class="text-left">Started</th>
                        <th class="text-right">Duration</th>
                        <th class="text-right">Rows</th>
                        <th class="text-left">Error</th>
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
                    ?>
                        <tr class="<?= $runTone ?>">
                            <td class="font-medium text-white"><?= htmlspecialchars($run['dataset_key'], ENT_QUOTES) ?></td>
                            <td><span class="badge <?= $statusBadge ?>"><?= htmlspecialchars(ucfirst($run['run_status']), ENT_QUOTES) ?></span></td>
                            <td class="text-slate-300"><?= htmlspecialchars(supplycore_relative_datetime($run['started_at']), ENT_QUOTES) ?></td>
                            <td class="text-right text-slate-300"><?= htmlspecialchars(human_duration_seconds((float) ($run['duration_seconds'] ?? 0)), ENT_QUOTES) ?></td>
                            <td class="text-right text-slate-300"><?= (int) $run['written_rows'] ?> / <?= (int) $run['source_rows'] ?></td>
                            <td class="max-w-xs truncate text-xs text-rose-200" title="<?= htmlspecialchars((string) ($run['error_message'] ?? ''), ENT_QUOTES) ?>"><?= htmlspecialchars((string) ($run['error_message'] ?? ''), ENT_QUOTES) ?></td>
                        </tr>
                    <?php endforeach; ?>
                </tbody>
            </table>
        </div>
    </div>
</section>

<?php include __DIR__ . '/../../src/views/partials/footer.php'; ?>
