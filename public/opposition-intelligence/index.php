<?php

declare(strict_types=1);
require_once __DIR__ . '/../../src/bootstrap.php';

$title = 'Opposition Intelligence — Daily SITREP';

// Guarded fetches — tables may not exist yet if the opposition intel migration hasn't run.
$tablesReadyError = null;
$latestDate = null;
$globalBriefing = null;
$allianceBriefings = [];
$snapshots = [];
$recentBriefings = [];

try {
    $latestDate = db_opposition_latest_snapshot_date();
} catch (Throwable $e) {
    $tablesReadyError = $e->getMessage();
}

$requestedDate = isset($_GET['date']) ? trim((string) $_GET['date']) : null;
$date = $requestedDate ?: ($latestDate ?: gmdate('Y-m-d'));

if ($tablesReadyError === null) {
    try {
        $globalBriefing = db_opposition_daily_briefing($date, 'global');
        $allianceBriefings = db_opposition_alliance_briefings($date);
        $snapshots = db_opposition_daily_snapshots($date);
        $recentBriefings = db_opposition_daily_briefings_recent(14);
    } catch (Throwable $e) {
        $tablesReadyError = $e->getMessage();
    }
}

// Threat assessment color map
$threatColors = [
    'critical' => 'bg-red-500/20 text-red-300 border-red-500/30',
    'high' => 'bg-orange-500/20 text-orange-300 border-orange-500/30',
    'elevated' => 'bg-amber-500/20 text-amber-300 border-amber-500/30',
    'moderate' => 'bg-sky-500/20 text-sky-300 border-sky-500/30',
    'low' => 'bg-emerald-500/20 text-emerald-300 border-emerald-500/30',
];

// Aggregate stats
$totalKills = array_sum(array_column($snapshots, 'kills'));
$totalLosses = array_sum(array_column($snapshots, 'losses'));
$totalIskDestroyed = array_sum(array_column($snapshots, 'isk_destroyed'));
$activeAlliances = count(array_filter($snapshots, static fn ($s): bool => ((int) $s['kills'] > 0 || (int) $s['losses'] > 0)));

// Human-readable ISK formatter: M (millions), B (billions), T (trillions).
$formatIsk = static function (float $value): string {
    if ($value <= 0) return '0';
    if ($value >= 1_000_000_000_000) return number_format($value / 1_000_000_000_000, 2) . 'T';
    if ($value >= 1_000_000_000) return number_format($value / 1_000_000_000, 2) . 'B';
    if ($value >= 1_000_000) return number_format($value / 1_000_000, 1) . 'M';
    if ($value >= 1_000) return number_format($value / 1_000, 1) . 'K';
    return number_format($value, 0);
};

// Resolve solar system names in bulk so the table shows "Jita" instead of "System #30000142".
$systemIdsToResolve = [];
foreach ($snapshots as $s) {
    $systems = $s['active_systems_json'] ?? [];
    if (is_string($systems)) $systems = json_decode($systems, true) ?: [];
    foreach ($systems as $sys) {
        if (isset($sys['system_id'])) {
            $systemIdsToResolve[(int) $sys['system_id']] = true;
        }
    }
}
$systemNameMap = [];
if ($systemIdsToResolve !== []) {
    try {
        foreach (db_ref_systems_by_ids(array_keys($systemIdsToResolve)) as $row) {
            $systemNameMap[(int) $row['system_id']] = (string) $row['system_name'];
        }
    } catch (Throwable $e) {
        // Non-fatal: fall back to whatever name is in the snapshot JSON.
    }
}

include __DIR__ . '/../../src/views/partials/header.php';
?>

<section class="surface-primary">
    <div class="flex items-center justify-between gap-4">
        <div>
            <p class="text-xs uppercase tracking-[0.16em] text-muted">Intelligence Center</p>
            <h1 class="mt-1 text-2xl font-semibold text-slate-50">Opposition Intelligence</h1>
            <p class="mt-2 text-sm text-muted">AI-generated daily SITREP briefings on opponent alliance activity, geographic presence, fleet composition, and threat assessment.</p>
        </div>
        <div class="flex items-center gap-3">
            <?php if ($globalBriefing && ($globalBriefing['threat_assessment'] ?? '')): ?>
                <?php $ta = $globalBriefing['threat_assessment']; ?>
                <span class="inline-block rounded-md border px-3 py-1.5 text-xs font-semibold uppercase tracking-wider <?= $threatColors[$ta] ?? $threatColors['moderate'] ?>">
                    Threat: <?= htmlspecialchars(strtoupper($ta)) ?>
                </span>
            <?php endif; ?>
            <a href="/alliance-dossiers" class="btn-secondary">Alliance Dossiers</a>
            <a href="/theater-intelligence" class="btn-secondary">Theater Intel</a>
            <form method="post" action="/opposition-intelligence/generate.php" class="inline"
                  onsubmit="this.querySelector('button').disabled=true;this.querySelector('button').innerText='Generating…';">
                <input type="hidden" name="_token" value="<?= htmlspecialchars(csrf_token(), ENT_QUOTES) ?>">
                <input type="hidden" name="date" value="<?= htmlspecialchars($date, ENT_QUOTES) ?>">
                <?php
                $oppoProviderLabel = ollama_provider_options()[(string) ($ollamaConfig['provider'] ?? 'local')] ?? 'AI';
                $oppoFeatureProvider = get_setting('ai_provider_opposition_global', 'default');
                if ($oppoFeatureProvider !== 'default' && isset(ollama_provider_options()[$oppoFeatureProvider])) {
                    $oppoProviderLabel = ollama_provider_options()[$oppoFeatureProvider];
                }
                ?>
                <button type="submit" class="btn-primary"
                        title="Queue a SITREP generation job for this date. Runs in the background via the AI worker.">
                    Generate Intel (<?= htmlspecialchars($oppoProviderLabel, ENT_QUOTES) ?>)
                </button>
            </form>
        </div>
    </div>
</section>

<?php
if (session_status() === PHP_SESSION_NONE) {
    @session_start();
}
$flash = $_SESSION['opposition_intel_flash'] ?? null;
if ($flash !== null) {
    unset($_SESSION['opposition_intel_flash']);
}
?>
<?php if ($flash): ?>
    <?php $flashStatus = (string) ($flash['status'] ?? ''); ?>
    <?php if ($flashStatus === 'queued'): ?>
        <section class="surface-primary mt-4 border border-sky-500/30 bg-sky-500/10">
            <p class="text-sm text-sky-200">
                <strong>Intel generation queued.</strong>
                Job #<?= (int) ($flash['job_id'] ?? 0) ?> is running in the background for
                <?= htmlspecialchars((string) ($flash['date'] ?? ''), ENT_QUOTES) ?>.
                Refresh this page in a minute to see the result.
            </p>
        </section>
    <?php elseif ($flashStatus === 'disabled'): ?>
        <section class="surface-primary mt-4 border border-amber-500/30 bg-amber-500/10">
            <p class="text-sm text-amber-200">
                <strong>Intel generation disabled.</strong>
                Enable AI briefings in Settings to queue a job.
            </p>
        </section>
    <?php else: ?>
        <section class="surface-primary mt-4 border border-red-500/30 bg-red-500/10">
            <p class="text-sm text-red-200">
                <strong>Intel generation failed.</strong>
                <?= htmlspecialchars((string) ($flash['error'] ?? 'Unknown error'), ENT_QUOTES) ?>
            </p>
        </section>
    <?php endif; ?>
<?php endif; ?>

<?php if ($tablesReadyError !== null): ?>
<section class="surface-primary mt-4 border border-amber-500/30 bg-amber-500/10">
    <p class="text-sm text-amber-200"><strong>Opposition Intelligence tables not ready.</strong> Run the migration <code>database/migrations/20260405_opposition_daily_intelligence.sql</code> and then the <code>compute_opposition_daily_snapshots</code> job to populate data.</p>
    <p class="mt-2 text-xs text-amber-200/70">Error: <?= htmlspecialchars($tablesReadyError, ENT_QUOTES) ?></p>
</section>
<?php endif; ?>

<!-- Date navigation -->
<section class="surface-primary mt-4">
    <div class="flex items-center gap-4">
        <span class="text-xs uppercase tracking-wider text-muted">Briefing Date:</span>
        <?php
        $prevDate = date('Y-m-d', strtotime($date . ' -1 day'));
        $nextDate = date('Y-m-d', strtotime($date . ' +1 day'));
        $isToday = $date === gmdate('Y-m-d');
        ?>
        <a href="?date=<?= urlencode($prevDate) ?>" class="btn-secondary text-xs px-2 py-1">&larr; Prev</a>
        <span class="text-sm font-medium text-slate-100"><?= htmlspecialchars($date) ?></span>
        <?php if (!$isToday): ?>
            <a href="?date=<?= urlencode($nextDate) ?>" class="btn-secondary text-xs px-2 py-1">Next &rarr;</a>
            <a href="?date=<?= urlencode(gmdate('Y-m-d')) ?>" class="text-xs text-accent ml-2">Today</a>
        <?php endif; ?>
    </div>
</section>

<!-- KPI Summary Cards -->
<section class="mt-4 grid grid-cols-2 gap-4 sm:grid-cols-4">
    <div class="surface-primary text-center">
        <p class="text-xs uppercase tracking-wider text-muted">Active Alliances</p>
        <p class="mt-1 text-2xl font-bold text-slate-100"><?= $activeAlliances ?></p>
    </div>
    <div class="surface-primary text-center">
        <p class="text-xs uppercase tracking-wider text-muted">Total Kills</p>
        <p class="mt-1 text-2xl font-bold text-red-300"><?= number_format($totalKills) ?></p>
    </div>
    <div class="surface-primary text-center">
        <p class="text-xs uppercase tracking-wider text-muted">Total Losses</p>
        <p class="mt-1 text-2xl font-bold text-emerald-300"><?= number_format($totalLosses) ?></p>
    </div>
    <div class="surface-primary text-center">
        <p class="text-xs uppercase tracking-wider text-muted">ISK Destroyed</p>
        <p class="mt-1 text-2xl font-bold text-amber-300"><?= htmlspecialchars($formatIsk((float) $totalIskDestroyed)) ?></p>
    </div>
</section>

<!-- Global SITREP Briefing -->
<section class="surface-primary mt-4">
    <div class="flex items-center justify-between mb-4">
        <h2 class="text-lg font-semibold text-slate-100">Daily SITREP</h2>
        <?php if ($globalBriefing): ?>
            <div class="flex items-center gap-3 text-xs text-muted">
                <span>Model: <?= htmlspecialchars((string) ($globalBriefing['model_name'] ?? 'N/A')) ?></span>
                <span>Generated: <?= htmlspecialchars((string) ($globalBriefing['computed_at'] ?? '')) ?></span>
                <?php if (($globalBriefing['generation_status'] ?? '') === 'fallback'): ?>
                    <span class="text-amber-400">(Deterministic fallback)</span>
                <?php endif; ?>
            </div>
        <?php endif; ?>
    </div>

    <?php if ($globalBriefing): ?>
        <?php if ($globalBriefing['headline'] ?? ''): ?>
            <p class="text-base font-semibold text-cyan-200 mb-3"><?= htmlspecialchars((string) $globalBriefing['headline']) ?></p>
        <?php endif; ?>

        <?php if ($globalBriefing['key_developments'] ?? ''): ?>
            <div class="mb-4">
                <h3 class="text-xs uppercase tracking-wider text-muted mb-2">Key Developments</h3>
                <div class="prose prose-invert prose-sm max-w-none text-slate-300">
                    <?= supplycore_markdown_to_html((string) $globalBriefing['key_developments']) ?>
                </div>
            </div>
        <?php endif; ?>

        <?php if ($globalBriefing['summary'] ?? ''): ?>
            <div class="mb-4">
                <h3 class="text-xs uppercase tracking-wider text-muted mb-2">Intelligence Summary</h3>
                <div class="prose prose-invert prose-sm max-w-none text-slate-300">
                    <?= supplycore_markdown_to_html((string) $globalBriefing['summary']) ?>
                </div>
            </div>
        <?php endif; ?>

        <?php if ($globalBriefing['action_items'] ?? ''): ?>
            <div class="mb-2">
                <h3 class="text-xs uppercase tracking-wider text-muted mb-2">Recommended Actions</h3>
                <div class="prose prose-invert prose-sm max-w-none text-slate-300">
                    <?= supplycore_markdown_to_html((string) $globalBriefing['action_items']) ?>
                </div>
            </div>
        <?php endif; ?>
    <?php else: ?>
        <p class="text-sm text-muted">No briefing available for this date. Run the opposition daily snapshot job and AI briefing generator to produce a SITREP.</p>
    <?php endif; ?>
</section>

<!-- Per-Alliance Activity Grid -->
<section class="surface-primary mt-4">
    <h2 class="text-lg font-semibold text-slate-100 mb-4">Opposition Alliance Activity</h2>

    <?php if ($snapshots): ?>
        <div class="table-shell">
            <table class="table-ui">
                <thead>
                    <tr class="border-b border-border/70 text-xs uppercase tracking-[0.15em] text-muted">
                        <th class="px-3 py-2 text-left">Alliance</th>
                        <th class="px-3 py-2 text-left">Posture</th>
                        <th class="px-3 py-2 text-right">Kills</th>
                        <th class="px-3 py-2 text-right">Losses</th>
                        <th class="px-3 py-2 text-right">ISK Destroyed</th>
                        <th class="px-3 py-2 text-right">ISK Lost</th>
                        <th class="px-3 py-2 text-right">Pilots</th>
                        <th class="px-3 py-2 text-left">Top Systems</th>
                        <th class="px-3 py-2 text-left">Intel</th>
                    </tr>
                </thead>
                <tbody>
                    <?php foreach ($snapshots as $s):
                        $kills = (int) $s['kills'];
                        $losses = (int) $s['losses'];
                        if ($kills === 0 && $losses === 0) continue;
                        $aid = (int) $s['alliance_id'];
                        $systems = $s['active_systems_json'] ?? [];
                        if (is_string($systems)) $systems = json_decode($systems, true) ?: [];
                        $topSystems = [];
                        foreach (array_slice($systems, 0, 3) as $sys) {
                            $sid = isset($sys['system_id']) ? (int) $sys['system_id'] : 0;
                            $topSystems[] = $systemNameMap[$sid] ?? (string) ($sys['system_name'] ?? ('System #' . $sid));
                        }

                        // Find alliance briefing if any
                        $allianceBriefing = null;
                        foreach ($allianceBriefings as $ab) {
                            if ((int) ($ab['alliance_id'] ?? 0) === $aid) {
                                $allianceBriefing = $ab;
                                break;
                            }
                        }
                    ?>
                        <tr class="border-b border-border/30 hover:bg-slate-800/40 transition-colors">
                            <td class="px-3 py-2 text-sm">
                                <a href="/alliance-dossiers/view.php?id=<?= $aid ?>" class="text-cyan-300 hover:text-cyan-100">
                                    <?= htmlspecialchars((string) $s['alliance_name']) ?>
                                </a>
                            </td>
                            <td class="px-3 py-2 text-xs">
                                <?php if ($s['posture'] ?? ''): ?>
                                    <span class="inline-block rounded border px-1.5 py-0.5 text-[10px] uppercase
                                        <?= match($s['posture']) {
                                            'aggressive' => 'border-red-500/30 text-red-300',
                                            'committed' => 'border-orange-500/30 text-orange-300',
                                            'opportunistic' => 'border-amber-500/30 text-amber-300',
                                            'balanced' => 'border-sky-500/30 text-sky-300',
                                            default => 'border-slate-500/30 text-slate-300',
                                        } ?>">
                                        <?= htmlspecialchars((string) $s['posture']) ?>
                                    </span>
                                <?php endif; ?>
                            </td>
                            <td class="px-3 py-2 text-sm text-right font-mono text-red-300"><?= number_format($kills) ?></td>
                            <td class="px-3 py-2 text-sm text-right font-mono text-emerald-300"><?= number_format($losses) ?></td>
                            <td class="px-3 py-2 text-sm text-right font-mono"><?= htmlspecialchars($formatIsk((float) $s['isk_destroyed'])) ?></td>
                            <td class="px-3 py-2 text-sm text-right font-mono"><?= htmlspecialchars($formatIsk((float) $s['isk_lost'])) ?></td>
                            <td class="px-3 py-2 text-sm text-right font-mono"><?= (int) $s['active_pilots'] ?></td>
                            <td class="px-3 py-2 text-xs text-muted"><?= htmlspecialchars(implode(', ', $topSystems)) ?></td>
                            <td class="px-3 py-2 text-xs">
                                <?php if ($allianceBriefing): ?>
                                    <?php $ta = $allianceBriefing['threat_assessment'] ?? 'moderate'; ?>
                                    <span class="inline-block rounded border px-1.5 py-0.5 text-[10px] uppercase <?= $threatColors[$ta] ?? $threatColors['moderate'] ?>">
                                        <?= htmlspecialchars(strtoupper($ta)) ?>
                                    </span>
                                <?php else: ?>
                                    <span class="text-muted">-</span>
                                <?php endif; ?>
                            </td>
                        </tr>
                    <?php endforeach; ?>
                </tbody>
            </table>
        </div>
    <?php else: ?>
        <p class="text-sm text-muted">No snapshot data available for <?= htmlspecialchars($date) ?>.</p>
    <?php endif; ?>
</section>

<!-- Tracked Alliance AI Briefings -->
<?php if ($allianceBriefings): ?>
<section class="surface-primary mt-4">
    <h2 class="text-lg font-semibold text-slate-100 mb-4">Tracked Alliance Intelligence Profiles</h2>
    <div class="grid gap-4 md:grid-cols-2">
        <?php foreach ($allianceBriefings as $ab): ?>
            <div class="rounded-lg border border-border/50 bg-slate-800/30 p-4">
                <div class="flex items-center justify-between mb-2">
                    <a href="/alliance-dossiers/view.php?id=<?= (int) $ab['alliance_id'] ?>" class="text-sm font-semibold text-cyan-200 hover:text-cyan-100">
                        <?= htmlspecialchars((string) ($ab['alliance_name'] ?? 'Unknown')) ?>
                    </a>
                    <?php $ta = $ab['threat_assessment'] ?? 'moderate'; ?>
                    <span class="inline-block rounded border px-1.5 py-0.5 text-[10px] uppercase <?= $threatColors[$ta] ?? $threatColors['moderate'] ?>">
                        <?= htmlspecialchars(strtoupper($ta)) ?>
                    </span>
                </div>
                <?php if ($ab['headline'] ?? ''): ?>
                    <p class="text-xs font-medium text-slate-200 mb-2"><?= htmlspecialchars((string) $ab['headline']) ?></p>
                <?php endif; ?>
                <?php if ($ab['key_developments'] ?? ''): ?>
                    <div class="prose prose-invert prose-xs max-w-none text-muted">
                        <?= supplycore_markdown_to_html((string) $ab['key_developments']) ?>
                    </div>
                <?php endif; ?>
                <div class="mt-2 text-[10px] text-muted">
                    <?= htmlspecialchars((string) ($ab['model_name'] ?? '')) ?>
                    <?php if (($ab['generation_status'] ?? '') === 'fallback'): ?>
                        <span class="text-amber-400">(fallback)</span>
                    <?php endif; ?>
                </div>
            </div>
        <?php endforeach; ?>
    </div>
</section>
<?php endif; ?>

<!-- Briefing History -->
<?php if ($recentBriefings): ?>
<section class="surface-primary mt-4">
    <h2 class="text-lg font-semibold text-slate-100 mb-4">Briefing History</h2>
    <div class="table-shell">
        <table class="table-ui">
            <thead>
                <tr class="border-b border-border/70 text-xs uppercase tracking-[0.15em] text-muted">
                    <th class="px-3 py-2 text-left">Date</th>
                    <th class="px-3 py-2 text-left">Headline</th>
                    <th class="px-3 py-2 text-left">Threat</th>
                    <th class="px-3 py-2 text-left">Model</th>
                    <th class="px-3 py-2 text-left">Status</th>
                </tr>
            </thead>
            <tbody>
                <?php foreach ($recentBriefings as $rb): ?>
                    <tr class="border-b border-border/30 hover:bg-slate-800/40 transition-colors <?= ($rb['briefing_date'] ?? '') === $date ? 'bg-slate-800/60' : '' ?>">
                        <td class="px-3 py-2 text-sm">
                            <a href="?date=<?= urlencode((string) $rb['briefing_date']) ?>" class="text-cyan-300 hover:text-cyan-100">
                                <?= htmlspecialchars((string) $rb['briefing_date']) ?>
                            </a>
                        </td>
                        <td class="px-3 py-2 text-sm text-slate-300"><?= htmlspecialchars(mb_substr((string) ($rb['headline'] ?? ''), 0, 100)) ?></td>
                        <td class="px-3 py-2">
                            <?php $ta = $rb['threat_assessment'] ?? ''; ?>
                            <?php if ($ta): ?>
                                <span class="inline-block rounded border px-1.5 py-0.5 text-[10px] uppercase <?= $threatColors[$ta] ?? $threatColors['moderate'] ?>">
                                    <?= htmlspecialchars(strtoupper($ta)) ?>
                                </span>
                            <?php endif; ?>
                        </td>
                        <td class="px-3 py-2 text-xs text-muted"><?= htmlspecialchars((string) ($rb['model_name'] ?? '')) ?></td>
                        <td class="px-3 py-2 text-xs">
                            <?= match($rb['generation_status'] ?? '') {
                                'ready' => '<span class="text-emerald-400">AI</span>',
                                'fallback' => '<span class="text-amber-400">Fallback</span>',
                                'failed' => '<span class="text-red-400">Failed</span>',
                                default => '<span class="text-muted">-</span>',
                            } ?>
                        </td>
                    </tr>
                <?php endforeach; ?>
            </tbody>
        </table>
    </div>
</section>
<?php endif; ?>

<?php include __DIR__ . '/../../src/views/partials/footer.php'; ?>
