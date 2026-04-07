<?php

declare(strict_types=1);

require_once __DIR__ . '/../../src/bootstrap.php';

$title = 'CIP Admin & Operations';

// ── Handle POST actions ──────────────────────────────────────────────────

if ($_SERVER['REQUEST_METHOD'] === 'POST') {
    $action = (string) ($_POST['action'] ?? '');

    // Toggle compound enabled/disabled
    if ($action === 'toggle_compound') {
        $compoundType = (string) ($_POST['compound_type'] ?? '');
        $enabled = ((string) ($_POST['enabled'] ?? '0')) === '1';
        db_compound_toggle_enabled($compoundType, $enabled);
        header('Location: /intelligence-events/admin.php?msg=compound_toggled');
        exit;
    }

    // Save calibration overrides
    if ($action === 'save_calibration_overrides') {
        $surge = trim((string) ($_POST['cip_override_surge_delta'] ?? ''));
        $rankJump = trim((string) ($_POST['cip_override_rank_jump'] ?? ''));
        $freshness = trim((string) ($_POST['cip_override_freshness_floor'] ?? ''));
        db_calibration_override_set('cip_override_surge_delta', $surge);
        db_calibration_override_set('cip_override_rank_jump', $rankJump);
        db_calibration_override_set('cip_override_freshness_floor', $freshness);
        header('Location: /intelligence-events/admin.php?msg=overrides_saved');
        exit;
    }

    // Toggle calibration freeze
    if ($action === 'toggle_calibration_freeze') {
        $frozen = ((string) ($_POST['frozen'] ?? '0')) === '1';
        db_calibration_override_set('cip_calibration_frozen', $frozen ? '1' : '0');
        header('Location: /intelligence-events/admin.php?msg=freeze_' . ($frozen ? 'on' : 'off'));
        exit;
    }
}

// ── Load data ────────────────────────────────────────────────────────────

$calibration = db_intelligence_calibration_latest();
$calibrationHistory = db_calibration_history(7);
$overrides = db_calibration_overrides_get();
$compounds = db_compound_definitions_all();
$compoundAnalytics = db_compound_analytics_latest();
$outcomes = db_compound_outcome_summary();
$health = db_cip_operational_health();

$isFrozen = ($overrides['cip_calibration_frozen'] ?? '0') === '1';

$msg = match ($_GET['msg'] ?? '') {
    'compound_toggled' => 'Compound toggle updated.',
    'overrides_saved'  => 'Calibration overrides saved. Takes effect on next calibration run.',
    'freeze_on'        => 'Calibration frozen. Thresholds will not change until unfrozen.',
    'freeze_off'       => 'Calibration unfrozen. Normal self-leveling resumed.',
    default            => '',
};

// Build analytics lookup
$analyticsMap = [];
foreach ($compoundAnalytics as $ca) {
    $analyticsMap[$ca['compound_type']] = $ca;
}

// Build outcome lookup
$outcomeMap = [];
foreach ($outcomes as $o) {
    $key = $o['compound_type'];
    if (!isset($outcomeMap[$key])) { $outcomeMap[$key] = []; }
    $outcomeMap[$key][$o['outcome']] = (int) $o['cnt'];
}

include __DIR__ . '/../../src/views/partials/header.php';
?>

<section class="surface-primary">
    <div class="flex items-center justify-between gap-4">
        <div>
            <p class="text-xs uppercase tracking-[0.16em] text-muted">Character Intelligence Profile</p>
            <h1 class="mt-1 text-2xl font-semibold text-slate-50">CIP Admin & Operations</h1>
            <p class="mt-2 text-sm text-muted">Manage compound signals, calibration thresholds, and monitor system health.</p>
        </div>
        <a href="/intelligence-events/" class="btn-secondary text-sm">Event Queue</a>
    </div>

    <?php if ($msg !== ''): ?>
        <div class="mt-3 rounded bg-emerald-900/40 border border-emerald-700/50 px-4 py-2 text-sm text-emerald-300"><?= htmlspecialchars($msg, ENT_QUOTES) ?></div>
    <?php endif; ?>
</section>

<!-- Operational Health -->
<section class="surface-primary mt-4">
    <h2 class="text-lg font-semibold text-slate-100">System Health</h2>
    <div class="mt-3 grid gap-3 sm:grid-cols-2 md:grid-cols-3 lg:grid-cols-6">
        <?php
        $profileStats = $health['profile_stats'] ?? [];
        $suppressStats = $health['suppress_stats'] ?? [];
        $eventStates = [];
        foreach (($health['events_by_state'] ?? []) as $es) { $eventStates[$es['state']] = (int) $es['cnt']; }
        ?>
        <div class="surface-tertiary">
            <p class="text-xs text-muted">Profiled chars</p>
            <p class="mt-1 text-xl font-semibold text-slate-100"><?= number_format((int) ($profileStats['total_profiles'] ?? 0)) ?></p>
        </div>
        <div class="surface-tertiary">
            <p class="text-xs text-muted">Avg signals</p>
            <p class="mt-1 text-xl font-semibold text-slate-100"><?= number_format((float) ($profileStats['avg_signals'] ?? 0), 1) ?></p>
        </div>
        <div class="surface-tertiary">
            <p class="text-xs text-muted">Avg confidence</p>
            <p class="mt-1 text-xl font-semibold text-slate-100"><?= number_format((float) ($profileStats['avg_confidence'] ?? 0), 3) ?></p>
        </div>
        <div class="surface-tertiary">
            <p class="text-xs text-muted">Avg freshness</p>
            <p class="mt-1 text-xl font-semibold text-slate-100"><?= number_format((float) ($profileStats['avg_freshness'] ?? 0), 3) ?></p>
        </div>
        <div class="surface-tertiary">
            <p class="text-xs text-muted">Active events</p>
            <p class="mt-1 text-xl font-semibold text-slate-100"><?= number_format($eventStates['active'] ?? 0) ?></p>
        </div>
        <div class="surface-tertiary">
            <p class="text-xs text-muted">Suppressed</p>
            <p class="mt-1 text-xl font-semibold text-slate-100"><?= number_format((int) ($suppressStats['still_suppressed'] ?? 0)) ?></p>
        </div>
    </div>

    <!-- Daily event creation trend -->
    <?php $dailyCreation = $health['daily_creation'] ?? []; ?>
    <?php if ($dailyCreation !== []): ?>
    <div class="mt-4">
        <p class="text-xs text-muted font-medium">Event creation (last 7 days)</p>
        <div class="mt-2 flex items-end gap-1 h-16">
            <?php
            $maxDaily = max(1, max(array_column($dailyCreation, 'cnt')));
            foreach (array_reverse($dailyCreation) as $dc):
                $pct = ((int) $dc['cnt'] / $maxDaily) * 100;
            ?>
                <div class="flex-1 flex flex-col items-center gap-0.5">
                    <div class="w-full rounded-t bg-accent/40" style="height: <?= max(2, (int) $pct) ?>%"></div>
                    <span class="text-[9px] text-muted"><?= (int) $dc['cnt'] ?></span>
                </div>
            <?php endforeach; ?>
        </div>
    </div>
    <?php endif; ?>
</section>

<div class="mt-4 grid gap-4 lg:grid-cols-2">
    <!-- Calibration -->
    <section class="surface-primary">
        <div class="flex items-center justify-between">
            <h2 class="text-lg font-semibold text-slate-100">Calibration</h2>
            <?php if ($isFrozen): ?>
                <span class="inline-flex items-center rounded bg-amber-900/60 text-amber-300 border border-amber-800/50 px-2.5 py-1 text-xs font-semibold">FROZEN</span>
            <?php endif; ?>
        </div>

        <?php if ($calibration !== null): ?>
        <div class="mt-3 grid gap-2 grid-cols-2">
            <div class="surface-tertiary">
                <p class="text-xs text-muted">Surge delta</p>
                <p class="mt-1 text-sm text-slate-200"><?= number_format((float) ($calibration['calibrated_surge_delta'] ?? 0), 4) ?></p>
            </div>
            <div class="surface-tertiary">
                <p class="text-xs text-muted">Rank jump</p>
                <p class="mt-1 text-sm text-slate-200"><?= (int) ($calibration['calibrated_rank_jump'] ?? 0) ?></p>
            </div>
            <div class="surface-tertiary">
                <p class="text-xs text-muted">Freshness floor</p>
                <p class="mt-1 text-sm text-slate-200"><?= number_format((float) ($calibration['calibrated_freshness_floor'] ?? 0), 2) ?></p>
            </div>
            <div class="surface-tertiary">
                <p class="text-xs text-muted">Noise ratio</p>
                <?php $noise = (float) ($calibration['event_noise_ratio'] ?? 0); ?>
                <p class="mt-1 text-sm <?= $noise >= 0.10 ? 'text-red-400' : ($noise >= 0.05 ? 'text-amber-400' : 'text-emerald-400') ?>"><?= number_format($noise, 4) ?></p>
            </div>
        </div>
        <div class="mt-2 grid gap-2 grid-cols-4">
            <div class="surface-tertiary">
                <p class="text-[10px] text-muted">Band: Critical</p>
                <p class="text-xs text-red-400"><?= number_format((float) ($calibration['band_critical_floor'] ?? 0), 4) ?></p>
            </div>
            <div class="surface-tertiary">
                <p class="text-[10px] text-muted">High</p>
                <p class="text-xs text-orange-400"><?= number_format((float) ($calibration['band_high_floor'] ?? 0), 4) ?></p>
            </div>
            <div class="surface-tertiary">
                <p class="text-[10px] text-muted">Moderate</p>
                <p class="text-xs text-amber-400"><?= number_format((float) ($calibration['band_moderate_floor'] ?? 0), 4) ?></p>
            </div>
            <div class="surface-tertiary">
                <p class="text-[10px] text-muted">Low</p>
                <p class="text-xs text-slate-400"><?= number_format((float) ($calibration['band_low_floor'] ?? 0), 4) ?></p>
            </div>
        </div>
        <p class="mt-2 text-[10px] text-muted">Snapshot: <?= htmlspecialchars((string) ($calibration['snapshot_date'] ?? ''), ENT_QUOTES) ?> | Pop: <?= number_format((int) ($calibration['total_profiled_characters'] ?? 0)) ?></p>
        <?php else: ?>
        <p class="mt-3 text-sm text-muted">No calibration snapshot yet. Run cip_calibration job.</p>
        <?php endif; ?>

        <!-- Overrides -->
        <form method="post" class="mt-4 border-t border-border/50 pt-4">
            <input type="hidden" name="action" value="save_calibration_overrides">
            <p class="text-sm font-medium text-slate-300">Manual overrides</p>
            <p class="text-xs text-muted mt-1">Set to "auto" or leave empty to use calibrated values.</p>
            <div class="mt-2 grid gap-2 grid-cols-3">
                <label class="block">
                    <span class="text-[10px] text-muted">Surge delta</span>
                    <input type="text" name="cip_override_surge_delta" value="<?= htmlspecialchars($overrides['cip_override_surge_delta'] ?? '', ENT_QUOTES) ?>" class="mt-0.5 w-full rounded border border-border bg-slate-800 px-2 py-1 text-xs text-slate-100" placeholder="auto">
                </label>
                <label class="block">
                    <span class="text-[10px] text-muted">Rank jump</span>
                    <input type="text" name="cip_override_rank_jump" value="<?= htmlspecialchars($overrides['cip_override_rank_jump'] ?? '', ENT_QUOTES) ?>" class="mt-0.5 w-full rounded border border-border bg-slate-800 px-2 py-1 text-xs text-slate-100" placeholder="auto">
                </label>
                <label class="block">
                    <span class="text-[10px] text-muted">Freshness floor</span>
                    <input type="text" name="cip_override_freshness_floor" value="<?= htmlspecialchars($overrides['cip_override_freshness_floor'] ?? '', ENT_QUOTES) ?>" class="mt-0.5 w-full rounded border border-border bg-slate-800 px-2 py-1 text-xs text-slate-100" placeholder="auto">
                </label>
            </div>
            <button type="submit" class="mt-2 btn-secondary text-xs">Save overrides</button>
        </form>

        <!-- Freeze toggle -->
        <form method="post" class="mt-3 border-t border-border/50 pt-3">
            <input type="hidden" name="action" value="toggle_calibration_freeze">
            <input type="hidden" name="frozen" value="<?= $isFrozen ? '0' : '1' ?>">
            <div class="flex items-center justify-between">
                <div>
                    <p class="text-sm text-slate-300"><?= $isFrozen ? 'Calibration is frozen' : 'Calibration is active' ?></p>
                    <p class="text-[10px] text-muted"><?= $isFrozen ? 'Thresholds will not change. Click to unfreeze.' : 'Self-leveling normally. Click to freeze current thresholds.' ?></p>
                </div>
                <button type="submit" class="<?= $isFrozen ? 'btn-primary' : 'btn-secondary' ?> text-xs"><?= $isFrozen ? 'Unfreeze' : 'Freeze' ?></button>
            </div>
        </form>
    </section>

    <!-- Compound Controls -->
    <section class="surface-primary">
        <h2 class="text-lg font-semibold text-slate-100">Compound Signals</h2>
        <p class="mt-1 text-xs text-muted">Enable/disable individual compound detections. Changes take effect on next evaluator run.</p>

        <?php if ($compounds === []): ?>
            <p class="mt-3 text-sm text-muted">No compound definitions found. Run seed job first.</p>
        <?php else: ?>
            <div class="mt-3 space-y-2">
                <?php foreach ($compounds as $comp): ?>
                    <?php
                    $ctype = (string) $comp['compound_type'];
                    $isEnabled = ((int) ($comp['enabled'] ?? 1)) === 1;
                    $analytics = $analyticsMap[$ctype] ?? null;
                    $compOutcomes = $outcomeMap[$ctype] ?? [];
                    $tp = (int) ($compOutcomes['true_positive'] ?? 0);
                    $fp = (int) ($compOutcomes['false_positive'] ?? 0);
                    $totalOutcomes = $tp + $fp + (int) ($compOutcomes['inconclusive'] ?? 0) + (int) ($compOutcomes['confirmed_clean'] ?? 0);
                    $precision = ($tp + $fp) > 0 ? $tp / ($tp + $fp) : null;
                    ?>
                    <div class="surface-tertiary flex items-center justify-between gap-3">
                        <div class="flex-1 min-w-0">
                            <div class="flex items-center gap-2">
                                <p class="text-sm text-slate-100 font-medium truncate"><?= htmlspecialchars((string) ($comp['display_name'] ?? $ctype), ENT_QUOTES) ?></p>
                                <span class="text-[10px] text-muted"><?= htmlspecialchars($ctype, ENT_QUOTES) ?></span>
                            </div>
                            <div class="mt-1 flex items-center gap-3 text-[10px] text-muted">
                                <?php if ($analytics !== null): ?>
                                    <span>Active: <?= (int) ($analytics['active_count'] ?? 0) ?></span>
                                    <span>New/24h: <?= (int) ($analytics['new_activations'] ?? 0) ?></span>
                                    <span>Overlap: <?= (int) ($analytics['overlap_with_simple_events'] ?? 0) ?></span>
                                <?php endif; ?>
                                <?php if ($totalOutcomes > 0): ?>
                                    <span>Outcomes: <?= $totalOutcomes ?></span>
                                    <?php if ($precision !== null): ?>
                                        <span class="<?= $precision >= 0.7 ? 'text-emerald-400' : ($precision >= 0.4 ? 'text-amber-400' : 'text-red-400') ?>">Precision: <?= number_format($precision * 100, 0) ?>%</span>
                                    <?php endif; ?>
                                <?php endif; ?>
                            </div>
                        </div>
                        <form method="post" class="shrink-0">
                            <input type="hidden" name="action" value="toggle_compound">
                            <input type="hidden" name="compound_type" value="<?= htmlspecialchars($ctype, ENT_QUOTES) ?>">
                            <input type="hidden" name="enabled" value="<?= $isEnabled ? '0' : '1' ?>">
                            <button type="submit" class="rounded px-3 py-1.5 text-xs font-medium <?= $isEnabled
                                ? 'bg-emerald-900/40 text-emerald-300 border border-emerald-700/50 hover:bg-emerald-900/60'
                                : 'bg-red-900/40 text-red-300 border border-red-700/50 hover:bg-red-900/60' ?>">
                                <?= $isEnabled ? 'Enabled' : 'Disabled' ?>
                            </button>
                        </form>
                    </div>
                <?php endforeach; ?>
            </div>
        <?php endif; ?>
    </section>
</div>

<!-- Calibration history -->
<?php if ($calibrationHistory !== []): ?>
<section class="surface-primary mt-4">
    <h2 class="text-lg font-semibold text-slate-100">Calibration history</h2>
    <p class="mt-1 text-xs text-muted">Last 7 daily snapshots showing threshold drift and noise metrics.</p>
    <div class="mt-3 table-shell">
        <table class="table-ui">
            <thead>
            <tr class="border-b border-border/70 text-xs uppercase tracking-[0.15em] text-muted">
                <th class="px-3 py-2 text-left">Date</th>
                <th class="px-3 py-2 text-right">Population</th>
                <th class="px-3 py-2 text-right">Active events</th>
                <th class="px-3 py-2 text-right">Created/24h</th>
                <th class="px-3 py-2 text-right">Surge &Delta;</th>
                <th class="px-3 py-2 text-right">Rank jump</th>
                <th class="px-3 py-2 text-right">Fresh. floor</th>
                <th class="px-3 py-2 text-right">Noise</th>
                <th class="px-3 py-2 text-right">Supp. rate</th>
            </tr>
            </thead>
            <tbody>
            <?php foreach ($calibrationHistory as $ch): ?>
                <?php $chNoise = (float) ($ch['event_noise_ratio'] ?? 0); ?>
                <tr class="border-b border-border/50">
                    <td class="px-3 py-2 text-sm text-slate-100"><?= htmlspecialchars((string) ($ch['snapshot_date'] ?? ''), ENT_QUOTES) ?></td>
                    <td class="px-3 py-2 text-right text-sm"><?= number_format((int) ($ch['total_profiled_characters'] ?? 0)) ?></td>
                    <td class="px-3 py-2 text-right text-sm"><?= number_format((int) ($ch['active_events_count'] ?? 0)) ?></td>
                    <td class="px-3 py-2 text-right text-sm"><?= number_format((int) ($ch['events_created_24h'] ?? 0)) ?></td>
                    <td class="px-3 py-2 text-right text-sm"><?= number_format((float) ($ch['calibrated_surge_delta'] ?? 0), 4) ?></td>
                    <td class="px-3 py-2 text-right text-sm"><?= (int) ($ch['calibrated_rank_jump'] ?? 0) ?></td>
                    <td class="px-3 py-2 text-right text-sm"><?= number_format((float) ($ch['calibrated_freshness_floor'] ?? 0), 2) ?></td>
                    <td class="px-3 py-2 text-right text-sm <?= $chNoise >= 0.10 ? 'text-red-400' : ($chNoise >= 0.05 ? 'text-amber-400' : 'text-emerald-400') ?>"><?= number_format($chNoise, 4) ?></td>
                    <td class="px-3 py-2 text-right text-sm"><?= number_format((float) ($ch['suppression_rate'] ?? 0), 3) ?></td>
                </tr>
            <?php endforeach; ?>
            </tbody>
        </table>
    </div>
</section>
<?php endif; ?>

<!-- Compound Analytics -->
<?php if ($compoundAnalytics !== []): ?>
<section class="surface-primary mt-4">
    <h2 class="text-lg font-semibold text-slate-100">Compound analytics</h2>
    <p class="mt-1 text-xs text-muted">Latest snapshot per compound type — activation, interaction, and overlap metrics.</p>
    <div class="mt-3 table-shell">
        <table class="table-ui">
            <thead>
            <tr class="border-b border-border/70 text-xs uppercase tracking-[0.15em] text-muted">
                <th class="px-3 py-2 text-left">Compound</th>
                <th class="px-3 py-2 text-left">Family</th>
                <th class="px-3 py-2 text-right">Active</th>
                <th class="px-3 py-2 text-right">New/24h</th>
                <th class="px-3 py-2 text-right">Avg score</th>
                <th class="px-3 py-2 text-right">Avg conf</th>
                <th class="px-3 py-2 text-right">Ack</th>
                <th class="px-3 py-2 text-right">Resolved</th>
                <th class="px-3 py-2 text-right">Suppressed</th>
                <th class="px-3 py-2 text-right">Overlap</th>
            </tr>
            </thead>
            <tbody>
            <?php foreach ($compoundAnalytics as $ca): ?>
                <?php
                $familyColors = ['infiltration' => 'text-red-400', 'coordination' => 'text-orange-400', 'prioritization' => 'text-amber-400', 'trust' => 'text-cyan-400'];
                $fam = (string) ($ca['compound_family'] ?? '');
                ?>
                <tr class="border-b border-border/50">
                    <td class="px-3 py-2 text-sm text-slate-100"><?= htmlspecialchars((string) ($ca['compound_type'] ?? ''), ENT_QUOTES) ?></td>
                    <td class="px-3 py-2 text-xs <?= $familyColors[$fam] ?? 'text-slate-400' ?>"><?= htmlspecialchars($fam, ENT_QUOTES) ?></td>
                    <td class="px-3 py-2 text-right text-sm"><?= (int) ($ca['active_count'] ?? 0) ?></td>
                    <td class="px-3 py-2 text-right text-sm"><?= (int) ($ca['new_activations'] ?? 0) ?></td>
                    <td class="px-3 py-2 text-right text-sm"><?= number_format((float) ($ca['avg_score'] ?? 0), 4) ?></td>
                    <td class="px-3 py-2 text-right text-sm"><?= number_format((float) ($ca['avg_confidence'] ?? 0), 3) ?></td>
                    <td class="px-3 py-2 text-right text-sm"><?= (int) ($ca['acknowledged_count'] ?? 0) ?></td>
                    <td class="px-3 py-2 text-right text-sm"><?= (int) ($ca['resolved_count'] ?? 0) ?></td>
                    <td class="px-3 py-2 text-right text-sm"><?= (int) ($ca['suppressed_count'] ?? 0) ?></td>
                    <td class="px-3 py-2 text-right text-sm <?= ((int) ($ca['overlap_with_simple_events'] ?? 0)) > ((int) ($ca['active_count'] ?? 1)) * 0.8 ? 'text-amber-400' : 'text-slate-300' ?>"><?= (int) ($ca['overlap_with_simple_events'] ?? 0) ?></td>
                </tr>
            <?php endforeach; ?>
            </tbody>
        </table>
    </div>
</section>
<?php endif; ?>

<?php include __DIR__ . '/../../src/views/partials/footer.php'; ?>
