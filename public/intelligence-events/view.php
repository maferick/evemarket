<?php

declare(strict_types=1);

require_once __DIR__ . '/../../src/bootstrap.php';

$eventId = max(0, (int) ($_GET['id'] ?? 0));
$title = 'Event Detail';

if ($eventId <= 0) {
    header('Location: /intelligence-events/');
    exit;
}

// Handle acknowledge
if ($_SERVER['REQUEST_METHOD'] === 'POST' && isset($_POST['acknowledge'])) {
    $reason = trim((string) ($_POST['reason'] ?? ''));
    db_intelligence_event_acknowledge($eventId, 'analyst', $reason !== '' ? $reason : null);
    header('Location: /intelligence-events/view.php?id=' . $eventId . '&action=acknowledged');
    exit;
}

// Handle resolve
if ($_SERVER['REQUEST_METHOD'] === 'POST' && isset($_POST['resolve'])) {
    $reason = trim((string) ($_POST['reason'] ?? ''));
    db_intelligence_event_resolve($eventId, 'analyst', $reason !== '' ? $reason : null);
    header('Location: /intelligence-events/view.php?id=' . $eventId . '&action=resolved');
    exit;
}

// Handle suppress
if ($_SERVER['REQUEST_METHOD'] === 'POST' && isset($_POST['suppress'])) {
    $reason = trim((string) ($_POST['reason'] ?? ''));
    $hours = max(1, min(720, (int) ($_POST['suppress_hours'] ?? 72)));
    db_intelligence_event_suppress($eventId, 'analyst', $hours, $reason !== '' ? $reason : null);
    header('Location: /intelligence-events/view.php?id=' . $eventId . '&action=suppressed');
    exit;
}

// Handle add note
if ($_SERVER['REQUEST_METHOD'] === 'POST' && isset($_POST['add_note'])) {
    $note = trim((string) ($_POST['note'] ?? ''));
    if ($note !== '') {
        db_intelligence_event_note_add($eventId, 'analyst', $note);
    }
    header('Location: /intelligence-events/view.php?id=' . $eventId . '&action=noted');
    exit;
}

// Load all data
$evidence = db_intelligence_event_evidence($eventId);
$event = $evidence['event'];

if ($event === null) {
    header('Location: /intelligence-events/');
    exit;
}

$profile = $evidence['profile'];
$signals = $evidence['signals'];
$compounds = $evidence['compounds'] ?? [];
$profileHistory = $evidence['history'];
$stateHistory = db_intelligence_event_history($eventId);
$notes = db_intelligence_event_notes($eventId);
$related = db_intelligence_event_related($eventId);

$detailData = json_decode((string) ($event['detail_json'] ?? '{}'), true);
if (!is_array($detailData)) {
    $detailData = [];
}

// Extract structured metadata from detail_json
$impactDecomp = (array) ($detailData['_impact_decomposition'] ?? []);
$thresholdInfo = (array) ($detailData['_threshold_info'] ?? []);
// Remove internal keys from display data
unset($detailData['_impact_decomposition'], $detailData['_threshold_info']);

$actionMessage = match ($_GET['action'] ?? '') {
    'acknowledged' => 'Event acknowledged.',
    'resolved'     => 'Event resolved.',
    'suppressed'   => 'Event suppressed.',
    'noted'        => 'Note added.',
    default        => '',
};

$sev = (string) ($event['severity'] ?? 'medium');
$sevClasses = match ($sev) {
    'critical' => 'bg-red-900/60 text-red-300 border border-red-800/50',
    'high'     => 'bg-orange-900/60 text-orange-300 border border-orange-800/50',
    'medium'   => 'bg-amber-900/60 text-amber-300 border border-amber-800/50',
    'low'      => 'bg-yellow-900/60 text-yellow-400 border border-yellow-800/50',
    default    => 'bg-slate-700 text-slate-300 border border-slate-600/50',
};

$stateClasses = match ($event['state'] ?? '') {
    'active'       => 'bg-blue-900/40 text-blue-300 border border-blue-700/50',
    'acknowledged' => 'bg-amber-900/40 text-amber-300 border border-amber-700/50',
    'suppressed'   => 'bg-slate-700 text-slate-400 border border-slate-600/50',
    'resolved'     => 'bg-emerald-900/40 text-emerald-300 border border-emerald-700/50',
    'expired'      => 'bg-slate-700 text-slate-400 border border-slate-600/50',
    default        => 'bg-slate-700 text-slate-300',
};

// Compute age metrics
$firstDetected = (string) ($event['first_detected_at'] ?? '');
$lastUpdated = (string) ($event['last_updated_at'] ?? '');
$eventAge = '';
$timeInState = '';
if ($firstDetected !== '') {
    $ts = strtotime($firstDetected);
    if ($ts !== false) {
        $diff = time() - $ts;
        if ($diff < 3600) { $eventAge = (int) ($diff / 60) . 'm'; }
        elseif ($diff < 86400) { $eventAge = (int) ($diff / 3600) . 'h ' . (int) (($diff % 3600) / 60) . 'm'; }
        else { $eventAge = (int) ($diff / 86400) . 'd ' . (int) (($diff % 86400) / 3600) . 'h'; }
    }
}
if ($lastUpdated !== '') {
    $ts = strtotime($lastUpdated);
    if ($ts !== false) {
        $diff = time() - $ts;
        if ($diff < 3600) { $timeInState = (int) ($diff / 60) . 'm'; }
        elseif ($diff < 86400) { $timeInState = (int) ($diff / 3600) . 'h ' . (int) (($diff % 3600) / 60) . 'm'; }
        else { $timeInState = (int) ($diff / 86400) . 'd ' . (int) (($diff % 86400) / 3600) . 'h'; }
    }
}

include __DIR__ . '/../../src/views/partials/header.php';
?>

<section class="surface-primary">
    <a href="/intelligence-events/" class="text-sm text-accent">&larr; Back to event queue</a>

    <?php if ($actionMessage !== ''): ?>
        <div class="mt-3 rounded bg-emerald-900/40 border border-emerald-700/50 px-4 py-2 text-sm text-emerald-300"><?= htmlspecialchars($actionMessage, ENT_QUOTES) ?></div>
    <?php endif; ?>

    <!-- Event header -->
    <div class="mt-4 flex flex-wrap items-start justify-between gap-4">
        <div>
            <p class="text-xs uppercase tracking-[0.16em] text-muted"><?= htmlspecialchars((string) ($event['event_type'] ?? ''), ENT_QUOTES) ?></p>
            <h1 class="mt-1 text-2xl font-semibold text-slate-50"><?= htmlspecialchars((string) ($event['title'] ?? 'Untitled Event'), ENT_QUOTES) ?></h1>
            <div class="mt-2 flex flex-wrap items-center gap-2">
                <span class="inline-flex items-center rounded px-2 py-0.5 text-xs font-medium <?= $sevClasses ?>"><?= strtoupper($sev) ?></span>
                <span class="inline-flex items-center rounded px-2 py-0.5 text-xs font-medium <?= $stateClasses ?>"><?= strtoupper((string) ($event['state'] ?? '')) ?></span>
                <span class="text-xs <?= ($event['event_family'] ?? '') === 'threat' ? 'text-red-400' : 'text-cyan-400' ?>"><?= ($event['event_family'] ?? '') === 'threat' ? 'Threat' : 'Profile Quality' ?></span>
                <?php if (((int) ($event['escalation_count'] ?? 1)) > 1): ?>
                    <span class="text-xs text-orange-400 font-medium">Escalated <?= (int) $event['escalation_count'] ?>x</span>
                <?php endif; ?>
            </div>
        </div>
        <div class="text-right text-xs text-muted space-y-0.5">
            <p>Entity: <?php if (($event['entity_name'] ?? '') !== ''): ?><a class="text-accent" href="/battle-intelligence/character.php?character_id=<?= urlencode((string) ((int) ($event['entity_id'] ?? 0))) ?>"><?= htmlspecialchars((string) $event['entity_name'], ENT_QUOTES) ?></a><?php else: ?><?= htmlspecialchars((string) ($event['entity_type'] ?? ''), ENT_QUOTES) ?> #<?= (int) ($event['entity_id'] ?? 0) ?><?php endif; ?></p>
            <p>First detected: <?= htmlspecialchars($firstDetected, ENT_QUOTES) ?> <span class="text-slate-400">(<?= $eventAge ?> ago)</span></p>
            <p>Last updated: <?= htmlspecialchars($lastUpdated, ENT_QUOTES) ?> <span class="text-slate-400">(<?= $timeInState ?> in state)</span></p>
            <p>Impact: <span class="text-slate-200 font-medium"><?= number_format((float) ($event['impact_score'] ?? 0), 4) ?></span></p>
            <?php if (($event['state'] ?? '') === 'suppressed' && ($event['suppressed_until'] ?? '') !== ''): ?>
                <p>Suppressed until: <span class="text-slate-300"><?= htmlspecialchars((string) $event['suppressed_until'], ENT_QUOTES) ?></span></p>
            <?php endif; ?>
        </div>
    </div>
</section>

<div class="mt-4 grid gap-4 lg:grid-cols-3">
    <!-- Left column: Trigger reason + Evidence + Signals (2/3 width) -->
    <div class="lg:col-span-2 space-y-4">

        <!-- 1. TRIGGER REASON: What changed that caused this event -->
        <section class="surface-primary">
            <h2 class="text-lg font-semibold text-slate-100">What triggered this event</h2>
            <p class="mt-1 text-xs text-muted">The specific change detected by the delta engine.</p>

            <?php
            // Extract the most important trigger fields from detail data
            // (exclude profile snapshot fields which are shown separately)
            $profileFields = ['risk_score', 'risk_rank', 'risk_percentile', 'confidence', 'freshness', 'effective_coverage'];
            $triggerFields = array_diff_key($detailData, array_flip($profileFields));
            ?>
            <?php if ($triggerFields !== []): ?>
                <div class="mt-3 grid gap-2 sm:grid-cols-2 lg:grid-cols-3">
                    <?php foreach ($triggerFields as $key => $val): ?>
                        <div class="surface-tertiary">
                            <p class="text-xs text-muted"><?= htmlspecialchars(str_replace('_', ' ', (string) $key), ENT_QUOTES) ?></p>
                            <p class="mt-1 text-sm text-slate-200">
                                <?php if (is_numeric($val)): ?>
                                    <?= number_format((float) $val, 4) ?>
                                <?php elseif (is_bool($val)): ?>
                                    <?= $val ? 'Yes' : 'No' ?>
                                <?php elseif (is_array($val)): ?>
                                    <?= htmlspecialchars(json_encode($val), ENT_QUOTES) ?>
                                <?php else: ?>
                                    <?= htmlspecialchars((string) $val, ENT_QUOTES) ?>
                                <?php endif; ?>
                            </p>
                        </div>
                    <?php endforeach; ?>
                </div>
            <?php else: ?>
                <p class="mt-2 text-sm text-muted">No detail payload recorded for this event.</p>
            <?php endif; ?>

            <!-- Threshold debug info -->
            <?php if ($thresholdInfo !== []): ?>
                <div class="mt-3 surface-tertiary">
                    <p class="text-xs text-muted font-medium">Threshold check</p>
                    <div class="mt-1 flex flex-wrap gap-x-4 gap-y-1 text-xs">
                        <?php foreach ($thresholdInfo as $tKey => $tVal): ?>
                            <span><span class="text-muted"><?= htmlspecialchars(str_replace('_', ' ', (string) $tKey), ENT_QUOTES) ?>:</span> <span class="text-slate-200"><?php if (is_numeric($tVal)): ?><?= number_format((float) $tVal, 4) ?><?php else: ?><?= htmlspecialchars((string) $tVal, ENT_QUOTES) ?><?php endif; ?></span></span>
                        <?php endforeach; ?>
                    </div>
                </div>
            <?php endif; ?>
        </section>

        <!-- 2. DELTA: Profile snapshot at detection vs current -->
        <?php if (($event['risk_score_at_event'] ?? null) !== null && $profile !== null): ?>
        <section class="surface-primary">
            <h2 class="text-lg font-semibold text-slate-100">State change</h2>
            <p class="mt-1 text-xs text-muted">Profile at detection vs. current state.</p>
            <div class="mt-3 table-shell">
                <table class="table-ui">
                    <thead>
                    <tr class="border-b border-border/70 text-xs uppercase tracking-[0.15em] text-muted">
                        <th class="px-3 py-2 text-left">Metric</th>
                        <th class="px-3 py-2 text-right">At detection</th>
                        <th class="px-3 py-2 text-right">Current</th>
                        <th class="px-3 py-2 text-right">Change</th>
                    </tr>
                    </thead>
                    <tbody>
                    <?php
                    $comparisons = [
                        ['Risk score', (float) ($event['risk_score_at_event'] ?? 0), (float) ($profile['risk_score'] ?? 0), 6],
                        ['Rank', (int) ($event['risk_rank_at_event'] ?? 0), (int) ($profile['risk_rank'] ?? 0), 0],
                        ['Percentile', (float) ($event['risk_percentile_at_event'] ?? 0) * 100, (float) ($profile['risk_percentile'] ?? 0) * 100, 2],
                    ];
                    foreach ($comparisons as [$label, $atEvent, $current, $decimals]): ?>
                        <?php
                        $delta = $current - $atEvent;
                        $deltaClass = $delta > 0 ? ($label === 'Rank' ? 'text-emerald-400' : 'text-red-400') : ($delta < 0 ? ($label === 'Rank' ? 'text-red-400' : 'text-emerald-400') : 'text-slate-400');
                        ?>
                        <tr class="border-b border-border/50">
                            <td class="px-3 py-2 text-sm text-slate-100"><?= $label ?></td>
                            <td class="px-3 py-2 text-right text-sm"><?= $label === 'Rank' ? '#' . number_format($atEvent) : ($label === 'Percentile' ? number_format($atEvent, $decimals) . '%' : number_format($atEvent, $decimals)) ?></td>
                            <td class="px-3 py-2 text-right text-sm"><?= $label === 'Rank' ? '#' . number_format($current) : ($label === 'Percentile' ? number_format($current, $decimals) . '%' : number_format($current, $decimals)) ?></td>
                            <td class="px-3 py-2 text-right text-sm <?= $deltaClass ?>"><?= ($delta >= 0 ? '+' : '') . ($label === 'Rank' ? number_format($delta) : number_format($delta, $decimals)) ?></td>
                        </tr>
                    <?php endforeach; ?>
                    </tbody>
                </table>
            </div>
        </section>
        <?php elseif (($event['risk_score_at_event'] ?? null) !== null): ?>
        <section class="surface-primary">
            <h2 class="text-lg font-semibold text-slate-100">Profile at detection</h2>
            <div class="mt-3 grid gap-2 sm:grid-cols-3">
                <div class="surface-tertiary">
                    <p class="text-xs text-muted">Risk score</p>
                    <p class="mt-1 text-sm text-slate-200"><?= number_format((float) $event['risk_score_at_event'], 6) ?></p>
                </div>
                <div class="surface-tertiary">
                    <p class="text-xs text-muted">Risk rank</p>
                    <p class="mt-1 text-sm text-slate-200">#<?= number_format((int) ($event['risk_rank_at_event'] ?? 0)) ?></p>
                </div>
                <div class="surface-tertiary">
                    <p class="text-xs text-muted">Risk percentile</p>
                    <p class="mt-1 text-sm text-slate-200"><?= number_format((float) ($event['risk_percentile_at_event'] ?? 0) * 100, 2) ?>%</p>
                </div>
            </div>
        </section>
        <?php endif; ?>

        <!-- 3. IMPACT DECOMPOSITION -->
        <?php if ($impactDecomp !== []): ?>
        <section class="surface-primary">
            <h2 class="text-lg font-semibold text-slate-100">Impact score breakdown</h2>
            <p class="mt-1 text-xs text-muted">How the impact score of <?= number_format((float) ($event['impact_score'] ?? 0), 4) ?> was computed.</p>
            <div class="mt-3 space-y-2">
                <?php
                $totalImpact = array_sum($impactDecomp);
                foreach ($impactDecomp as $factor => $contribution): ?>
                    <?php $pct = $totalImpact > 0 ? ($contribution / $totalImpact) * 100 : 0; ?>
                    <div class="flex items-center gap-3">
                        <span class="text-xs text-muted w-40 shrink-0"><?= htmlspecialchars(str_replace('_', ' ', (string) $factor), ENT_QUOTES) ?></span>
                        <div class="flex-1 h-2 rounded-full bg-slate-700 overflow-hidden">
                            <div class="h-full rounded-full <?= $contribution >= 0.2 ? 'bg-orange-500' : ($contribution >= 0.1 ? 'bg-amber-500' : 'bg-slate-500') ?>" style="width: <?= number_format(min(100, $pct), 1) ?>%"></div>
                        </div>
                        <span class="text-xs text-slate-200 w-16 text-right"><?= number_format($contribution, 4) ?></span>
                    </div>
                <?php endforeach; ?>
            </div>
        </section>
        <?php endif; ?>

        <!-- 4. Contributing signals -->
        <?php if ($signals !== []): ?>
        <section class="surface-primary">
            <h2 class="text-lg font-semibold text-slate-100">Contributing signals</h2>
            <p class="mt-1 text-xs text-muted">Active signals on this character's intelligence profile, sorted by effective contribution.</p>
            <div class="mt-3 table-shell">
                <table class="table-ui">
                    <thead>
                    <tr class="border-b border-border/70 text-xs uppercase tracking-[0.15em] text-muted">
                        <th class="px-3 py-2 text-left">Signal</th>
                        <th class="px-3 py-2 text-left">Domain</th>
                        <th class="px-3 py-2 text-right">Value</th>
                        <th class="px-3 py-2 text-right">Confidence</th>
                        <th class="px-3 py-2 text-right">Weight</th>
                        <th class="px-3 py-2 text-right">Contribution</th>
                    </tr>
                    </thead>
                    <tbody>
                    <?php foreach ($signals as $sig): ?>
                        <?php
                        $sigVal = (float) ($sig['value'] ?? 0);
                        $sigConf = (float) ($sig['confidence'] ?? 0);
                        $sigWeight = (float) ($sig['weight_default'] ?? 0);
                        $contribution = $sigVal * $sigConf * $sigWeight;
                        ?>
                        <tr class="border-b border-border/50">
                            <td class="px-3 py-2 text-sm text-slate-100"><?= htmlspecialchars((string) ($sig['display_name'] ?? $sig['signal_type']), ENT_QUOTES) ?></td>
                            <td class="px-3 py-2 text-xs"><span class="rounded-full bg-slate-700 px-2 py-0.5 text-[10px] uppercase tracking-wider text-slate-300"><?= htmlspecialchars((string) ($sig['signal_domain'] ?? ''), ENT_QUOTES) ?></span></td>
                            <td class="px-3 py-2 text-right text-sm"><?= number_format($sigVal, 4) ?></td>
                            <td class="px-3 py-2 text-right text-sm <?= $sigConf >= 0.7 ? 'text-emerald-400' : ($sigConf >= 0.4 ? 'text-amber-400' : 'text-slate-400') ?>"><?= number_format($sigConf, 3) ?></td>
                            <td class="px-3 py-2 text-right text-sm text-muted"><?= number_format($sigWeight, 2) ?></td>
                            <td class="px-3 py-2 text-right text-sm <?= $contribution >= 0.01 ? 'text-orange-400 font-medium' : 'text-slate-400' ?>"><?= number_format($contribution, 4) ?></td>
                        </tr>
                    <?php endforeach; ?>
                    </tbody>
                </table>
            </div>
        </section>
        <?php endif; ?>

        <!-- 5. Compound signals -->
        <?php if ($compounds !== []): ?>
        <section class="surface-primary">
            <h2 class="text-lg font-semibold text-slate-100">Compound detections</h2>
            <p class="mt-1 text-xs text-muted">Active compound signals — co-occurring patterns across multiple signal types.</p>
            <div class="mt-3 space-y-3">
                <?php foreach ($compounds as $comp): ?>
                    <?php
                    $compScore = (float) ($comp['score'] ?? 0);
                    $compConf = (float) ($comp['confidence'] ?? 0);
                    $compEvidence = json_decode((string) ($comp['evidence_json'] ?? '[]'), true);
                    if (!is_array($compEvidence)) { $compEvidence = []; }
                    $compScoreClass = $compScore >= 0.5 ? 'text-red-400 font-semibold' : ($compScore >= 0.25 ? 'text-orange-400' : 'text-slate-300');
                    ?>
                    <div class="surface-tertiary">
                        <div class="flex items-center justify-between">
                            <div>
                                <p class="text-sm text-slate-100 font-medium"><?= htmlspecialchars((string) ($comp['display_name'] ?? $comp['compound_type']), ENT_QUOTES) ?></p>
                                <?php if (($comp['compound_description'] ?? '') !== ''): ?>
                                    <p class="text-xs text-muted mt-0.5"><?= htmlspecialchars((string) $comp['compound_description'], ENT_QUOTES) ?></p>
                                <?php endif; ?>
                            </div>
                            <div class="text-right">
                                <p class="text-sm <?= $compScoreClass ?>"><?= number_format($compScore, 4) ?></p>
                                <p class="text-[10px] text-muted">conf <?= number_format($compConf, 3) ?></p>
                            </div>
                        </div>
                        <?php if ($compEvidence !== []): ?>
                            <div class="mt-2 flex flex-wrap gap-2">
                                <?php foreach ($compEvidence as $ce): ?>
                                    <span class="inline-flex items-center rounded bg-slate-700 px-2 py-0.5 text-[10px] text-slate-300">
                                        <?= htmlspecialchars((string) ($ce['signal_type'] ?? ''), ENT_QUOTES) ?>: <?= number_format((float) ($ce['value'] ?? 0), 3) ?>
                                        <span class="text-muted ml-1">(min <?= number_format((float) ($ce['min_required'] ?? 0), 2) ?>)</span>
                                    </span>
                                <?php endforeach; ?>
                            </div>
                        <?php endif; ?>
                        <div class="mt-1 text-[10px] text-muted">First detected: <?= htmlspecialchars((string) ($comp['first_detected_at'] ?? ''), ENT_QUOTES) ?></div>
                    </div>
                <?php endforeach; ?>
            </div>
        </section>
        <?php endif; ?>

        <!-- 6. Profile trend -->
        <?php if ($profileHistory !== []): ?>
        <section class="surface-primary">
            <h2 class="text-lg font-semibold text-slate-100">Profile trend</h2>
            <p class="mt-1 text-xs text-muted">Daily CIP profile snapshots — most recent first.</p>
            <div class="mt-3 table-shell">
                <table class="table-ui">
                    <thead>
                    <tr class="border-b border-border/70 text-xs uppercase tracking-[0.15em] text-muted">
                        <th class="px-3 py-2 text-left">Date</th>
                        <th class="px-3 py-2 text-right">Risk score</th>
                        <th class="px-3 py-2 text-right">Rank</th>
                        <th class="px-3 py-2 text-right">Percentile</th>
                        <th class="px-3 py-2 text-right">Confidence</th>
                        <th class="px-3 py-2 text-right">Freshness</th>
                        <th class="px-3 py-2 text-right">Coverage</th>
                    </tr>
                    </thead>
                    <tbody>
                    <?php foreach ($profileHistory as $snap): ?>
                        <tr class="border-b border-border/50">
                            <td class="px-3 py-2 text-sm text-slate-100"><?= htmlspecialchars((string) ($snap['snapshot_date'] ?? ''), ENT_QUOTES) ?></td>
                            <td class="px-3 py-2 text-right text-sm"><?= number_format((float) ($snap['risk_score'] ?? 0), 6) ?></td>
                            <td class="px-3 py-2 text-right text-sm">#<?= number_format((int) ($snap['risk_rank'] ?? 0)) ?></td>
                            <td class="px-3 py-2 text-right text-sm"><?= number_format((float) ($snap['risk_percentile'] ?? 0) * 100, 2) ?>%</td>
                            <td class="px-3 py-2 text-right text-sm"><?= number_format((float) ($snap['confidence'] ?? 0), 3) ?></td>
                            <td class="px-3 py-2 text-right text-sm"><?= number_format((float) ($snap['freshness'] ?? 0), 3) ?></td>
                            <td class="px-3 py-2 text-right text-sm"><?= number_format((float) ($snap['effective_coverage'] ?? 0), 3) ?></td>
                        </tr>
                    <?php endforeach; ?>
                    </tbody>
                </table>
            </div>
        </section>
        <?php endif; ?>
    </div>

    <!-- Right column: Actions + History + Notes + Related + Profile (1/3 width) -->
    <div class="space-y-4">
        <!-- Actions -->
        <?php if (in_array($event['state'], ['active', 'acknowledged'], true)): ?>
        <section class="surface-primary">
            <h2 class="text-lg font-semibold text-slate-100">Actions</h2>
            <form method="post" class="mt-3 space-y-3">
                <div>
                    <label for="reason" class="text-xs text-muted">Reason (optional)</label>
                    <input type="text" id="reason" name="reason" class="mt-1 w-full rounded border border-border bg-slate-800 px-3 py-2 text-sm text-slate-100 placeholder-slate-500" placeholder="Why are you taking this action?">
                </div>
                <?php if ($event['state'] === 'active'): ?>
                    <button type="submit" name="acknowledge" value="1" class="btn-secondary w-full text-sm">Acknowledge</button>
                <?php endif; ?>
                <button type="submit" name="resolve" value="1" class="btn-primary w-full text-sm">Resolve</button>
                <div class="border-t border-border/50 pt-3">
                    <p class="text-[10px] text-muted mb-2">Suppress: hide from queue for a period unless materially worsened.</p>
                    <div class="flex gap-2">
                        <select name="suppress_hours" class="flex-1 rounded border border-border bg-slate-800 px-2 py-1.5 text-xs text-slate-100">
                            <option value="24">24 hours</option>
                            <option value="72" selected>3 days</option>
                            <option value="168">7 days</option>
                            <option value="336">14 days</option>
                            <option value="720">30 days</option>
                        </select>
                        <button type="submit" name="suppress" value="1" class="btn-secondary text-xs">Suppress</button>
                    </div>
                </div>
            </form>
        </section>
        <?php endif; ?>

        <!-- Add note -->
        <section class="surface-primary">
            <h2 class="text-lg font-semibold text-slate-100">Add note</h2>
            <form method="post" class="mt-3">
                <textarea name="note" rows="3" class="w-full rounded border border-border bg-slate-800 px-3 py-2 text-sm text-slate-100 placeholder-slate-500" placeholder="Analyst notes, reasoning, follow-up instructions..."></textarea>
                <button type="submit" name="add_note" value="1" class="mt-2 btn-secondary text-xs">Save note</button>
            </form>
        </section>

        <!-- Notes -->
        <?php if ($notes !== []): ?>
        <section class="surface-primary">
            <h2 class="text-lg font-semibold text-slate-100">Notes</h2>
            <div class="mt-3 space-y-2">
                <?php foreach ($notes as $n): ?>
                    <div class="surface-tertiary">
                        <div class="flex items-center justify-between text-xs text-muted">
                            <span><?= htmlspecialchars((string) ($n['analyst'] ?? ''), ENT_QUOTES) ?></span>
                            <span><?= htmlspecialchars((string) ($n['created_at'] ?? ''), ENT_QUOTES) ?></span>
                        </div>
                        <p class="mt-1 text-sm text-slate-200"><?= nl2br(htmlspecialchars((string) ($n['note'] ?? ''), ENT_QUOTES)) ?></p>
                    </div>
                <?php endforeach; ?>
            </div>
        </section>
        <?php endif; ?>

        <!-- State history (audit log) -->
        <section class="surface-primary">
            <h2 class="text-lg font-semibold text-slate-100">State history</h2>
            <?php if ($stateHistory === []): ?>
                <p class="mt-2 text-sm text-muted">No state transitions recorded yet.</p>
            <?php else: ?>
                <div class="mt-3 space-y-2">
                    <?php foreach ($stateHistory as $h): ?>
                        <div class="surface-tertiary">
                            <div class="flex items-center gap-2 text-xs">
                                <span class="text-slate-400"><?= htmlspecialchars((string) ($h['previous_state'] ?? ''), ENT_QUOTES) ?></span>
                                <span class="text-muted">&rarr;</span>
                                <span class="text-slate-200 font-medium"><?= htmlspecialchars((string) ($h['new_state'] ?? ''), ENT_QUOTES) ?></span>
                                <?php if (($h['previous_severity'] ?? '') !== ($h['new_severity'] ?? '')): ?>
                                    <span class="text-muted">|</span>
                                    <span class="text-orange-400 text-[10px]"><?= htmlspecialchars((string) ($h['previous_severity'] ?? ''), ENT_QUOTES) ?> &rarr; <?= htmlspecialchars((string) ($h['new_severity'] ?? ''), ENT_QUOTES) ?></span>
                                <?php endif; ?>
                            </div>
                            <div class="mt-1 flex items-center justify-between text-[10px] text-muted">
                                <span>by <?= htmlspecialchars((string) ($h['changed_by'] ?? 'system'), ENT_QUOTES) ?></span>
                                <span><?= htmlspecialchars((string) ($h['created_at'] ?? ''), ENT_QUOTES) ?></span>
                            </div>
                            <?php if (($h['reason'] ?? '') !== ''): ?>
                                <p class="mt-1 text-xs text-slate-400"><?= htmlspecialchars((string) $h['reason'], ENT_QUOTES) ?></p>
                            <?php endif; ?>
                        </div>
                    <?php endforeach; ?>
                </div>
            <?php endif; ?>
        </section>

        <!-- Related events -->
        <?php if ($related !== []): ?>
        <section class="surface-primary">
            <h2 class="text-lg font-semibold text-slate-100">Related events</h2>
            <p class="mt-1 text-xs text-muted">Other events for the same entity.</p>
            <div class="mt-3 space-y-2">
                <?php foreach ($related as $rel): ?>
                    <?php
                    $relSev = (string) ($rel['severity'] ?? 'medium');
                    $relSevClass = match ($relSev) {
                        'critical' => 'text-red-400',
                        'high'     => 'text-orange-400',
                        'medium'   => 'text-amber-400',
                        default    => 'text-slate-400',
                    };
                    ?>
                    <div class="surface-tertiary">
                        <a href="/intelligence-events/view.php?id=<?= (int) $rel['id'] ?>" class="text-sm text-accent hover:underline"><?= htmlspecialchars((string) ($rel['title'] ?? $rel['event_type']), ENT_QUOTES) ?></a>
                        <div class="mt-1 flex items-center gap-2 text-[10px]">
                            <span class="<?= $relSevClass ?>"><?= strtoupper($relSev) ?></span>
                            <span class="text-muted"><?= htmlspecialchars((string) ($rel['state'] ?? ''), ENT_QUOTES) ?></span>
                            <span class="text-muted"><?= number_format((float) ($rel['impact_score'] ?? 0), 3) ?></span>
                        </div>
                    </div>
                <?php endforeach; ?>
            </div>
        </section>
        <?php endif; ?>

        <!-- Current profile -->
        <?php if ($profile !== null): ?>
        <section class="surface-primary">
            <h2 class="text-lg font-semibold text-slate-100">Current profile</h2>
            <div class="mt-3 grid gap-2 grid-cols-2">
                <div class="surface-tertiary">
                    <p class="text-xs text-muted">Risk score</p>
                    <p class="mt-1 text-sm text-slate-200"><?= number_format((float) ($profile['risk_score'] ?? 0), 6) ?></p>
                </div>
                <div class="surface-tertiary">
                    <p class="text-xs text-muted">Rank</p>
                    <p class="mt-1 text-sm text-slate-200">#<?= number_format((int) ($profile['risk_rank'] ?? 0)) ?></p>
                </div>
                <div class="surface-tertiary">
                    <p class="text-xs text-muted">Percentile</p>
                    <p class="mt-1 text-sm text-slate-200"><?= number_format((float) ($profile['risk_percentile'] ?? 0) * 100, 2) ?>%</p>
                </div>
                <div class="surface-tertiary">
                    <p class="text-xs text-muted">Confidence</p>
                    <p class="mt-1 text-sm text-slate-200"><?= number_format((float) ($profile['confidence'] ?? 0), 3) ?></p>
                </div>
                <div class="surface-tertiary">
                    <p class="text-xs text-muted">Freshness</p>
                    <p class="mt-1 text-sm text-slate-200"><?= number_format((float) ($profile['freshness'] ?? 0), 3) ?></p>
                </div>
                <div class="surface-tertiary">
                    <p class="text-xs text-muted">Coverage</p>
                    <p class="mt-1 text-sm text-slate-200"><?= number_format((float) ($profile['effective_coverage'] ?? 0), 3) ?></p>
                </div>
            </div>
            <div class="mt-2 grid gap-2 grid-cols-2">
                <div class="surface-tertiary">
                    <p class="text-xs text-muted">24h delta</p>
                    <?php $d24 = (float) ($profile['risk_delta_24h'] ?? 0); ?>
                    <p class="mt-1 text-sm <?= $d24 > 0 ? 'text-red-400' : ($d24 < 0 ? 'text-emerald-400' : 'text-slate-400') ?>"><?= ($d24 >= 0 ? '+' : '') . number_format($d24, 6) ?></p>
                </div>
                <div class="surface-tertiary">
                    <p class="text-xs text-muted">7d delta</p>
                    <?php $d7 = (float) ($profile['risk_delta_7d'] ?? 0); ?>
                    <p class="mt-1 text-sm <?= $d7 > 0 ? 'text-red-400' : ($d7 < 0 ? 'text-emerald-400' : 'text-slate-400') ?>"><?= ($d7 >= 0 ? '+' : '') . number_format($d7, 6) ?></p>
                </div>
            </div>
        </section>
        <?php endif; ?>
    </div>
</div>

<?php include __DIR__ . '/../../src/views/partials/footer.php'; ?>
