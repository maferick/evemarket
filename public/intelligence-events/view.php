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

// Handle compound outcome recording
if ($_SERVER['REQUEST_METHOD'] === 'POST' && isset($_POST['compound_outcome'])) {
    $outcome = (string) ($_POST['compound_outcome'] ?? '');
    $outcomeNotes = trim((string) ($_POST['outcome_notes'] ?? ''));
    $characterId = max(0, (int) ($_POST['character_id'] ?? 0));
    if ($characterId > 0 && $outcome !== '') {
        $count = db_compound_analyst_outcome_record(
            $characterId, $outcome, 'analyst', $eventId,
            $outcomeNotes !== '' ? $outcomeNotes : null
        );
    }
    header('Location: /intelligence-events/view.php?id=' . $eventId . '&action=outcome_recorded');
    exit;
}

// Load all data
$evidence = db_intelligence_event_evidence($eventId);
$event = $evidence['event'];

if ($event === null) {
    header('Location: /intelligence-events/');
    exit;
}

// Set page title to character name for browser tab
$title = ($event['entity_name'] ?? '') !== ''
    ? htmlspecialchars((string) $event['entity_name'], ENT_QUOTES) . ' — Event Detail'
    : 'Event Detail';

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
    'acknowledged'    => 'Event acknowledged.',
    'resolved'        => 'Event resolved.',
    'suppressed'      => 'Event suppressed.',
    'noted'           => 'Note added.',
    'outcome_recorded' => 'Compound outcome recorded.',
    default           => '',
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
    <?php
    // Human-readable event type labels
    $eventTypeLabel = match ($event['event_type'] ?? '') {
        'risk_rank_entry_top50'        => 'Top 50 Risk Entry',
        'risk_rank_entry_top200'       => 'Top 200 Risk Entry',
        'percentile_escalation'        => 'Percentile Escalation',
        'risk_score_surge'             => 'Risk Score Surge',
        'rank_jump'                    => 'Significant Rank Jump',
        'new_high_weight_signal'       => 'New High-Weight Signal',
        'multi_domain_activation'      => 'Multi-Domain Activation',
        'freshness_degradation'        => 'Profile Freshness Degradation',
        'coverage_expansion'           => 'Coverage Expansion',
        'compound_signal_activated'    => 'Compound Signal Activated',
        'compound_signal_strengthened' => 'Compound Signal Strengthened',
        default                        => htmlspecialchars((string) ($event['event_type'] ?? ''), ENT_QUOTES),
    };
    $eventExplanation = match ($event['event_type'] ?? '') {
        'risk_rank_entry_top50'        => 'This character entered the top 50 risk-ranked profiles. They are now among the most suspicious characters being tracked.',
        'risk_rank_entry_top200'       => 'This character entered the top 200 risk-ranked profiles, indicating elevated suspicion across multiple signal domains.',
        'percentile_escalation'        => 'This character moved into a higher risk percentile bucket, indicating a worsening intelligence picture.',
        'risk_score_surge'             => 'The fused risk score for this character increased significantly in the last 24 hours.',
        'rank_jump'                    => 'This character jumped significantly in the overall risk rankings in a single computation cycle.',
        'new_high_weight_signal'       => 'A new signal with high confidence and significant weight appeared for this character.',
        'multi_domain_activation'      => 'This character now has active signals across 4+ independent domains (behavioral, graph, temporal, movement, relational) — convergent evidence from different analysis methods.',
        'freshness_degradation'        => 'The signals backing this character\'s profile are going stale. The trust surface freshness dropped below the operational threshold.',
        'coverage_expansion'           => 'Signal coverage for this character materially expanded — more signal domains are now contributing to their profile.',
        'compound_signal_activated'    => 'A compound detection was triggered — multiple independent signals co-occurred in a pattern that indicates a specific operational concern.',
        'compound_signal_strengthened' => 'An existing compound detection strengthened — the pattern became more pronounced.',
        default                        => '',
    };
    ?>

    <div class="mt-4">
        <!-- Character identity — biggest, most visible element -->
        <?php if (($event['entity_name'] ?? '') !== ''): ?>
            <div class="flex items-baseline gap-3">
                <a href="/battle-intelligence/character.php?character_id=<?= urlencode((string) ((int) ($event['entity_id'] ?? 0))) ?>" class="text-2xl text-accent font-semibold hover:underline"><?= htmlspecialchars((string) $event['entity_name'], ENT_QUOTES) ?></a>
                <?php if (($event['corporation_name'] ?? '') !== '' || ($event['alliance_name'] ?? '') !== ''): ?>
                    <span class="text-base text-slate-400"><?php
                        $orgParts = [];
                        if (($event['corporation_name'] ?? '') !== '') { $orgParts[] = htmlspecialchars((string) $event['corporation_name'], ENT_QUOTES); }
                        if (($event['alliance_name'] ?? '') !== '') { $orgParts[] = '[' . htmlspecialchars((string) $event['alliance_name'], ENT_QUOTES) . ']'; }
                        echo implode(' ', $orgParts);
                    ?></span>
                <?php endif; ?>
            </div>
        <?php endif; ?>

        <!-- Event title and type -->
        <p class="mt-2 text-xs uppercase tracking-[0.16em] text-muted"><?= $eventTypeLabel ?></p>
        <h1 class="mt-1 text-xl font-semibold text-slate-50"><?= htmlspecialchars((string) ($event['title'] ?? 'Untitled Event'), ENT_QUOTES) ?></h1>
        <?php if ($eventExplanation !== ''): ?>
            <p class="mt-2 text-sm text-slate-400"><?= $eventExplanation ?></p>
        <?php endif; ?>

        <!-- Status badges + metadata row -->
        <div class="mt-3 flex flex-wrap items-center justify-between gap-4">
            <div class="flex flex-wrap items-center gap-2">
                <span class="inline-flex items-center rounded px-2 py-0.5 text-xs font-medium <?= $sevClasses ?>"><?= strtoupper($sev) ?></span>
                <span class="inline-flex items-center rounded px-2 py-0.5 text-xs font-medium <?= $stateClasses ?>"><?= strtoupper((string) ($event['state'] ?? '')) ?></span>
                <span class="text-xs <?= ($event['event_family'] ?? '') === 'threat' ? 'text-red-400' : 'text-cyan-400' ?>"><?= ($event['event_family'] ?? '') === 'threat' ? 'Threat' : 'Profile Quality' ?></span>
                <?php if (((int) ($event['escalation_count'] ?? 1)) > 1): ?>
                    <span class="text-xs text-orange-400 font-medium">Escalated <?= (int) $event['escalation_count'] ?>x</span>
                <?php endif; ?>
                <?php if (($event['state'] ?? '') === 'suppressed' && ($event['suppressed_until'] ?? '') !== ''): ?>
                    <span class="text-xs text-slate-400">Suppressed until <?= htmlspecialchars((string) $event['suppressed_until'], ENT_QUOTES) ?></span>
                <?php endif; ?>
            </div>
            <div class="flex flex-wrap items-center gap-4 text-xs text-muted">
                <span>Detected <?= $eventAge ?> ago</span>
                <span>In state <?= $timeInState ?></span>
                <span>Impact <span class="text-slate-200 font-medium"><?= number_format((float) ($event['impact_score'] ?? 0), 4) ?></span></span>
            </div>
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
            // (exclude profile snapshot fields and compound evidence which are shown separately)
            $profileFields = ['risk_score', 'risk_rank', 'risk_percentile', 'confidence', 'freshness', 'effective_coverage'];
            $compoundFields = ['contributing_signals', 'compound_family', 'score_mode', 'confidence_derivation', 'profile_conditions_met'];
            $triggerFields = array_diff_key($detailData, array_flip(array_merge($profileFields, $compoundFields)));

            // Human-readable labels for common trigger field names
            $fieldLabels = [
                'compound_type' => 'Detection pattern',
                'compound_score' => 'Pattern strength',
                'compound_confidence' => 'Pattern confidence',
                'signal_type' => 'Signal',
                'signal_value' => 'Signal strength',
                'signal_weight' => 'Signal weight',
                'signal_confidence' => 'Signal confidence',
                'delta_24h' => '24-hour change',
                'new_signals_24h' => 'New signals (24h)',
                'rank' => 'Current rank',
                'previous_rank' => 'Previous rank',
                'jump' => 'Positions jumped',
                'domain_count' => 'Active domains',
                'previous_bucket' => 'Previous percentile bucket',
                'current_bucket' => 'Current percentile bucket',
                'percentile' => 'Exact percentile',
            ];
            ?>
            <?php if ($triggerFields !== []): ?>
                <div class="mt-3 grid gap-2 sm:grid-cols-2 lg:grid-cols-3">
                    <?php foreach ($triggerFields as $key => $val): ?>
                        <div class="surface-tertiary">
                            <p class="text-xs text-muted"><?= htmlspecialchars($fieldLabels[$key] ?? ucfirst(str_replace('_', ' ', (string) $key)), ENT_QUOTES) ?></p>
                            <p class="mt-1 text-sm text-slate-200">
                                <?php if (is_numeric($val)): ?>
                                    <?= number_format((float) $val, 4) ?>
                                <?php elseif (is_bool($val)): ?>
                                    <?= $val ? 'Yes' : 'No' ?>
                                <?php elseif (is_array($val)): ?>
                                    <?php if (isset($val[0]) && is_array($val[0])): ?>
                                        <?php // Array of objects — show count ?>
                                        <span class="text-slate-400"><?= count($val) ?> item<?= count($val) !== 1 ? 's' : '' ?></span>
                                    <?php elseif (array_keys($val) !== range(0, count($val) - 1)): ?>
                                        <?php // Associative array — show key: value pairs ?>
                                        <?php foreach ($val as $k => $v): ?>
                                            <span class="inline-block mr-2 text-xs"><?= htmlspecialchars((string) $k, ENT_QUOTES) ?>: <?= is_numeric($v) ? number_format((float) $v, 2) : htmlspecialchars((string) $v, ENT_QUOTES) ?></span>
                                        <?php endforeach; ?>
                                    <?php else: ?>
                                        <?= htmlspecialchars(implode(', ', array_map('strval', $val)), ENT_QUOTES) ?>
                                    <?php endif; ?>
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
                            <span><span class="text-muted"><?= htmlspecialchars(str_replace('_', ' ', (string) $tKey), ENT_QUOTES) ?>:</span> <span class="text-slate-200"><?php if (is_numeric($tVal)): ?><?= number_format((float) $tVal, 4) ?><?php elseif (is_array($tVal)): ?><?php foreach ($tVal as $ak => $av): ?><span class="mr-1"><?= htmlspecialchars((string) $ak, ENT_QUOTES) ?>:<?= is_numeric($av) ? number_format((float) $av, 2) : htmlspecialchars((string) $av, ENT_QUOTES) ?></span><?php endforeach; ?><?php else: ?><?= htmlspecialchars((string) $tVal, ENT_QUOTES) ?><?php endif; ?></span></span>
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
                        $sigVal = (float) ($sig['signal_value'] ?? 0);
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
                    $compEvidence = json_decode((string) ($comp['evidence_json'] ?? '{}'), true);
                    if (!is_array($compEvidence)) { $compEvidence = []; }
                    // Support both new enriched format and legacy array format
                    $compSignals = isset($compEvidence['signals']) ? (array) $compEvidence['signals'] : $compEvidence;
                    $confDerivation = (array) ($compEvidence['confidence_derivation'] ?? []);
                    $compFamily = (string) ($compEvidence['compound_family'] ?? '');
                    $compScoreMode = (string) ($compEvidence['score_mode'] ?? '');
                    $profileCondsMet = (array) ($compEvidence['profile_conditions_met'] ?? []);
                    $compScoreClass = $compScore >= 0.5 ? 'text-red-400 font-semibold' : ($compScore >= 0.25 ? 'text-orange-400' : 'text-slate-300');
                    $familyColors = ['infiltration' => 'text-red-400', 'coordination' => 'text-orange-400', 'prioritization' => 'text-amber-400', 'trust' => 'text-cyan-400'];
                    ?>
                    <div class="surface-tertiary">
                        <div class="flex items-center justify-between">
                            <div>
                                <div class="flex items-center gap-2">
                                    <p class="text-sm text-slate-100 font-medium"><?= htmlspecialchars((string) ($comp['display_name'] ?? $comp['compound_type']), ENT_QUOTES) ?></p>
                                    <?php if ($compFamily !== ''): ?>
                                        <span class="rounded-full bg-slate-700 px-2 py-0.5 text-[10px] uppercase tracking-wider <?= $familyColors[$compFamily] ?? 'text-slate-300' ?>"><?= htmlspecialchars($compFamily, ENT_QUOTES) ?></span>
                                    <?php endif; ?>
                                </div>
                                <?php if (($comp['compound_description'] ?? '') !== ''): ?>
                                    <p class="text-xs text-muted mt-0.5"><?= htmlspecialchars((string) $comp['compound_description'], ENT_QUOTES) ?></p>
                                <?php endif; ?>
                            </div>
                            <div class="text-right">
                                <p class="text-sm <?= $compScoreClass ?>"><?= number_format($compScore, 4) ?></p>
                                <p class="text-[10px] text-muted">conf <?= number_format($compConf, 3) ?><?php if ($compScoreMode !== ''): ?> (<?= $compScoreMode ?>)<?php endif; ?></p>
                            </div>
                        </div>
                        <?php if ($compSignals !== []): ?>
                            <div class="mt-2 flex flex-wrap gap-2">
                                <?php foreach ($compSignals as $ce): ?>
                                    <?php if (!is_array($ce)) { continue; } ?>
                                    <span class="inline-flex items-center rounded bg-slate-700 px-2 py-0.5 text-[10px] text-slate-300">
                                        <?= htmlspecialchars((string) ($ce['signal_type'] ?? ''), ENT_QUOTES) ?>: <?= number_format((float) ($ce['value'] ?? 0), 3) ?>
                                        <span class="text-muted ml-1">(min <?= number_format((float) ($ce['min_required'] ?? 0), 2) ?>)</span>
                                    </span>
                                <?php endforeach; ?>
                            </div>
                        <?php endif; ?>
                        <?php if ($profileCondsMet !== []): ?>
                            <div class="mt-1 flex flex-wrap gap-2">
                                <?php foreach ($profileCondsMet as $condCol => $condVal): ?>
                                    <span class="inline-flex items-center rounded bg-slate-600/50 px-2 py-0.5 text-[10px] text-cyan-300">
                                        <?= htmlspecialchars(str_replace('_', ' ', (string) $condCol), ENT_QUOTES) ?>: <?= number_format((float) $condVal, 4) ?>
                                    </span>
                                <?php endforeach; ?>
                            </div>
                        <?php endif; ?>
                        <!-- Confidence derivation -->
                        <?php if ($confDerivation !== [] && isset($confDerivation['per_signal'])): ?>
                            <div class="mt-2 text-[10px] text-muted">
                                <span>Confidence: <?= htmlspecialchars((string) ($confDerivation['mode'] ?? 'min_signal'), ENT_QUOTES) ?></span>
                                <?php if (($confDerivation['weakest_signal'] ?? '') !== ''): ?>
                                    <span class="ml-2">weakest: <span class="text-amber-400"><?= htmlspecialchars((string) $confDerivation['weakest_signal'], ENT_QUOTES) ?></span>
                                    (<?= number_format((float) ($confDerivation['per_signal'][$confDerivation['weakest_signal']] ?? 0), 3) ?>)</span>
                                <?php endif; ?>
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
                            <td class="px-3 py-2 text-right text-sm"><?= number_format((float) ($snap['signal_coverage'] ?? 0), 3) ?></td>
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

        <!-- Compound outcome capture -->
        <?php if ($compounds !== [] && ($event['entity_type'] ?? '') === 'character'): ?>
        <section class="surface-primary">
            <h2 class="text-lg font-semibold text-slate-100">Rate compound accuracy</h2>
            <p class="mt-1 text-xs text-muted">Record whether the compound detections on this character were useful. This feedback trains compound validation metrics.</p>
            <form method="post" class="mt-3 space-y-2">
                <input type="hidden" name="character_id" value="<?= (int) ($event['entity_id'] ?? 0) ?>">
                <div class="flex flex-wrap gap-2">
                    <button type="submit" name="compound_outcome" value="true_positive" class="rounded border border-emerald-700/50 bg-emerald-900/30 px-3 py-1.5 text-xs text-emerald-300 hover:bg-emerald-900/50">True positive</button>
                    <button type="submit" name="compound_outcome" value="false_positive" class="rounded border border-red-700/50 bg-red-900/30 px-3 py-1.5 text-xs text-red-300 hover:bg-red-900/50">False positive</button>
                    <button type="submit" name="compound_outcome" value="inconclusive" class="rounded border border-amber-700/50 bg-amber-900/30 px-3 py-1.5 text-xs text-amber-300 hover:bg-amber-900/50">Inconclusive</button>
                    <button type="submit" name="compound_outcome" value="confirmed_clean" class="rounded border border-slate-600/50 bg-slate-700/30 px-3 py-1.5 text-xs text-slate-300 hover:bg-slate-700/50">Confirmed clean</button>
                </div>
                <input type="text" name="outcome_notes" class="w-full rounded border border-border bg-slate-800 px-3 py-1.5 text-xs text-slate-100 placeholder-slate-500" placeholder="Optional: why this verdict?">
            </form>
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
