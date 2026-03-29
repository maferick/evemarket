<?php

declare(strict_types=1);

require_once __DIR__ . '/../../src/bootstrap.php';

$characterId = max(0, (int) ($_GET['character_id'] ?? 0));
$title = 'Character Intelligence';
$data = battle_intelligence_character_data($characterId);
$character = $data['character'] ?? null;
$battles = (array) ($data['battles'] ?? []);
$evidence = (array) ($data['evidence'] ?? []);
$orgHistory = (array) ($data['org_history'] ?? []);

// KGv2 enhanced data
$temporalMetrics = db_character_temporal_metrics($characterId);
$typedInteractions = db_character_typed_interactions($characterId, 30);
$communityInfo = db_graph_community_assignments($characterId);
$evidencePaths = db_character_evidence_paths($characterId);
$analystFeedback = db_analyst_feedback_for_character($characterId);

// Handle feedback submission
if ($_SERVER['REQUEST_METHOD'] === 'POST' && isset($_POST['feedback_label']) && $characterId > 0) {
    $label = (string) $_POST['feedback_label'];
    $confidence = (float) ($_POST['feedback_confidence'] ?? 0.5);
    $notes = trim((string) ($_POST['feedback_notes'] ?? ''));
    $contextJson = json_encode([
        'review_priority_score' => (float) ($character['review_priority_score'] ?? 0),
        'confidence_score' => (float) ($character['confidence_score'] ?? 0),
        'percentile_rank' => (float) ($character['percentile_rank'] ?? 0),
    ]);
    db_analyst_feedback_save($characterId, $label, $confidence, $notes !== '' ? $notes : null, $contextJson);
    header('Location: /battle-intelligence/character.php?character_id=' . $characterId . '&feedback=saved');
    exit;
}

/**
 * Human-friendly helpers for the character detail view.
 */

/** Return a risk-level label and Tailwind color classes based on review priority score. */
function ci_risk_level(float $score): array
{
    if ($score >= 0.75) {
        return ['label' => 'High', 'bg' => 'bg-red-900/60', 'text' => 'text-red-300', 'bar' => 'bg-red-500'];
    }
    if ($score >= 0.45) {
        return ['label' => 'Medium', 'bg' => 'bg-yellow-900/60', 'text' => 'text-yellow-300', 'bar' => 'bg-yellow-500'];
    }
    if ($score >= 0.20) {
        return ['label' => 'Low', 'bg' => 'bg-blue-900/60', 'text' => 'text-blue-300', 'bar' => 'bg-blue-500'];
    }
    return ['label' => 'Minimal', 'bg' => 'bg-green-900/60', 'text' => 'text-green-300', 'bar' => 'bg-green-500'];
}

/** Return color classes for a 0-1 metric (higher = more suspicious). */
function ci_metric_color(float $value, float $cautionAt = 0.4, float $dangerAt = 0.7): string
{
    if ($value >= $dangerAt) {
        return 'text-red-400';
    }
    if ($value >= $cautionAt) {
        return 'text-yellow-400';
    }
    return 'text-green-400';
}

/** Return color for lift values (>1 means enemy survives more than expected). */
function ci_lift_color(float $lift): string
{
    if ($lift >= 1.5) {
        return 'text-red-400';
    }
    if ($lift >= 1.15) {
        return 'text-yellow-400';
    }
    return 'text-green-400';
}

/** Color for overperformance scores. */
function ci_overperf_color(float $score): string
{
    if ($score >= 1.3) {
        return 'text-red-400';
    }
    if ($score >= 1.1) {
        return 'text-yellow-400';
    }
    if ($score >= 0.9) {
        return 'text-slate-100';
    }
    return 'text-blue-400';
}

/** Return a human-readable label for an anomaly class. */
function ci_anomaly_label(string $class): array
{
    return match ($class) {
        'high_enemy_overperformance' => ['label' => 'Suspicious', 'bg' => 'bg-red-900/60', 'text' => 'text-red-300'],
        'underperforming' => ['label' => 'Under-performing', 'bg' => 'bg-blue-900/60', 'text' => 'text-blue-300'],
        default => ['label' => 'Normal', 'bg' => 'bg-slate-700', 'text' => 'text-slate-300'],
    };
}

/** Convert snake_case evidence keys to human-readable labels. */
function ci_evidence_label(string $key): string
{
    $map = [
        'anomalous_battle_presence_count' => 'Anomalous battle appearances',
        'enemy_same_hull_survival_lift_detail' => 'Enemy hull survival lift',
        'enemy_sustain_lift' => 'Enemy sustain lift',
        'graph_copresence_cluster_proximity' => 'Network clustering',
        'repeatability_across_battles_windows' => 'Pattern repeatability',
        'anomalous_presence_rate' => 'Anomalous presence rate',
        'presence_rate_delta' => 'Presence rate delta',
        'org_history_movement_180d' => 'Corp movement (180 days)',
    ];
    return $map[$key] ?? ucwords(str_replace('_', ' ', $key));
}

/** Render a slim progress bar (0-1 range). */
function ci_progress_bar(float $value, string $barColor = 'bg-cyan-500', float $max = 1.0): string
{
    $pct = min(100, max(0, ($value / $max) * 100));
    return '<div class="mt-1 h-1.5 w-full rounded-full bg-slate-700/60 overflow-hidden">'
         . '<div class="h-full rounded-full ' . $barColor . '" style="width:' . number_format($pct, 1) . '%"></div>'
         . '</div>';
}

/** Render a plain-English one-line verdict based on key metrics. */
function ci_verdict_summary(array $char): string
{
    $score = (float) ($char['review_priority_score'] ?? 0);
    $pctile = (float) ($char['percentile_rank'] ?? 0) * 100;
    $lift = (float) ($char['enemy_sustain_lift'] ?? 0);
    $repeat = (float) ($char['repeatability_score'] ?? 0);
    $hops = (float) ($char['corp_hop_frequency_180d'] ?? 0);

    $parts = [];

    if ($score >= 0.75) {
        $parts[] = 'This character has a <strong class="text-red-400">high</strong> review priority and warrants close investigation.';
    } elseif ($score >= 0.45) {
        $parts[] = 'This character shows <strong class="text-yellow-400">moderate</strong> suspicious signals.';
    } elseif ($score >= 0.20) {
        $parts[] = 'This character has <strong class="text-blue-400">low-level</strong> flags &mdash; likely not a priority.';
    } else {
        $parts[] = 'This character shows <strong class="text-green-400">minimal</strong> suspicious activity.';
    }

    if ($lift >= 1.15) {
        $parts[] = 'Enemies survive <strong class="text-yellow-400">' . number_format(($lift - 1) * 100, 0) . '% more</strong> than expected when this pilot is on the field.';
    }

    if ($repeat >= 0.5) {
        $parts[] = 'Suspicious patterns repeat across multiple time windows.';
    }

    if ($hops >= 3) {
        $parts[] = 'Frequent corp-hopping detected (' . number_format($hops, 0) . ' changes in 180 days).';
    }

    if ($pctile >= 95) {
        $parts[] = 'Ranks in the <strong>top ' . number_format(100 - $pctile, 1) . '%</strong> of all tracked characters.';
    }

    return implode(' ', $parts);
}

include __DIR__ . '/../../src/views/partials/header.php';
?>
<section class="surface-primary">
    <a href="/battle-intelligence" class="text-sm text-accent">&larr; Back to leaderboard</a>
    <?php if (!is_array($character)): ?>
        <p class="mt-4 text-sm text-muted">No character intelligence found.</p>
    <?php else: ?>
        <?php
            $riskLevel = ci_risk_level((float) ($character['review_priority_score'] ?? 0));
            $priorityScore = (float) ($character['review_priority_score'] ?? 0);
            $confidenceScore = (float) ($character['confidence_score'] ?? 0);
            $percentileRank = (float) ($character['percentile_rank'] ?? 0);
            $repeatabilityScore = (float) ($character['repeatability_score'] ?? 0);
            $evidenceCount = (int) ($character['evidence_count'] ?? 0);
            $enemySustainLift = (float) ($character['enemy_sustain_lift'] ?? 0);
            $hullLift = (float) ($character['enemy_same_hull_survival_lift'] ?? 0);
            $bridgeScore = (float) ($character['graph_bridge_score'] ?? 0);
            $coPres = (float) ($character['co_presence_anomalous_density'] ?? 0);
            $corpHops = (float) ($character['corp_hop_frequency_180d'] ?? 0);
            $shortTenure = (float) ($character['short_tenure_ratio_180d'] ?? 0);
        ?>
        <div class="mt-2 flex items-center gap-3">
            <h1 class="text-2xl font-semibold text-slate-50"><?= htmlspecialchars((string) ($character['character_name'] ?? 'Unknown'), ENT_QUOTES) ?></h1>
            <span class="inline-block rounded-full px-3 py-0.5 text-xs font-semibold uppercase tracking-wider <?= $riskLevel['bg'] ?> <?= $riskLevel['text'] ?>"><?= $riskLevel['label'] ?> risk</span>
        </div>

        <!-- Verdict summary -->
        <div class="mt-3 rounded border border-border/40 bg-slate-800/50 px-4 py-3 text-sm text-slate-300 leading-relaxed">
            <?= ci_verdict_summary($character) ?>
        </div>

        <!-- Primary metrics -->
        <div class="mt-4 grid gap-3 md:grid-cols-3">
            <div class="surface-tertiary">
                <p class="text-xs text-muted">Review priority</p>
                <p class="mt-1 text-xl <?= $riskLevel['text'] ?>"><?= number_format($priorityScore * 100, 1) ?>%</p>
                <?= ci_progress_bar($priorityScore, $riskLevel['bar']) ?>
                <p class="mt-1 text-[11px] text-muted">Combined suspicion score &mdash; higher means more suspicious</p>
            </div>
            <div class="surface-tertiary">
                <p class="text-xs text-muted">Confidence / Percentile</p>
                <p class="mt-1 text-xl text-slate-100"><?= number_format($confidenceScore * 100, 0) ?>% confident &middot; top <?= number_format((1 - $percentileRank) * 100, 1) ?>%</p>
                <?= ci_progress_bar($confidenceScore, 'bg-cyan-500') ?>
                <p class="mt-1 text-[11px] text-muted">How certain the model is, and where they rank vs. everyone</p>
            </div>
            <div class="surface-tertiary">
                <p class="text-xs text-muted">Repeatability / Evidence</p>
                <p class="mt-1 text-xl text-slate-100">
                    <span class="<?= ci_metric_color($repeatabilityScore, 0.4, 0.7) ?>"><?= number_format($repeatabilityScore * 100, 0) ?>%</span>
                    <span class="text-sm text-muted">repeat</span>
                    &middot;
                    <?= $evidenceCount ?> <span class="text-sm text-muted">signals</span>
                </p>
                <?= ci_progress_bar($repeatabilityScore, 'bg-cyan-500') ?>
                <p class="mt-1 text-[11px] text-muted">Pattern consistency across time windows &middot; number of supporting signals</p>
            </div>
        </div>

        <!-- Secondary metrics -->
        <div class="mt-3 grid gap-3 md:grid-cols-3">
            <div class="surface-tertiary">
                <p class="text-xs text-muted">Enemy survival boost</p>
                <p class="mt-1 text-base <?= ci_lift_color($enemySustainLift) ?>">
                    <?php if ($enemySustainLift >= 1.0): ?>
                        +<?= number_format(($enemySustainLift - 1) * 100, 0) ?>% above expected
                    <?php else: ?>
                        <?= number_format(($enemySustainLift - 1) * 100, 0) ?>% below expected
                    <?php endif; ?>
                </p>
                <p class="mt-0.5 text-xs text-muted">Hull lift: <span class="<?= ci_lift_color($hullLift) ?>"><?php if ($hullLift >= 1.0): ?>+<?= number_format(($hullLift - 1) * 100, 0) ?>%<?php else: ?><?= number_format(($hullLift - 1) * 100, 0) ?>%<?php endif; ?></span></p>
                <p class="mt-1 text-[11px] text-muted">How much more enemies survive when this pilot is present</p>
            </div>
            <div class="surface-tertiary">
                <p class="text-xs text-muted">Network position</p>
                <p class="mt-1 text-base text-slate-100">Bridge: <span class="font-semibold"><?= number_format($bridgeScore, 1) ?></span> &middot; Co-presence: <span class="font-semibold"><?= number_format($coPres * 100, 0) ?>%</span></p>
                <?= ci_progress_bar($coPres, 'bg-cyan-500') ?>
                <p class="mt-1 text-[11px] text-muted">Bridge = links between groups &middot; Co-presence = how often seen with flagged pilots</p>
            </div>
            <div class="surface-tertiary">
                <p class="text-xs text-muted">Corp movement (180 days)</p>
                <p class="mt-1 text-base text-slate-100">
                    <?php if ($corpHops == 0 && $shortTenure == 0): ?>
                        <span class="text-green-400">Stable</span> &mdash; no corp changes
                    <?php else: ?>
                        <span class="<?= $corpHops >= 3 ? 'text-red-400' : ($corpHops >= 1 ? 'text-yellow-400' : 'text-green-400') ?>"><?= number_format($corpHops, 0) ?> hops</span>
                        &middot;
                        <span class="<?= $shortTenure >= 0.5 ? 'text-red-400' : ($shortTenure >= 0.2 ? 'text-yellow-400' : 'text-green-400') ?>"><?= number_format($shortTenure * 100, 0) ?>% short stays</span>
                    <?php endif; ?>
                </p>
                <p class="mt-1 text-[11px] text-muted">Frequent corp-hopping can indicate spy behaviour</p>
            </div>
        </div>

        <details class="mt-4 surface-tertiary"><summary class="cursor-pointer text-sm text-slate-100">Org history cache context</summary><pre class="mt-3 overflow-auto text-xs text-slate-300"><?= htmlspecialchars(json_encode(['source' => (string) ($character['org_history_source'] ?? ''), 'fetched_at' => (string) ($character['org_history_fetched_at'] ?? ''), 'corp_hops_180d' => (int) ($character['corp_hops_180d'] ?? 0), 'short_tenure_hops_180d' => (int) ($character['short_tenure_hops_180d'] ?? 0), 'hostile_adjacent_hops_180d' => (int) ($character['hostile_adjacent_hops_180d'] ?? 0), 'history' => $orgHistory], JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES) ?: '{}', ENT_QUOTES) ?></pre></details>

        <!-- Evidence rows -->
        <h2 class="mt-6 text-lg font-semibold text-slate-100">Evidence breakdown</h2>
        <p class="mt-1 text-xs text-muted">Each signal that contributes to the overall suspicion score.</p>
        <div class="mt-3 table-shell"><table class="table-ui"><thead><tr class="border-b border-border/70 text-xs text-muted uppercase"><th class="px-3 py-2 text-left">Signal</th><th class="px-3 py-2 text-right">Weight</th><th class="px-3 py-2 text-left">What it means</th></tr></thead><tbody><?php foreach ($evidence as $evidenceRow): ?><tr class="border-b border-border/40"><td class="px-3 py-2 text-sm"><?= htmlspecialchars(ci_evidence_label((string) ($evidenceRow['evidence_key'] ?? '')), ENT_QUOTES) ?></td><td class="px-3 py-2 text-right font-mono text-sm <?= ci_metric_color((float) ($evidenceRow['evidence_value'] ?? 0), 0.3, 0.7) ?>"><?= htmlspecialchars(number_format((float) ($evidenceRow['evidence_value'] ?? 0), 2), ENT_QUOTES) ?></td><td class="px-3 py-2"><div class="text-sm text-slate-300"><?= htmlspecialchars((string) ($evidenceRow['evidence_text'] ?? ''), ENT_QUOTES) ?></div><?php if (is_array($evidenceRow['evidence_payload'] ?? null)): ?><details class="mt-1"><summary class="text-[11px] text-muted cursor-pointer">Show raw data</summary><pre class="mt-1 overflow-auto text-[11px] text-slate-400"><?= htmlspecialchars(json_encode($evidenceRow['evidence_payload'], JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES) ?: '{}', ENT_QUOTES) ?></pre></details><?php endif; ?></td></tr><?php endforeach; ?></tbody></table></div>

        <!-- Supporting battles -->
        <h2 class="mt-6 text-lg font-semibold text-slate-100">Supporting battles</h2>
        <p class="mt-1 text-xs text-muted">Battles where this character was present. Red rows indicate enemy over-performance anomalies.</p>
        <div class="mt-3 table-shell"><table class="table-ui"><thead><tr class="border-b border-border/70 text-xs text-muted uppercase"><th class="px-3 py-2 text-left">Battle</th><th class="px-3 py-2 text-left">Side</th><th class="px-3 py-2 text-left">Classification</th><th class="px-3 py-2 text-right">Enemy boost</th><th class="px-3 py-2 text-right">Inspect</th></tr></thead><tbody><?php foreach ($battles as $battle): ?>
            <?php
                $anomalyClass = (string) ($battle['anomaly_class'] ?? 'normal');
                $anomaly = ci_anomaly_label($anomalyClass);
                $overperf = (float) ($battle['overperformance_score'] ?? 0);
            ?>
            <tr class="border-b border-border/40"><td class="px-3 py-2"><?= htmlspecialchars((string) ($battle['system_name'] ?? 'Unknown'), ENT_QUOTES) ?><div class="text-xs text-muted"><?= htmlspecialchars((string) ($battle['started_at'] ?? ''), ENT_QUOTES) ?></div></td><td class="px-3 py-2"><?= htmlspecialchars((string) ($battle['side_name'] ?? $battle['side_key'] ?? 'unknown'), ENT_QUOTES) ?></td><td class="px-3 py-2"><span class="inline-block rounded-full px-2 py-0.5 text-[10px] uppercase tracking-wider <?= $anomaly['bg'] ?> <?= $anomaly['text'] ?>"><?= $anomaly['label'] ?></span></td><td class="px-3 py-2 text-right <?= ci_overperf_color($overperf) ?>"><?php if ($overperf >= 1.0): ?>+<?= number_format(($overperf - 1) * 100, 0) ?>%<?php else: ?><?= number_format(($overperf - 1) * 100, 0) ?>%<?php endif; ?></td><td class="px-3 py-2 text-right"><a class="text-accent" href="/battle-intelligence/battle.php?battle_id=<?= urlencode((string) ($battle['battle_id'] ?? '')) ?>">View</a></td></tr><?php endforeach; ?></tbody></table></div>

        <?php if ($temporalMetrics !== []): ?>
        <h2 class="mt-6 text-lg font-semibold text-slate-100">Temporal drift</h2>
        <p class="mt-1 text-xs text-muted">How this character's activity changes over different time windows. Divergence between windows can indicate recent escalation.</p>
        <div class="mt-3 grid gap-3 md:grid-cols-3">
            <?php foreach ($temporalMetrics as $tm): ?>
                <?php
                    $windowLabel = (string) ($tm['window_label'] ?? '');
                    $windowTitle = match ($windowLabel) {
                        '7d' => 'Last 7 days',
                        '30d' => 'Last 30 days',
                        '90d' => 'Last 90 days',
                        default => $windowLabel . ' window',
                    };
                ?>
                <div class="surface-tertiary">
                    <p class="text-xs text-muted font-semibold"><?= htmlspecialchars($windowTitle, ENT_QUOTES) ?></p>
                    <div class="mt-2 grid grid-cols-2 gap-1 text-sm">
                        <span class="text-muted">Battles</span><span class="text-slate-100 text-right"><?= (int) ($tm['battles_present'] ?? 0) ?></span>
                        <span class="text-muted">Kills</span><span class="text-slate-100 text-right"><?= (int) ($tm['kills_total'] ?? 0) ?></span>
                        <span class="text-muted">Losses</span><span class="text-slate-100 text-right"><?= (int) ($tm['losses_total'] ?? 0) ?></span>
                        <span class="text-muted">Suspicion</span><span class="text-right <?= ci_metric_color((float) ($tm['suspicion_score'] ?? 0), 0.3, 0.6) ?>"><?php
                            $susp = (float) ($tm['suspicion_score'] ?? 0);
                            echo $susp < 0.01 ? 'None' : number_format($susp * 100, 0) . '%';
                        ?></span>
                        <span class="text-muted">Co-presence</span><span class="text-right text-slate-100"><?= number_format((float) ($tm['co_presence_density'] ?? 0), 1) ?></span>
                        <span class="text-muted">Engage rate</span><span class="text-right text-slate-100"><?php
                            $engRate = (float) ($tm['engagement_rate_avg'] ?? 0);
                            echo $engRate < 0.01 ? 'None' : number_format($engRate * 100, 0) . '%';
                        ?></span>
                    </div>
                </div>
            <?php endforeach; ?>
        </div>
        <?php endif; ?>

        <?php if (is_array($communityInfo)): ?>
        <h2 class="mt-6 text-lg font-semibold text-slate-100">Community assignment</h2>
        <p class="mt-1 text-xs text-muted">Graph-based grouping showing who this character is connected to and how important they are in the network.</p>
        <div class="mt-3 grid gap-3 md:grid-cols-4">
            <div class="surface-tertiary"><p class="text-xs text-muted">Community</p><p class="mt-1 text-lg text-slate-100">#<?= (int) ($communityInfo['community_id'] ?? 0) ?> <span class="text-xs text-muted">(<?= (int) ($communityInfo['community_size'] ?? 0) ?> members)</span></p></div>
            <div class="surface-tertiary"><p class="text-xs text-muted">PageRank</p><p class="mt-1 text-lg text-slate-100"><?= number_format((float) ($communityInfo['pagerank_score'] ?? 0), 4) ?></p><p class="mt-0.5 text-[11px] text-muted">Importance in the network (higher = more central)</p></div>
            <div class="surface-tertiary"><p class="text-xs text-muted">Betweenness</p><p class="mt-1 text-lg text-slate-100"><?= number_format((float) ($communityInfo['betweenness_centrality'] ?? 0), 4) ?></p><p class="mt-0.5 text-[11px] text-muted">Sits between many groups (higher = more bridging)</p></div>
            <div class="surface-tertiary"><p class="text-xs text-muted">Connections / Bridge</p><p class="mt-1 text-lg text-slate-100"><?= (int) ($communityInfo['degree_centrality'] ?? 0) ?> connections / <?= ((int) ($communityInfo['is_bridge'] ?? 0)) ? '<span class="text-yellow-400 font-semibold">Yes</span>' : '<span class="text-green-400">No</span>' ?></p><p class="mt-0.5 text-[11px] text-muted">Direct links and whether they bridge communities</p></div>
        </div>
        <?php endif; ?>

        <?php if ($typedInteractions !== []): ?>
        <h2 class="mt-6 text-lg font-semibold text-slate-100">Interactions with other characters</h2>
        <p class="mt-1 text-xs text-muted">Who this character has interacted with and how.</p>
        <div class="mt-3 table-shell">
            <table class="table-ui">
                <thead><tr class="border-b border-border/70 text-xs text-muted uppercase">
                    <th class="px-3 py-2 text-left">Character</th>
                    <th class="px-3 py-2 text-left">Type</th>
                    <th class="px-3 py-2 text-right">Count</th>
                    <th class="px-3 py-2 text-right">Last seen</th>
                </tr></thead>
                <tbody>
                <?php foreach ($typedInteractions as $ti): ?>
                    <tr class="border-b border-border/40">
                        <td class="px-3 py-2"><a class="text-accent" href="?character_id=<?= (int) ($ti['other_character_id'] ?? 0) ?>"><?= htmlspecialchars((string) ($ti['other_character_name'] ?? 'Unknown'), ENT_QUOTES) ?></a></td>
                        <td class="px-3 py-2"><span class="inline-block rounded-full px-2 py-0.5 text-[10px] uppercase tracking-wider <?php
                            $type = (string) ($ti['interaction_type'] ?? '');
                            echo match($type) {
                                'direct_combat' => 'bg-red-900/60 text-red-300',
                                'assisted_kill' => 'bg-orange-900/60 text-orange-300',
                                'same_fleet' => 'bg-blue-900/60 text-blue-300',
                                default => 'bg-slate-700 text-slate-300',
                            };
                        ?>"><?= htmlspecialchars(ucwords(str_replace('_', ' ', $type)), ENT_QUOTES) ?></span></td>
                        <td class="px-3 py-2 text-right"><?= (int) ($ti['interaction_count'] ?? 0) ?></td>
                        <td class="px-3 py-2 text-right text-xs text-muted"><?= htmlspecialchars((string) ($ti['last_interaction_at'] ?? ''), ENT_QUOTES) ?></td>
                    </tr>
                <?php endforeach; ?>
                </tbody>
            </table>
        </div>
        <?php endif; ?>

        <?php if ($evidencePaths !== []): ?>
        <h2 class="mt-6 text-lg font-semibold text-slate-100">Evidence paths</h2>
        <p class="mt-1 text-xs text-muted">Chains of connections linking this character to known suspicious actors. Stronger scores = more concerning.</p>
        <div class="mt-3 space-y-2">
            <?php foreach ($evidencePaths as $ep): ?>
                <?php $pathScore = (float) ($ep['path_score'] ?? 0); ?>
                <div class="surface-tertiary">
                    <div class="flex items-center justify-between">
                        <span class="text-xs text-muted">Path #<?= (int) ($ep['path_rank'] ?? 0) ?></span>
                        <span class="text-xs font-semibold <?= ci_metric_color($pathScore, 0.4, 0.6) ?>">Strength: <?= number_format($pathScore * 100, 0) ?>%</span>
                    </div>
                    <p class="mt-1 text-sm text-slate-100"><?= htmlspecialchars((string) ($ep['path_description'] ?? ''), ENT_QUOTES) ?></p>
                    <?= ci_progress_bar($pathScore, $pathScore >= 0.6 ? 'bg-red-500' : ($pathScore >= 0.4 ? 'bg-yellow-500' : 'bg-cyan-500')) ?>
                </div>
            <?php endforeach; ?>
        </div>
        <?php endif; ?>

        <h2 class="mt-6 text-lg font-semibold text-slate-100">Analyst feedback</h2>
        <?php if (isset($_GET['feedback']) && $_GET['feedback'] === 'saved'): ?>
            <p class="mt-2 text-sm text-green-400">Feedback saved successfully.</p>
        <?php endif; ?>

        <?php if ($analystFeedback !== []): ?>
        <div class="mt-3 table-shell">
            <table class="table-ui">
                <thead><tr class="border-b border-border/70 text-xs text-muted uppercase">
                    <th class="px-3 py-2 text-left">Verdict</th>
                    <th class="px-3 py-2 text-right">Confidence</th>
                    <th class="px-3 py-2 text-left">Notes</th>
                    <th class="px-3 py-2 text-right">Date</th>
                </tr></thead>
                <tbody>
                <?php foreach ($analystFeedback as $fb): ?>
                    <tr class="border-b border-border/40">
                        <td class="px-3 py-2"><span class="inline-block rounded-full px-2 py-0.5 text-[10px] uppercase tracking-wider <?php
                            $lbl = (string) ($fb['label'] ?? '');
                            echo match($lbl) {
                                'true_positive' => 'bg-red-900/60 text-red-300',
                                'false_positive' => 'bg-green-900/60 text-green-300',
                                'confirmed_clean' => 'bg-green-900/60 text-green-300',
                                default => 'bg-slate-700 text-slate-300',
                            };
                        ?>"><?= htmlspecialchars(ucwords(str_replace('_', ' ', $lbl)), ENT_QUOTES) ?></span></td>
                        <td class="px-3 py-2 text-right"><?= number_format((float) ($fb['confidence'] ?? 0) * 100, 0) ?>%</td>
                        <td class="px-3 py-2 text-sm"><?= htmlspecialchars((string) ($fb['analyst_notes'] ?? ''), ENT_QUOTES) ?></td>
                        <td class="px-3 py-2 text-right text-xs text-muted"><?= htmlspecialchars((string) ($fb['created_at'] ?? ''), ENT_QUOTES) ?></td>
                    </tr>
                <?php endforeach; ?>
                </tbody>
            </table>
        </div>
        <?php endif; ?>

        <form method="POST" class="mt-3 surface-tertiary">
            <p class="text-sm font-semibold text-slate-100 mb-2">Submit feedback</p>
            <div class="grid gap-3 md:grid-cols-4 items-end">
                <div>
                    <label class="text-xs text-muted block mb-1">Label</label>
                    <select name="feedback_label" class="w-full rounded bg-slate-800 border border-border px-2 py-1.5 text-sm text-slate-100">
                        <option value="true_positive">True positive</option>
                        <option value="false_positive">False positive</option>
                        <option value="needs_review">Needs review</option>
                        <option value="confirmed_clean">Confirmed clean</option>
                    </select>
                </div>
                <div>
                    <label class="text-xs text-muted block mb-1">Confidence (0-1)</label>
                    <input type="number" name="feedback_confidence" value="0.5" min="0" max="1" step="0.05" class="w-full rounded bg-slate-800 border border-border px-2 py-1.5 text-sm text-slate-100">
                </div>
                <div>
                    <label class="text-xs text-muted block mb-1">Notes</label>
                    <input type="text" name="feedback_notes" placeholder="Optional notes..." class="w-full rounded bg-slate-800 border border-border px-2 py-1.5 text-sm text-slate-100">
                </div>
                <button type="submit" class="btn-secondary h-fit">Submit</button>
            </div>
        </form>
    <?php endif; ?>
</section>
<?php include __DIR__ . '/../../src/views/partials/footer.php'; ?>
