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

// Generalized co-presence signals and top edges
$copresenceSignals = db_character_copresence_signals($characterId);
$copresenceEdges = db_character_copresence_top_edges_preferred($characterId, '30d', 15);

// Temporal behavior detection signals
$temporalBehaviorSignals = db_character_temporal_behavior_signals($characterId);
$featureHistograms = db_character_feature_histograms($characterId);

// Neo4j intelligence graph signals (EveWho corp history)
$neo4jIntel = (bool) config('neo4j.enabled', false) ? db_neo4j_character_intelligence($characterId) : null;

// Movement footprint data
$movementFootprints = db_character_movement_footprints($characterId);
$systemDistribution = db_character_system_distribution($characterId, '30d', 15);
$neo4jMovement = (bool) config('neo4j.enabled', false) ? db_neo4j_character_movement_graph($characterId) : null;

// Resolve corporation names for org history timeline
$orgCorpNames = [];
if ($orgHistory !== []) {
    $historyRecords = (array) ($orgHistory['history'] ?? []);
    $corpIds = array_filter(array_unique(array_map(static fn(array $r): int => (int) ($r['corporation_id'] ?? 0), $historyRecords)), static fn(int $id): bool => $id > 0);
    if ($corpIds !== []) {
        $corpRows = db_entity_metadata_cache_get_many('corporation', $corpIds);
        foreach ($corpRows as $cr) {
            $orgCorpNames[(int) $cr['entity_id']] = (string) $cr['entity_name'];
        }
    }
}

// Resolve community members for display
$communityMembers = [];
if (is_array($communityInfo) && ((int) ($communityInfo['community_id'] ?? 0)) !== 0) {
    $communityMembers = db_graph_community_top_members((int) $communityInfo['community_id'], 10);
}

// Resolve character names for evidence paths
$pathNodeNames = [];
if ($evidencePaths !== []) {
    $pathCharIds = [];
    foreach ($evidencePaths as $ep) {
        $nodes = json_decode((string) ($ep['path_nodes_json'] ?? '[]'), true);
        if (is_array($nodes)) {
            foreach ($nodes as $node) {
                if (($node['type'] ?? '') === 'Character' && is_numeric($node['id'] ?? null)) {
                    $pathCharIds[] = (int) $node['id'];
                }
            }
        }
    }
    $pathCharIds = array_unique($pathCharIds);
    if ($pathCharIds !== []) {
        $charRows = db_entity_metadata_cache_get_many('character', $pathCharIds);
        foreach ($charRows as $cr) {
            $pathNodeNames[(int) $cr['entity_id']] = (string) $cr['entity_name'];
        }
    }
}

// Handle on-demand intelligence computation
if ($_SERVER['REQUEST_METHOD'] === 'POST' && isset($_POST['compute_intelligence']) && $characterId > 0) {
    $result = compute_character_intelligence_on_demand($characterId);
    flash('success', (string) ($result['message'] ?? 'Computation finished.'));
    header('Location: /battle-intelligence/character.php?character_id=' . $characterId);
    exit;
}

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

require_once __DIR__ . '/../../src/views/partials/character-intel-helpers.php';

include __DIR__ . '/../../src/views/partials/header.php';
?>
<section class="surface-primary">
    <div class="flex flex-wrap items-center gap-4">
        <a href="/battle-intelligence" class="text-sm text-accent">&larr; Back to leaderboard</a>
        <?php if ($characterId > 0): ?>
            <a href="/battle-intelligence/pilot-lookup.php?character_id=<?= $characterId ?>" class="text-sm text-accent">Pilot lookup &rarr;</a>
        <?php endif; ?>
    </div>
    <?php if (!is_array($character)): ?>
        <div class="mt-4">
            <p class="text-sm text-muted">No character intelligence found.</p>
            <?php if ($characterId > 0): ?>
                <form method="POST" class="mt-3 inline">
                    <input type="hidden" name="compute_intelligence" value="1">
                    <button type="submit" class="btn btn-sm btn-accent">Compute now</button>
                    <span class="ml-2 text-xs text-muted">Analyze this character's battle history and generate intelligence scores immediately.</span>
                </form>
            <?php endif; ?>
        </div>
    <?php else: ?>
        <?php $dataSource = (string) ($character['data_source'] ?? 'counterintel'); ?>
        <?php if ($dataSource === 'suspicion_v2'): ?>
            <div class="mt-3 rounded border border-amber-500/30 bg-amber-950/30 px-4 py-2.5 text-sm text-amber-200/90">
                <strong>Limited data</strong> &mdash; showing batch suspicion scores. The full counter-intel pipeline has not processed this character yet.
                <?php if ($characterId > 0): ?>
                    <form method="POST" class="mt-1.5 inline">
                        <input type="hidden" name="compute_intelligence" value="1">
                        <button type="submit" class="btn btn-sm btn-accent">Compute full analysis</button>
                    </form>
                <?php endif; ?>
            </div>
        <?php elseif ($dataSource === 'below_threshold'): ?>
            <div class="mt-3 rounded border border-slate-500/30 bg-slate-800/50 px-4 py-2.5 text-sm text-slate-300">
                <strong>Insufficient data</strong> &mdash; this character has <?= (int) ($character['total_battle_count'] ?? 0) ?> battle(s) (<?= (int) ($character['eligible_battle_count'] ?? 0) ?> eligible), below the minimum of 5 required for scoring. Scores below are placeholder zeros.
                <?php if ($characterId > 0): ?>
                    <form method="POST" class="mt-1.5 inline">
                        <input type="hidden" name="compute_intelligence" value="1">
                        <button type="submit" class="btn btn-sm btn-accent">Compute now</button>
                        <span class="ml-2 text-xs text-muted">On-demand computation may still produce results with fewer battles.</span>
                    </form>
                <?php endif; ?>
            </div>
        <?php endif; ?>
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

        <!-- Org history timeline -->
        <?php
            $historyRecords = array_reverse((array) ($orgHistory['history'] ?? []));
            $orgInfo = (array) ($orgHistory['info'] ?? []);
            $currentCorpId = isset($orgInfo[0]) ? (int) ($orgInfo[0]['corporation_id'] ?? 0) : 0;
        ?>
        <?php if ($historyRecords !== []): ?>
        <h2 class="mt-6 text-lg font-semibold text-slate-100">Corporation history</h2>
        <p class="mt-1 text-xs text-muted">Timeline of corp memberships. Short stays and frequent moves can indicate spy alts.</p>
        <div class="mt-3 space-y-0">
            <?php foreach ($historyRecords as $idx => $rec):
                $corpId = (int) ($rec['corporation_id'] ?? 0);
                $corpName = $orgCorpNames[$corpId] ?? ('Corp #' . $corpId);
                $startDate = (string) ($rec['start_date'] ?? '');
                $endDate = $rec['end_date'] ?? null;
                $duration = ci_corp_duration($startDate, $endDate);
                $isCurrent = $endDate === null;
                $isShort = false;
                if ($startDate !== '' && $endDate !== null) {
                    $daysDiff = (int) round((strtotime(str_replace('/', '-', $endDate)) - strtotime(str_replace('/', '-', $startDate))) / 86400);
                    $isShort = $daysDiff < 30;
                }
            ?>
                <div class="flex items-start gap-3 <?= $idx > 0 ? '' : '' ?>">
                    <div class="flex flex-col items-center">
                        <div class="h-3 w-3 rounded-full mt-1 <?= $isCurrent ? 'bg-green-500' : ($isShort ? 'bg-red-500' : 'bg-slate-500') ?>"></div>
                        <?php if ($idx < count($historyRecords) - 1): ?>
                            <div class="w-0.5 h-8 bg-slate-700"></div>
                        <?php endif; ?>
                    </div>
                    <div class="pb-3">
                        <p class="text-sm <?= $isCurrent ? 'text-green-400 font-semibold' : ($isShort ? 'text-red-400' : 'text-slate-100') ?>">
                            <?= htmlspecialchars($corpName, ENT_QUOTES) ?>
                            <?php if ($duration !== ''): ?>
                                <span class="text-xs text-muted ml-1">(<?= $duration ?>)</span>
                            <?php endif; ?>
                            <?php if ($isCurrent): ?>
                                <span class="text-[10px] rounded-full bg-green-900/60 text-green-300 px-1.5 py-0.5 ml-1">current</span>
                            <?php elseif ($isShort): ?>
                                <span class="text-[10px] rounded-full bg-red-900/60 text-red-300 px-1.5 py-0.5 ml-1">short stay</span>
                            <?php endif; ?>
                        </p>
                        <p class="text-[11px] text-muted"><?= htmlspecialchars($startDate, ENT_QUOTES) ?><?= $endDate !== null ? ' &mdash; ' . htmlspecialchars($endDate, ENT_QUOTES) : ' &mdash; present' ?></p>
                    </div>
                </div>
            <?php endforeach; ?>
        </div>
        <details class="mt-2 surface-tertiary"><summary class="cursor-pointer text-[11px] text-muted">Show raw org data</summary><pre class="mt-2 overflow-auto text-[11px] text-slate-400"><?= htmlspecialchars(json_encode(['source' => (string) ($character['org_history_source'] ?? ''), 'fetched_at' => (string) ($character['org_history_fetched_at'] ?? ''), 'corp_hops_180d' => (int) ($character['corp_hops_180d'] ?? 0), 'short_tenure_hops_180d' => (int) ($character['short_tenure_hops_180d'] ?? 0), 'hostile_adjacent_hops_180d' => (int) ($character['hostile_adjacent_hops_180d'] ?? 0), 'history' => $orgHistory], JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES) ?: '{}', ENT_QUOTES) ?></pre></details>
        <?php else: ?>
        <details class="mt-4 surface-tertiary"><summary class="cursor-pointer text-sm text-slate-100">Org history (no records)</summary><p class="mt-2 text-xs text-muted">No corporation history data available.</p></details>
        <?php endif; ?>

        <!-- Evidence rows -->
        <h2 class="mt-6 text-lg font-semibold text-slate-100">Evidence breakdown</h2>
        <p class="mt-1 text-xs text-muted">Each signal that contributes to the overall suspicion score.</p>
        <div class="mt-3 table-shell"><table class="table-ui"><thead><tr class="border-b border-border/70 text-xs text-muted uppercase"><th class="px-3 py-2 text-left">Signal</th><th class="px-3 py-2 text-right">Weight</th><th class="px-3 py-2 text-left">What it means</th></tr></thead><tbody><?php foreach ($evidence as $evidenceRow): ?><tr class="border-b border-border/40"><td class="px-3 py-2 text-sm"><?= htmlspecialchars(ci_evidence_label((string) ($evidenceRow['evidence_key'] ?? '')), ENT_QUOTES) ?></td><td class="px-3 py-2 text-right font-mono text-sm <?= ci_metric_color((float) ($evidenceRow['evidence_value'] ?? 0), 0.3, 0.7) ?>"><?= htmlspecialchars(number_format((float) ($evidenceRow['evidence_value'] ?? 0), 2), ENT_QUOTES) ?></td><td class="px-3 py-2"><div class="text-sm text-slate-300"><?= htmlspecialchars((string) ($evidenceRow['evidence_text'] ?? ''), ENT_QUOTES) ?></div><?php if (is_array($evidenceRow['evidence_payload'] ?? null)): ?><details class="mt-1"><summary class="text-[11px] text-muted cursor-pointer">Show raw data</summary><pre class="mt-1 overflow-auto text-[11px] text-slate-400"><?= htmlspecialchars(json_encode($evidenceRow['evidence_payload'], JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES) ?: '{}', ENT_QUOTES) ?></pre></details><?php endif; ?></td></tr><?php endforeach; ?>
<?php if (is_array($neo4jIntel)): ?>
<?php
    $neo4jOverlap = (int) ($neo4jIntel['enemy_overlap_count'] ?? 0);
    $neo4jSharedCorps = (int) ($neo4jIntel['shared_corps'] ?? 0);
    $neo4jDefector = (bool) ($neo4jIntel['is_recent_defector'] ?? false);
    $neo4jDefectorCorp = (string) ($neo4jIntel['defector_corp_name'] ?? '');
    $neo4jDefectorDays = (int) ($neo4jIntel['defector_days_ago'] ?? 0);
    $neo4jHostileNeighbors = (int) ($neo4jIntel['hostile_neighbors'] ?? 0);

    // Cross-side corp overlap signal
    $overlapWeight = min(1.0, $neo4jSharedCorps > 0 ? 0.35 : 0.0);
    $overlapText = $neo4jSharedCorps > 0
        ? "Was in {$neo4jSharedCorps} corp(s) alongside {$neo4jOverlap} current enemies"
        : 'No cross-side corp history found';
?>
<?php if ($neo4jSharedCorps > 0): ?>
<tr class="border-b border-border/40 bg-orange-950/20"><td class="px-3 py-2 text-sm"><?= htmlspecialchars(ci_evidence_label('neo4j_cross_side_overlap'), ENT_QUOTES) ?></td><td class="px-3 py-2 text-right font-mono text-sm <?= ci_metric_color($overlapWeight, 0.3, 0.7) ?>"><?= number_format($overlapWeight, 2) ?></td><td class="px-3 py-2"><div class="text-sm text-slate-300"><?= htmlspecialchars($overlapText, ENT_QUOTES) ?></div></td></tr>
<?php endif; ?>
<?php if ($neo4jDefector): ?>
<?php $defectorWeight = 0.60; ?>
<tr class="border-b border-border/40 bg-red-950/20"><td class="px-3 py-2 text-sm"><?= htmlspecialchars(ci_evidence_label('neo4j_recent_defector'), ENT_QUOTES) ?></td><td class="px-3 py-2 text-right font-mono text-sm <?= ci_metric_color($defectorWeight, 0.3, 0.7) ?>"><?= number_format($defectorWeight, 2) ?></td><td class="px-3 py-2"><div class="text-sm text-slate-300">Left <?= htmlspecialchars($neo4jDefectorCorp, ENT_QUOTES) ?> <?= $neo4jDefectorDays ?> days ago</div></td></tr>
<?php endif; ?>
<?php if ($neo4jHostileNeighbors > 0): ?>
<?php $adjacencyWeight = min(1.0, 0.20); ?>
<tr class="border-b border-border/40 bg-orange-950/20"><td class="px-3 py-2 text-sm"><?= htmlspecialchars(ci_evidence_label('neo4j_hostile_adjacency'), ENT_QUOTES) ?></td><td class="px-3 py-2 text-right font-mono text-sm <?= ci_metric_color($adjacencyWeight, 0.3, 0.7) ?>"><?= number_format($adjacencyWeight, 2) ?></td><td class="px-3 py-2"><div class="text-sm text-slate-300">Corp also contains <?= $neo4jHostileNeighbors ?> hostile pilot(s)</div></td></tr>
<?php endif; ?>
<?php endif; ?>
</tbody></table></div>

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

        <?php if ($movementFootprints !== []): ?>
        <h2 class="mt-6 text-lg font-semibold text-slate-100">Movement footprint</h2>
        <p class="mt-1 text-xs text-muted">Geographic operational footprint across time windows. Sudden expansion, contraction, or hostile-overlap shifts can indicate operational changes.</p>

        <!-- Footprint signal cards -->
        <?php
            $fp30 = null;
            foreach ($movementFootprints as $mf) {
                if (($mf['window_label'] ?? '') === '30d') { $fp30 = $mf; break; }
            }
            if ($fp30 === null && $movementFootprints !== []) {
                $fp30 = $movementFootprints[0];
            }
        ?>
        <?php if ($fp30 !== null): ?>
        <?php
            $fpExpansion = (float) ($fp30['footprint_expansion_score'] ?? 0);
            $fpContraction = (float) ($fp30['footprint_contraction_score'] ?? 0);
            $fpNewArea = (float) ($fp30['new_area_entry_score'] ?? 0);
            $fpHostileChange = (float) ($fp30['hostile_overlap_change_score'] ?? 0);
            $fpCohortPctile = (float) ($fp30['cohort_percentile_footprint'] ?? 0);
        ?>
        <div class="mt-3 grid gap-3 md:grid-cols-4">
            <div class="surface-tertiary">
                <p class="text-xs text-muted">Expansion</p>
                <p class="mt-1 text-lg <?= ci_metric_color($fpExpansion, 0.3, 0.6) ?>"><?= number_format($fpExpansion * 100, 0) ?>%</p>
                <?= ci_progress_bar($fpExpansion, $fpExpansion >= 0.6 ? 'bg-red-500' : ($fpExpansion >= 0.3 ? 'bg-yellow-500' : 'bg-cyan-500')) ?>
                <p class="mt-1 text-[11px] text-muted">Operating in more systems than before</p>
            </div>
            <div class="surface-tertiary">
                <p class="text-xs text-muted">Contraction</p>
                <p class="mt-1 text-lg <?= ci_metric_color($fpContraction, 0.3, 0.6) ?>"><?= number_format($fpContraction * 100, 0) ?>%</p>
                <?= ci_progress_bar($fpContraction, $fpContraction >= 0.6 ? 'bg-red-500' : ($fpContraction >= 0.3 ? 'bg-yellow-500' : 'bg-cyan-500')) ?>
                <p class="mt-1 text-[11px] text-muted">Operating in fewer systems than before</p>
            </div>
            <div class="surface-tertiary">
                <p class="text-xs text-muted">New area entry</p>
                <p class="mt-1 text-lg <?= ci_metric_color($fpNewArea, 0.3, 0.6) ?>"><?= number_format($fpNewArea * 100, 0) ?>%</p>
                <?= ci_progress_bar($fpNewArea, $fpNewArea >= 0.6 ? 'bg-red-500' : ($fpNewArea >= 0.3 ? 'bg-yellow-500' : 'bg-cyan-500')) ?>
                <p class="mt-1 text-[11px] text-muted">Regions not previously seen in</p>
            </div>
            <div class="surface-tertiary">
                <p class="text-xs text-muted">Hostile overlap shift</p>
                <p class="mt-1 text-lg <?= ci_metric_color($fpHostileChange, 0.3, 0.6) ?>"><?= number_format($fpHostileChange * 100, 0) ?>%</p>
                <?= ci_progress_bar($fpHostileChange, $fpHostileChange >= 0.6 ? 'bg-red-500' : ($fpHostileChange >= 0.3 ? 'bg-yellow-500' : 'bg-cyan-500')) ?>
                <p class="mt-1 text-[11px] text-muted">Change in overlap with hostile-active systems</p>
            </div>
        </div>
        <?php if ($fpCohortPctile > 0): ?>
        <p class="mt-2 text-xs text-muted">Cohort footprint percentile: <span class="<?= $fpCohortPctile >= 0.9 ? 'text-red-400' : ($fpCohortPctile >= 0.7 ? 'text-yellow-400' : 'text-slate-100') ?>"><?= number_format($fpCohortPctile * 100, 0) ?>%</span> &mdash; higher means larger operational area relative to peers</p>
        <?php endif; ?>
        <?php endif; ?>

        <!-- Per-window footprint table -->
        <div class="mt-3 grid gap-3 md:grid-cols-<?= min(4, count($movementFootprints)) ?>">
            <?php foreach ($movementFootprints as $mf): ?>
                <?php
                    $wLabel = (string) ($mf['window_label'] ?? '');
                    $wTitle = match ($wLabel) {
                        '7d' => 'Last 7 days',
                        '30d' => 'Last 30 days',
                        '90d' => 'Last 90 days',
                        'lifetime' => 'Lifetime',
                        default => $wLabel . ' window',
                    };
                    $uniqueSys = (int) ($mf['unique_systems_count'] ?? 0);
                    $uniqueReg = (int) ($mf['unique_regions_count'] ?? 0);
                    $battlesWin = (int) ($mf['battles_in_window'] ?? 0);
                    $sysEntropy = (float) ($mf['system_entropy'] ?? 0);
                    $sysHhi = (float) ($mf['system_hhi'] ?? 0);
                    $hostileRatio = (float) ($mf['hostile_system_overlap_ratio'] ?? 0);
                    $jsDivSys = $mf['js_divergence_systems'] ?? null;
                    $cosDist = $mf['cosine_distance_systems'] ?? null;
                    $domRatio = (float) ($mf['dominant_system_ratio'] ?? 0);
                ?>
                <div class="surface-tertiary">
                    <p class="text-xs text-muted font-semibold"><?= htmlspecialchars($wTitle, ENT_QUOTES) ?></p>
                    <div class="mt-2 grid grid-cols-2 gap-1 text-sm">
                        <span class="text-muted">Systems</span><span class="text-slate-100 text-right"><?= $uniqueSys ?></span>
                        <span class="text-muted">Regions</span><span class="text-slate-100 text-right"><?= $uniqueReg ?></span>
                        <span class="text-muted">Battles</span><span class="text-slate-100 text-right"><?= $battlesWin ?></span>
                        <span class="text-muted">Entropy</span><span class="text-right text-slate-100"><?= number_format($sysEntropy, 2) ?> bits</span>
                        <span class="text-muted">Concentration</span><span class="text-right <?= $sysHhi >= 0.5 ? 'text-yellow-400' : 'text-slate-100' ?>"><?= number_format($sysHhi * 100, 0) ?>% HHI</span>
                        <span class="text-muted">Dominant sys</span><span class="text-right text-slate-100"><?= number_format($domRatio * 100, 0) ?>%</span>
                        <span class="text-muted">Hostile overlap</span><span class="text-right <?= ci_metric_color($hostileRatio, 0.3, 0.6) ?>"><?= number_format($hostileRatio * 100, 0) ?>%</span>
                        <?php if ($jsDivSys !== null): ?>
                        <span class="text-muted">JS divergence</span><span class="text-right <?= (float) $jsDivSys >= 0.3 ? 'text-yellow-400' : 'text-slate-100' ?>"><?= number_format((float) $jsDivSys, 4) ?></span>
                        <?php endif; ?>
                        <?php if ($cosDist !== null): ?>
                        <span class="text-muted">Cosine dist</span><span class="text-right text-slate-100"><?= number_format((float) $cosDist, 4) ?></span>
                        <?php endif; ?>
                    </div>
                </div>
            <?php endforeach; ?>
        </div>

        <!-- System distribution table (30d) -->
        <?php if ($systemDistribution !== []): ?>
        <h3 class="mt-4 text-sm font-semibold text-slate-100">System distribution (30 days)</h3>
        <div class="mt-2 table-shell">
            <table class="table-ui">
                <thead><tr class="border-b border-border/70 text-xs text-muted uppercase">
                    <th class="px-3 py-2 text-left">System</th>
                    <th class="px-3 py-2 text-left">Region</th>
                    <th class="px-3 py-2 text-right">Security</th>
                    <th class="px-3 py-2 text-right">Battles</th>
                    <th class="px-3 py-2 text-right">Share</th>
                    <th class="px-3 py-2 text-left" style="width:120px"></th>
                </tr></thead>
                <tbody>
                <?php foreach ($systemDistribution as $sd): ?>
                    <?php
                        $sec = (float) ($sd['security'] ?? 0);
                        $secColor = $sec >= 0.5 ? 'text-green-400' : ($sec >= 0.1 ? 'text-yellow-400' : 'text-red-400');
                        $shareRatio = (float) ($sd['ratio'] ?? 0);
                    ?>
                    <tr class="border-b border-border/40">
                        <td class="px-3 py-2 text-sm"><?= htmlspecialchars((string) ($sd['system_name'] ?? ''), ENT_QUOTES) ?></td>
                        <td class="px-3 py-2 text-sm text-muted"><?= htmlspecialchars((string) ($sd['region_name'] ?? ''), ENT_QUOTES) ?></td>
                        <td class="px-3 py-2 text-right text-sm <?= $secColor ?>"><?= number_format($sec, 1) ?></td>
                        <td class="px-3 py-2 text-right text-sm"><?= (int) ($sd['battle_count'] ?? 0) ?></td>
                        <td class="px-3 py-2 text-right text-sm"><?= number_format($shareRatio * 100, 1) ?>%</td>
                        <td class="px-3 py-2"><?= ci_progress_bar($shareRatio, 'bg-cyan-500') ?></td>
                    </tr>
                <?php endforeach; ?>
                </tbody>
            </table>
        </div>
        <?php endif; ?>

        <!-- Neo4j movement graph data -->
        <?php if (is_array($neo4jMovement) && !empty($neo4jMovement['co_located_characters'])): ?>
        <h3 class="mt-4 text-sm font-semibold text-slate-100">Co-located characters (Neo4j)</h3>
        <p class="mt-1 text-xs text-muted">Characters frequently seen in the same battle locations (3+ shared battles).</p>
        <div class="mt-2 flex flex-wrap gap-2">
            <?php foreach ($neo4jMovement['co_located_characters'] as $cl): ?>
                <a href="?character_id=<?= (int) ($cl['character_id'] ?? 0) ?>" class="inline-flex items-center gap-1 rounded-full px-2.5 py-1 text-xs bg-slate-700/60 text-slate-300 hover:bg-slate-600/60">
                    Character #<?= (int) ($cl['character_id'] ?? 0) ?>
                    <span class="text-muted">(<?= (int) ($cl['shared_battles'] ?? 0) ?> battles)</span>
                </a>
            <?php endforeach; ?>
        </div>
        <?php endif; ?>

        <?php endif; ?>

        <?php if ($copresenceSignals !== []): ?>
        <h2 class="mt-6 text-lg font-semibold text-slate-100">Co-presence detection signals</h2>
        <p class="mt-1 text-xs text-muted">Generalized co-presence metrics across battle, side, system, engagement, and area event types. Deltas show change since last computation.</p>
        <div class="mt-3 grid gap-3 md:grid-cols-3">
            <?php foreach ($copresenceSignals as $cs): ?>
                <?php
                    $csWindow = (string) ($cs['window_label'] ?? '');
                    $csTitle = match ($csWindow) {
                        '7d' => 'Last 7 days',
                        '30d' => 'Last 30 days',
                        '90d' => 'Last 90 days',
                        default => $csWindow . ' window',
                    };
                    $pairDelta = (float) ($cs['pair_frequency_delta'] ?? 0);
                    $oocRatio = (float) ($cs['out_of_cluster_ratio'] ?? 0);
                    $oocDelta = (float) ($cs['out_of_cluster_ratio_delta'] ?? 0);
                    $clusterDecay = (float) ($cs['expected_cluster_decay'] ?? 0);
                    $totalWeight = (float) ($cs['total_edge_weight'] ?? 0);
                    $uniqueAssoc = (int) ($cs['unique_associates'] ?? 0);
                    $recurringPairs = (int) ($cs['recurring_pair_count'] ?? 0);
                    $cohortPct = (float) ($cs['cohort_percentile'] ?? 0);
                ?>
                <div class="surface-tertiary">
                    <p class="text-xs text-muted font-semibold"><?= htmlspecialchars($csTitle, ENT_QUOTES) ?></p>
                    <div class="mt-2 grid grid-cols-2 gap-1 text-sm">
                        <span class="text-muted">Edge weight</span><span class="text-slate-100 text-right"><?= number_format($totalWeight, 1) ?></span>
                        <span class="text-muted">Unique associates</span><span class="text-slate-100 text-right"><?= $uniqueAssoc ?></span>
                        <span class="text-muted">Recurring pairs</span><span class="text-right <?= $recurringPairs >= 5 ? 'text-yellow-400' : 'text-slate-100' ?>"><?= $recurringPairs ?></span>
                        <span class="text-muted">Pair freq &Delta;</span><span class="text-right <?= $pairDelta > 2.0 ? 'text-red-400' : ($pairDelta > 0.5 ? 'text-yellow-400' : 'text-slate-100') ?>"><?= ($pairDelta >= 0 ? '+' : '') . number_format($pairDelta, 1) ?></span>
                        <span class="text-muted">Out-of-cluster</span><span class="text-right <?= $oocRatio >= 0.4 ? 'text-red-400' : ($oocRatio >= 0.2 ? 'text-yellow-400' : 'text-green-400') ?>"><?= number_format($oocRatio * 100, 0) ?>%</span>
                        <span class="text-muted">OOC &Delta;</span><span class="text-right <?= $oocDelta > 0.1 ? 'text-red-400' : ($oocDelta > 0 ? 'text-yellow-400' : 'text-slate-100') ?>"><?= ($oocDelta >= 0 ? '+' : '') . number_format($oocDelta * 100, 0) ?>%</span>
                        <span class="text-muted">Cluster decay</span><span class="text-right <?= $clusterDecay >= 0.3 ? 'text-red-400' : ($clusterDecay >= 0.1 ? 'text-yellow-400' : 'text-green-400') ?>"><?= number_format($clusterDecay * 100, 0) ?>%</span>
                        <span class="text-muted">Cohort %ile</span><span class="text-right <?= $cohortPct >= 0.9 ? 'text-red-400' : ($cohortPct >= 0.7 ? 'text-yellow-400' : 'text-slate-100') ?>"><?= number_format($cohortPct * 100, 0) ?>%</span>
                    </div>
                    <?= ci_progress_bar($cohortPct, $cohortPct >= 0.9 ? 'bg-red-500' : ($cohortPct >= 0.7 ? 'bg-yellow-500' : 'bg-cyan-500')) ?>
                </div>
            <?php endforeach; ?>
        </div>
        <?php endif; ?>

        <?php if ($copresenceEdges !== []): ?>
        <h2 class="mt-6 text-lg font-semibold text-slate-100">Top co-presence edges (30d)</h2>
        <p class="mt-1 text-xs text-muted">Strongest pairwise connections by event type. Higher weight = more frequent co-appearance.</p>
        <div class="mt-3 table-shell"><table class="table-ui"><thead><tr class="border-b border-border/70 text-xs text-muted uppercase"><th class="px-3 py-2 text-left">Character</th><th class="px-3 py-2 text-left">Event type</th><th class="px-3 py-2 text-right">Weight</th><th class="px-3 py-2 text-right">Count</th><th class="px-3 py-2 text-right">Last seen</th></tr></thead><tbody>
        <?php foreach ($copresenceEdges as $ce): ?>
            <?php
                $ceType = (string) ($ce['event_type'] ?? '');
                $ceTypeLabel = ucwords(str_replace('_', ' ', $ceType));
                $ceTypeBg = match ($ceType) {
                    'same_battle' => 'bg-red-900/60 text-red-300',
                    'same_side' => 'bg-blue-900/60 text-blue-300',
                    'same_system_time_window' => 'bg-purple-900/60 text-purple-300',
                    'related_engagement' => 'bg-orange-900/60 text-orange-300',
                    'same_operational_area' => 'bg-green-900/60 text-green-300',
                    default => 'bg-slate-700 text-slate-300',
                };
            ?>
            <tr class="border-b border-border/40">
                <td class="px-3 py-2"><a class="text-accent" href="?character_id=<?= (int) ($ce['other_character_id'] ?? 0) ?>"><?= htmlspecialchars((string) ($ce['other_character_name'] ?? 'Unknown'), ENT_QUOTES) ?></a></td>
                <td class="px-3 py-2"><span class="inline-block rounded-full px-2 py-0.5 text-[10px] uppercase tracking-wider <?= $ceTypeBg ?>"><?= htmlspecialchars($ceTypeLabel, ENT_QUOTES) ?></span></td>
                <td class="px-3 py-2 text-right font-mono text-sm"><?= number_format((float) ($ce['edge_weight'] ?? 0), 1) ?></td>
                <td class="px-3 py-2 text-right"><?= (int) ($ce['event_count'] ?? 0) ?></td>
                <td class="px-3 py-2 text-right text-xs text-muted"><?= htmlspecialchars((string) ($ce['last_event_at'] ?? ''), ENT_QUOTES) ?></td>
            </tr>
        <?php endforeach; ?>
        </tbody></table></div>
        <?php endif; ?>

        <?php if ($temporalBehaviorSignals !== [] || $featureHistograms !== []): ?>
        <h2 class="mt-6 text-lg font-semibold text-slate-100">Temporal behavior analysis</h2>
        <p class="mt-1 text-xs text-muted">Hour-of-day and day-of-week cadence profiling. Detects schedule shifts, bursty activity patterns, and reactivation after dormancy.</p>

        <?php if ($temporalBehaviorSignals !== []): ?>
        <div class="mt-3 grid gap-3 md:grid-cols-2">
            <?php foreach ($temporalBehaviorSignals as $tbs):
                $eKey = (string) ($tbs['evidence_key'] ?? '');
                $eVal = (float) ($tbs['evidence_value'] ?? 0);
                $zScore = (float) ($tbs['z_score'] ?? 0);
                $madScore = (float) ($tbs['mad_score'] ?? 0);
                $percentile = (float) ($tbs['cohort_percentile'] ?? 0);
                $confidence = (string) ($tbs['confidence_flag'] ?? 'low');
                $eText = (string) ($tbs['evidence_text'] ?? '');
                $payload = json_decode((string) ($tbs['evidence_payload_json'] ?? '{}'), true);

                $signalLabel = match ($eKey) {
                    'active_hour_shift' => 'Active Hour Shift',
                    'weekday_profile_shift' => 'Weekday Profile Shift',
                    'cadence_burstiness' => 'Cadence Burstiness',
                    'reactivation_after_dormancy' => 'Reactivation After Dormancy',
                    default => ucwords(str_replace('_', ' ', $eKey)),
                };
                $signalIcon = match ($eKey) {
                    'active_hour_shift' => '&#9201;',
                    'weekday_profile_shift' => '&#128197;',
                    'cadence_burstiness' => '&#9889;',
                    'reactivation_after_dormancy' => '&#128064;',
                    default => '&#9679;',
                };

                $severityBg = $eVal >= 0.5 ? 'bg-red-900/40 border-red-800/60' : ($eVal >= 0.25 ? 'bg-yellow-900/40 border-yellow-800/60' : 'bg-slate-800/60 border-border/40');
                $barColor = $eVal >= 0.5 ? 'bg-red-500' : ($eVal >= 0.25 ? 'bg-yellow-500' : 'bg-cyan-500');
                $confidenceBadge = match ($confidence) {
                    'high' => 'bg-green-900/60 text-green-300',
                    'medium' => 'bg-yellow-900/60 text-yellow-300',
                    default => 'bg-slate-700 text-slate-400',
                };
            ?>
                <div class="rounded-lg border p-3 <?= $severityBg ?>">
                    <div class="flex items-center justify-between">
                        <span class="text-sm font-semibold text-slate-100"><span class="mr-1"><?= $signalIcon ?></span><?= htmlspecialchars($signalLabel, ENT_QUOTES) ?></span>
                        <span class="inline-block rounded-full px-2 py-0.5 text-[10px] uppercase tracking-wider <?= $confidenceBadge ?>"><?= htmlspecialchars($confidence, ENT_QUOTES) ?></span>
                    </div>
                    <div class="mt-2 h-1.5 w-full rounded-full bg-slate-700/60 overflow-hidden"><div class="h-full rounded-full <?= $barColor ?>" style="width: <?= min(100, round($eVal * 100)) ?>%"></div></div>
                    <div class="mt-2 grid grid-cols-3 gap-2 text-xs">
                        <div><span class="text-muted">Score</span><br><span class="font-mono text-slate-100"><?= number_format($eVal, 3) ?></span></div>
                        <div><span class="text-muted">z-score</span><br><span class="font-mono <?= abs($zScore) >= 2.0 ? 'text-red-300' : 'text-slate-100' ?>"><?= number_format($zScore, 2) ?></span></div>
                        <div><span class="text-muted">Percentile</span><br><span class="font-mono text-slate-100"><?= number_format($percentile * 100, 0) ?>%</span></div>
                    </div>
                    <p class="mt-2 text-[11px] text-muted"><?= htmlspecialchars($eText, ENT_QUOTES) ?></p>
                    <?php if ($eKey === 'active_hour_shift' && is_array($payload)): ?>
                        <?php $cusum = $payload['cusum'] ?? []; ?>
                        <?php if (($cusum['alarm'] ?? false)): ?>
                            <p class="mt-1 text-[11px] text-red-400">CUSUM alarm triggered (max <?= number_format((float) ($cusum['cusum_max'] ?? 0), 2) ?>, n=<?= (int) ($cusum['sample_count'] ?? 0) ?>)</p>
                        <?php endif; ?>
                    <?php endif; ?>
                    <?php if ($eKey === 'reactivation_after_dormancy' && is_array($payload) && ($payload['reactivated'] ?? false)): ?>
                        <p class="mt-1 text-[11px] text-yellow-400">Dormant for <?= number_format((float) ($payload['dormancy_days'] ?? 0), 0) ?> days, then <?= (int) ($payload['recent_burst_count'] ?? 0) ?> events in the last week</p>
                    <?php endif; ?>
                    <details class="mt-1"><summary class="text-[11px] text-muted cursor-pointer">Raw data</summary><pre class="mt-1 overflow-auto text-[11px] text-slate-400"><?= htmlspecialchars(json_encode($payload, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES) ?: '{}', ENT_QUOTES) ?></pre></details>
                </div>
            <?php endforeach; ?>
        </div>
        <?php endif; ?>

        <?php if ($featureHistograms !== []): ?>
        <div class="mt-4 grid gap-3 md:grid-cols-2">
            <?php foreach ($featureHistograms as $fh):
                $wl = (string) ($fh['window_label'] ?? '');
                $windowTitle = match ($wl) {
                    '7d' => 'Last 7 days',
                    '30d' => 'Last 30 days',
                    '90d' => 'Last 90 days',
                    'lifetime' => 'Lifetime',
                    default => $wl,
                };
                $hourHist = json_decode((string) ($fh['hour_histogram'] ?? '{}'), true) ?: [];
                $dowHist = json_decode((string) ($fh['weekday_histogram'] ?? '{}'), true) ?: [];
                $hourMax = max(1, max(array_values($hourHist) ?: [1]));
                $dowMax = max(1, max(array_values($dowHist) ?: [1]));
                $dowLabels = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
            ?>
                <div class="surface-tertiary">
                    <p class="text-xs text-muted font-semibold mb-2"><?= htmlspecialchars($windowTitle, ENT_QUOTES) ?> activity profile</p>
                    <p class="text-[10px] text-muted mb-1">Hour of day (EVE time)</p>
                    <div class="flex items-end gap-px h-10">
                        <?php for ($h = 0; $h < 24; $h++):
                            $count = (int) ($hourHist[(string) $h] ?? 0);
                            $pct = round(($count / $hourMax) * 100);
                            $barBg = $count > 0 ? ($pct >= 70 ? 'bg-cyan-400' : 'bg-cyan-700') : 'bg-slate-700/40';
                        ?>
                            <div class="flex-1 rounded-t <?= $barBg ?>" style="height: <?= max(2, $pct) ?>%" title="<?= $h ?>:00 — <?= $count ?> events"></div>
                        <?php endfor; ?>
                    </div>
                    <div class="flex gap-px text-[8px] text-muted mt-0.5">
                        <?php for ($h = 0; $h < 24; $h += 6): ?>
                            <span class="flex-1"><?= $h ?></span>
                        <?php endfor; ?>
                    </div>
                    <p class="text-[10px] text-muted mt-2 mb-1">Day of week</p>
                    <div class="flex items-end gap-1 h-8">
                        <?php for ($d = 1; $d <= 7; $d++):
                            $count = (int) ($dowHist[(string) $d] ?? 0);
                            $pct = round(($count / $dowMax) * 100);
                            $barBg = $count > 0 ? ($pct >= 70 ? 'bg-purple-400' : 'bg-purple-700') : 'bg-slate-700/40';
                        ?>
                            <div class="flex-1 text-center">
                                <div class="rounded-t <?= $barBg ?> mx-auto" style="height: <?= max(2, $pct) ?>%; width: 100%"></div>
                                <span class="text-[8px] text-muted"><?= $dowLabels[$d - 1] ?? $d ?></span>
                            </div>
                        <?php endfor; ?>
                    </div>
                </div>
            <?php endforeach; ?>
        </div>
        <?php endif; ?>
        <?php endif; ?>

        <?php if (is_array($communityInfo)): ?>
        <?php
            $communitySize = (int) ($communityInfo['community_size'] ?? 0);
            $pagerank = (float) ($communityInfo['pagerank_score'] ?? 0);
            $betweenness = (float) ($communityInfo['betweenness_centrality'] ?? 0);
            $degree = (int) ($communityInfo['degree_centrality'] ?? 0);
            $isBridge = (bool) ((int) ($communityInfo['is_bridge'] ?? 0));
        ?>
        <h2 class="mt-6 text-lg font-semibold text-slate-100">Community &amp; network</h2>
        <p class="mt-1 text-xs text-muted">This character belongs to a group of <?= $communitySize ?> connected pilots. Their role in the network:</p>
        <div class="mt-3 grid gap-3 md:grid-cols-3">
            <div class="surface-tertiary">
                <p class="text-xs text-muted">Network importance</p>
                <p class="mt-1 text-lg text-slate-100">
                    <?php if ($pagerank >= 0.5): ?>
                        <span class="text-yellow-400">High influence</span>
                    <?php elseif ($pagerank >= 0.1): ?>
                        <span class="text-slate-100">Moderate influence</span>
                    <?php else: ?>
                        <span class="text-slate-400">Low influence</span>
                    <?php endif; ?>
                </p>
                <p class="mt-0.5 text-[11px] text-muted">PageRank <?= number_format($pagerank, 4) ?> &mdash; how central they are to the group</p>
            </div>
            <div class="surface-tertiary">
                <p class="text-xs text-muted">Bridging role</p>
                <p class="mt-1 text-lg text-slate-100">
                    <?php if ($isBridge): ?>
                        <span class="text-yellow-400 font-semibold">Bridge node</span> &mdash; connects separate groups
                    <?php elseif ($betweenness > 0.1): ?>
                        <span class="text-slate-100">Partial bridge</span>
                    <?php else: ?>
                        <span class="text-green-400">Not a bridge</span> &mdash; stays within group
                    <?php endif; ?>
                </p>
                <p class="mt-0.5 text-[11px] text-muted">Betweenness <?= number_format($betweenness, 4) ?> &mdash; how much they sit between clusters</p>
            </div>
            <div class="surface-tertiary">
                <p class="text-xs text-muted">Direct connections</p>
                <p class="mt-1 text-lg text-slate-100"><?= $degree ?> pilot<?= $degree !== 1 ? 's' : '' ?></p>
                <p class="mt-0.5 text-[11px] text-muted">Characters directly linked in the co-occurrence graph</p>
            </div>
        </div>

        <?php if ($communityMembers !== []): ?>
        <div class="mt-3 surface-tertiary">
            <p class="text-xs text-muted mb-2">Community members (top <?= count($communityMembers) ?> of <?= $communitySize ?>)</p>
            <div class="flex flex-wrap gap-2">
                <?php foreach ($communityMembers as $cm):
                    $isCurrentChar = ((int) ($cm['character_id'] ?? 0)) === $characterId;
                    $memberBridge = (bool) ((int) ($cm['is_bridge'] ?? 0));
                ?>
                    <a href="?character_id=<?= (int) ($cm['character_id'] ?? 0) ?>" class="inline-flex items-center gap-1 rounded-full px-2.5 py-1 text-xs <?= $isCurrentChar ? 'bg-cyan-900/60 text-cyan-300 font-semibold' : 'bg-slate-700/60 text-slate-300 hover:bg-slate-600/60' ?>">
                        <?= htmlspecialchars((string) ($cm['character_name'] ?? 'Unknown'), ENT_QUOTES) ?>
                        <?php if ($memberBridge): ?>
                            <span class="text-yellow-400" title="Bridge node">&#9670;</span>
                        <?php endif; ?>
                    </a>
                <?php endforeach; ?>
            </div>
        </div>
        <?php endif; ?>

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
                <?php
                    $pathScore = (float) ($ep['path_score'] ?? 0);
                    $nodesJson = (string) ($ep['path_nodes_json'] ?? '[]');
                    $edgesJson = (string) ($ep['path_edges_json'] ?? '[]');
                    $hasStructuredData = $nodesJson !== '[]' && $nodesJson !== '';
                ?>
                <div class="surface-tertiary">
                    <div class="flex items-center justify-between">
                        <span class="text-xs text-muted">Path #<?= (int) ($ep['path_rank'] ?? 0) ?></span>
                        <span class="text-xs font-semibold <?= ci_metric_color($pathScore, 0.4, 0.6) ?>">Strength: <?= number_format($pathScore * 100, 0) ?>%</span>
                    </div>
                    <div class="mt-1 text-sm leading-relaxed">
                        <?php if ($hasStructuredData): ?>
                            <?= ci_render_evidence_path($nodesJson, $edgesJson, $pathNodeNames) ?>
                        <?php else: ?>
                            <span class="text-slate-100"><?= htmlspecialchars((string) ($ep['path_description'] ?? ''), ENT_QUOTES) ?></span>
                        <?php endif; ?>
                    </div>
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
