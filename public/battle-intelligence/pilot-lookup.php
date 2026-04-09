<?php

declare(strict_types=1);
require_once __DIR__ . '/../../src/bootstrap.php';
require_once __DIR__ . '/../../src/views/partials/character-intel-helpers.php';

$title = 'Pilot Lookup';
$searchQuery = trim((string) ($_GET['q'] ?? ''));
$characterId = max(0, (int) ($_GET['character_id'] ?? 0));

$searchResults = [];
$profile = null;

// Extended intelligence data (only loaded when viewing a specific character)
$temporalMetrics   = [];
$copresenceSignals = [];
$copresenceEdges   = [];

// Character intelligence data
$ciData = null;
$ciCharacter = null;
$ciBattles = [];
$ciEvidence = [];
$ciOrgHistory = [];
$ciTypedInteractions = [];
$ciCommunityInfo = null;
$ciCommunityMembers = [];
$ciEvidencePaths = [];
$ciAnalystFeedback = [];
$ciTemporalBehaviorSignals = [];
$ciFeatureHistograms = [];
$ciNeo4jIntel = null;
$ciMovementFootprints = [];
$ciSystemDistribution = [];
$ciNeo4jMovement = null;
$ciPipelineStatus = null;
$ciOrgCorpNames = [];
$ciPathNodeNames = [];

// Blended intelligence (two-lane score)
$blended = null;
$behavioralScore = null;
$behavioralSignals = [];
$smallCopresence = [];

if ($characterId > 0) {
    $profile = db_pilot_profile($characterId);
    if ($profile !== null) {
        $title = htmlspecialchars((string) ($profile['character']['character_name'] ?? 'Unknown'), ENT_QUOTES) . ' - Pilot Lookup';
        $temporalMetrics   = db_character_temporal_metrics($characterId);
        $copresenceSignals = db_character_copresence_signals($characterId);
        $copresenceEdges   = db_character_copresence_top_edges_preferred($characterId, '30d', 15);

        // Blended intelligence & behavioral signals for universal overview
        $blended = db_character_blended_intelligence($characterId);
        $behavioralScore = db_character_behavioral_score($characterId);
        $behavioralSignals = db_character_behavioral_signals($characterId);
        $smallCopresence = db_character_small_engagement_copresence($characterId, 15);

        // Load full character intelligence data (read-only — pipeline processes in background)
        $ciData = battle_intelligence_character_data($characterId);
        $ciCharacter = $ciData['character'] ?? null;
        $ciBattles = (array) ($ciData['battles'] ?? []);
        $ciEvidence = (array) ($ciData['evidence'] ?? []);
        $ciOrgHistory = (array) ($ciData['org_history'] ?? []);
        $ciPipelineStatus = db_character_pipeline_status($characterId);

        // Ensure this character is in the processing queue.
        // Priority 5.0 = elevated for viewed characters but capped below max (ingestion=0, view=5).
        // UPSERT uses GREATEST so repeated page loads are idempotent, not escalating.
        db_character_pipeline_enqueue([$characterId], 'pilot_lookup', 5.0);

        $ciTypedInteractions = db_character_typed_interactions($characterId, 30);
        $ciCommunityInfo = db_graph_community_assignments($characterId);
        $ciEvidencePaths = db_character_evidence_paths($characterId);
        $ciAnalystFeedback = db_analyst_feedback_for_character($characterId);
        $ciTemporalBehaviorSignals = db_character_temporal_behavior_signals($characterId);
        $ciFeatureHistograms = db_character_feature_histograms($characterId);
        $ciNeo4jIntel = (bool) config('neo4j.enabled', false) ? db_neo4j_character_intelligence($characterId) : null;
        $ciMovementFootprints = db_character_movement_footprints($characterId);
        $ciSystemDistribution = db_character_system_distribution($characterId, '30d', 15);
        $ciNeo4jMovement = (bool) config('neo4j.enabled', false) ? db_neo4j_character_movement_graph($characterId) : null;

        // Resolve corporation names for org history
        if ($ciOrgHistory !== []) {
            $historyRecords = (array) ($ciOrgHistory['history'] ?? []);
            $corpIds = array_filter(array_unique(array_map(static fn(array $r): int => (int) ($r['corporation_id'] ?? 0), $historyRecords)), static fn(int $id): bool => $id > 0);
            if ($corpIds !== []) {
                $corpRows = db_entity_metadata_cache_get_many('corporation', $corpIds);
                foreach ($corpRows as $cr) {
                    $ciOrgCorpNames[(int) $cr['entity_id']] = (string) $cr['entity_name'];
                }
            }
        }

        // Resolve community members
        if (is_array($ciCommunityInfo) && ((int) ($ciCommunityInfo['community_id'] ?? 0)) !== 0) {
            $ciCommunityMembers = db_graph_community_top_members((int) $ciCommunityInfo['community_id'], 10);
        }

        // Resolve character names for evidence paths
        if ($ciEvidencePaths !== []) {
            $pathCharIds = [];
            foreach ($ciEvidencePaths as $ep) {
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
                    $ciPathNodeNames[(int) $cr['entity_id']] = (string) $cr['entity_name'];
                }
            }
        }
    }
} elseif ($searchQuery !== '' && mb_strlen($searchQuery) >= 2) {
    $searchResults = db_pilot_search($searchQuery);
}

// Handle feedback submission
if ($_SERVER['REQUEST_METHOD'] === 'POST' && isset($_POST['feedback_label']) && $characterId > 0) {
    $label = (string) $_POST['feedback_label'];
    $confidence = (float) ($_POST['feedback_confidence'] ?? 0.5);
    $notes = trim((string) ($_POST['feedback_notes'] ?? ''));
    $contextJson = json_encode([
        'review_priority_score' => (float) ($ciCharacter['review_priority_score'] ?? 0),
        'confidence_score' => (float) ($ciCharacter['confidence_score'] ?? 0),
        'percentile_rank' => (float) ($ciCharacter['percentile_rank'] ?? 0),
    ]);
    db_analyst_feedback_save($characterId, $label, $confidence, $notes !== '' ? $notes : null, $contextJson);
    header('Location: /battle-intelligence/pilot-lookup.php?character_id=' . $characterId . '&feedback=saved');
    exit;
}

include __DIR__ . '/../../src/views/partials/header.php';
?>

<section class="surface-primary">
    <p class="text-xs uppercase tracking-[0.16em] text-muted">Battle Intelligence</p>
    <h1 class="mt-1 text-2xl font-semibold text-slate-50">Pilot Lookup</h1>

    <!-- Search form -->
    <form method="GET" class="mt-4 flex gap-2 items-end">
        <div class="flex-1">
            <label class="text-xs text-muted block mb-1">Search by pilot name</label>
            <input type="text" name="q" value="<?= htmlspecialchars($searchQuery, ENT_QUOTES) ?>"
                   placeholder="Enter pilot name (min 2 characters)..."
                   class="w-full rounded bg-slate-800 border border-border px-3 py-2 text-sm text-slate-100 focus:border-accent focus:outline-none"
                   autofocus>
        </div>
        <button type="submit" class="btn-secondary px-4 py-2">Search</button>
    </form>

    <?php if ($searchQuery !== '' && $characterId === 0): ?>
        <!-- Search results -->
        <?php if ($searchResults === []): ?>
            <p class="mt-4 text-sm text-muted">No pilots found matching "<?= htmlspecialchars($searchQuery, ENT_QUOTES) ?>".</p>
        <?php else: ?>
            <div class="mt-4 table-shell">
                <table class="table-ui">
                    <thead>
                        <tr class="border-b border-border/70 text-xs text-muted uppercase">
                            <th class="px-3 py-2 text-left">Pilot</th>
                            <th class="px-3 py-2 text-left">Alliance</th>
                            <th class="px-3 py-2 text-left">Corporation</th>
                            <th class="px-3 py-2 text-left">Role</th>
                            <th class="px-3 py-2 text-right">Battles</th>
                            <th class="px-3 py-2 text-right"></th>
                        </tr>
                    </thead>
                    <tbody>
                    <?php foreach ($searchResults as $r): ?>
                        <tr class="border-b border-border/40 hover:bg-slate-800/50">
                            <td class="px-3 py-2 font-medium text-slate-100"><?= htmlspecialchars((string) ($r['character_name'] ?? '?'), ENT_QUOTES) ?></td>
                            <td class="px-3 py-2 text-sm"><?= htmlspecialchars((string) ($r['alliance_name'] ?? '-'), ENT_QUOTES) ?></td>
                            <td class="px-3 py-2 text-sm"><?= htmlspecialchars((string) ($r['corporation_name'] ?? '-'), ENT_QUOTES) ?></td>
                            <td class="px-3 py-2">
                                <?php $ff = (string) ($r['fleet_function'] ?? ''); if ($ff !== ''): ?>
                                    <span class="inline-block rounded-full px-2 py-0.5 text-[10px] uppercase tracking-wider <?= fleet_function_color_class($ff) ?>"><?= htmlspecialchars(fleet_function_label($ff), ENT_QUOTES) ?></span>
                                <?php else: ?>
                                    <span class="text-muted">-</span>
                                <?php endif; ?>
                            </td>
                            <td class="px-3 py-2 text-right"><?= (int) ($r['battle_count'] ?? 0) ?></td>
                            <td class="px-3 py-2 text-right">
                                <a href="?character_id=<?= (int) ($r['character_id'] ?? 0) ?>" class="text-accent text-sm">View</a>
                            </td>
                        </tr>
                    <?php endforeach; ?>
                    </tbody>
                </table>
            </div>
        <?php endif; ?>
    <?php endif; ?>

    <?php if ($profile !== null):
        $char = $profile['character'];
        $stats = $profile['stats'];
        $behavStats = $profile['behavioral_stats'];
        $kmLifetime = $profile['killmail_lifetime'];
        $ships = $profile['ships'];
        $suspicion = $profile['suspicion'];
        $theaterHistory = $profile['theater_history'];
        $associates = $profile['associates'];

        $charName     = (string) ($char['character_name'] ?? 'Unknown');
        $charId       = (int) ($char['character_id'] ?? 0);
        $corpName     = (string) ($profile['corporation_name'] ?? '');
        $allianceName = (string) ($profile['alliance_name'] ?? '');
        $fleetFunc    = (string) ($profile['fleet_function'] ?? '');

        // Theater-based stats
        $theaterKills   = (int) ($stats['total_kills'] ?? 0);
        $theaterDeaths  = (int) ($stats['total_deaths'] ?? 0);
        $theaterBattles = (int) ($stats['total_battles'] ?? 0);
        $totalTheaters  = (int) ($stats['theater_count'] ?? 0);
        $theaterDamageDone  = (float) ($stats['total_damage_done'] ?? 0);
        $theaterDamageTaken = (float) ($stats['total_damage_taken'] ?? 0);

        // Killmail-based stats (from behavioral scoring + temporal metrics)
        $kmKills   = (int) ($behavStats['total_kill_count'] ?? 0);
        $kmSolo    = (int) ($behavStats['solo_kill_count'] ?? 0);
        $kmGang    = (int) ($behavStats['gang_kill_count'] ?? 0);
        $kmLarge   = (int) ($behavStats['large_battle_count'] ?? 0);
        $kmDeaths  = (int) ($kmLifetime['km_losses'] ?? 0);
        $kmDamage  = (float) ($kmLifetime['km_damage'] ?? 0);
        $kmBattles = (int) ($kmLifetime['km_battles'] ?? 0);

        // Best available: use theater stats when they have data, else fall back to killmail pipeline
        $hasTheaterData = $theaterBattles > 0 || $theaterKills > 0;
        $hasKillmailData = $kmKills > 0 || $kmDeaths > 0 || $kmBattles > 0;
        $totalKills   = $hasTheaterData ? $theaterKills   : $kmKills;
        $totalDeaths  = $hasTheaterData ? $theaterDeaths  : $kmDeaths;
        $totalBattles = $hasTheaterData ? $theaterBattles : $kmBattles;
        $totalDamageDone  = $hasTheaterData ? $theaterDamageDone  : $kmDamage;
        $totalDamageTaken = $hasTheaterData ? $theaterDamageTaken : 0;
        $kd = $totalDeaths > 0 ? round($totalKills / $totalDeaths, 2) : (float) $totalKills;
        $statsSource = $hasTheaterData ? 'theater' : ($hasKillmailData ? 'killmail' : 'none');

        $suspScore    = (float) ($suspicion['suspicion_score'] ?? 0);
        $combinedRisk = (float) ($suspicion['combined_risk_score'] ?? 0);
        $overlapScore = (float) ($suspicion['historical_overlap_score'] ?? 0);
        $flags        = is_array($suspicion) ? (array) json_decode((string) ($suspicion['suspicion_flags'] ?? '[]'), true) : [];

        $riskLevel = match(true) {
            $combinedRisk >= 0.6 => ['label' => 'HIGH RISK',     'tone' => 'text-red-300',    'bg' => 'bg-red-900/30 border-red-700/40'],
            $combinedRisk >= 0.3 => ['label' => 'MODERATE RISK', 'tone' => 'text-yellow-300', 'bg' => 'bg-yellow-900/25 border-yellow-700/40'],
            $combinedRisk > 0    => ['label' => 'LOW RISK',       'tone' => 'text-emerald-300','bg' => 'bg-emerald-900/25 border-emerald-700/40'],
            default              => ['label' => 'NOT ASSESSED',   'tone' => 'text-slate-400',  'bg' => 'bg-slate-800/40 border-slate-700/40'],
        };

        // Blended intelligence values for universal overview
        $bldScore = $blended !== null ? (float) ($blended['blended_score'] ?? 0) : null;
        $bldPct   = $blended !== null ? (float) ($blended['blended_percentile'] ?? 0) : null;
        $bldConf  = $blended !== null ? (float) ($blended['blended_confidence'] ?? 0) : null;
        $bldSource = (string) ($blended['evidence_source'] ?? '');
        $bldRiskLevel = $bldScore !== null ? ci_risk_level($bldScore) : null;

        $temporalByWindow = [];
        foreach ($temporalMetrics as $tm) {
            $temporalByWindow[(string) ($tm['window_label'] ?? '')] = $tm;
        }
        $copresenceByWindow = [];
        foreach ($copresenceSignals as $cs) {
            $copresenceByWindow[(string) ($cs['window_label'] ?? '')] = $cs;
        }
    ?>
        <!-- Pilot profile — Universal overview -->
        <div class="mt-6">
            <a href="?q=<?= urlencode($searchQuery) ?>" class="text-sm text-accent">&larr; Back to search</a>

            <!-- ── Identity header ───────────────────────────────── -->
            <div class="mt-4 flex items-start gap-5">
                <img src="https://images.evetech.net/characters/<?= $charId ?>/portrait?size=128"
                     alt="" class="w-24 h-24 rounded-xl border border-white/10 shadow-lg" loading="lazy">
                <div class="flex-1 min-w-0">
                    <div class="flex flex-wrap items-center gap-3">
                        <h2 class="text-2xl font-semibold text-slate-50"><?= htmlspecialchars($charName, ENT_QUOTES) ?></h2>
                        <?php if ($fleetFunc !== ''): ?>
                        <span class="inline-block rounded-full px-2.5 py-0.5 text-[10px] uppercase tracking-wider <?= fleet_function_color_class($fleetFunc) ?>">
                            <?= htmlspecialchars(fleet_function_label($fleetFunc), ENT_QUOTES) ?>
                        </span>
                        <?php endif; ?>
                        <?php if ($bldRiskLevel !== null): ?>
                        <span class="inline-block rounded-full px-2.5 py-0.5 text-[10px] font-semibold uppercase tracking-wider <?= $bldRiskLevel['bg'] ?> <?= $bldRiskLevel['text'] ?>">
                            <?= $bldRiskLevel['label'] ?> risk
                        </span>
                        <?php endif; ?>
                    </div>
                    <p class="mt-1 text-sm text-slate-300">
                        <?= htmlspecialchars($corpName ?: '—', ENT_QUOTES) ?>
                        <?php if ($allianceName !== ''): ?>
                            <span class="text-muted mx-1">&middot;</span><?= htmlspecialchars($allianceName, ENT_QUOTES) ?>
                        <?php endif; ?>
                    </p>
                    <div class="mt-2 flex flex-wrap items-center gap-3 text-xs">
                        <a href="/battle-intelligence/character.php?character_id=<?= $charId ?>"
                           class="text-accent hover:underline">Full CI Dossier &rarr;</a>
                        <?php if ($statsSource === 'killmail'): ?>
                            <span class="text-slate-500">Stats from killmail pipeline</span>
                        <?php elseif ($statsSource === 'theater'): ?>
                            <span class="text-slate-500">Stats from theater participation</span>
                        <?php endif; ?>
                    </div>
                </div>
            </div>

            <!-- ══════════════════════════════════════════════════════ -->
            <!-- ── UNIVERSAL OVERVIEW ────────────────────────────── -->
            <!-- ══════════════════════════════════════════════════════ -->

            <!-- ── Blended intelligence banner ──────────────────── -->
            <?php if ($blended !== null && $bldScore !== null): ?>
            <?php
                $bldScorePct = $bldScore * 100;
                $bldConfPct  = ($bldConf ?? 0) * 100;
                $bldPctRank  = (1 - ($bldPct ?? 0)) * 100;
            ?>
            <div class="mt-5 rounded-xl border px-5 py-4 <?= $bldRiskLevel['bg'] ?> border-white/10">
                <div class="flex flex-wrap items-center gap-6">
                    <div>
                        <p class="text-[10px] uppercase tracking-widest text-muted">Blended risk score</p>
                        <p class="text-2xl font-bold <?= $bldRiskLevel['text'] ?>"><?= number_format($bldScorePct, 1) ?>%</p>
                        <?= ci_progress_bar($bldScore, $bldRiskLevel['bar']) ?>
                    </div>
                    <div class="h-12 w-px bg-white/10 hidden sm:block"></div>
                    <div>
                        <p class="text-[10px] uppercase tracking-widest text-muted">Percentile</p>
                        <p class="text-lg font-semibold text-slate-100">Top <?= number_format($bldPctRank, 1) ?>%</p>
                    </div>
                    <div class="h-12 w-px bg-white/10 hidden sm:block"></div>
                    <div>
                        <p class="text-[10px] uppercase tracking-widest text-muted">Confidence</p>
                        <p class="text-lg font-semibold text-slate-100"><?= number_format($bldConfPct, 0) ?>%</p>
                    </div>
                    <div class="h-12 w-px bg-white/10 hidden sm:block"></div>
                    <div>
                        <p class="text-[10px] uppercase tracking-widest text-muted">Evidence source</p>
                        <p class="text-sm text-slate-300"><?= htmlspecialchars(str_replace('_', ' ', ucfirst($bldSource)), ENT_QUOTES) ?></p>
                        <?php if ($blended['lane1_score'] !== null): ?>
                            <p class="text-[10px] text-muted">L1 <?= number_format(((float) $blended['lane1_weight']) * 100, 0) ?>% &middot; L2 <?= number_format(((float) $blended['lane2_weight']) * 100, 0) ?>%</p>
                        <?php endif; ?>
                    </div>
                </div>
            </div>
            <?php elseif (is_array($suspicion) && $combinedRisk > 0): ?>
            <!-- Fallback: old-style risk banner when blended intelligence unavailable -->
            <div class="mt-5 rounded-xl border px-4 py-3 flex flex-wrap items-center gap-4 <?= $riskLevel['bg'] ?>">
                <div>
                    <p class="text-[10px] uppercase tracking-widest text-muted">Overall Risk</p>
                    <p class="text-lg font-bold <?= $riskLevel['tone'] ?>"><?= $riskLevel['label'] ?></p>
                </div>
                <div class="h-10 w-px bg-white/10 hidden sm:block"></div>
                <div>
                    <p class="text-[10px] uppercase tracking-widest text-muted">Combined Risk</p>
                    <p class="text-lg font-semibold <?= $combinedRisk >= 0.6 ? 'text-red-300' : ($combinedRisk >= 0.3 ? 'text-yellow-300' : 'text-slate-200') ?>">
                        <?= number_format($combinedRisk, 4) ?>
                    </p>
                </div>
                <div class="h-10 w-px bg-white/10 hidden sm:block"></div>
                <div>
                    <p class="text-[10px] uppercase tracking-widest text-muted">Suspicion Score</p>
                    <p class="text-lg font-semibold <?= $suspScore >= 0.5 ? 'text-red-300' : ($suspScore >= 0.2 ? 'text-yellow-300' : 'text-slate-200') ?>">
                        <?= number_format($suspScore, 4) ?>
                    </p>
                </div>
            </div>
            <?php endif; ?>

            <!-- ── Core metrics ──────────────────────────────────── -->
            <div class="mt-5 grid gap-3 sm:grid-cols-4 xl:grid-cols-8">
                <?php if ($totalTheaters > 0): ?>
                <div class="surface-tertiary">
                    <p class="text-xs text-muted">Theaters</p>
                    <p class="text-xl font-semibold text-slate-50"><?= number_format($totalTheaters) ?></p>
                </div>
                <?php endif; ?>
                <div class="surface-tertiary">
                    <p class="text-xs text-muted">Battles</p>
                    <p class="text-xl font-semibold text-slate-50"><?= number_format($totalBattles) ?></p>
                </div>
                <div class="surface-tertiary">
                    <p class="text-xs text-muted">Kills</p>
                    <p class="text-xl font-semibold text-slate-50"><?= number_format($totalKills) ?></p>
                </div>
                <div class="surface-tertiary">
                    <p class="text-xs text-muted">Deaths</p>
                    <p class="text-xl font-semibold text-slate-50"><?= number_format($totalDeaths) ?></p>
                </div>
                <div class="surface-tertiary">
                    <p class="text-xs text-muted">K/D Ratio</p>
                    <p class="text-xl font-semibold <?= $kd >= 2 ? 'text-emerald-400' : ($kd < 1 ? 'text-red-400' : 'text-slate-50') ?>">
                        <?= number_format($kd, 2) ?>
                    </p>
                </div>
                <?php if ($totalDamageDone > 0): ?>
                <div class="surface-tertiary">
                    <p class="text-xs text-muted">Damage Done</p>
                    <p class="text-xl font-semibold text-slate-50"><?= number_format($totalDamageDone, 0) ?></p>
                </div>
                <?php endif; ?>
                <?php if ($totalDamageTaken > 0): ?>
                <div class="surface-tertiary">
                    <p class="text-xs text-muted">Damage Taken</p>
                    <p class="text-xl font-semibold text-slate-50"><?= number_format($totalDamageTaken, 0) ?></p>
                </div>
                <?php endif; ?>
                <?php if ($kmKills > 0 && $hasKillmailData): ?>
                <div class="surface-tertiary">
                    <p class="text-xs text-muted">Kill breakdown</p>
                    <p class="text-sm font-semibold text-slate-50">
                        <span class="text-slate-300"><?= $kmSolo ?></span> <span class="text-[10px] text-muted">solo</span>
                        <span class="text-slate-300 ml-1"><?= $kmGang ?></span> <span class="text-[10px] text-muted">gang</span>
                        <span class="text-slate-300 ml-1"><?= $kmLarge ?></span> <span class="text-[10px] text-muted">fleet</span>
                    </p>
                </div>
                <?php endif; ?>
            </div>

            <!-- ── Behavioral quick-glance (when behavioral data exists) ── -->
            <?php if (is_array($behavStats)): ?>
            <?php
                $bsRisk = (float) ($behavStats['behavioral_risk_score'] ?? 0);
                $bsFleetAbsence = (float) ($behavStats['fleet_absence_ratio'] ?? 0);
                $bsCompanion = (float) ($behavStats['companion_consistency_score'] ?? 0);
                $bsCrossSide = (float) ($behavStats['cross_side_small_rate'] ?? 0);
                $bsAsymmetry = (float) ($behavStats['asymmetry_preference'] ?? 0);
                $bsGeoConc = (float) ($behavStats['geographic_concentration_score'] ?? 0);
                $bsTempReg = (float) ($behavStats['temporal_regularity_score'] ?? 0);
                $bsContinuation = (float) ($behavStats['post_engagement_continuation_rate'] ?? 0);
                $bsConfTier = (string) ($behavStats['confidence_tier'] ?? 'low');
            ?>
            <div class="mt-5 rounded-xl border border-border/40 bg-slate-800/30 px-5 py-4">
                <div class="flex items-center gap-3 mb-3">
                    <h3 class="text-base font-semibold text-slate-100">Behavioral profile</h3>
                    <span class="text-xs text-muted"><?= $kmKills ?> kills analyzed &middot; <?= $bsConfTier ?> confidence</span>
                </div>
                <div class="grid gap-3 sm:grid-cols-3 lg:grid-cols-4">
                    <div class="surface-tertiary">
                        <p class="text-xs text-muted">Behavioral risk</p>
                        <p class="text-lg font-semibold <?= ci_metric_color($bsRisk, 0.3, 0.6) ?>"><?= number_format($bsRisk * 100, 1) ?>%</p>
                        <?= ci_progress_bar($bsRisk, $bsRisk >= 0.6 ? 'bg-red-500' : ($bsRisk >= 0.3 ? 'bg-yellow-500' : 'bg-cyan-500')) ?>
                    </div>
                    <div class="surface-tertiary">
                        <p class="text-xs text-muted">Fleet absence</p>
                        <p class="text-lg font-semibold <?= $bsFleetAbsence >= 0.8 ? 'text-yellow-400' : 'text-slate-100' ?>">
                            <?= number_format($bsFleetAbsence * 100, 0) ?>% <span class="text-xs text-muted">small-only</span>
                        </p>
                        <p class="mt-0.5 text-[10px] text-muted"><?= $kmSolo ?> solo &middot; <?= $kmGang ?> gang &middot; <?= $kmLarge ?> fleet</p>
                    </div>
                    <div class="surface-tertiary">
                        <p class="text-xs text-muted">Companion consistency</p>
                        <p class="text-lg font-semibold text-slate-100"><?= number_format($bsCompanion * 100, 0) ?>%</p>
                        <p class="mt-0.5 text-[10px] text-muted">Recurring partners in small kills</p>
                    </div>
                    <div class="surface-tertiary">
                        <p class="text-xs text-muted">Continuation rate</p>
                        <p class="text-lg font-semibold <?= $bsContinuation <= 0.2 ? 'text-yellow-400' : 'text-slate-100' ?>">
                            <?= number_format($bsContinuation * 100, 0) ?>%
                        </p>
                        <p class="mt-0.5 text-[10px] text-muted"><?= $bsContinuation <= 0.2 ? 'Low — appears then disappears' : ($bsContinuation >= 0.6 ? 'High — persistent activity' : 'Moderate') ?></p>
                    </div>
                    <?php if ($bsCrossSide > 0): ?>
                    <div class="surface-tertiary">
                        <p class="text-xs text-muted">Cross-side (small)</p>
                        <p class="text-lg font-semibold <?= $bsCrossSide >= 0.5 ? 'text-red-400' : ($bsCrossSide >= 0.2 ? 'text-yellow-400' : 'text-slate-100') ?>">
                            <?= number_format($bsCrossSide * 100, 1) ?>%
                        </p>
                        <p class="mt-0.5 text-[10px] text-muted">Kills against own alliance in small fights</p>
                    </div>
                    <?php endif; ?>
                    <div class="surface-tertiary">
                        <p class="text-xs text-muted">Asymmetry preference</p>
                        <p class="text-lg font-semibold text-slate-100"><?= number_format($bsAsymmetry * 100, 0) ?>%</p>
                        <p class="mt-0.5 text-[10px] text-muted">Preference for 5:1+ ganks</p>
                    </div>
                    <div class="surface-tertiary">
                        <p class="text-xs text-muted">Geographic focus</p>
                        <p class="text-lg font-semibold text-slate-100">Gini <?= number_format($bsGeoConc, 2) ?></p>
                        <p class="mt-0.5 text-[10px] text-muted"><?= $bsGeoConc >= 0.7 ? 'Concentrated' : ($bsGeoConc >= 0.3 ? 'Moderate spread' : 'Distributed') ?></p>
                    </div>
                    <div class="surface-tertiary">
                        <p class="text-xs text-muted">Temporal burstiness</p>
                        <p class="text-lg font-semibold text-slate-100"><?= number_format($bsTempReg * 100, 0) ?>%</p>
                        <p class="mt-0.5 text-[10px] text-muted"><?= $bsTempReg >= 0.7 ? 'Highly bursty' : ($bsTempReg >= 0.4 ? 'Moderately bursty' : 'Regular cadence') ?></p>
                    </div>
                </div>
            </div>
            <?php endif; ?>

            <!-- ── Small-engagement companions ───────────────────── -->
            <?php if ($smallCopresence !== []): ?>
            <h3 class="mt-6 text-base font-semibold text-slate-100">Small-engagement companions</h3>
            <p class="text-xs text-muted mt-0.5">Characters who repeatedly appear alongside this pilot in small kills.</p>
            <div class="mt-3 table-shell">
                <table class="table-ui">
                    <thead>
                        <tr class="border-b border-border/70 text-xs text-muted uppercase">
                            <th class="px-3 py-2 text-left">Companion</th>
                            <th class="px-3 py-2 text-right">Co-kills</th>
                            <th class="px-3 py-2 text-right">Victims</th>
                            <th class="px-3 py-2 text-right">Systems</th>
                            <th class="px-3 py-2 text-right">Weight</th>
                            <th class="px-3 py-2 text-right">Last seen</th>
                        </tr>
                    </thead>
                    <tbody>
                    <?php foreach ($smallCopresence as $sc):
                        $scName = (string) ($sc['companion_name'] ?? '?');
                        if (ci_is_placeholder_name($scName)) continue;
                        $scIdA = (int) ($sc['character_id_a'] ?? 0);
                        $scIdB = (int) ($sc['character_id_b'] ?? 0);
                        $scId = $scIdA === $charId ? $scIdB : $scIdA;
                    ?>
                        <tr class="border-b border-border/40 hover:bg-slate-800/50">
                            <td class="px-3 py-2 font-medium text-slate-100">
                                <?= htmlspecialchars($scName, ENT_QUOTES) ?>
                                <?php if ($scId > 0): ?>
                                    <a href="?character_id=<?= $scId ?>" class="ml-1 text-accent text-xs">view</a>
                                <?php endif; ?>
                            </td>
                            <td class="px-3 py-2 text-right"><?= (int) ($sc['co_kill_count'] ?? 0) ?></td>
                            <td class="px-3 py-2 text-right"><?= (int) ($sc['unique_victim_count'] ?? 0) ?></td>
                            <td class="px-3 py-2 text-right"><?= (int) ($sc['unique_system_count'] ?? 0) ?></td>
                            <td class="px-3 py-2 text-right font-semibold text-slate-200"><?= number_format((float) ($sc['edge_weight'] ?? 0), 1) ?></td>
                            <td class="px-3 py-2 text-right text-xs text-muted"><?= htmlspecialchars((string) ($sc['last_event_at'] ?? ''), ENT_QUOTES) ?></td>
                        </tr>
                    <?php endforeach; ?>
                    </tbody>
                </table>
            </div>
            <?php endif; ?>

            <!-- ── Temporal windows ──────────────────────────────── -->
            <?php if ($temporalMetrics !== []): ?>
            <h3 class="mt-8 text-base font-semibold text-slate-100">Performance by Time Window</h3>
            <p class="text-xs text-muted mt-0.5">Rolling activity across the last 7, 30, and 90 days.</p>
            <div class="mt-3 table-shell">
                <table class="table-ui">
                    <thead>
                        <tr class="border-b border-border/70 text-xs text-muted uppercase">
                            <th class="px-3 py-2 text-left">Window</th>
                            <th class="px-3 py-2 text-right">Battles</th>
                            <th class="px-3 py-2 text-right">Kills</th>
                            <th class="px-3 py-2 text-right">Losses</th>
                            <th class="px-3 py-2 text-right">Damage</th>
                            <th class="px-3 py-2 text-right">Suspicion</th>
                            <th class="px-3 py-2 text-right">Engagement Rate</th>
                            <th class="px-3 py-2 text-right">Co-presence Density</th>
                        </tr>
                    </thead>
                    <tbody>
                    <?php foreach (['7d', '30d', '90d'] as $wl):
                        $tm = $temporalByWindow[$wl] ?? null;
                        if ($tm === null) continue;
                        $tmSusp = (float) ($tm['suspicion_score'] ?? 0);
                        $tmEngRate = $tm['engagement_rate_avg'] !== null ? (float) $tm['engagement_rate_avg'] : null;
                        $tmCoDensity = $tm['co_presence_density'] !== null ? (float) $tm['co_presence_density'] : null;
                    ?>
                        <tr class="border-b border-border/40">
                            <td class="px-3 py-2 font-medium text-slate-200 uppercase text-xs tracking-wider"><?= htmlspecialchars($wl, ENT_QUOTES) ?></td>
                            <td class="px-3 py-2 text-right"><?= number_format((int) ($tm['battles_present'] ?? 0)) ?></td>
                            <td class="px-3 py-2 text-right"><?= number_format((int) ($tm['kills_total'] ?? 0)) ?></td>
                            <td class="px-3 py-2 text-right"><?= number_format((int) ($tm['losses_total'] ?? 0)) ?></td>
                            <td class="px-3 py-2 text-right"><?= number_format((float) ($tm['damage_total'] ?? 0), 0) ?></td>
                            <td class="px-3 py-2 text-right <?= $tmSusp >= 0.5 ? 'text-red-400' : ($tmSusp >= 0.2 ? 'text-yellow-400' : '') ?>">
                                <?= $tmSusp > 0 ? number_format($tmSusp, 4) : '<span class="text-muted">—</span>' ?>
                            </td>
                            <td class="px-3 py-2 text-right">
                                <?= $tmEngRate !== null ? number_format($tmEngRate, 3) : '<span class="text-muted">—</span>' ?>
                            </td>
                            <td class="px-3 py-2 text-right">
                                <?= $tmCoDensity !== null ? number_format($tmCoDensity, 3) : '<span class="text-muted">—</span>' ?>
                            </td>
                        </tr>
                    <?php endforeach; ?>
                    </tbody>
                </table>
            </div>
            <?php endif; ?>

            <!-- ── Intelligence signals breakdown ───────────────── -->
            <?php if (is_array($suspicion)): ?>
            <h3 class="mt-8 text-base font-semibold text-slate-100">Intelligence Signals Breakdown</h3>
            <p class="text-xs text-muted mt-0.5">Individual scoring components that feed into the suspicion and risk scores.</p>
            <div class="mt-3 grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
                <?php
                $signals = [
                    ['key' => 'selective_non_engagement_score', 'label' => 'Selective Non-Engagement',   'desc' => 'Avoids engaging when present in fleet'],
                    ['key' => 'high_presence_low_output_score', 'label' => 'High Presence / Low Output', 'desc' => 'In many battles but low contribution'],
                    ['key' => 'token_participation_score',      'label' => 'Token Participation',         'desc' => 'Appears briefly without meaningful action'],
                    ['key' => 'loss_without_attack_ratio',      'label' => 'Loss Without Attack',         'desc' => 'Dies without landing a single hit'],
                    ['key' => 'peer_normalized_kills_delta',    'label' => 'Peer Kill Delta',             'desc' => 'Kill output vs fleet cohort peers'],
                    ['key' => 'peer_normalized_damage_delta',   'label' => 'Peer Damage Delta',           'desc' => 'Damage output vs fleet cohort peers'],
                ];
                foreach ($signals as $sig):
                    $val = (float) ($suspicion[$sig['key']] ?? 0);
                    $isNegativeBad = in_array($sig['key'], ['peer_normalized_kills_delta', 'peer_normalized_damage_delta'], true);
                    $isBad  = $isNegativeBad ? $val < -0.4 : $val > 0.5;
                    $isWarn = $isNegativeBad ? ($val < -0.2 && !$isBad) : ($val > 0.2 && !$isBad);
                    $valClass = $isBad ? 'text-red-400' : ($isWarn ? 'text-yellow-400' : 'text-slate-100');
                ?>
                <div class="surface-tertiary flex flex-col gap-0.5">
                    <p class="text-xs text-muted"><?= htmlspecialchars($sig['label'], ENT_QUOTES) ?></p>
                    <p class="text-base font-semibold <?= $valClass ?>"><?= number_format($val, 4) ?></p>
                    <p class="text-[10px] text-slate-500 mt-0.5"><?= htmlspecialchars($sig['desc'], ENT_QUOTES) ?></p>
                </div>
                <?php endforeach; ?>
            </div>
            <?php endif; ?>

            <!-- ── Ships flown ───────────────────────────────────── -->
            <?php if ($ships !== []): ?>
            <h3 class="mt-8 text-base font-semibold text-slate-100">Ships Flown</h3>
            <p class="text-xs text-muted mt-0.5">Top ship types by times flown across all tracked theaters.</p>
            <div class="mt-3 grid gap-2 sm:grid-cols-2 md:grid-cols-5">
                <?php foreach ($ships as $ship): ?>
                <div class="surface-tertiary flex items-center gap-3">
                    <img src="https://images.evetech.net/types/<?= (int) $ship['type_id'] ?>/icon?size=32"
                         alt="" class="w-8 h-8 rounded flex-shrink-0" loading="lazy">
                    <div class="min-w-0">
                        <p class="text-sm text-slate-100 truncate"><?= htmlspecialchars((string) $ship['type_name'], ENT_QUOTES) ?></p>
                        <p class="text-xs text-muted">
                            <?= htmlspecialchars(killmail_ship_class_label($ship['group_id'] ?: null), ENT_QUOTES) ?>
                            &middot; <?= (int) $ship['times_flown'] ?>×
                        </p>
                    </div>
                </div>
                <?php endforeach; ?>
            </div>
            <?php endif; ?>

            <!-- ── Top co-presence edges (30d) ───────────────────── -->
            <?php if ($copresenceEdges !== []): ?>
            <h3 class="mt-8 text-base font-semibold text-slate-100">Top Associates <span class="text-muted font-normal text-xs ml-1">30-day window</span></h3>
            <p class="text-xs text-muted mt-0.5">Pilots with the strongest co-presence edge weight in the last 30 days, indicating frequent battlefield pairing.</p>
            <div class="mt-3 table-shell">
                <table class="table-ui">
                    <thead>
                        <tr class="border-b border-border/70 text-xs text-muted uppercase">
                            <th class="px-3 py-2 text-left">Pilot</th>
                            <th class="px-3 py-2 text-left">Event Type</th>
                            <th class="px-3 py-2 text-right">Events</th>
                            <th class="px-3 py-2 text-right">Edge Weight</th>
                            <th class="px-3 py-2 text-right">Last Seen</th>
                            <th class="px-3 py-2 text-right"></th>
                        </tr>
                    </thead>
                    <tbody>
                    <?php
                    $viewedAllianceId = (int) ($profile['alliance_id'] ?? 0);
                    $trackedAllianceIds = array_map('intval', array_column(db_killmail_tracked_alliances_active(), 'alliance_id'));
                    $edgeSuppressedCount = 0;
                    foreach ($copresenceEdges as $edge):
                        $otherRawName = (string) ($edge['other_character_name'] ?? '?');
                        if (ci_is_placeholder_name($otherRawName)) { $edgeSuppressedCount++; continue; }
                        $otherName = htmlspecialchars($otherRawName, ENT_QUOTES);
                        $otherId   = (int) ($edge['other_character_id'] ?? 0);
                        $lastAt    = (string) ($edge['last_event_at'] ?? '');
                        $otherAllianceId = (int) ($edge['other_alliance_id'] ?? 0);
                        $isSameAlliance = $viewedAllianceId > 0 && $otherAllianceId === $viewedAllianceId;
                        $isTrackedAlliance = $otherAllianceId > 0 && in_array($otherAllianceId, $trackedAllianceIds, true);
                    ?>
                        <tr class="border-b border-border/40 hover:bg-slate-800/50">
                            <td class="px-3 py-2 font-medium text-slate-100">
                                <?= $otherName ?>
                                <?php if ($isSameAlliance): ?>
                                    <span class="ml-1 inline-block rounded-full px-1.5 py-0.5 text-[9px] uppercase tracking-wider bg-cyan-900/60 text-cyan-300">ally</span>
                                <?php elseif ($isTrackedAlliance): ?>
                                    <span class="ml-1 inline-block rounded-full px-1.5 py-0.5 text-[9px] uppercase tracking-wider bg-emerald-900/60 text-emerald-300">coalition</span>
                                <?php endif; ?>
                            </td>
                            <td class="px-3 py-2 text-xs text-muted capitalize"><?= htmlspecialchars(str_replace('_', ' ', (string) ($edge['event_type'] ?? '')), ENT_QUOTES) ?></td>
                            <td class="px-3 py-2 text-right"><?= number_format((int) ($edge['event_count'] ?? 0)) ?></td>
                            <td class="px-3 py-2 text-right font-semibold text-slate-200"><?= number_format((float) ($edge['edge_weight'] ?? 0), 2) ?></td>
                            <td class="px-3 py-2 text-right text-xs text-muted"><?= htmlspecialchars($lastAt, ENT_QUOTES) ?></td>
                            <td class="px-3 py-2 text-right">
                                <?php if ($otherId > 0): ?>
                                    <a href="?character_id=<?= $otherId ?>" class="text-accent text-sm">View</a>
                                <?php endif; ?>
                            </td>
                        </tr>
                    <?php endforeach; ?>
                    <?php if ($edgeSuppressedCount > 0): ?>
                        <tr class="border-b border-border/40"><td colspan="6" class="px-3 py-2 text-xs text-muted italic"><?= $edgeSuppressedCount ?> unresolved character<?= $edgeSuppressedCount !== 1 ? 's' : '' ?> suppressed</td></tr>
                    <?php endif; ?>
                    </tbody>
                </table>
            </div>
            <?php endif; ?>

            <!-- ── Network signals by window ─────────────────────── -->
            <?php if ($copresenceSignals !== []): ?>
            <h3 class="mt-8 text-base font-semibold text-slate-100">Network Signals</h3>
            <p class="text-xs text-muted mt-0.5">Co-presence clustering and out-of-cluster ratio across time windows.</p>
            <div class="mt-3 table-shell">
                <table class="table-ui">
                    <thead>
                        <tr class="border-b border-border/70 text-xs text-muted uppercase">
                            <th class="px-3 py-2 text-left">Window</th>
                            <th class="px-3 py-2 text-right">Unique Associates</th>
                            <th class="px-3 py-2 text-right">Recurring Pairs</th>
                            <th class="px-3 py-2 text-right">Total Edge Weight</th>
                            <th class="px-3 py-2 text-right">Out-of-Cluster Ratio</th>
                            <th class="px-3 py-2 text-right">Cohort Percentile</th>
                        </tr>
                    </thead>
                    <tbody>
                    <?php foreach (['7d', '30d', '90d'] as $wl):
                        $cs = $copresenceByWindow[$wl] ?? null;
                        if ($cs === null) continue;
                        $ocRatio   = $cs['out_of_cluster_ratio'] !== null ? (float) $cs['out_of_cluster_ratio'] : null;
                        $cohortPct = $cs['cohort_percentile'] !== null ? (float) $cs['cohort_percentile'] : null;
                    ?>
                        <tr class="border-b border-border/40">
                            <td class="px-3 py-2 font-medium text-slate-200 uppercase text-xs tracking-wider"><?= htmlspecialchars($wl, ENT_QUOTES) ?></td>
                            <td class="px-3 py-2 text-right"><?= number_format((int) ($cs['unique_associates'] ?? 0)) ?></td>
                            <td class="px-3 py-2 text-right"><?= number_format((int) ($cs['recurring_pair_count'] ?? 0)) ?></td>
                            <td class="px-3 py-2 text-right"><?= number_format((float) ($cs['total_edge_weight'] ?? 0), 2) ?></td>
                            <td class="px-3 py-2 text-right <?= ($ocRatio !== null && $ocRatio > 0.5) ? 'text-yellow-400' : '' ?>">
                                <?= $ocRatio !== null ? number_format($ocRatio, 3) : '<span class="text-muted">—</span>' ?>
                            </td>
                            <td class="px-3 py-2 text-right">
                                <?= $cohortPct !== null ? number_format($cohortPct, 1) . '%' : '<span class="text-muted">—</span>' ?>
                            </td>
                        </tr>
                    <?php endforeach; ?>
                    </tbody>
                </table>
            </div>
            <?php endif; ?>

            <!-- ── Theater history ────────────────────────────────── -->
            <?php if ($theaterHistory !== []): ?>
            <h3 class="mt-8 text-base font-semibold text-slate-100">Theater History <span class="text-muted font-normal text-xs ml-1">last 20</span></h3>
            <div class="mt-3 table-shell">
                <table class="table-ui">
                    <thead>
                        <tr class="border-b border-border/70 text-xs text-muted uppercase">
                            <th class="px-3 py-2 text-left">Theater</th>
                            <th class="px-3 py-2 text-left">Alliance</th>
                            <th class="px-3 py-2 text-left">Role</th>
                            <th class="px-3 py-2 text-left">Side</th>
                            <th class="px-3 py-2 text-right">Kills</th>
                            <th class="px-3 py-2 text-right">Deaths</th>
                            <th class="px-3 py-2 text-right">Damage</th>
                            <th class="px-3 py-2 text-right">Suspicion</th>
                            <th class="px-3 py-2 text-right"></th>
                        </tr>
                    </thead>
                    <tbody>
                    <?php foreach ($theaterHistory as $th):
                        $thSusp = (float) ($th['suspicion_score'] ?? 0);
                        $thSide = (string) ($th['side'] ?? '');
                        $sideClass = match($thSide) {
                            'friendly'    => 'text-blue-300',
                            'opponent'    => 'text-red-300',
                            'third_party' => 'text-slate-400',
                            default       => 'text-muted',
                        };
                    ?>
                        <tr class="border-b border-border/40 hover:bg-slate-800/40">
                            <td class="px-3 py-2">
                                <p class="text-slate-100 text-sm"><?= htmlspecialchars((string) ($th['primary_system_name'] ?? '?'), ENT_QUOTES) ?></p>
                                <p class="text-xs text-muted"><?= htmlspecialchars((string) ($th['region_name'] ?? ''), ENT_QUOTES) ?><?= ($th['start_time'] ?? '') !== '' ? ' &middot; ' . htmlspecialchars((string) $th['start_time'], ENT_QUOTES) : '' ?></p>
                            </td>
                            <td class="px-3 py-2 text-sm"><?= htmlspecialchars((string) ($th['alliance_name'] ?? '—'), ENT_QUOTES) ?></td>
                            <td class="px-3 py-2">
                                <?php $thRole = (string) ($th['role_proxy'] ?? ''); ?>
                                <span class="inline-block rounded-full px-2 py-0.5 text-[10px] uppercase tracking-wider <?= fleet_function_color_class($thRole) ?>">
                                    <?= htmlspecialchars(fleet_function_label($thRole), ENT_QUOTES) ?>
                                </span>
                            </td>
                            <td class="px-3 py-2 text-xs uppercase tracking-wide <?= $sideClass ?>">
                                <?= htmlspecialchars(str_replace('_', ' ', $thSide) ?: '—', ENT_QUOTES) ?>
                            </td>
                            <td class="px-3 py-2 text-right"><?= (int) ($th['kills'] ?? 0) ?></td>
                            <td class="px-3 py-2 text-right"><?= (int) ($th['deaths'] ?? 0) ?></td>
                            <td class="px-3 py-2 text-right"><?= number_format((float) ($th['damage_done'] ?? 0), 0) ?></td>
                            <td class="px-3 py-2 text-right <?= $thSusp >= 0.5 ? 'text-red-400 font-semibold' : ($thSusp > 0 ? 'text-yellow-400' : '') ?>">
                                <?= $thSusp > 0 ? number_format($thSusp, 3) : '<span class="text-muted">—</span>' ?>
                            </td>
                            <td class="px-3 py-2 text-right">
                                <a href="/theater-intelligence/view.php?theater_id=<?= urlencode((string) ($th['theater_id'] ?? '')) ?>" class="text-accent text-sm">Theater</a>
                            </td>
                        </tr>
                    <?php endforeach; ?>
                    </tbody>
                </table>
            </div>
            <?php endif; ?>

            <!-- ── Associates (split: internal fleetmates vs external) ── -->
            <?php if ($associates !== []):
                $internalAssociates = [];
                $externalAssociates = [];
                $assocSuppressedCount = 0;
                foreach ($associates as $a) {
                    $aName = (string) ($a['assoc_name'] ?? '?');
                    if (ci_is_placeholder_name($aName)) { $assocSuppressedCount++; continue; }
                    $aAllianceId = (int) ($a['assoc_alliance_id'] ?? 0);
                    $isInternal = ($viewedAllianceId > 0 && $aAllianceId === $viewedAllianceId)
                                  || ($aAllianceId > 0 && in_array($aAllianceId, $trackedAllianceIds, true));
                    if ($isInternal) {
                        $internalAssociates[] = $a;
                    } else {
                        $externalAssociates[] = $a;
                    }
                }
            ?>

            <?php if ($internalAssociates !== []): ?>
            <h3 class="mt-8 text-base font-semibold text-slate-100">Frequent Fleetmates <span class="text-muted font-normal text-xs ml-1">alliance &amp; coalition</span></h3>
            <p class="text-xs text-muted mt-0.5">Same-side associates from your alliance or coalition across tracked theaters.</p>
            <div class="mt-3 table-shell">
                <table class="table-ui">
                    <thead>
                        <tr class="border-b border-border/70 text-xs text-muted uppercase">
                            <th class="px-3 py-2 text-left">Pilot</th>
                            <th class="px-3 py-2 text-left">Alliance</th>
                            <th class="px-3 py-2 text-left">Role</th>
                            <th class="px-3 py-2 text-right">Shared Theaters</th>
                            <th class="px-3 py-2 text-right"></th>
                        </tr>
                    </thead>
                    <tbody>
                    <?php foreach ($internalAssociates as $a): ?>
                        <tr class="border-b border-border/40 hover:bg-slate-800/50">
                            <td class="px-3 py-2 font-medium text-slate-100"><?= htmlspecialchars((string) ($a['assoc_name'] ?? '?'), ENT_QUOTES) ?></td>
                            <td class="px-3 py-2 text-sm"><?= htmlspecialchars((string) ($a['assoc_alliance'] ?? '—'), ENT_QUOTES) ?></td>
                            <td class="px-3 py-2">
                                <?php $aRole = (string) ($a['assoc_role'] ?? ''); ?>
                                <?php if ($aRole !== ''): ?>
                                    <span class="inline-block rounded-full px-2 py-0.5 text-[10px] uppercase tracking-wider <?= fleet_function_color_class($aRole) ?>">
                                        <?= htmlspecialchars(fleet_function_label($aRole), ENT_QUOTES) ?>
                                    </span>
                                <?php endif; ?>
                            </td>
                            <td class="px-3 py-2 text-right font-semibold"><?= number_format((int) ($a['shared_theaters'] ?? 0)) ?></td>
                            <td class="px-3 py-2 text-right">
                                <a href="?character_id=<?= (int) ($a['assoc_character_id'] ?? 0) ?>"
                                   class="text-accent text-sm">View</a>
                            </td>
                        </tr>
                    <?php endforeach; ?>
                    </tbody>
                </table>
            </div>
            <?php endif; ?>

            <?php if ($externalAssociates !== []): ?>
            <h3 class="mt-8 text-base font-semibold text-slate-100">Notable External Associations <span class="text-muted font-normal text-xs ml-1">non-coalition same-side</span></h3>
            <p class="text-xs text-muted mt-0.5">Same-side associates outside your alliance and coalition. May indicate cross-boundary relationships worth investigating.</p>
            <div class="mt-3 table-shell">
                <table class="table-ui">
                    <thead>
                        <tr class="border-b border-border/70 text-xs text-muted uppercase">
                            <th class="px-3 py-2 text-left">Pilot</th>
                            <th class="px-3 py-2 text-left">Alliance</th>
                            <th class="px-3 py-2 text-left">Role</th>
                            <th class="px-3 py-2 text-right">Shared Theaters</th>
                            <th class="px-3 py-2 text-right"></th>
                        </tr>
                    </thead>
                    <tbody>
                    <?php foreach ($externalAssociates as $a): ?>
                        <tr class="border-b border-border/40 hover:bg-slate-800/50">
                            <td class="px-3 py-2 font-medium text-slate-100"><?= htmlspecialchars((string) ($a['assoc_name'] ?? '?'), ENT_QUOTES) ?></td>
                            <td class="px-3 py-2 text-sm"><?= htmlspecialchars((string) ($a['assoc_alliance'] ?? '—'), ENT_QUOTES) ?></td>
                            <td class="px-3 py-2">
                                <?php $aRole = (string) ($a['assoc_role'] ?? ''); ?>
                                <?php if ($aRole !== ''): ?>
                                    <span class="inline-block rounded-full px-2 py-0.5 text-[10px] uppercase tracking-wider <?= fleet_function_color_class($aRole) ?>">
                                        <?= htmlspecialchars(fleet_function_label($aRole), ENT_QUOTES) ?>
                                    </span>
                                <?php endif; ?>
                            </td>
                            <td class="px-3 py-2 text-right font-semibold"><?= number_format((int) ($a['shared_theaters'] ?? 0)) ?></td>
                            <td class="px-3 py-2 text-right">
                                <a href="?character_id=<?= (int) ($a['assoc_character_id'] ?? 0) ?>"
                                   class="text-accent text-sm">View</a>
                            </td>
                        </tr>
                    <?php endforeach; ?>
                    </tbody>
                </table>
            </div>
            <?php endif; ?>

            <?php if ($assocSuppressedCount > 0): ?>
                <p class="mt-2 text-xs text-muted italic"><?= $assocSuppressedCount ?> unresolved character<?= $assocSuppressedCount !== 1 ? 's' : '' ?> suppressed from associate lists</p>
            <?php endif; ?>

            <?php endif; ?>

            <!-- ══════════════════════════════════════════════════════════ -->
            <!-- ── CHARACTER INTELLIGENCE DOSSIER ─────────────────────── -->
            <!-- ══════════════════════════════════════════════════════════ -->
            <?php if (is_array($ciCharacter)): ?>
            <?php
                $ciDataSource = (string) ($ciCharacter['data_source'] ?? 'counterintel');
                $psFresh = is_array($ciPipelineStatus) && !empty($ciPipelineStatus['fully_fresh']);
                $psExists = is_array($ciPipelineStatus);
                $psHasError = $psExists && (($ciPipelineStatus['histogram_error'] ?? null) !== null || ($ciPipelineStatus['counterintel_error'] ?? null) !== null);

                if ($ciDataSource === 'counterintel' && $psFresh): ?>
                    <?php /* Full analysis ready — no banner needed */ ?>
                <?php elseif ($ciDataSource === 'counterintel' && $psExists && !$psFresh): ?>
                    <div class="mt-6 rounded border border-blue-500/30 bg-blue-950/30 px-4 py-2.5 text-sm text-blue-200/90">
                        <strong>Partial analysis available</strong> &mdash; new source data detected. Background pipeline will refresh stale stages automatically.
                        <span class="text-xs text-blue-300/50 ml-2"><?php
                            $stageLabels = [];
                            if (!empty($ciPipelineStatus['histogram_fresh'])) { $stageLabels[] = '<span class="text-green-400">histogram</span>'; } else { $stageLabels[] = '<span class="text-amber-400">histogram</span>'; }
                            if (!empty($ciPipelineStatus['counterintel_fresh'])) { $stageLabels[] = '<span class="text-green-400">counterintel</span>'; } else { $stageLabels[] = '<span class="text-amber-400">counterintel</span>'; }
                            if (!empty($ciPipelineStatus['temporal_fresh'])) { $stageLabels[] = '<span class="text-green-400">temporal</span>'; } else { $stageLabels[] = '<span class="text-amber-400">temporal</span>'; }
                            echo implode(' &middot; ', $stageLabels);
                        ?></span>
                    </div>
                <?php elseif ($psHasError): ?>
                    <div class="mt-6 rounded border border-red-500/30 bg-red-950/30 px-4 py-2.5 text-sm text-red-200/90">
                        <strong>Processing error</strong> &mdash; retry pending.
                        <?php if (($ciPipelineStatus['histogram_error'] ?? null) !== null): ?>
                            <span class="text-xs text-red-300/60 block mt-1">Histogram: <?= htmlspecialchars(mb_substr((string) $ciPipelineStatus['histogram_error'], 0, 120), ENT_QUOTES) ?></span>
                        <?php endif; ?>
                        <?php if (($ciPipelineStatus['counterintel_error'] ?? null) !== null): ?>
                            <span class="text-xs text-red-300/60 block mt-1">Counterintel: <?= htmlspecialchars(mb_substr((string) $ciPipelineStatus['counterintel_error'], 0, 120), ENT_QUOTES) ?></span>
                        <?php endif; ?>
                    </div>
                <?php elseif ($ciDataSource === 'suspicion_v2'): ?>
                    <div class="mt-6 rounded border border-amber-500/30 bg-amber-950/30 px-4 py-2.5 text-sm text-amber-200/90">
                        <strong>Processing queued</strong> &mdash; showing batch suspicion scores. Full analysis will run automatically in the background.
                    </div>
                <?php elseif ($ciDataSource === 'below_threshold'): ?>
                    <div class="mt-6 rounded border border-slate-500/30 bg-slate-800/50 px-4 py-2.5 text-sm text-slate-300">
                        <strong>Insufficient source data</strong> &mdash; <?= (int) ($ciCharacter['total_battle_count'] ?? 0) ?> battle(s) detected. Analysis will run when more activity data is available.
                    </div>
                <?php endif; ?>
            <?php
                $ciRiskLevel = ci_risk_level((float) ($ciCharacter['review_priority_score'] ?? 0));
                $ciPriorityScore = (float) ($ciCharacter['review_priority_score'] ?? 0);
                $ciConfidenceScore = (float) ($ciCharacter['confidence_score'] ?? 0);
                $ciPercentileRank = (float) ($ciCharacter['percentile_rank'] ?? 0);
                $ciRepeatabilityScore = (float) ($ciCharacter['repeatability_score'] ?? 0);
                $ciEvidenceCount = (int) ($ciCharacter['evidence_count'] ?? 0);
                $ciEnemySustainLift = (float) ($ciCharacter['enemy_sustain_lift'] ?? 0);
                $ciHullLift = (float) ($ciCharacter['enemy_same_hull_survival_lift'] ?? 0);
                $ciBridgeScore = (float) ($ciCharacter['graph_bridge_score'] ?? 0);
                $ciCoPres = (float) ($ciCharacter['co_presence_anomalous_density'] ?? 0);
                $ciCorpHops = (float) ($ciCharacter['corp_hop_frequency_180d'] ?? 0);
                $ciShortTenure = (float) ($ciCharacter['short_tenure_ratio_180d'] ?? 0);
            ?>

            <div class="mt-10 border-t border-border/40 pt-8">
                <div class="flex items-center gap-3 mb-4">
                    <h2 class="text-xl font-semibold text-slate-50">Character Intelligence Dossier</h2>
                    <span class="inline-block rounded-full px-3 py-0.5 text-xs font-semibold uppercase tracking-wider <?= $ciRiskLevel['bg'] ?> <?= $ciRiskLevel['text'] ?>"><?= $ciRiskLevel['label'] ?> risk</span>
                </div>

                <!-- Verdict summary -->
                <div class="rounded border border-border/40 bg-slate-800/50 px-4 py-3 text-sm text-slate-300 leading-relaxed">
                    <?= ci_verdict_summary($ciCharacter) ?>
                </div>

                <!-- Primary metrics -->
                <div class="mt-4 grid gap-3 md:grid-cols-3">
                    <div class="surface-tertiary">
                        <p class="text-xs text-muted">Review priority</p>
                        <p class="mt-1 text-xl <?= $ciRiskLevel['text'] ?>"><?= number_format($ciPriorityScore * 100, 1) ?>%</p>
                        <?= ci_progress_bar($ciPriorityScore, $ciRiskLevel['bar']) ?>
                        <p class="mt-1 text-[11px] text-muted">Combined suspicion score &mdash; higher means more suspicious</p>
                    </div>
                    <div class="surface-tertiary">
                        <p class="text-xs text-muted">Confidence / Percentile</p>
                        <p class="mt-1 text-xl text-slate-100"><?= number_format($ciConfidenceScore * 100, 0) ?>% confident &middot; top <?= number_format((1 - $ciPercentileRank) * 100, 1) ?>%</p>
                        <?= ci_progress_bar($ciConfidenceScore, 'bg-cyan-500') ?>
                        <p class="mt-1 text-[11px] text-muted">How certain the model is, and where they rank vs. everyone</p>
                    </div>
                    <div class="surface-tertiary">
                        <p class="text-xs text-muted">Repeatability / Evidence</p>
                        <p class="mt-1 text-xl text-slate-100">
                            <span class="<?= ci_metric_color($ciRepeatabilityScore, 0.4, 0.7) ?>"><?= number_format($ciRepeatabilityScore * 100, 0) ?>%</span>
                            <span class="text-sm text-muted">repeat</span>
                            &middot;
                            <?= $ciEvidenceCount ?> <span class="text-sm text-muted">signals</span>
                        </p>
                        <?= ci_progress_bar($ciRepeatabilityScore, 'bg-cyan-500') ?>
                        <p class="mt-1 text-[11px] text-muted">Pattern consistency across time windows &middot; number of supporting signals</p>
                    </div>
                </div>

                <!-- Secondary metrics -->
                <div class="mt-3 grid gap-3 md:grid-cols-3">
                    <div class="surface-tertiary">
                        <p class="text-xs text-muted">Enemy survival boost</p>
                        <p class="mt-1 text-base <?= ci_lift_color($ciEnemySustainLift) ?>">
                            <?php if ($ciEnemySustainLift >= 1.0): ?>
                                +<?= number_format(($ciEnemySustainLift - 1) * 100, 0) ?>% above expected
                            <?php else: ?>
                                <?= number_format(($ciEnemySustainLift - 1) * 100, 0) ?>% below expected
                            <?php endif; ?>
                        </p>
                        <p class="mt-0.5 text-xs text-muted">Hull lift: <span class="<?= ci_lift_color($ciHullLift) ?>"><?php if ($ciHullLift >= 1.0): ?>+<?= number_format(($ciHullLift - 1) * 100, 0) ?>%<?php else: ?><?= number_format(($ciHullLift - 1) * 100, 0) ?>%<?php endif; ?></span></p>
                        <p class="mt-1 text-[11px] text-muted">How much more enemies survive when this pilot is present</p>
                    </div>
                    <div class="surface-tertiary">
                        <p class="text-xs text-muted">Network position</p>
                        <p class="mt-1 text-base text-slate-100">Bridge: <span class="font-semibold"><?= number_format($ciBridgeScore, 1) ?></span> &middot; Co-presence: <span class="font-semibold"><?= number_format($ciCoPres * 100, 0) ?>%</span></p>
                        <?= ci_progress_bar($ciCoPres, 'bg-cyan-500') ?>
                        <p class="mt-1 text-[11px] text-muted">Bridge = links between groups &middot; Co-presence = how often seen with flagged pilots</p>
                    </div>
                    <div class="surface-tertiary">
                        <p class="text-xs text-muted">Corp movement (180 days)</p>
                        <p class="mt-1 text-base text-slate-100">
                            <?php if ($ciCorpHops == 0 && $ciShortTenure == 0): ?>
                                <span class="text-green-400">Stable</span> &mdash; no corp changes
                            <?php else: ?>
                                <span class="<?= $ciCorpHops >= 3 ? 'text-red-400' : ($ciCorpHops >= 1 ? 'text-yellow-400' : 'text-green-400') ?>"><?= number_format($ciCorpHops, 0) ?> hops</span>
                                &middot;
                                <span class="<?= $ciShortTenure >= 0.5 ? 'text-red-400' : ($ciShortTenure >= 0.2 ? 'text-yellow-400' : 'text-green-400') ?>"><?= number_format($ciShortTenure * 100, 0) ?>% short stays</span>
                            <?php endif; ?>
                        </p>
                        <p class="mt-1 text-[11px] text-muted">Frequent corp-hopping can indicate spy behaviour</p>
                    </div>
                </div>

                <!-- Org history timeline -->
                <?php
                    $ciHistoryRecords = array_reverse((array) ($ciOrgHistory['history'] ?? []));
                    $ciOrgInfo = (array) ($ciOrgHistory['info'] ?? []);
                ?>
                <?php if ($ciHistoryRecords !== []): ?>
                <h3 class="mt-6 text-base font-semibold text-slate-100">Corporation history</h3>
                <p class="mt-1 text-xs text-muted">Timeline of corp memberships. Short stays and frequent moves can indicate spy alts.</p>
                <div class="mt-3 space-y-0">
                    <?php foreach ($ciHistoryRecords as $idx => $rec):
                        $corpId = (int) ($rec['corporation_id'] ?? 0);
                        $corpName = $ciOrgCorpNames[$corpId] ?? ('Corp #' . $corpId);
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
                        <div class="flex items-start gap-3">
                            <div class="flex flex-col items-center">
                                <div class="h-3 w-3 rounded-full mt-1 <?= $isCurrent ? 'bg-green-500' : ($isShort ? 'bg-red-500' : 'bg-slate-500') ?>"></div>
                                <?php if ($idx < count($ciHistoryRecords) - 1): ?>
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
                <?php else: ?>
                <details class="mt-4 surface-tertiary"><summary class="cursor-pointer text-sm text-slate-100">Org history (no records)</summary><p class="mt-2 text-xs text-muted">No corporation history data available.</p></details>
                <?php endif; ?>

                <!-- Evidence breakdown -->
                <?php if ($ciEvidence !== []): ?>
                <h3 class="mt-6 text-base font-semibold text-slate-100">Evidence breakdown</h3>
                <p class="mt-1 text-xs text-muted">Each signal that contributes to the overall suspicion score.</p>
                <div class="mt-3 table-shell"><table class="table-ui"><thead><tr class="border-b border-border/70 text-xs text-muted uppercase"><th class="px-3 py-2 text-left">Signal</th><th class="px-3 py-2 text-right">Weight</th><th class="px-3 py-2 text-left">What it means</th></tr></thead><tbody><?php foreach ($ciEvidence as $evidenceRow): ?><tr class="border-b border-border/40"><td class="px-3 py-2 text-sm"><?= htmlspecialchars(ci_evidence_label((string) ($evidenceRow['evidence_key'] ?? '')), ENT_QUOTES) ?></td><td class="px-3 py-2 text-right font-mono text-sm <?= ci_metric_color((float) ($evidenceRow['evidence_value'] ?? 0), 0.3, 0.7) ?>"><?= htmlspecialchars(number_format((float) ($evidenceRow['evidence_value'] ?? 0), 2), ENT_QUOTES) ?></td><td class="px-3 py-2"><div class="text-sm text-slate-300"><?= htmlspecialchars((string) ($evidenceRow['evidence_text'] ?? ''), ENT_QUOTES) ?></div><?php if (is_array($evidenceRow['evidence_payload'] ?? null)): ?><details class="mt-1"><summary class="text-[11px] text-muted cursor-pointer">Show raw data</summary><pre class="mt-1 overflow-auto text-[11px] text-slate-400"><?= htmlspecialchars(json_encode($evidenceRow['evidence_payload'], JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES) ?: '{}', ENT_QUOTES) ?></pre></details><?php endif; ?></td></tr><?php endforeach; ?>
<?php if (is_array($ciNeo4jIntel)): ?>
<?php
    $neo4jOverlap = (int) ($ciNeo4jIntel['enemy_overlap_count'] ?? 0);
    $neo4jSharedCorps = (int) ($ciNeo4jIntel['shared_corps'] ?? 0);
    $neo4jDefector = (bool) ($ciNeo4jIntel['is_recent_defector'] ?? false);
    $neo4jDefectorCorp = (string) ($ciNeo4jIntel['defector_corp_name'] ?? '');
    $neo4jDefectorDays = (int) ($ciNeo4jIntel['defector_days_ago'] ?? 0);
    $neo4jHostileNeighbors = (int) ($ciNeo4jIntel['hostile_neighbors'] ?? 0);
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
                <?php endif; ?>

                <!-- Supporting battles -->
                <?php if ($ciBattles !== []): ?>
                <h3 class="mt-6 text-base font-semibold text-slate-100">Supporting battles</h3>
                <p class="mt-1 text-xs text-muted">Battles where this character was present. Red rows indicate enemy over-performance anomalies.</p>
                <div class="mt-3 table-shell"><table class="table-ui"><thead><tr class="border-b border-border/70 text-xs text-muted uppercase"><th class="px-3 py-2 text-left">Battle</th><th class="px-3 py-2 text-left">Side</th><th class="px-3 py-2 text-left">Classification</th><th class="px-3 py-2 text-right">Enemy boost</th><th class="px-3 py-2 text-right">Inspect</th></tr></thead><tbody><?php foreach ($ciBattles as $battle): ?>
                    <?php
                        $anomalyClass = (string) ($battle['anomaly_class'] ?? 'normal');
                        $anomaly = ci_anomaly_label($anomalyClass);
                        $overperf = (float) ($battle['overperformance_score'] ?? 0);
                    ?>
                    <tr class="border-b border-border/40"><td class="px-3 py-2"><?= htmlspecialchars((string) ($battle['system_name'] ?? 'Unknown'), ENT_QUOTES) ?><div class="text-xs text-muted"><?= htmlspecialchars((string) ($battle['started_at'] ?? ''), ENT_QUOTES) ?></div></td><td class="px-3 py-2"><?= htmlspecialchars((string) ($battle['side_name'] ?? $battle['side_key'] ?? 'unknown'), ENT_QUOTES) ?></td><td class="px-3 py-2"><span class="inline-block rounded-full px-2 py-0.5 text-[10px] uppercase tracking-wider <?= $anomaly['bg'] ?> <?= $anomaly['text'] ?>"><?= $anomaly['label'] ?></span></td><td class="px-3 py-2 text-right <?= ci_overperf_color($overperf) ?>"><?php if ($overperf >= 1.0): ?>+<?= number_format(($overperf - 1) * 100, 0) ?>%<?php else: ?><?= number_format(($overperf - 1) * 100, 0) ?>%<?php endif; ?></td><td class="px-3 py-2 text-right"><a class="text-accent" href="/battle-intelligence/battle.php?battle_id=<?= urlencode((string) ($battle['battle_id'] ?? '')) ?>">View</a></td></tr><?php endforeach; ?></tbody></table></div>
                <?php endif; ?>

                <!-- Temporal drift -->
                <?php if ($temporalMetrics !== []): ?>
                <h3 class="mt-6 text-base font-semibold text-slate-100">Temporal drift</h3>
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

                <!-- Movement footprint -->
                <?php if ($ciMovementFootprints !== []): ?>
                <h3 class="mt-6 text-base font-semibold text-slate-100">Movement footprint</h3>
                <p class="mt-1 text-xs text-muted">Geographic operational footprint across time windows.</p>
                <?php
                    $fp30 = null;
                    foreach ($ciMovementFootprints as $mf) {
                        if (($mf['window_label'] ?? '') === '30d') { $fp30 = $mf; break; }
                    }
                    if ($fp30 === null && $ciMovementFootprints !== []) {
                        $fp30 = $ciMovementFootprints[0];
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
                <p class="mt-2 text-xs text-muted">Cohort footprint percentile: <span class="<?= $fpCohortPctile >= 0.9 ? 'text-red-400' : ($fpCohortPctile >= 0.7 ? 'text-yellow-400' : 'text-slate-100') ?>"><?= number_format($fpCohortPctile * 100, 0) ?>%</span></p>
                <?php endif; ?>
                <?php endif; ?>

                <!-- Per-window footprint -->
                <div class="mt-3 grid gap-3 md:grid-cols-<?= min(4, count($ciMovementFootprints)) ?>">
                    <?php foreach ($ciMovementFootprints as $mf): ?>
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
                            </div>
                        </div>
                    <?php endforeach; ?>
                </div>

                <!-- System distribution -->
                <?php if ($ciSystemDistribution !== []): ?>
                <h4 class="mt-4 text-sm font-semibold text-slate-100">System distribution (30 days)</h4>
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
                        <?php foreach ($ciSystemDistribution as $sd): ?>
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
                <?php endif; ?>

                <!-- Co-presence detection signals -->
                <?php if ($copresenceSignals !== []): ?>
                <h3 class="mt-6 text-base font-semibold text-slate-100">Co-presence detection signals</h3>
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

                <!-- Top co-presence edges -->
                <?php if ($copresenceEdges !== []): ?>
                <h3 class="mt-6 text-base font-semibold text-slate-100">Top co-presence edges (30d)</h3>
                <p class="mt-1 text-xs text-muted">Strongest pairwise connections by event type. Higher weight = more frequent co-appearance.</p>
                <div class="mt-3 table-shell"><table class="table-ui"><thead><tr class="border-b border-border/70 text-xs text-muted uppercase"><th class="px-3 py-2 text-left">Character</th><th class="px-3 py-2 text-left">Event type</th><th class="px-3 py-2 text-right">Weight</th><th class="px-3 py-2 text-right">Count</th><th class="px-3 py-2 text-right">Last seen</th></tr></thead><tbody>
                <?php
                $dossierCeSuppressed = 0;
                foreach ($copresenceEdges as $ce):
                    $ceRawName = (string) ($ce['other_character_name'] ?? 'Unknown');
                    if (ci_is_placeholder_name($ceRawName)) { $dossierCeSuppressed++; continue; }
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
                    $ceOtherAlliance = (int) ($ce['other_alliance_id'] ?? 0);
                    $ceIsSameAlliance = $viewedAllianceId > 0 && $ceOtherAlliance === $viewedAllianceId;
                    $ceIsTracked = $ceOtherAlliance > 0 && in_array($ceOtherAlliance, $trackedAllianceIds, true);
                ?>
                    <tr class="border-b border-border/40">
                        <td class="px-3 py-2">
                            <a class="text-accent" href="?character_id=<?= (int) ($ce['other_character_id'] ?? 0) ?>"><?= htmlspecialchars($ceRawName, ENT_QUOTES) ?></a>
                            <?php if ($ceIsSameAlliance): ?>
                                <span class="ml-1 inline-block rounded-full px-1.5 py-0.5 text-[9px] uppercase tracking-wider bg-cyan-900/60 text-cyan-300">ally</span>
                            <?php elseif ($ceIsTracked): ?>
                                <span class="ml-1 inline-block rounded-full px-1.5 py-0.5 text-[9px] uppercase tracking-wider bg-emerald-900/60 text-emerald-300">coalition</span>
                            <?php endif; ?>
                        </td>
                        <td class="px-3 py-2"><span class="inline-block rounded-full px-2 py-0.5 text-[10px] uppercase tracking-wider <?= $ceTypeBg ?>"><?= htmlspecialchars($ceTypeLabel, ENT_QUOTES) ?></span></td>
                        <td class="px-3 py-2 text-right font-mono text-sm"><?= number_format((float) ($ce['edge_weight'] ?? 0), 1) ?></td>
                        <td class="px-3 py-2 text-right"><?= (int) ($ce['event_count'] ?? 0) ?></td>
                        <td class="px-3 py-2 text-right text-xs text-muted"><?= htmlspecialchars((string) ($ce['last_event_at'] ?? ''), ENT_QUOTES) ?></td>
                    </tr>
                <?php endforeach; ?>
                <?php if ($dossierCeSuppressed > 0): ?>
                    <tr class="border-b border-border/40"><td colspan="5" class="px-3 py-2 text-xs text-muted italic"><?= $dossierCeSuppressed ?> unresolved character<?= $dossierCeSuppressed !== 1 ? 's' : '' ?> suppressed</td></tr>
                <?php endif; ?>
                </tbody></table></div>
                <?php endif; ?>

                <!-- Temporal behavior analysis -->
                <?php if ($ciTemporalBehaviorSignals !== [] || $ciFeatureHistograms !== []): ?>
                <h3 class="mt-6 text-base font-semibold text-slate-100">Temporal behavior analysis</h3>
                <p class="mt-1 text-xs text-muted">Hour-of-day and day-of-week cadence profiling. Detects schedule shifts, bursty activity patterns, and reactivation after dormancy.</p>

                <?php if ($ciTemporalBehaviorSignals !== []): ?>
                <div class="mt-3 grid gap-3 md:grid-cols-2">
                    <?php foreach ($ciTemporalBehaviorSignals as $tbs):
                        $eKey = (string) ($tbs['evidence_key'] ?? '');
                        $eVal = (float) ($tbs['evidence_value'] ?? 0);
                        $zScore = (float) ($tbs['z_score'] ?? 0);
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
                            <?php if ($eKey === 'reactivation_after_dormancy' && is_array($payload) && ($payload['reactivated'] ?? false)): ?>
                                <p class="mt-1 text-[11px] text-yellow-400">Dormant for <?= number_format((float) ($payload['dormancy_days'] ?? 0), 0) ?> days, then <?= (int) ($payload['recent_burst_count'] ?? 0) ?> events in the last week</p>
                            <?php endif; ?>
                            <details class="mt-1"><summary class="text-[11px] text-muted cursor-pointer">Raw data</summary><pre class="mt-1 overflow-auto text-[11px] text-slate-400"><?= htmlspecialchars(json_encode($payload, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES) ?: '{}', ENT_QUOTES) ?></pre></details>
                        </div>
                    <?php endforeach; ?>
                </div>
                <?php endif; ?>

                <?php if ($ciFeatureHistograms !== []): ?>
                <div class="mt-4 grid gap-3 md:grid-cols-2">
                    <?php foreach ($ciFeatureHistograms as $fh):
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

                <!-- Community & network -->
                <?php if (is_array($ciCommunityInfo)): ?>
                <?php
                    $communitySize = (int) ($ciCommunityInfo['community_size'] ?? 0);
                    $pagerank = (float) ($ciCommunityInfo['pagerank_score'] ?? 0);
                    $betweenness = (float) ($ciCommunityInfo['betweenness_centrality'] ?? 0);
                    $degree = (int) ($ciCommunityInfo['degree_centrality'] ?? 0);
                    $isBridge = (bool) ((int) ($ciCommunityInfo['is_bridge'] ?? 0));
                ?>
                <h3 class="mt-6 text-base font-semibold text-slate-100">Community &amp; network</h3>
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

                <?php if ($ciCommunityMembers !== []): ?>
                <div class="mt-3 surface-tertiary">
                    <p class="text-xs text-muted mb-2">Community members (top <?= count($ciCommunityMembers) ?> of <?= $communitySize ?>)</p>
                    <div class="flex flex-wrap gap-2">
                        <?php foreach ($ciCommunityMembers as $cm):
                            $cmName = (string) ($cm['character_name'] ?? 'Unknown');
                            if (ci_is_placeholder_name($cmName)) { continue; }
                            $isCurrentChar = ((int) ($cm['character_id'] ?? 0)) === $characterId;
                            $memberBridge = (bool) ((int) ($cm['is_bridge'] ?? 0));
                        ?>
                            <a href="?character_id=<?= (int) ($cm['character_id'] ?? 0) ?>" class="inline-flex items-center gap-1 rounded-full px-2.5 py-1 text-xs <?= $isCurrentChar ? 'bg-cyan-900/60 text-cyan-300 font-semibold' : 'bg-slate-700/60 text-slate-300 hover:bg-slate-600/60' ?>">
                                <?= htmlspecialchars($cmName, ENT_QUOTES) ?>
                                <?php if ($memberBridge): ?>
                                    <span class="text-yellow-400" title="Bridge node">&#9670;</span>
                                <?php endif; ?>
                            </a>
                        <?php endforeach; ?>
                    </div>
                </div>
                <?php endif; ?>
                <?php endif; ?>

                <!-- Evidence paths -->
                <?php if ($ciEvidencePaths !== []): ?>
                <h3 class="mt-6 text-base font-semibold text-slate-100">Evidence paths</h3>
                <p class="mt-1 text-xs text-muted">Chains of connections linking this character to known suspicious actors.</p>
                <div class="mt-3 space-y-2">
                    <?php foreach ($ciEvidencePaths as $ep): ?>
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
                                    <?= ci_render_evidence_path($nodesJson, $edgesJson, $ciPathNodeNames) ?>
                                <?php else: ?>
                                    <span class="text-slate-100"><?= htmlspecialchars((string) ($ep['path_description'] ?? ''), ENT_QUOTES) ?></span>
                                <?php endif; ?>
                            </div>
                            <?= ci_progress_bar($pathScore, $pathScore >= 0.6 ? 'bg-red-500' : ($pathScore >= 0.4 ? 'bg-yellow-500' : 'bg-cyan-500')) ?>
                        </div>
                    <?php endforeach; ?>
                </div>
                <?php endif; ?>

                <!-- Analyst feedback -->
                <h3 class="mt-6 text-base font-semibold text-slate-100">Analyst feedback</h3>
                <?php if (isset($_GET['feedback']) && $_GET['feedback'] === 'saved'): ?>
                    <p class="mt-2 text-sm text-green-400">Feedback saved successfully.</p>
                <?php endif; ?>

                <?php if ($ciAnalystFeedback !== []): ?>
                <div class="mt-3 table-shell">
                    <table class="table-ui">
                        <thead><tr class="border-b border-border/70 text-xs text-muted uppercase">
                            <th class="px-3 py-2 text-left">Verdict</th>
                            <th class="px-3 py-2 text-right">Confidence</th>
                            <th class="px-3 py-2 text-left">Notes</th>
                            <th class="px-3 py-2 text-right">Date</th>
                        </tr></thead>
                        <tbody>
                        <?php foreach ($ciAnalystFeedback as $fb): ?>
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

            </div>
            <?php endif; ?>
        </div>
    <?php endif; ?>
</section>

<?php include __DIR__ . '/../../src/views/partials/footer.php'; ?>
