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

include __DIR__ . '/../../src/views/partials/header.php';
?>
<section class="surface-primary">
    <a href="/battle-intelligence" class="text-sm text-accent">← Back to leaderboard</a>
    <?php if (!is_array($character)): ?>
        <p class="mt-4 text-sm text-muted">No character intelligence found.</p>
    <?php else: ?>
        <h1 class="mt-2 text-2xl font-semibold text-slate-50"><?= htmlspecialchars((string) ($character['character_name'] ?? 'Unknown'), ENT_QUOTES) ?></h1>
        <div class="mt-4 grid gap-3 md:grid-cols-3">
            <div class="surface-tertiary"><p class="text-xs text-muted">Review priority</p><p class="mt-1 text-xl text-slate-100"><?= htmlspecialchars(number_format((float) ($character['review_priority_score'] ?? 0), 4), ENT_QUOTES) ?></p></div>
            <div class="surface-tertiary"><p class="text-xs text-muted">Confidence / Percentile</p><p class="mt-1 text-xl text-slate-100"><?= htmlspecialchars(number_format((float) ($character['confidence_score'] ?? 0), 3), ENT_QUOTES) ?> / <?= htmlspecialchars(number_format((float) ($character['percentile_rank'] ?? 0) * 100, 1), ENT_QUOTES) ?>%</p></div>
            <div class="surface-tertiary"><p class="text-xs text-muted">Repeatability / Evidence count</p><p class="mt-1 text-xl text-slate-100"><?= htmlspecialchars(number_format((float) ($character['repeatability_score'] ?? 0), 3), ENT_QUOTES) ?> / <?= (int) ($character['evidence_count'] ?? 0) ?></p></div>
        </div>
        <div class="mt-3 grid gap-3 md:grid-cols-3">
            <div class="surface-tertiary"><p class="text-xs text-muted">Enemy sustain / hull lift</p><p class="mt-1 text-base text-slate-100"><?= htmlspecialchars(number_format((float) ($character['enemy_sustain_lift'] ?? 0), 3), ENT_QUOTES) ?> / <?= htmlspecialchars(number_format((float) ($character['enemy_same_hull_survival_lift'] ?? 0), 3), ENT_QUOTES) ?></p></div>
            <div class="surface-tertiary"><p class="text-xs text-muted">Bridge / Co-presence density</p><p class="mt-1 text-base text-slate-100"><?= htmlspecialchars(number_format((float) ($character['graph_bridge_score'] ?? 0), 3), ENT_QUOTES) ?> / <?= htmlspecialchars(number_format((float) ($character['co_presence_anomalous_density'] ?? 0), 3), ENT_QUOTES) ?></p></div>
            <div class="surface-tertiary"><p class="text-xs text-muted">Corp hops / short-tenure ratio</p><p class="mt-1 text-base text-slate-100"><?= htmlspecialchars(number_format((float) ($character['corp_hop_frequency_180d'] ?? 0), 3), ENT_QUOTES) ?> / <?= htmlspecialchars(number_format((float) ($character['short_tenure_ratio_180d'] ?? 0), 3), ENT_QUOTES) ?></p></div>
        </div>

        <details class="mt-4 surface-tertiary"><summary class="cursor-pointer text-sm text-slate-100">Org history cache context</summary><pre class="mt-3 overflow-auto text-xs text-slate-300"><?= htmlspecialchars(json_encode(['source' => (string) ($character['org_history_source'] ?? ''), 'fetched_at' => (string) ($character['org_history_fetched_at'] ?? ''), 'corp_hops_180d' => (int) ($character['corp_hops_180d'] ?? 0), 'short_tenure_hops_180d' => (int) ($character['short_tenure_hops_180d'] ?? 0), 'hostile_adjacent_hops_180d' => (int) ($character['hostile_adjacent_hops_180d'] ?? 0), 'history' => $orgHistory], JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES) ?: '{}', ENT_QUOTES) ?></pre></details>

        <h2 class="mt-6 text-lg font-semibold text-slate-100">Evidence rows</h2>
        <div class="mt-3 table-shell"><table class="table-ui"><thead><tr class="border-b border-border/70 text-xs text-muted uppercase"><th class="px-3 py-2 text-left">Key</th><th class="px-3 py-2 text-right">Value</th><th class="px-3 py-2 text-left">Details</th></tr></thead><tbody><?php foreach ($evidence as $evidenceRow): ?><tr class="border-b border-border/40"><td class="px-3 py-2 font-mono text-xs"><?= htmlspecialchars((string) ($evidenceRow['evidence_key'] ?? ''), ENT_QUOTES) ?></td><td class="px-3 py-2 text-right"><?= htmlspecialchars(number_format((float) ($evidenceRow['evidence_value'] ?? 0), 4), ENT_QUOTES) ?></td><td class="px-3 py-2"><div><?= htmlspecialchars((string) ($evidenceRow['evidence_text'] ?? ''), ENT_QUOTES) ?></div><?php if (is_array($evidenceRow['evidence_payload'] ?? null)): ?><pre class="mt-2 overflow-auto text-[11px] text-slate-300"><?= htmlspecialchars(json_encode($evidenceRow['evidence_payload'], JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES) ?: '{}', ENT_QUOTES) ?></pre><?php endif; ?></td></tr><?php endforeach; ?></tbody></table></div>

        <h2 class="mt-6 text-lg font-semibold text-slate-100">Supporting battles</h2>
        <div class="mt-3 table-shell"><table class="table-ui"><thead><tr class="border-b border-border/70 text-xs text-muted uppercase"><th class="px-3 py-2 text-left">Battle</th><th class="px-3 py-2 text-left">Side</th><th class="px-3 py-2 text-left">Class</th><th class="px-3 py-2 text-right">Overperf.</th><th class="px-3 py-2 text-right">Inspect</th></tr></thead><tbody><?php foreach ($battles as $battle): ?><tr class="border-b border-border/40"><td class="px-3 py-2"><?= htmlspecialchars((string) ($battle['system_name'] ?? 'Unknown'), ENT_QUOTES) ?><div class="text-xs text-muted"><?= htmlspecialchars((string) ($battle['started_at'] ?? ''), ENT_QUOTES) ?></div></td><td class="px-3 py-2"><?= htmlspecialchars((string) ($battle['side_name'] ?? $battle['side_key'] ?? 'unknown'), ENT_QUOTES) ?></td><td class="px-3 py-2"><?= htmlspecialchars((string) ($battle['anomaly_class'] ?? 'normal'), ENT_QUOTES) ?></td><td class="px-3 py-2 text-right"><?= htmlspecialchars(number_format((float) ($battle['overperformance_score'] ?? 0), 3), ENT_QUOTES) ?></td><td class="px-3 py-2 text-right"><a class="text-accent" href="/battle-intelligence/battle.php?battle_id=<?= urlencode((string) ($battle['battle_id'] ?? '')) ?>">Battle</a></td></tr><?php endforeach; ?></tbody></table></div>

        <?php if ($temporalMetrics !== []): ?>
        <h2 class="mt-6 text-lg font-semibold text-slate-100">Temporal drift (rolling windows)</h2>
        <div class="mt-3 grid gap-3 md:grid-cols-3">
            <?php foreach ($temporalMetrics as $tm): ?>
                <div class="surface-tertiary">
                    <p class="text-xs text-muted font-semibold"><?= htmlspecialchars((string) ($tm['window_label'] ?? ''), ENT_QUOTES) ?> window</p>
                    <div class="mt-2 grid grid-cols-2 gap-1 text-xs">
                        <span class="text-muted">Battles</span><span class="text-slate-100 text-right"><?= (int) ($tm['battles_present'] ?? 0) ?></span>
                        <span class="text-muted">Kills</span><span class="text-slate-100 text-right"><?= (int) ($tm['kills_total'] ?? 0) ?></span>
                        <span class="text-muted">Losses</span><span class="text-slate-100 text-right"><?= (int) ($tm['losses_total'] ?? 0) ?></span>
                        <span class="text-muted">Suspicion</span><span class="text-slate-100 text-right"><?= number_format((float) ($tm['suspicion_score'] ?? 0), 4) ?></span>
                        <span class="text-muted">Co-presence</span><span class="text-slate-100 text-right"><?= number_format((float) ($tm['co_presence_density'] ?? 0), 4) ?></span>
                        <span class="text-muted">Engage rate</span><span class="text-slate-100 text-right"><?= number_format((float) ($tm['engagement_rate_avg'] ?? 0), 4) ?></span>
                    </div>
                </div>
            <?php endforeach; ?>
        </div>
        <?php endif; ?>

        <?php if (is_array($communityInfo)): ?>
        <h2 class="mt-6 text-lg font-semibold text-slate-100">Community assignment</h2>
        <div class="mt-3 grid gap-3 md:grid-cols-4">
            <div class="surface-tertiary"><p class="text-xs text-muted">Community</p><p class="mt-1 text-lg text-slate-100">#<?= (int) ($communityInfo['community_id'] ?? 0) ?> <span class="text-xs text-muted">(<?= (int) ($communityInfo['community_size'] ?? 0) ?> members)</span></p></div>
            <div class="surface-tertiary"><p class="text-xs text-muted">PageRank</p><p class="mt-1 text-lg text-slate-100"><?= number_format((float) ($communityInfo['pagerank_score'] ?? 0), 6) ?></p></div>
            <div class="surface-tertiary"><p class="text-xs text-muted">Betweenness</p><p class="mt-1 text-lg text-slate-100"><?= number_format((float) ($communityInfo['betweenness_centrality'] ?? 0), 6) ?></p></div>
            <div class="surface-tertiary"><p class="text-xs text-muted">Degree / Bridge</p><p class="mt-1 text-lg text-slate-100"><?= (int) ($communityInfo['degree_centrality'] ?? 0) ?> / <?= ((int) ($communityInfo['is_bridge'] ?? 0)) ? '<span class="text-yellow-400">Yes</span>' : 'No' ?></p></div>
        </div>
        <?php endif; ?>

        <?php if ($typedInteractions !== []): ?>
        <h2 class="mt-6 text-lg font-semibold text-slate-100">Typed interactions</h2>
        <div class="mt-3 table-shell">
            <table class="table-ui">
                <thead><tr class="border-b border-border/70 text-xs text-muted uppercase">
                    <th class="px-3 py-2 text-left">Character</th>
                    <th class="px-3 py-2 text-left">Type</th>
                    <th class="px-3 py-2 text-right">Count</th>
                    <th class="px-3 py-2 text-right">Last interaction</th>
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
                        ?>"><?= htmlspecialchars($type, ENT_QUOTES) ?></span></td>
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
        <p class="mt-1 text-xs text-muted">Top explainable paths linking this character to suspicious patterns.</p>
        <div class="mt-3 space-y-2">
            <?php foreach ($evidencePaths as $ep): ?>
                <div class="surface-tertiary">
                    <div class="flex items-center justify-between">
                        <span class="text-xs text-muted">Path #<?= (int) ($ep['path_rank'] ?? 0) ?></span>
                        <span class="text-xs text-slate-300">Score: <?= number_format((float) ($ep['path_score'] ?? 0), 4) ?></span>
                    </div>
                    <p class="mt-1 text-sm text-slate-100"><?= htmlspecialchars((string) ($ep['path_description'] ?? ''), ENT_QUOTES) ?></p>
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
                    <th class="px-3 py-2 text-left">Label</th>
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
                        ?>"><?= htmlspecialchars($lbl, ENT_QUOTES) ?></span></td>
                        <td class="px-3 py-2 text-right"><?= number_format((float) ($fb['confidence'] ?? 0), 3) ?></td>
                        <td class="px-3 py-2 text-xs"><?= htmlspecialchars((string) ($fb['analyst_notes'] ?? ''), ENT_QUOTES) ?></td>
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
