<?php

declare(strict_types=1);

require_once __DIR__ . '/../../src/bootstrap.php';

$communityId = max(0, (int) ($_GET['community_id'] ?? 0));

if ($communityId === 0) {
    http_response_code(400);
    exit('Missing community_id');
}

$members = db_graph_community_top_members($communityId, 200);

if ($members === []) {
    http_response_code(404);
    exit('Community not found');
}

// Derive display name from the top-ranked member (first row, ordered by pagerank DESC)
$topMemberName = (string) ($members[0]['character_name'] ?? '');
$communityLabel = $topMemberName !== '' ? $topMemberName . "'s cluster" : 'Community #' . $communityId;

$title = $communityLabel;

$memberCount = count($members);
$bridges     = array_filter($members, static fn(array $m): bool => (bool) ($m['is_bridge'] ?? false));
$bridgeCount = count($bridges);

// Fetch counterintel suspicion scores for all community members
$charIds = array_map(static fn(array $m): int => (int) ($m['character_id'] ?? 0), $members);
$counterintel = db_counterintel_scores_for_characters($charIds);

// Identify flagged members (any non-zero review_priority_score)
$flaggedMembers = [];
foreach ($members as $m) {
    $cid = (int) ($m['character_id'] ?? 0);
    $score = (float) ($counterintel[$cid]['review_priority_score'] ?? 0);
    if ($score > 0) {
        $flaggedMembers[] = array_merge($m, ['priority_score' => $score, 'percentile_rank' => (float) ($counterintel[$cid]['percentile_rank'] ?? 0)]);
    }
}
usort($flaggedMembers, static fn(array $a, array $b): int => $b['priority_score'] <=> $a['priority_score']);

include __DIR__ . '/../../src/views/partials/header.php';
?>
<section class="surface-primary">
    <div class="flex items-center justify-between gap-4">
        <div>
            <p class="text-xs uppercase tracking-[0.16em] text-muted">Battle intelligence &rsaquo; Community</p>
            <h1 class="mt-1 text-2xl font-semibold text-slate-50"><?= htmlspecialchars($communityLabel, ENT_QUOTES) ?></h1>
            <p class="mt-2 text-xs text-muted">Community #<?= $communityId ?> &middot; <?= $memberCount ?> members detected via label-propagation</p>
        </div>
        <a href="/battle-intelligence/" class="btn-secondary">&larr; Back to leaderboard</a>
    </div>

    <?php if ($flaggedMembers !== []): ?>
    <div class="mt-5 surface-tertiary">
        <p class="text-xs uppercase tracking-[0.16em] text-red-400">Flagged members</p>
        <p class="mt-1 text-xs text-muted"><?= count($flaggedMembers) ?> member<?= count($flaggedMembers) !== 1 ? 's' : '' ?> in this cluster also appear on the suspicion leaderboard.</p>
        <div class="mt-3 flex flex-col gap-2">
            <?php foreach ($flaggedMembers as $fm): ?>
                <?php
                $fmScore = (float) $fm['priority_score'];
                if ($fmScore > 0.15) { $fmLabel = 'CRITICAL'; $fmClass = 'bg-red-900/60 text-red-300 border border-red-800/50'; }
                elseif ($fmScore > 0.05) { $fmLabel = 'HIGH'; $fmClass = 'bg-orange-900/60 text-orange-300 border border-orange-800/50'; }
                elseif ($fmScore > 0.01) { $fmLabel = 'ELEVATED'; $fmClass = 'bg-amber-900/60 text-amber-300 border border-amber-800/50'; }
                else { $fmLabel = 'WATCH'; $fmClass = 'bg-yellow-900/60 text-yellow-400 border border-yellow-800/50'; }
                $fmPct = (float) $fm['percentile_rank'] * 100;
                ?>
                <div class="flex items-center gap-3">
                    <span class="inline-flex items-center rounded px-2 py-0.5 text-xs font-medium <?= $fmClass ?>"><?= $fmLabel ?></span>
                    <a class="text-accent hover:underline" href="/battle-intelligence/character.php?character_id=<?= urlencode((string) ((int) $fm['character_id'])) ?>"><?= htmlspecialchars((string) $fm['character_name'], ENT_QUOTES) ?></a>
                    <span class="text-xs text-muted"><?= number_format($fmPct, 1) ?>% percentile</span>
                </div>
            <?php endforeach; ?>
        </div>
    </div>
    <?php endif; ?>

    <?php if ($bridgeCount > 0): ?>
    <div class="mt-4 surface-tertiary">
        <p class="text-xs uppercase tracking-[0.16em] text-amber-400">Bridge characters</p>
        <p class="mt-1 text-xs text-muted">These <?= $bridgeCount ?> character<?= $bridgeCount !== 1 ? 's connect' : ' connects' ?> otherwise separate groups in the network &mdash; they may relay intel between sides or act as cross-corp intermediaries.</p>
        <div class="mt-3 flex flex-wrap gap-2">
            <?php foreach ($bridges as $bridge): ?>
                <a class="inline-flex items-center gap-1.5 rounded-full bg-amber-900/40 border border-amber-800/40 px-3 py-1 text-sm text-amber-200 hover:bg-amber-900/60 transition-colors"
                   href="/battle-intelligence/character.php?character_id=<?= urlencode((string) ((int) ($bridge['character_id'] ?? 0))) ?>">
                    <?= htmlspecialchars((string) ($bridge['character_name'] ?? 'Unknown'), ENT_QUOTES) ?>
                    <span class="text-[10px] text-amber-400/60"><?= (int) ($bridge['degree_centrality'] ?? 0) ?> connections</span>
                </a>
            <?php endforeach; ?>
        </div>
    </div>
    <?php endif; ?>

    <h2 class="mt-6 text-lg font-semibold text-slate-100">All members</h2>
    <p class="mt-1 text-xs text-muted">Sorted by influence (most central first). Click any name to see their full intelligence profile.</p>
    <div class="mt-3 table-shell">
        <table class="table-ui">
            <thead>
            <tr class="border-b border-border/70 text-xs uppercase tracking-[0.15em] text-muted">
                <th class="px-3 py-2 text-left">Character</th>
                <th class="px-3 py-2 text-left">Role</th>
                <th class="px-3 py-2 text-right" title="Number of direct connections to other characters in the graph">Connections</th>
                <th class="px-3 py-2 text-right" title="Suspicion status from the counterintel pipeline — only shown if this character has been scored">Suspicion</th>
                <th class="px-3 py-2 text-right">Inspect</th>
            </tr>
            </thead>
            <tbody>
            <?php foreach ($members as $idx => $member): ?>
                <?php
                $pagerank    = (float) ($member['pagerank_score'] ?? 0);
                $betweenness = (float) ($member['betweenness_centrality'] ?? 0);
                $degree      = (int)   ($member['degree_centrality'] ?? 0);
                $isBridge    = (bool)  ($member['is_bridge'] ?? false);
                $charId      = (int)   ($member['character_id'] ?? 0);
                $charName    = (string) ($member['character_name'] ?? 'Unknown');

                // Determine role badges
                $roles = [];
                if ($idx === 0) {
                    $roles[] = ['label' => 'Leader', 'class' => 'bg-sky-900/60 text-sky-300 border border-sky-800/50'];
                } elseif ($pagerank >= 1.3) {
                    $roles[] = ['label' => 'Key member', 'class' => 'bg-indigo-900/60 text-indigo-300 border border-indigo-800/50'];
                }
                if ($isBridge) {
                    $roles[] = ['label' => 'Bridge', 'class' => 'bg-amber-900/60 text-amber-300 border border-amber-800/50'];
                }
                if ($betweenness > 0.01) {
                    $roles[] = ['label' => 'Relay', 'class' => 'bg-purple-900/60 text-purple-300 border border-purple-800/50'];
                }

                // Suspicion from counterintel
                $ciScore = (float) ($counterintel[$charId]['review_priority_score'] ?? 0);
                if ($ciScore > 0.15) { $ciLabel = 'CRITICAL'; $ciClass = 'text-red-400'; }
                elseif ($ciScore > 0.05) { $ciLabel = 'HIGH'; $ciClass = 'text-orange-400'; }
                elseif ($ciScore > 0.01) { $ciLabel = 'ELEVATED'; $ciClass = 'text-amber-400'; }
                elseif ($ciScore > 0) { $ciLabel = 'WATCH'; $ciClass = 'text-yellow-400'; }
                else { $ciLabel = null; $ciClass = ''; }
                ?>
                <tr class="border-b border-border/50">
                    <td class="px-3 py-2 text-slate-100"><?= htmlspecialchars($charName, ENT_QUOTES) ?></td>
                    <td class="px-3 py-2">
                        <?php if ($roles !== []): ?>
                            <div class="flex flex-wrap gap-1">
                                <?php foreach ($roles as $role): ?>
                                    <span class="inline-block rounded-full px-2 py-0.5 text-[10px] uppercase tracking-wider font-medium <?= $role['class'] ?>"><?= $role['label'] ?></span>
                                <?php endforeach; ?>
                            </div>
                        <?php else: ?>
                            <span class="text-muted text-xs">Member</span>
                        <?php endif; ?>
                    </td>
                    <td class="px-3 py-2 text-right"><?= $degree ?></td>
                    <td class="px-3 py-2 text-right">
                        <?php if ($ciLabel !== null): ?>
                            <span class="text-xs font-medium <?= $ciClass ?>"><?= $ciLabel ?></span>
                        <?php else: ?>
                            <span class="text-muted text-xs">—</span>
                        <?php endif; ?>
                    </td>
                    <td class="px-3 py-2 text-right">
                        <a class="text-accent" href="/battle-intelligence/character.php?character_id=<?= urlencode((string) $charId) ?>">Drilldown</a>
                    </td>
                </tr>
            <?php endforeach; ?>
            </tbody>
        </table>
    </div>
</section>

<?php include __DIR__ . '/../../src/views/partials/footer.php'; ?>
