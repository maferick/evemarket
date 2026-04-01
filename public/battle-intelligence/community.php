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

$memberCount  = count($members);
$bridgeCount  = count(array_filter($members, static fn(array $m): bool => (bool) ($m['is_bridge'] ?? false)));
$avgPagerank  = $memberCount > 0 ? array_sum(array_column($members, 'pagerank_score')) / $memberCount : 0.0;
$maxPagerank  = $memberCount > 0 ? max(array_column($members, 'pagerank_score')) : 0.0;

include __DIR__ . '/../../src/views/partials/header.php';
?>
<section class="surface-primary">
    <div class="flex items-center justify-between gap-4">
        <div>
            <p class="text-xs uppercase tracking-[0.16em] text-muted">Battle intelligence &rsaquo; Community</p>
            <h1 class="mt-1 text-2xl font-semibold text-slate-50"><?= htmlspecialchars($communityLabel, ENT_QUOTES) ?></h1>
            <p class="mt-2 text-xs text-muted">Community ID #<?= $communityId ?> &middot; label-propagation detection</p>
        </div>
        <a href="/battle-intelligence/" class="btn-secondary">&larr; Back to leaderboard</a>
    </div>

    <div class="mt-5 grid gap-3 md:grid-cols-4">
        <div class="surface-tertiary">
            <p class="text-xs text-muted">Members</p>
            <p class="mt-1 text-xl text-slate-100"><?= $memberCount ?></p>
        </div>
        <div class="surface-tertiary">
            <p class="text-xs text-muted" title="Characters that act as connectors between otherwise separate groups — high risk for intel relay">Bridges</p>
            <p class="mt-1 text-xl <?= $bridgeCount > 0 ? 'text-amber-400' : 'text-slate-100' ?>"><?= $bridgeCount ?></p>
        </div>
        <div class="surface-tertiary">
            <p class="text-xs text-muted" title="Average PageRank — how interconnected members are on average">Avg influence</p>
            <p class="mt-1 text-xl text-slate-100"><?= number_format($avgPagerank, 4) ?></p>
        </div>
        <div class="surface-tertiary">
            <p class="text-xs text-muted" title="PageRank of the most central member in this community">Top influence</p>
            <p class="mt-1 text-xl text-slate-100"><?= number_format($maxPagerank, 4) ?></p>
        </div>
    </div>

    <div class="mt-5 table-shell">
        <table class="table-ui">
            <thead>
            <tr class="border-b border-border/70 text-xs uppercase tracking-[0.15em] text-muted">
                <th class="px-3 py-2 text-left">Character</th>
                <th class="px-3 py-2 text-right" title="PageRank score — higher means more central and influential within this community">Influence</th>
                <th class="px-3 py-2 text-right" title="How many shortest paths between other members pass through this character — high values suggest an intel relay role">Betweenness</th>
                <th class="px-3 py-2 text-right" title="Number of direct connections to other characters in the graph">Connections</th>
                <th class="px-3 py-2 text-right" title="Bridge characters connect otherwise separate groups — elevated cross-side risk">Bridge</th>
                <th class="px-3 py-2 text-right">Inspect</th>
            </tr>
            </thead>
            <tbody>
            <?php foreach ($members as $member): ?>
                <?php
                $pagerank    = (float) ($member['pagerank_score'] ?? 0);
                $betweenness = (float) ($member['betweenness_centrality'] ?? 0);
                $degree      = (int)   ($member['degree_centrality'] ?? 0);
                $isBridge    = (bool)  ($member['is_bridge'] ?? false);
                $charId      = (int)   ($member['character_id'] ?? 0);
                $charName    = (string) ($member['character_name'] ?? 'Unknown');

                $pagerankClass   = $pagerank >= 1.5 ? 'text-orange-400 font-semibold' : ($pagerank >= 1.1 ? 'text-amber-400' : 'text-slate-300');
                $betweennessDisp = $betweenness > 0 ? number_format($betweenness, 4) : '<span class="text-muted">—</span>';
                ?>
                <tr class="border-b border-border/50">
                    <td class="px-3 py-2 text-slate-100"><?= htmlspecialchars($charName, ENT_QUOTES) ?></td>
                    <td class="px-3 py-2 text-right"><span class="<?= $pagerankClass ?>"><?= number_format($pagerank, 4) ?></span></td>
                    <td class="px-3 py-2 text-right"><?= $betweennessDisp ?></td>
                    <td class="px-3 py-2 text-right"><?= $degree ?></td>
                    <td class="px-3 py-2 text-right">
                        <?php if ($isBridge): ?>
                            <span class="inline-block rounded-full bg-amber-900/60 border border-amber-800/50 px-2 py-0.5 text-[10px] uppercase tracking-wider text-amber-300">Yes</span>
                        <?php else: ?>
                            <span class="text-muted">—</span>
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
