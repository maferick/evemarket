<?php

declare(strict_types=1);
require_once __DIR__ . '/../../src/bootstrap.php';

$allianceId = (int) ($_GET['alliance_id'] ?? 0);
if ($allianceId <= 0) {
    header('Location: /alliance-dossiers');
    exit;
}

$dossier = db_alliance_dossier($allianceId);
if ($dossier === null) {
    $title = 'Alliance Not Found';
    include __DIR__ . '/../../src/views/partials/header.php';
    echo '<section class="surface-primary"><p class="text-muted">No dossier found for alliance #' . $allianceId . '. The alliance may not have enough battle data, or the dossier job has not run yet.</p><a href="/alliance-dossiers" class="text-accent mt-3 inline-block">&larr; Back to Dossiers</a></section>';
    include __DIR__ . '/../../src/views/partials/footer.php';
    exit;
}

$allianceName = htmlspecialchars((string) ($dossier['ref_alliance_name'] ?? $dossier['alliance_name'] ?? 'Alliance #' . $allianceId), ENT_QUOTES);
$title = $allianceName . ' — Alliance Dossier';

$posture = (string) ($dossier['posture'] ?? 'unknown');
$postureColors = [
    'aggressive' => 'bg-red-900/60 text-red-300 ring-1 ring-red-400/30',
    'defensive' => 'bg-blue-900/60 text-blue-300 ring-1 ring-blue-400/30',
    'balanced' => 'bg-amber-900/60 text-amber-300 ring-1 ring-amber-400/30',
    'skirmish' => 'bg-purple-900/60 text-purple-300 ring-1 ring-purple-400/30',
];
$postureClass = $postureColors[$posture] ?? 'bg-slate-700/60 text-slate-300';

$coPresent = $dossier['top_co_present'] ?? [];
$enemies = $dossier['top_enemies'] ?? [];
$topRegions = $dossier['top_regions'] ?? [];
$topSystems = $dossier['top_systems'] ?? [];
$topShipClasses = $dossier['top_ship_classes'] ?? [];
$topShipTypes = $dossier['top_ship_types'] ?? [];
$behavior = $dossier['behavior_summary'] ?? [];
$trend = $dossier['trend_summary'] ?? [];

include __DIR__ . '/../../src/views/partials/header.php';
?>

<section class="surface-primary">
    <a href="/alliance-dossiers" class="text-sm text-accent">&larr; Back to Alliance Dossiers</a>

    <div class="mt-3 flex items-start gap-4">
        <img src="https://images.evetech.net/alliances/<?= $allianceId ?>/logo?size=128"
             alt="" class="w-16 h-16 rounded-lg" loading="lazy">
        <div class="flex-1">
            <p class="text-xs uppercase tracking-[0.16em] text-muted">Alliance Dossier</p>
            <h1 class="mt-1 text-2xl font-semibold text-slate-50"><?= $allianceName ?></h1>
            <div class="mt-2 flex flex-wrap items-center gap-3 text-sm">
                <span class="rounded-full px-2.5 py-0.5 text-[10px] uppercase tracking-wider <?= $postureClass ?>"><?= ucfirst($posture) ?> posture</span>
                <?php if ($dossier['primary_region_name']): ?>
                    <span class="text-muted">Primary region: <span class="text-slate-300"><?= htmlspecialchars($dossier['primary_region_name'], ENT_QUOTES) ?></span></span>
                <?php endif; ?>
                <?php if ($dossier['first_seen_at']): ?>
                    <span class="text-muted">First seen: <?= date('M j, Y', strtotime($dossier['first_seen_at'])) ?></span>
                <?php endif; ?>
            </div>
        </div>
    </div>

    <div class="mt-4 grid gap-3 sm:grid-cols-2 md:grid-cols-3 xl:grid-cols-6">
        <div class="surface-tertiary">
            <p class="text-xs text-muted">Total Battles</p>
            <p class="text-lg text-slate-50 font-semibold"><?= number_format((int) ($dossier['total_battles'] ?? 0)) ?></p>
        </div>
        <div class="surface-tertiary">
            <p class="text-xs text-muted">Recent Battles</p>
            <p class="text-lg text-slate-50 font-semibold"><?= number_format((int) ($dossier['recent_battles'] ?? 0)) ?></p>
        </div>
        <div class="surface-tertiary">
            <p class="text-xs text-muted">Engagement Rate</p>
            <p class="text-lg text-slate-50 font-semibold"><?= $dossier['avg_engagement_rate'] !== null ? number_format((float) $dossier['avg_engagement_rate'] * 100, 1) . '%' : '—' ?></p>
        </div>
        <div class="surface-tertiary">
            <p class="text-xs text-muted">Token Participation</p>
            <p class="text-lg text-slate-50 font-semibold"><?= $dossier['avg_token_participation'] !== null ? number_format((float) $dossier['avg_token_participation'] * 100, 1) . '%' : '—' ?></p>
        </div>
        <div class="surface-tertiary">
            <p class="text-xs text-muted">Overperformance</p>
            <p class="text-lg text-slate-50 font-semibold"><?= $dossier['avg_overperformance'] !== null ? number_format((float) $dossier['avg_overperformance'], 2) : '—' ?></p>
        </div>
        <div class="surface-tertiary">
            <p class="text-xs text-muted">Last Active</p>
            <p class="text-lg text-slate-50 font-semibold"><?= $dossier['last_seen_at'] ? date('M j', strtotime($dossier['last_seen_at'])) : '—' ?></p>
        </div>
    </div>
</section>

<div class="mt-4 grid gap-4 lg:grid-cols-2">
    <!-- Co-Present Alliances (Graph-derived) -->
    <section class="surface-primary">
        <h2 class="text-sm font-semibold text-slate-200 uppercase tracking-wider">Co-Present Alliances <span class="text-[10px] text-muted font-normal ml-1">via Neo4j</span></h2>
        <p class="mt-1 text-xs text-muted">Alliances most frequently appearing in the same battles. Higher co-occurrence suggests coalition alignment.</p>
        <?php if ($coPresent === []): ?>
            <p class="mt-3 text-sm text-muted">No co-presence data available.</p>
        <?php else: ?>
            <div class="mt-3 space-y-1.5">
                <?php foreach (array_slice($coPresent, 0, 10) as $cp): ?>
                    <div class="flex items-center justify-between rounded bg-slate-800/50 px-3 py-1.5">
                        <div class="flex items-center gap-2">
                            <img src="https://images.evetech.net/alliances/<?= (int) ($cp['alliance_id'] ?? 0) ?>/logo?size=32"
                                 alt="" class="w-4 h-4 rounded" loading="lazy">
                            <a href="/alliance-dossiers/view.php?alliance_id=<?= (int) ($cp['alliance_id'] ?? 0) ?>"
                               class="text-sm text-accent hover:underline"><?= htmlspecialchars((string) ($cp['alliance_name'] ?? 'Unknown'), ENT_QUOTES) ?></a>
                        </div>
                        <span class="text-xs text-muted"><?= (int) ($cp['shared_battles'] ?? $cp['count'] ?? 0) ?> shared battles</span>
                    </div>
                <?php endforeach; ?>
            </div>
        <?php endif; ?>
    </section>

    <!-- Top Enemies (Graph-derived) -->
    <section class="surface-primary">
        <h2 class="text-sm font-semibold text-slate-200 uppercase tracking-wider">Primary Enemies <span class="text-[10px] text-muted font-normal ml-1">via Neo4j</span></h2>
        <p class="mt-1 text-xs text-muted">Alliances most frequently fought against on opposing sides.</p>
        <?php if ($enemies === []): ?>
            <p class="mt-3 text-sm text-muted">No enemy data available.</p>
        <?php else: ?>
            <div class="mt-3 space-y-1.5">
                <?php foreach (array_slice($enemies, 0, 10) as $en): ?>
                    <div class="flex items-center justify-between rounded bg-slate-800/50 px-3 py-1.5">
                        <div class="flex items-center gap-2">
                            <img src="https://images.evetech.net/alliances/<?= (int) ($en['alliance_id'] ?? 0) ?>/logo?size=32"
                                 alt="" class="w-4 h-4 rounded" loading="lazy">
                            <a href="/alliance-dossiers/view.php?alliance_id=<?= (int) ($en['alliance_id'] ?? 0) ?>"
                               class="text-sm text-red-300 hover:underline"><?= htmlspecialchars((string) ($en['alliance_name'] ?? 'Unknown'), ENT_QUOTES) ?></a>
                        </div>
                        <span class="text-xs text-muted"><?= (int) ($en['engagements'] ?? $en['count'] ?? 0) ?> engagements</span>
                    </div>
                <?php endforeach; ?>
            </div>
        <?php endif; ?>
    </section>
</div>

<div class="mt-4 grid gap-4 lg:grid-cols-3">
    <!-- Geographic Presence -->
    <section class="surface-primary">
        <h2 class="text-sm font-semibold text-slate-200 uppercase tracking-wider">Geographic Presence</h2>
        <?php if ($topRegions === []): ?>
            <p class="mt-3 text-sm text-muted">No geographic data.</p>
        <?php else: ?>
            <div class="mt-3 space-y-1">
                <?php foreach (array_slice($topRegions, 0, 8) as $r): ?>
                    <div class="flex items-center justify-between text-sm">
                        <span class="text-slate-300"><?= htmlspecialchars((string) ($r['region_name'] ?? ''), ENT_QUOTES) ?></span>
                        <span class="text-xs text-muted"><?= (int) ($r['battle_count'] ?? 0) ?> battles</span>
                    </div>
                <?php endforeach; ?>
            </div>
        <?php endif; ?>

        <?php if ($topSystems !== []): ?>
            <h3 class="mt-4 text-xs font-semibold text-slate-400 uppercase tracking-wider">Top Systems</h3>
            <div class="mt-2 space-y-1">
                <?php foreach (array_slice($topSystems, 0, 6) as $s): ?>
                    <div class="flex items-center justify-between text-sm">
                        <span class="text-slate-300"><?= htmlspecialchars((string) ($s['system_name'] ?? ''), ENT_QUOTES) ?></span>
                        <span class="text-xs text-muted"><?= (int) ($s['battle_count'] ?? 0) ?></span>
                    </div>
                <?php endforeach; ?>
            </div>
        <?php endif; ?>
    </section>

    <!-- Fleet Composition -->
    <section class="surface-primary">
        <h2 class="text-sm font-semibold text-slate-200 uppercase tracking-wider">Fleet Composition</h2>
        <?php if ($topShipClasses === []): ?>
            <p class="mt-3 text-sm text-muted">No ship data.</p>
        <?php else: ?>
            <h3 class="mt-2 text-xs font-semibold text-slate-400 uppercase tracking-wider">Ship Classes</h3>
            <div class="mt-2 space-y-1">
                <?php foreach (array_slice($topShipClasses, 0, 6) as $sc): ?>
                    <div class="flex items-center justify-between text-sm">
                        <span class="text-slate-300"><?= htmlspecialchars((string) ($sc['ship_class'] ?? $sc['fleet_function'] ?? ''), ENT_QUOTES) ?></span>
                        <span class="text-xs text-muted"><?= (int) ($sc['count'] ?? 0) ?></span>
                    </div>
                <?php endforeach; ?>
            </div>
        <?php endif; ?>

        <?php if ($topShipTypes !== []): ?>
            <h3 class="mt-4 text-xs font-semibold text-slate-400 uppercase tracking-wider">Top Ship Types</h3>
            <div class="mt-2 space-y-1">
                <?php foreach (array_slice($topShipTypes, 0, 6) as $st): ?>
                    <div class="flex items-center justify-between text-sm">
                        <span class="text-slate-300"><?= htmlspecialchars((string) ($st['ship_name'] ?? $st['type_name'] ?? ''), ENT_QUOTES) ?></span>
                        <span class="text-xs text-muted"><?= (int) ($st['count'] ?? 0) ?></span>
                    </div>
                <?php endforeach; ?>
            </div>
        <?php endif; ?>
    </section>

    <!-- Behavior & Trends -->
    <section class="surface-primary">
        <h2 class="text-sm font-semibold text-slate-200 uppercase tracking-wider">Behavior Profile</h2>
        <?php if ($behavior !== []): ?>
            <div class="mt-3 space-y-2">
                <?php foreach ($behavior as $key => $val): ?>
                    <div class="flex items-center justify-between text-sm">
                        <span class="text-muted"><?= htmlspecialchars(ucfirst(str_replace('_', ' ', (string) $key)), ENT_QUOTES) ?></span>
                        <span class="text-slate-300"><?= is_numeric($val) ? number_format((float) $val, 2) : htmlspecialchars((string) $val, ENT_QUOTES) ?></span>
                    </div>
                <?php endforeach; ?>
            </div>
        <?php else: ?>
            <p class="mt-3 text-sm text-muted">No behavior data.</p>
        <?php endif; ?>

        <?php if ($trend !== []): ?>
            <h3 class="mt-4 text-xs font-semibold text-slate-400 uppercase tracking-wider">Activity Trend</h3>
            <div class="mt-2 space-y-1.5">
                <?php foreach ($trend as $window => $data): ?>
                    <?php
                        $battles = is_array($data) ? (int) ($data['battles'] ?? 0) : (int) $data;
                        $label = is_string($window) ? htmlspecialchars($window, ENT_QUOTES) : $window;
                    ?>
                    <div class="flex items-center justify-between text-sm">
                        <span class="text-muted"><?= $label ?></span>
                        <span class="text-slate-300"><?= number_format($battles) ?> battles</span>
                    </div>
                <?php endforeach; ?>
            </div>
        <?php endif; ?>
    </section>
</div>

<section class="surface-primary mt-4">
    <p class="text-xs text-muted">Dossier computed at <?= htmlspecialchars((string) ($dossier['computed_at'] ?? ''), ENT_QUOTES) ?>. Graph-derived metrics sourced from Neo4j co-occurrence and engagement analysis.</p>
</section>

<?php include __DIR__ . '/../../src/views/partials/footer.php'; ?>
