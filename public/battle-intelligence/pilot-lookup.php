<?php

declare(strict_types=1);
require_once __DIR__ . '/../../src/bootstrap.php';

$title = 'Pilot Lookup';
$searchQuery = trim((string) ($_GET['q'] ?? ''));
$characterId = max(0, (int) ($_GET['character_id'] ?? 0));

$searchResults = [];
$profile = null;

if ($characterId > 0) {
    $profile = db_pilot_profile($characterId);
    if ($profile !== null) {
        $title = htmlspecialchars((string) ($profile['character']['character_name'] ?? 'Unknown'), ENT_QUOTES) . ' - Pilot Lookup';
    }
} elseif ($searchQuery !== '' && mb_strlen($searchQuery) >= 2) {
    $searchResults = db_pilot_search($searchQuery);
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
        $ships = $profile['ships'];
        $suspicion = $profile['suspicion'];
        $theaterHistory = $profile['theater_history'];
        $associates = $profile['associates'];
        $totalKills = (int) ($stats['total_kills'] ?? 0);
        $totalDeaths = (int) ($stats['total_deaths'] ?? 0);
        $totalBattles = (int) ($stats['total_battles'] ?? 0);
        $kd = $totalDeaths > 0 ? round($totalKills / $totalDeaths, 1) : $totalKills;
    ?>
        <!-- Pilot profile -->
        <div class="mt-6">
            <a href="?q=<?= urlencode($searchQuery) ?>" class="text-sm text-accent">&larr; Back to search</a>

            <div class="mt-3 flex items-center gap-4">
                <img src="https://images.evetech.net/characters/<?= (int) ($char['character_id'] ?? 0) ?>/portrait?size=64"
                     alt="" class="w-16 h-16 rounded" loading="lazy">
                <div>
                    <h2 class="text-2xl font-semibold text-slate-50"><?= htmlspecialchars((string) ($char['character_name'] ?? 'Unknown'), ENT_QUOTES) ?></h2>
                    <p class="text-sm text-slate-300">
                        <?= htmlspecialchars($profile['corporation_name'] ?: '-', ENT_QUOTES) ?>
                        <?php if ($profile['alliance_name'] !== ''): ?>
                            &middot; <?= htmlspecialchars($profile['alliance_name'], ENT_QUOTES) ?>
                        <?php endif; ?>
                    </p>
                    <span class="inline-block mt-1 rounded-full px-2 py-0.5 text-[10px] uppercase tracking-wider <?= fleet_function_color_class($profile['fleet_function']) ?>">
                        <?= htmlspecialchars(fleet_function_label($profile['fleet_function']), ENT_QUOTES) ?>
                    </span>
                </div>
            </div>

            <!-- Quick stats -->
            <div class="mt-4 grid gap-3 md:grid-cols-6">
                <div class="surface-tertiary">
                    <p class="text-xs text-muted">Theaters</p>
                    <p class="text-lg text-slate-50 font-semibold"><?= (int) ($stats['theater_count'] ?? 0) ?></p>
                </div>
                <div class="surface-tertiary">
                    <p class="text-xs text-muted">Battles</p>
                    <p class="text-lg text-slate-50 font-semibold"><?= number_format($totalBattles) ?></p>
                </div>
                <div class="surface-tertiary">
                    <p class="text-xs text-muted">Kills</p>
                    <p class="text-lg text-slate-50 font-semibold"><?= number_format($totalKills) ?></p>
                </div>
                <div class="surface-tertiary">
                    <p class="text-xs text-muted">Deaths</p>
                    <p class="text-lg text-slate-50 font-semibold"><?= number_format($totalDeaths) ?></p>
                </div>
                <div class="surface-tertiary">
                    <p class="text-xs text-muted">K/D Ratio</p>
                    <p class="text-lg font-semibold <?= $kd >= 2 ? 'text-green-400' : ($kd < 1 ? 'text-red-400' : 'text-slate-50') ?>"><?= number_format($kd, 1) ?></p>
                </div>
                <div class="surface-tertiary">
                    <p class="text-xs text-muted">Total Damage</p>
                    <p class="text-lg text-slate-50 font-semibold"><?= number_format((float) ($stats['total_damage_done'] ?? 0), 0) ?></p>
                </div>
            </div>

            <!-- Ships flown -->
            <?php if ($ships !== []): ?>
            <h3 class="mt-6 text-lg font-semibold text-slate-100">Ships Flown</h3>
            <div class="mt-3 grid gap-2 md:grid-cols-5">
                <?php foreach ($ships as $ship): ?>
                    <div class="surface-tertiary flex items-center gap-3">
                        <img src="https://images.evetech.net/types/<?= (int) $ship['type_id'] ?>/icon?size=32"
                             alt="" class="w-8 h-8 rounded" loading="lazy">
                        <div>
                            <p class="text-sm text-slate-100"><?= htmlspecialchars($ship['type_name'], ENT_QUOTES) ?></p>
                            <p class="text-xs text-muted"><?= htmlspecialchars(killmail_ship_class_label($ship['group_id'] ?: null), ENT_QUOTES) ?> &middot; <?= $ship['times_flown'] ?>x</p>
                        </div>
                    </div>
                <?php endforeach; ?>
            </div>
            <?php endif; ?>

            <!-- Suspicion signals -->
            <?php if (is_array($suspicion)): ?>
            <h3 class="mt-6 text-lg font-semibold text-slate-100">Intelligence Signals</h3>
            <?php
                $flags = json_decode((string) ($suspicion['suspicion_flags'] ?? '[]'), true);
                $flags = is_array($flags) ? $flags : [];
            ?>
            <div class="mt-3 grid gap-3 md:grid-cols-4">
                <div class="surface-tertiary">
                    <p class="text-xs text-muted">Suspicion Score</p>
                    <?php $ss = (float) ($suspicion['suspicion_score'] ?? 0); ?>
                    <p class="text-lg font-semibold <?= $ss > 0.5 ? 'text-red-400' : ($ss > 0.2 ? 'text-yellow-400' : 'text-slate-50') ?>"><?= number_format($ss, 4) ?></p>
                </div>
                <div class="surface-tertiary">
                    <p class="text-xs text-muted">Fleet Function</p>
                    <p class="text-lg text-slate-50"><?= htmlspecialchars(fleet_function_label((string) ($suspicion['primary_fleet_function'] ?? 'mainline_dps')), ENT_QUOTES) ?></p>
                </div>
                <div class="surface-tertiary">
                    <p class="text-xs text-muted">Overlap Score</p>
                    <p class="text-lg text-slate-50"><?= number_format((float) ($suspicion['historical_overlap_score'] ?? 0), 4) ?></p>
                </div>
                <div class="surface-tertiary">
                    <p class="text-xs text-muted">Combined Risk</p>
                    <?php $cr = (float) ($suspicion['combined_risk_score'] ?? 0); ?>
                    <p class="text-lg font-semibold <?= $cr > 0.5 ? 'text-red-400' : ($cr > 0.2 ? 'text-yellow-400' : 'text-slate-50') ?>"><?= number_format($cr, 4) ?></p>
                </div>
            </div>
            <?php if ($flags !== []): ?>
                <div class="mt-2 flex flex-wrap gap-1">
                    <?php foreach ($flags as $flag): ?>
                        <span class="inline-block rounded-full px-2.5 py-0.5 text-[10px] uppercase tracking-wider bg-red-900/40 text-red-300">
                            <?= htmlspecialchars(str_replace('_', ' ', (string) $flag), ENT_QUOTES) ?>
                        </span>
                    <?php endforeach; ?>
                </div>
            <?php endif; ?>
            <div class="mt-3 grid gap-3 md:grid-cols-3">
                <div class="surface-tertiary">
                    <p class="text-xs text-muted">Selective Non-Engagement</p>
                    <p class="text-base text-slate-100"><?= number_format((float) ($suspicion['selective_non_engagement_score'] ?? 0), 4) ?></p>
                </div>
                <div class="surface-tertiary">
                    <p class="text-xs text-muted">High Presence / Low Output</p>
                    <p class="text-base text-slate-100"><?= number_format((float) ($suspicion['high_presence_low_output_score'] ?? 0), 4) ?></p>
                </div>
                <div class="surface-tertiary">
                    <p class="text-xs text-muted">Token Participation</p>
                    <p class="text-base text-slate-100"><?= number_format((float) ($suspicion['token_participation_score'] ?? 0), 4) ?></p>
                </div>
                <div class="surface-tertiary">
                    <p class="text-xs text-muted">Loss Without Attack</p>
                    <p class="text-base text-slate-100"><?= number_format((float) ($suspicion['loss_without_attack_ratio'] ?? 0), 4) ?></p>
                </div>
                <div class="surface-tertiary">
                    <p class="text-xs text-muted">Peer Kill Delta</p>
                    <?php $pkd = (float) ($suspicion['peer_normalized_kills_delta'] ?? 0); ?>
                    <p class="text-base <?= $pkd < -0.4 ? 'text-red-400' : ($pkd > 0.4 ? 'text-green-400' : 'text-slate-100') ?>"><?= number_format($pkd, 4) ?></p>
                </div>
                <div class="surface-tertiary">
                    <p class="text-xs text-muted">Peer Damage Delta</p>
                    <?php $pdd = (float) ($suspicion['peer_normalized_damage_delta'] ?? 0); ?>
                    <p class="text-base <?= $pdd < -0.4 ? 'text-red-400' : ($pdd > 0.4 ? 'text-green-400' : 'text-slate-100') ?>"><?= number_format($pdd, 4) ?></p>
                </div>
            </div>
            <?php if ((int) ($suspicion['character_id'] ?? 0) > 0): ?>
                <p class="mt-2"><a href="/battle-intelligence/character.php?character_id=<?= (int) $suspicion['character_id'] ?>" class="text-accent text-sm">Full counterintel dossier &rarr;</a></p>
            <?php endif; ?>
            <?php endif; ?>

            <!-- Theater history -->
            <?php if ($theaterHistory !== []): ?>
            <h3 class="mt-6 text-lg font-semibold text-slate-100">Theater History</h3>
            <div class="mt-3 table-shell">
                <table class="table-ui">
                    <thead>
                        <tr class="border-b border-border/70 text-xs text-muted uppercase">
                            <th class="px-3 py-2 text-left">Theater</th>
                            <th class="px-3 py-2 text-left">Alliance</th>
                            <th class="px-3 py-2 text-left">Role</th>
                            <th class="px-3 py-2 text-right">Kills</th>
                            <th class="px-3 py-2 text-right">Deaths</th>
                            <th class="px-3 py-2 text-right">Damage</th>
                            <th class="px-3 py-2 text-right">Suspicion</th>
                            <th class="px-3 py-2 text-right"></th>
                        </tr>
                    </thead>
                    <tbody>
                    <?php foreach ($theaterHistory as $th): ?>
                        <?php $thSusp = (float) ($th['suspicion_score'] ?? 0); ?>
                        <tr class="border-b border-border/40">
                            <td class="px-3 py-2">
                                <span class="text-slate-100"><?= htmlspecialchars((string) ($th['primary_system_name'] ?? '?'), ENT_QUOTES) ?></span>
                                <span class="text-xs text-muted ml-1"><?= htmlspecialchars((string) ($th['region_name'] ?? ''), ENT_QUOTES) ?></span>
                                <div class="text-xs text-muted"><?= htmlspecialchars((string) ($th['start_time'] ?? ''), ENT_QUOTES) ?></div>
                            </td>
                            <td class="px-3 py-2 text-sm"><?= htmlspecialchars((string) ($th['alliance_name'] ?? '-'), ENT_QUOTES) ?></td>
                            <td class="px-3 py-2">
                                <?php $thRole = (string) ($th['role_proxy'] ?? ''); ?>
                                <span class="inline-block rounded-full px-2 py-0.5 text-[10px] uppercase tracking-wider <?= fleet_function_color_class($thRole) ?>"><?= htmlspecialchars(fleet_function_label($thRole), ENT_QUOTES) ?></span>
                            </td>
                            <td class="px-3 py-2 text-right"><?= (int) ($th['kills'] ?? 0) ?></td>
                            <td class="px-3 py-2 text-right"><?= (int) ($th['deaths'] ?? 0) ?></td>
                            <td class="px-3 py-2 text-right"><?= number_format((float) ($th['damage_done'] ?? 0), 0) ?></td>
                            <td class="px-3 py-2 text-right <?= $thSusp > 0.5 ? 'text-red-400' : ($thSusp > 0 ? 'text-yellow-400' : '') ?>"><?= $thSusp > 0 ? number_format($thSusp, 3) : '-' ?></td>
                            <td class="px-3 py-2 text-right">
                                <a href="/theater-intelligence/view.php?theater_id=<?= urlencode((string) ($th['theater_id'] ?? '')) ?>" class="text-accent text-sm">Theater</a>
                            </td>
                        </tr>
                    <?php endforeach; ?>
                    </tbody>
                </table>
            </div>
            <?php endif; ?>

            <!-- Associates -->
            <?php if ($associates !== []): ?>
            <h3 class="mt-6 text-lg font-semibold text-slate-100">Frequent Associates</h3>
            <p class="text-xs text-muted mt-1">Pilots who appear on the same side in multiple theaters.</p>
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
                    <?php foreach ($associates as $a): ?>
                        <tr class="border-b border-border/40 hover:bg-slate-800/50">
                            <td class="px-3 py-2 text-slate-100"><?= htmlspecialchars((string) ($a['assoc_name'] ?? '?'), ENT_QUOTES) ?></td>
                            <td class="px-3 py-2 text-sm"><?= htmlspecialchars((string) ($a['assoc_alliance'] ?? '-'), ENT_QUOTES) ?></td>
                            <td class="px-3 py-2">
                                <?php $aRole = (string) ($a['assoc_role'] ?? ''); ?>
                                <?php if ($aRole !== ''): ?>
                                    <span class="inline-block rounded-full px-2 py-0.5 text-[10px] uppercase tracking-wider <?= fleet_function_color_class($aRole) ?>"><?= htmlspecialchars(fleet_function_label($aRole), ENT_QUOTES) ?></span>
                                <?php endif; ?>
                            </td>
                            <td class="px-3 py-2 text-right"><?= (int) ($a['shared_theaters'] ?? 0) ?></td>
                            <td class="px-3 py-2 text-right">
                                <a href="?character_id=<?= (int) ($a['assoc_character_id'] ?? 0) ?>" class="text-accent text-sm">View</a>
                            </td>
                        </tr>
                    <?php endforeach; ?>
                    </tbody>
                </table>
            </div>
            <?php endif; ?>
        </div>
    <?php endif; ?>
</section>

<?php include __DIR__ . '/../../src/views/partials/footer.php'; ?>
