<?php
/**
 * Participants partial — upgraded side-by-side layout with client-side
 * filtering, sorting, flying/lost ship split, death badges, ISK heat bars.
 *
 * Falls back to single-column when a specific side/suspicious filter is active.
 */

// Split participants by classification for the multi-column layout
$friendlyParticipants = [];
$enemyParticipants = [];
$thirdPartyParticipants = [];
foreach ($participantsAll as $p) {
    $pSide = $classifyAlliance((int) ($p['alliance_id'] ?? 0));
    if ($pSide === 'friendly') {
        $friendlyParticipants[] = $p;
    } elseif ($pSide === 'opponent') {
        $enemyParticipants[] = $p;
    } else {
        $thirdPartyParticipants[] = $p;
    }
}

// When a specific filter is active, show single filtered list
$showSideBySide = ($sideFilter === null && !$suspiciousOnly);
$filteredList = $showSideBySide ? [] : $participants;

// Compute max damage across ALL participants for consistent bar scaling
$maxDamageDone = 0;
$allForMax = $showSideBySide ? $participantsAll : $participants;
foreach ($allForMax as $p) {
    $dmg = (float) ($p['damage_done'] ?? 0);
    if ($dmg > $maxDamageDone) $maxDamageDone = $dmg;
}

/**
 * Render a single participant row for the side-by-side table.
 * Extracted to avoid duplicating HTML across both panels.
 */
function _render_participant_row(array $p, array $resolvedEntities, array $shipTypeNames, float $maxDmgForPanel): string {
    $isSusp = (int) ($p['is_suspicious'] ?? 0);
    $charName = killmail_entity_preferred_name($resolvedEntities, 'character', (int) ($p['character_id'] ?? 0), (string) ($p['character_name'] ?? ''), 'Character');
    $fleetRole = (string) ($p['role_proxy'] ?? 'mainline_dps');
    $kills = (int) ($p['kills'] ?? 0);
    $deaths = (int) ($p['deaths'] ?? 0);
    $dmgDone = (float) ($p['damage_done'] ?? 0);
    $iskLost = (float) ($p['isk_lost'] ?? 0);
    $dmgPct = $maxDmgForPanel > 0 ? ($dmgDone / $maxDmgForPanel) * 100 : 0;
    $hasDeath = $deaths > 0;

    // ── Flying ship (most common attacker ship) ──
    $flyingShipId = (int) ($p['flying_ship_type_id'] ?? 0);
    $flyingShipName = '';
    if ($flyingShipId > 0) {
        $flyingShipName = (string) ($shipTypeNames[$flyingShipId] ?? '');
    }
    // Fallback to first ship_type_ids entry if no flying_ship_type_id
    if ($flyingShipId <= 0) {
        $shipIds = [];
        $shipJson = $p['ship_type_ids'] ?? null;
        if (is_string($shipJson)) {
            $decoded = json_decode($shipJson, true);
            if (is_array($decoded)) $shipIds = $decoded;
        }
        if ($shipIds) {
            $flyingShipId = (int) $shipIds[0];
            $flyingShipName = (string) ($shipTypeNames[$flyingShipId] ?? '');
        }
    }

    // ── Lost ship (highest-ISK non-pod from ships_lost_detail) ──
    $lostShipId = 0;
    $lostShipName = '';
    $lostShipCount = 0;
    $lostSameAsFlying = false;
    $lostDetail = [];
    $lostJson = $p['ships_lost_detail'] ?? null;
    if (is_string($lostJson)) {
        $decoded = json_decode($lostJson, true);
        if (is_array($decoded)) $lostDetail = $decoded;
    }
    // Filter out pods
    $lostDisplay = array_values(array_filter($lostDetail, static fn(array $e): bool => !in_array((int) ($e['ship_type_id'] ?? 0), [670, 33328], true)));
    if ($lostDisplay !== []) {
        usort($lostDisplay, static fn(array $a, array $b): int => (float) ($b['isk_lost'] ?? 0) <=> (float) ($a['isk_lost'] ?? 0));
        $lostShipId = (int) ($lostDisplay[0]['ship_type_id'] ?? 0);
        $lostShipName = (string) ($shipTypeNames[$lostShipId] ?? '');
        // Total non-pod loss count
        $lostShipCount = 0;
        foreach ($lostDisplay as $entry) {
            $lostShipCount += (int) ($entry['count'] ?? 1);
        }
        $lostSameAsFlying = ($lostShipId === $flyingShipId);
    }

    // ── Death styling ──
    $deathCls = '';
    if ($deaths === 0) $deathCls = 'text-slate-600';
    elseif ($deaths <= 2) $deathCls = 'text-orange-400';
    elseif ($deaths <= 5) $deathCls = 'text-red-400 font-semibold';
    else $deathCls = 'text-red-300 font-semibold';

    // ── Row data attributes for client-side filtering/sorting ──
    $kdRatio = $deaths > 0 ? $kills / $deaths : $kills;

    ob_start();
    ?>
    <tr class="border-b border-border/50 hover:bg-slate-800/40 transition-colors <?= $hasDeath ? 'border-l-2 border-l-red-500/40 bg-red-950/5' : '' ?> <?= $fleetRole === 'fc' ? 'border-l-2 border-l-yellow-400/60' : '' ?>"
        data-deaths="<?= $deaths ?>"
        data-kills="<?= $kills ?>"
        data-kd="<?= number_format($kdRatio, 4) ?>"
        data-damage="<?= $dmgDone ?>"
        data-isk="<?= $iskLost ?>">
        <td class="px-2 py-1.5">
            <div class="flex items-center gap-1.5">
                <img src="https://images.evetech.net/characters/<?= (int) ($p['character_id'] ?? 0) ?>/portrait?size=32" alt="" class="w-5 h-5 rounded-full flex-shrink-0" loading="lazy">
                <a class="text-accent text-xs truncate max-w-[8rem]" href="/battle-intelligence/character.php?character_id=<?= (int) ($p['character_id'] ?? 0) ?>" title="<?= htmlspecialchars($charName, ENT_QUOTES) ?>">
                    <?= htmlspecialchars($charName, ENT_QUOTES) ?>
                </a>
                <?php if ($isSusp): ?>
                    <span class="inline-block w-1.5 h-1.5 rounded-full bg-red-400 flex-shrink-0" title="Suspicious"></span>
                <?php endif; ?>
                <?php if ($hasDeath): ?>
                    <span class="inline-flex items-center justify-center w-3.5 h-3.5 rounded-full bg-red-950 ring-1 ring-red-500/60 text-[8px] text-red-400 flex-shrink-0" title="<?= $deaths ?> loss(es)">&#x2715;</span>
                <?php endif; ?>
            </div>
        </td>
        <td class="px-2 py-1.5">
            <div class="flex flex-col gap-0.5">
                <?php if ($flyingShipId > 0): ?>
                    <div class="flex items-center gap-1 opacity-60">
                        <img class="w-4 h-4 flex-shrink-0" src="https://images.evetech.net/types/<?= $flyingShipId ?>/icon?size=32" loading="lazy">
                        <span class="text-[11px] text-slate-400 truncate max-w-[6rem]"><?= htmlspecialchars($flyingShipName, ENT_QUOTES) ?></span>
                    </div>
                <?php endif; ?>
                <?php if ($lostShipId > 0 && !$lostSameAsFlying): ?>
                    <div class="flex items-center gap-1">
                        <img class="w-4 h-4 flex-shrink-0" src="https://images.evetech.net/types/<?= $lostShipId ?>/icon?size=32" loading="lazy">
                        <span class="text-[11px] text-red-400 truncate max-w-[5rem]"><?= htmlspecialchars($lostShipName, ENT_QUOTES) ?></span>
                        <?php if ($lostShipCount > 1): ?>
                            <span class="text-[10px] text-red-500">&times;<?= $lostShipCount ?></span>
                        <?php endif; ?>
                    </div>
                <?php elseif ($lostSameAsFlying && $hasDeath): ?>
                    <div class="flex items-center gap-1 opacity-70">
                        <span class="text-[10px] text-red-500/60">&darr; lost<?= $lostShipCount > 1 ? ' &times;' . $lostShipCount : '' ?></span>
                    </div>
                <?php endif; ?>
            </div>
        </td>
        <td class="px-2 py-1.5">
            <span class="inline-block rounded-full px-1.5 py-0.5 text-[9px] uppercase tracking-wider <?= fleet_function_color_class($fleetRole) ?>">
                <?= htmlspecialchars(fleet_function_label($fleetRole), ENT_QUOTES) ?>
            </span>
        </td>
        <td class="px-2 py-1.5 text-right text-xs whitespace-nowrap">
            <span class="text-green-400"><?= $kills ?></span><span class="text-slate-600 mx-px">/</span><span class="<?= $deathCls ?>"><?= $deaths ?></span>
        </td>
        <td class="px-2 py-1.5 text-right">
            <div class="flex items-center justify-end gap-1">
                <div class="w-10 h-[3px] rounded-full bg-slate-800 overflow-hidden">
                    <div class="h-full bg-blue-500/50 rounded-full" style="width: <?= number_format($dmgPct, 1) ?>%"></div>
                </div>
                <span class="text-[11px] text-slate-400 min-w-[3.2rem] text-right"><?= _fmt_damage($dmgDone) ?></span>
            </div>
        </td>
        <td class="px-2 py-1.5 text-right relative overflow-hidden">
            <?php if ($iskLost > 0): ?>
                <?php $iskMaxForPanel = 1; /* will be set via JS data attr */ ?>
                <span class="text-xs text-red-400"><?= supplycore_format_isk($iskLost) ?></span>
            <?php else: ?>
                <span class="text-xs text-slate-600">&mdash;</span>
            <?php endif; ?>
        </td>
        <td class="px-2 py-1.5 text-right">
            <a class="text-accent text-[11px]" href="/battle-intelligence/character.php?character_id=<?= (int) ($p['character_id'] ?? 0) ?>">Intel</a>
        </td>
    </tr>
    <?php
    return ob_get_clean();
}

function _fmt_damage(float $v): string {
    if ($v <= 0) return '0';
    if ($v >= 1e6) return number_format($v / 1e6, 1) . 'M';
    if ($v >= 1e3) return number_format($v / 1e3, 0) . 'k';
    return number_format($v, 0);
}
?>
<section class="surface-primary mt-4">
    <div class="flex items-center justify-between gap-4">
        <h2 class="text-lg font-semibold text-slate-50">Participants</h2>
        <div class="flex gap-2 text-sm">
            <a href="?theater_id=<?= urlencode($theaterId) ?>" class="<?= $sideFilter === null && !$suspiciousOnly ? 'text-slate-50 font-semibold' : 'text-accent' ?>">All</a>
            <a href="?theater_id=<?= urlencode($theaterId) ?>&side=friendly" class="<?= $sideFilter === 'friendly' ? 'text-blue-300 font-semibold' : 'text-accent' ?>"><?= htmlspecialchars($sideLabels['friendly'] ?? 'Friendlies', ENT_QUOTES) ?></a>
            <a href="?theater_id=<?= urlencode($theaterId) ?>&side=opponent" class="<?= $sideFilter === 'opponent' ? 'text-red-300 font-semibold' : 'text-accent' ?>"><?= htmlspecialchars($sideLabels['opponent'] ?? 'Opposition', ENT_QUOTES) ?></a>
            <a href="?theater_id=<?= urlencode($theaterId) ?>&side=third_party" class="<?= $sideFilter === 'third_party' ? 'text-slate-400 font-semibold' : 'text-accent' ?>">Third Party</a>
            <a href="?theater_id=<?= urlencode($theaterId) ?>&suspicious=1" class="<?= $suspiciousOnly ? 'text-yellow-300 font-semibold' : 'text-accent' ?>">Suspicious</a>
        </div>
    </div>
    <p class="text-xs text-muted mt-1">Kill Involvements = killmails where pilot was an attacker. Damage Done = HP damage. ISK Lost = total value of all ships destroyed.</p>

<?php if ($showSideBySide): ?>
    <div class="mt-3 grid gap-4 md:grid-cols-2">
        <?php
        $enemyCombinedParticipants = array_merge($enemyParticipants, $thirdPartyParticipants);
        $enemyLabel = ($sideLabels['opponent'] ?? 'Opposition');
        if ($thirdPartyParticipants !== []) {
            $enemyLabel .= ' + Third Party';
        }
        $panelSets = [
            ['label' => $sideLabels['friendly'] ?? 'Friendlies', 'side' => 'friendly', 'rows' => $friendlyParticipants, 'colorClass' => 'text-blue-300', 'borderClass' => 'border-blue-500/30', 'badgeClass' => 'bg-green-950 text-green-400 ring-1 ring-green-600/60', 'badgeLabel' => 'Friendly'],
            ['label' => $enemyLabel, 'side' => 'opponent', 'rows' => $enemyCombinedParticipants, 'colorClass' => 'text-red-300', 'borderClass' => 'border-red-500/30', 'badgeClass' => 'bg-red-950 text-red-400 ring-1 ring-red-600/60', 'badgeLabel' => 'Opponent'],
        ];
        foreach ($panelSets as $panel):
            // Max damage for this panel
            $panelMaxDmg = 0;
            foreach ($panel['rows'] as $pr) {
                $d = (float) ($pr['damage_done'] ?? 0);
                if ($d > $panelMaxDmg) $panelMaxDmg = $d;
            }
        ?>
        <div data-panel="<?= $panel['side'] ?>">
            <div class="flex items-center justify-between mb-2">
                <h3 class="text-sm font-semibold <?= $panel['colorClass'] ?>">
                    <?= htmlspecialchars($panel['label'], ENT_QUOTES) ?>
                    <span class="<?= $panel['badgeClass'] ?> text-[10px] rounded px-1.5 py-0.5 ml-1.5"><?= $panel['badgeLabel'] ?></span>
                    <span class="text-muted font-normal text-xs ml-1" data-panel-count>
                        (<?= count($panel['rows']) ?>)
                    </span>
                </h3>
            </div>
            <div class="flex items-center gap-1.5 mb-2 flex-wrap">
                <button type="button" class="sc-filter-btn active text-[11px] px-2.5 py-1 rounded bg-blue-600/80 text-slate-100 border border-blue-500/60 cursor-pointer transition-colors" data-filter="all">All</button>
                <button type="button" class="sc-filter-btn text-[11px] px-2.5 py-1 rounded bg-slate-800 text-slate-400 border border-slate-700 cursor-pointer transition-colors hover:bg-slate-700 hover:text-slate-200" data-filter="dead">&#x2715; Deaths only</button>
                <button type="button" class="sc-filter-btn text-[11px] px-2.5 py-1 rounded bg-slate-800 text-slate-400 border border-slate-700 cursor-pointer transition-colors hover:bg-slate-700 hover:text-slate-200" data-filter="clean">&#10003; No losses</button>
                <select class="sc-sort-select text-[11px] bg-slate-800 border border-slate-700 rounded text-slate-400 px-2 py-1 ml-auto cursor-pointer focus:outline-none focus:border-blue-500/60">
                    <option value="kd_ratio">Sort: K/D ratio</option>
                    <option value="kills">Sort: kills</option>
                    <option value="isk_lost">Sort: ISK lost</option>
                    <option value="damage">Sort: damage</option>
                </select>
            </div>
            <div class="overflow-x-auto border border-slate-800 rounded-md">
                <table class="table-ui w-full">
                    <thead>
                        <tr class="border-b border-border/70 text-[11px] uppercase tracking-[0.15em] text-muted">
                            <th class="px-2 py-2 text-left" style="width:150px">Pilot</th>
                            <th class="px-2 py-2 text-left" style="width:120px">Ship</th>
                            <th class="px-2 py-2 text-left" style="width:80px">Role</th>
                            <th class="px-2 py-2 text-right" style="width:64px">K/D</th>
                            <th class="px-2 py-2 text-right" style="width:90px">Damage</th>
                            <th class="px-2 py-2 text-right" style="width:80px">ISK Lost</th>
                            <th class="px-2 py-2 text-right" style="width:40px"></th>
                        </tr>
                    </thead>
                    <tbody class="sc-tbody">
                        <?php if ($panel['rows'] === []): ?>
                            <tr><td colspan="7" class="px-2 py-4 text-sm text-muted text-center">No participants.</td></tr>
                        <?php else: ?>
                            <?php foreach ($panel['rows'] as $p): ?>
                                <?= _render_participant_row($p, $resolvedEntities, $shipTypeNames, $panelMaxDmg) ?>
                            <?php endforeach; ?>
                        <?php endif; ?>
                    </tbody>
                </table>
            </div>
        </div>
        <?php endforeach; ?>
    </div>

    <script>
    (function() {
        document.querySelectorAll('[data-panel]').forEach(function(panel) {
            var tbody = panel.querySelector('.sc-tbody');
            if (!tbody) return;
            var allRows = Array.from(tbody.querySelectorAll('tr[data-deaths]'));
            var filterBtns = panel.querySelectorAll('.sc-filter-btn');
            var sortSelect = panel.querySelector('.sc-sort-select');
            var countEl = panel.querySelector('[data-panel-count]');
            var currentFilter = 'all';

            function applyFilter() {
                var shown = 0;
                allRows.forEach(function(row) {
                    var deaths = parseInt(row.getAttribute('data-deaths') || '0', 10);
                    var visible = true;
                    if (currentFilter === 'dead' && deaths === 0) visible = false;
                    if (currentFilter === 'clean' && deaths > 0) visible = false;
                    row.style.display = visible ? '' : 'none';
                    if (visible) shown++;
                });
                if (countEl) countEl.textContent = '(' + shown + ')';
            }

            function applySort(mode) {
                allRows.sort(function(a, b) {
                    if (mode === 'kd_ratio') return parseFloat(b.dataset.kd) - parseFloat(a.dataset.kd);
                    if (mode === 'kills') return parseInt(b.dataset.kills) - parseInt(a.dataset.kills);
                    if (mode === 'isk_lost') return parseFloat(b.dataset.isk) - parseFloat(a.dataset.isk);
                    if (mode === 'damage') return parseFloat(b.dataset.damage) - parseFloat(a.dataset.damage);
                    return 0;
                });
                allRows.forEach(function(row) { tbody.appendChild(row); });
                applyFilter();
            }

            filterBtns.forEach(function(btn) {
                btn.addEventListener('click', function() {
                    filterBtns.forEach(function(b) {
                        b.className = 'sc-filter-btn text-[11px] px-2.5 py-1 rounded bg-slate-800 text-slate-400 border border-slate-700 cursor-pointer transition-colors hover:bg-slate-700 hover:text-slate-200';
                    });
                    var mode = btn.getAttribute('data-filter');
                    currentFilter = mode;
                    if (mode === 'all') {
                        btn.className = 'sc-filter-btn active text-[11px] px-2.5 py-1 rounded bg-blue-600/80 text-slate-100 border border-blue-500/60 cursor-pointer transition-colors';
                    } else if (mode === 'dead') {
                        btn.className = 'sc-filter-btn active text-[11px] px-2.5 py-1 rounded bg-red-900/60 text-red-300 border border-red-600/60 cursor-pointer transition-colors';
                    } else if (mode === 'clean') {
                        btn.className = 'sc-filter-btn active text-[11px] px-2.5 py-1 rounded bg-green-900/60 text-green-300 border border-green-600/60 cursor-pointer transition-colors';
                    }
                    applyFilter();
                });
            });

            if (sortSelect) {
                sortSelect.addEventListener('change', function() {
                    applySort(sortSelect.value);
                });
            }
        });
    })();
    </script>

<?php else: ?>
    <!-- Single-column filtered view -->
    <div class="mt-3 table-shell">
        <table class="table-ui">
            <thead>
                <tr class="border-b border-border/70 text-xs uppercase tracking-[0.15em] text-muted">
                    <th class="px-3 py-2 text-left">Pilot</th>
                    <th class="px-3 py-2 text-left">Alliance / Corp</th>
                    <th class="px-3 py-2 text-left">Ship</th>
                    <th class="px-3 py-2 text-left">Role</th>
                    <th class="px-3 py-2 text-right">K/D</th>
                    <th class="px-3 py-2 text-right">Damage Done</th>
                    <th class="px-3 py-2 text-right">Damage Taken</th>
                    <th class="px-3 py-2 text-right">ISK Lost</th>
                    <th class="px-3 py-2 text-right">Suspicion</th>
                    <th class="px-3 py-2 text-right"></th>
                </tr>
            </thead>
            <tbody>
                <?php if ($filteredList === []): ?>
                    <tr><td colspan="10" class="px-3 py-6 text-sm text-muted">No participants found.</td></tr>
                <?php else: ?>
                    <?php foreach ($filteredList as $p): ?>
                        <?php
                            $pSide = $classifyAlliance((int) ($p['alliance_id'] ?? 0));
                            $pSideClass = $sideColorClass[$pSide] ?? 'text-slate-300';
                            $pSusp = (float) ($p['suspicion_score'] ?? 0);
                            $isSusp = (int) ($p['is_suspicious'] ?? 0);
                            $charName = killmail_entity_preferred_name($resolvedEntities, 'character', (int) ($p['character_id'] ?? 0), (string) ($p['character_name'] ?? ''), 'Character');
                            $resolvedAlliance = killmail_entity_preferred_name($resolvedEntities, 'alliance', (int) ($p['alliance_id'] ?? 0), (string) ($p['alliance_name'] ?? ''), 'Alliance');
                            $resolvedCorp = killmail_entity_preferred_name($resolvedEntities, 'corporation', (int) ($p['corporation_id'] ?? 0), (string) ($p['corporation_name'] ?? ''), 'Corp');
                            $fleetRole = (string) ($p['role_proxy'] ?? 'mainline_dps');
                            $kills = (int) ($p['kills'] ?? 0);
                            $deaths = (int) ($p['deaths'] ?? 0);
                            $dmgDone = (float) ($p['damage_done'] ?? 0);
                            $dmgTaken = (float) ($p['damage_taken'] ?? 0);
                            $iskLost = (float) ($p['isk_lost'] ?? 0);
                            $dmgPct = $maxDamageDone > 0 ? ($dmgDone / $maxDamageDone) * 100 : 0;
                            $hasDeath = $deaths > 0;

                            // Flying ship
                            $flyingShipId2 = (int) ($p['flying_ship_type_id'] ?? 0);
                            $flyingShipName2 = '';
                            if ($flyingShipId2 > 0) {
                                $flyingShipName2 = (string) ($shipTypeNames[$flyingShipId2] ?? '');
                            }
                            if ($flyingShipId2 <= 0) {
                                $shipIds = [];
                                $shipJson = $p['ship_type_ids'] ?? null;
                                if (is_string($shipJson)) {
                                    $decoded = json_decode($shipJson, true);
                                    if (is_array($decoded)) $shipIds = $decoded;
                                }
                                if ($shipIds) {
                                    $flyingShipId2 = (int) $shipIds[0];
                                    $flyingShipName2 = (string) ($shipTypeNames[$flyingShipId2] ?? '');
                                }
                            }

                            // Lost ship
                            $lostDetail2 = [];
                            $lostJson2 = $p['ships_lost_detail'] ?? null;
                            if (is_string($lostJson2)) {
                                $decoded2 = json_decode($lostJson2, true);
                                if (is_array($decoded2)) $lostDetail2 = $decoded2;
                            }
                            $lostDisplay2 = array_values(array_filter($lostDetail2, static fn(array $e): bool => !in_array((int) ($e['ship_type_id'] ?? 0), [670, 33328], true)));
                            $lostShipId2 = 0;
                            $lostShipName2 = '';
                            $lostShipCount2 = 0;
                            $lostSameAsFlying2 = false;
                            if ($lostDisplay2 !== []) {
                                usort($lostDisplay2, static fn(array $a, array $b): int => (float) ($b['isk_lost'] ?? 0) <=> (float) ($a['isk_lost'] ?? 0));
                                $lostShipId2 = (int) ($lostDisplay2[0]['ship_type_id'] ?? 0);
                                $lostShipName2 = (string) ($shipTypeNames[$lostShipId2] ?? '');
                                foreach ($lostDisplay2 as $entry2) {
                                    $lostShipCount2 += (int) ($entry2['count'] ?? 1);
                                }
                                $lostSameAsFlying2 = ($lostShipId2 === $flyingShipId2);
                            }

                            $deathCls2 = '';
                            if ($deaths === 0) $deathCls2 = 'text-slate-600';
                            elseif ($deaths <= 2) $deathCls2 = 'text-orange-400';
                            elseif ($deaths <= 5) $deathCls2 = 'text-red-400 font-semibold';
                            else $deathCls2 = 'text-red-300 font-semibold';
                        ?>
                        <tr class="border-b border-border/50 <?= $isSusp ? 'bg-red-900/10' : '' ?> <?= $hasDeath ? 'border-l-2 border-l-red-500/40 bg-red-950/5' : '' ?> <?= $fleetRole === 'fc' ? 'border-l-2 border-l-yellow-400/60' : '' ?>">
                            <td class="px-3 py-2">
                                <div class="flex items-center gap-1.5">
                                    <img src="https://images.evetech.net/characters/<?= (int) ($p['character_id'] ?? 0) ?>/portrait?size=32" alt="" class="w-5 h-5 rounded-full" loading="lazy">
                                    <a class="text-accent text-sm" href="/battle-intelligence/character.php?character_id=<?= (int) ($p['character_id'] ?? 0) ?>">
                                        <?= htmlspecialchars($charName, ENT_QUOTES) ?>
                                    </a>
                                    <?php if ($isSusp): ?>
                                        <span class="inline-block w-1.5 h-1.5 rounded-full bg-red-400 ml-1" title="Suspicious"></span>
                                    <?php endif; ?>
                                    <?php if ($hasDeath): ?>
                                        <span class="inline-flex items-center justify-center w-3.5 h-3.5 rounded-full bg-red-950 ring-1 ring-red-500/60 text-[8px] text-red-400 flex-shrink-0" title="<?= $deaths ?> loss(es)">&#x2715;</span>
                                    <?php endif; ?>
                                    <span class="inline-block rounded-full px-1.5 py-0.5 text-[9px] uppercase tracking-wider ml-1 <?= $sideBgClass[$pSide] ?? 'bg-slate-700' ?> <?= $pSideClass ?>">
                                        <?= htmlspecialchars($sideLabels[$pSide] ?? $pSide, ENT_QUOTES) ?>
                                    </span>
                                </div>
                            </td>
                            <td class="px-3 py-2 text-slate-300 text-xs">
                                <div class="flex items-center gap-1.5">
                                    <?php $allianceId = (int) ($p['alliance_id'] ?? 0); $corpId = (int) ($p['corporation_id'] ?? 0); ?>
                                    <?php if ($allianceId > 0): ?>
                                        <img src="https://images.evetech.net/alliances/<?= $allianceId ?>/logo?size=32" alt="" class="w-4 h-4" loading="lazy">
                                    <?php elseif ($corpId > 0): ?>
                                        <img src="https://images.evetech.net/corporations/<?= $corpId ?>/logo?size=32" alt="" class="w-4 h-4" loading="lazy">
                                    <?php endif; ?>
                                    <?php if (!str_starts_with($resolvedAlliance, 'Alliance #') && !str_starts_with($resolvedAlliance, 'Alliance 0')): ?>
                                        <span class="text-slate-100"><?= htmlspecialchars($resolvedAlliance, ENT_QUOTES) ?></span>
                                    <?php elseif (!str_starts_with($resolvedCorp, 'Corp #') && !str_starts_with($resolvedCorp, 'Corp 0')): ?>
                                        <span class="text-slate-300"><?= htmlspecialchars($resolvedCorp, ENT_QUOTES) ?></span>
                                    <?php else: ?>
                                        <span class="text-slate-500">-</span>
                                    <?php endif; ?>
                                </div>
                            </td>
                            <td class="px-3 py-2">
                                <div class="flex flex-col gap-0.5">
                                    <?php if ($flyingShipId2 > 0): ?>
                                        <div class="flex items-center gap-1 opacity-60">
                                            <img class="w-4 h-4" src="https://images.evetech.net/types/<?= $flyingShipId2 ?>/icon?size=32" loading="lazy">
                                            <span class="text-[11px] text-slate-400 truncate max-w-[6rem]"><?= htmlspecialchars($flyingShipName2, ENT_QUOTES) ?></span>
                                        </div>
                                    <?php endif; ?>
                                    <?php if ($lostShipId2 > 0 && !$lostSameAsFlying2): ?>
                                        <div class="flex items-center gap-1">
                                            <img class="w-4 h-4" src="https://images.evetech.net/types/<?= $lostShipId2 ?>/icon?size=32" loading="lazy">
                                            <span class="text-[11px] text-red-400 truncate max-w-[5rem]"><?= htmlspecialchars($lostShipName2, ENT_QUOTES) ?></span>
                                            <?php if ($lostShipCount2 > 1): ?>
                                                <span class="text-[10px] text-red-500">&times;<?= $lostShipCount2 ?></span>
                                            <?php endif; ?>
                                        </div>
                                    <?php elseif ($lostSameAsFlying2 && $hasDeath): ?>
                                        <div class="flex items-center gap-1 opacity-70">
                                            <span class="text-[10px] text-red-500/60">&darr; lost<?= $lostShipCount2 > 1 ? ' &times;' . $lostShipCount2 : '' ?></span>
                                        </div>
                                    <?php endif; ?>
                                </div>
                            </td>
                            <td class="px-3 py-2">
                                <span class="inline-block rounded-full px-2 py-0.5 text-[10px] uppercase tracking-wider <?= fleet_function_color_class($fleetRole) ?>">
                                    <?= htmlspecialchars(fleet_function_label($fleetRole), ENT_QUOTES) ?>
                                </span>
                            </td>
                            <td class="px-3 py-2 text-right text-sm whitespace-nowrap">
                                <span class="text-green-400"><?= $kills ?></span><span class="text-slate-600 mx-px">/</span><span class="<?= $deathCls2 ?>"><?= $deaths ?></span>
                            </td>
                            <td class="px-3 py-2 text-right">
                                <div class="flex items-center justify-end gap-1.5">
                                    <div class="w-16 h-1.5 rounded-full bg-slate-800 overflow-hidden">
                                        <div class="h-full bg-blue-500/70 rounded-full" style="width: <?= number_format($dmgPct, 1) ?>%"></div>
                                    </div>
                                    <span class="text-xs text-slate-300 min-w-[3rem] text-right"><?= _fmt_damage($dmgDone) ?></span>
                                </div>
                            </td>
                            <td class="px-3 py-2 text-right text-xs text-slate-300"><?= $dmgTaken > 0 ? number_format($dmgTaken, 0) : '-' ?></td>
                            <td class="px-3 py-2 text-right text-xs <?= $iskLost > 0 ? 'text-red-300' : 'text-slate-500' ?>"><?= $iskLost > 0 ? supplycore_format_isk($iskLost) : '-' ?></td>
                            <td class="px-3 py-2 text-right">
                                <?php if ($pSusp > 0): ?>
                                    <span class="text-xs <?= $pSusp >= 0.5 ? 'text-red-400 font-semibold' : ($pSusp >= 0.3 ? 'text-yellow-400' : 'text-slate-300') ?>"><?= number_format($pSusp, 3) ?></span>
                                <?php else: ?>
                                    <span class="text-slate-500 text-xs">-</span>
                                <?php endif; ?>
                            </td>
                            <td class="px-3 py-2 text-right">
                                <a class="text-accent text-sm" href="/battle-intelligence/character.php?character_id=<?= (int) ($p['character_id'] ?? 0) ?>">Intel</a>
                            </td>
                        </tr>
                    <?php endforeach; ?>
                <?php endif; ?>
            </tbody>
        </table>
    </div>
<?php endif; ?>
</section>
