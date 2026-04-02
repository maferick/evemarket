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
    $pSide = $classifyAlliance((int) ($p['alliance_id'] ?? 0), (int) ($p['corporation_id'] ?? 0));
    if ($pSide === 'friendly') {
        $friendlyParticipants[] = $p;
    } elseif ($pSide === 'opponent') {
        $enemyParticipants[] = $p;
    } else {
        $thirdPartyParticipants[] = $p;
    }
}

// Split structure kills by owning alliance side
$friendlyStructureKills = [];
$enemyStructureKills = [];
$thirdPartyStructureKills = [];
$structureKills = $structureKills ?? [];
foreach ($structureKills as $sk) {
    $skSide = $classifyAlliance((int) ($sk['victim_alliance_id'] ?? 0), (int) ($sk['victim_corporation_id'] ?? 0));
    if ($skSide === 'friendly') {
        $friendlyStructureKills[] = $sk;
    } elseif ($skSide === 'opponent') {
        $enemyStructureKills[] = $sk;
    } else {
        $thirdPartyStructureKills[] = $sk;
    }
}

// Detect 2 vs 3 column mode (mirrors _battle_report.php logic)
$_hasOpponent = $enemyParticipants !== [];
$_hasThirdParty = $thirdPartyParticipants !== [];
$_isThreeColumnParticipants = $_hasOpponent && $_hasThirdParty;

// When a specific filter is active, show single filtered list
$showSideBySide = ($sideFilter === null && !$suspiciousOnly);
$filteredList = $showSideBySide ? [] : $participants;

function _is_monitor_or_flag_ship_name(string $shipName): bool {
    $normalized = strtolower(trim($shipName));
    return $normalized !== '' && (str_contains($normalized, 'monitor') || str_contains($normalized, 'flag cruiser'));
}

function _participant_flying_ship_name(array $participant, array $shipTypeNames): string {
    $_podIds = [670, 33328];
    $flyingShipId = (int) ($participant['flying_ship_type_id'] ?? 0);
    if ($flyingShipId > 0 && !in_array($flyingShipId, $_podIds, true)) {
        return (string) ($shipTypeNames[$flyingShipId] ?? '');
    }

    $shipJson = $participant['ship_type_ids'] ?? null;
    if (!is_string($shipJson)) {
        return '';
    }
    $decoded = json_decode($shipJson, true);
    if (!is_array($decoded) || $decoded === []) {
        return '';
    }
    foreach ($decoded as $stid) {
        $stid = (int) $stid;
        if ($stid > 0 && !in_array($stid, $_podIds, true)) {
            return (string) ($shipTypeNames[$stid] ?? '');
        }
    }
    return '';
}

$hasMonitorOrFlagShips = false;
foreach ($participantsAll as $participantRow) {
    if (_is_monitor_or_flag_ship_name(_participant_flying_ship_name($participantRow, $shipTypeNames))) {
        $hasMonitorOrFlagShips = true;
        break;
    }
}

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
function _render_participant_row(array $p, array $resolvedEntities, array $shipTypeNames, float $maxDmgForPanel, bool $hasMonitorOrFlagShips, array $victimKmLookup = []): string {
    $isSusp = (int) ($p['is_suspicious'] ?? 0);
    $charName = killmail_entity_preferred_name($resolvedEntities, 'character', (int) ($p['character_id'] ?? 0), (string) ($p['character_name'] ?? ''), 'Character');
    $fleetRole = (string) ($p['role_proxy'] ?? 'mainline_dps');
    $roleRank = match ($fleetRole) {
        'fc' => 0,
        'supercapital', 'capital_dps', 'capital_logistics', 'capital' => 1,
        default => 2,
    };
    $kills = (int) ($p['kills'] ?? 0);
    $deaths = (int) ($p['deaths'] ?? 0);
    $dmgDone = (float) ($p['damage_done'] ?? 0);
    $iskLost = (float) ($p['isk_lost'] ?? 0);
    $dmgPct = $maxDmgForPanel > 0 ? ($dmgDone / $maxDmgForPanel) * 100 : 0;
    $hasDeath = $deaths > 0;

    // ── Lost ship (highest-ISK non-pod from ships_lost_detail) — computed first ──
    // so we can fall back to it when the flying ship is only a capsule.
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
    // Filter out pods (for ship display), and separately collect pod losses for the indicator
    $_podIds = [670, 33328];
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
    }

    // ── Capsule (pod) losses — shown as a secondary indicator ──
    $podIsk = 0.0;
    $wasPodded = false;
    foreach ($lostDetail as $_pe) {
        if (in_array((int) ($_pe['ship_type_id'] ?? 0), [670, 33328], true)) {
            $podIsk += (float) ($_pe['isk_lost'] ?? 0);
            $wasPodded = true;
        }
    }

    // ── Flying ship (most common attacker ship, never a bare capsule) ──
    // A capsule is always the pilot's escape pod / "driver" — the actual ship
    // they flew is what matters for display.  If the analysis resolved flying
    // ship to a capsule (because the pilot only appeared as a victim), use the
    // best non-pod ship from their loss record instead.
    $_podTypeIds = [670, 33328];
    $flyingShipId = (int) ($p['flying_ship_type_id'] ?? 0);
    $flyingShipName = '';
    if ($flyingShipId > 0 && !in_array($flyingShipId, $_podTypeIds, true)) {
        $flyingShipName = (string) ($shipTypeNames[$flyingShipId] ?? '');
    } else {
        // flying_ship_type_id is absent or is a capsule — prefer the highest-ISK
        // lost ship (already filtered to non-pod above), then fall back to
        // ship_type_ids list (also skipping pods).
        $flyingShipId = 0;
        if ($lostShipId > 0) {
            $flyingShipId   = $lostShipId;
            $flyingShipName = $lostShipName;
        } else {
            $shipIds = [];
            $shipJson = $p['ship_type_ids'] ?? null;
            if (is_string($shipJson)) {
                $decoded = json_decode($shipJson, true);
                if (is_array($decoded)) $shipIds = $decoded;
            }
            foreach ($shipIds as $stid) {
                $stid = (int) $stid;
                if ($stid > 0 && !in_array($stid, $_podTypeIds, true)) {
                    $flyingShipId   = $stid;
                    $flyingShipName = (string) ($shipTypeNames[$stid] ?? '');
                    break;
                }
            }
        }
    }

    // When flying ship came from the loss record, mark it so the row doesn't
    // also render a redundant "lost" badge for the same hull.
    $lostSameAsFlying = ($lostShipId > 0 && $lostShipId === $flyingShipId);

    if ($fleetRole === 'command') {
        if (_is_monitor_or_flag_ship_name($flyingShipName)) {
            $fleetRole = 'links';
        } elseif (!$hasMonitorOrFlagShips) {
            $fleetRole = 'fc_links';
        }
    }

    // ── Death styling ──
    $deathCls = '';
    if ($deaths === 0) $deathCls = 'text-slate-600';
    elseif ($deaths <= 2) $deathCls = 'text-orange-400';
    elseif ($deaths <= 5) $deathCls = 'text-red-400 font-semibold';
    else $deathCls = 'text-red-300 font-semibold';

    // ── Row data attributes for client-side filtering/sorting ──
    $kdRatio = $deaths > 0 ? $kills / $deaths : $kills;
    $sortHull = strtolower(trim($flyingShipName !== '' ? $flyingShipName : 'zzzz-unknown'));

    // Resolve killmail sequence_id for this participant.
    $charId = (int) ($p['character_id'] ?? 0);
    $kmSeqId = 0;
    if ($flyingShipId > 0 && $charId > 0 && isset($victimKmLookup[$charId][$flyingShipId])) {
        $kmSeqId = $victimKmLookup[$charId][$flyingShipId];
    }
    if ($kmSeqId === 0 && $lostShipId > 0 && $charId > 0 && isset($victimKmLookup[$charId][$lostShipId])) {
        $kmSeqId = $victimKmLookup[$charId][$lostShipId];
    }
    if ($kmSeqId === 0) {
        foreach ($lostDisplay as $ld) {
            foreach ((array) ($ld['killmail_ids'] ?? []) as $kmRef) {
                $seqId = (int) ($kmRef['sequence_id'] ?? 0);
                if ($seqId > 0) { $kmSeqId = $seqId; break 2; }
            }
        }
    }

    // Build flat list of loss icons
    $lossIcons = [];
    foreach ($lostDisplay as $ld) {
        $stid = (int) ($ld['ship_type_id'] ?? 0);
        $sname = (string) ($shipTypeNames[$stid] ?? 'Unknown');
        $cnt = max(1, (int) ($ld['count'] ?? 1));
        $kmIds = (array) ($ld['killmail_ids'] ?? []);
        for ($i = 0; $i < $cnt; $i++) {
            $seqId = 0;
            if (isset($kmIds[$i]['sequence_id'])) {
                $seqId = (int) $kmIds[$i]['sequence_id'];
            } elseif ($stid > 0 && $charId > 0 && isset($victimKmLookup[$charId][$stid])) {
                $seqId = $victimKmLookup[$charId][$stid];
            }
            $lossIcons[] = ['type_id' => $stid, 'name' => $sname, 'seq_id' => $seqId, 'pod' => false];
        }
    }
    foreach ($lostDetail as $_pe) {
        $ptid = (int) ($_pe['ship_type_id'] ?? 0);
        if (!in_array($ptid, [670, 33328], true)) continue;
        $cnt = max(1, (int) ($_pe['count'] ?? 1));
        $kmIds = (array) ($_pe['killmail_ids'] ?? []);
        for ($i = 0; $i < $cnt; $i++) {
            $seqId = 0;
            if (isset($kmIds[$i]['sequence_id'])) {
                $seqId = (int) $kmIds[$i]['sequence_id'];
            } elseif ($ptid > 0 && $charId > 0 && isset($victimKmLookup[$charId][$ptid])) {
                $seqId = $victimKmLookup[$charId][$ptid];
            }
            $lossIcons[] = ['type_id' => $ptid, 'name' => 'Capsule', 'seq_id' => $seqId, 'pod' => true];
        }
    }

    $roleGlowCls = $fleetRole === 'fc'
        ? 'border border-[rgba(204,255,0,0.2)] text-[#ccff00] shadow-[0_0_5px_2px_rgba(204,255,0,0.7)] [text-shadow:0_0_3px_rgb(204,255,0)]'
        : '';

    ob_start();
    ?>
    <tr class="border-b border-border/50 hover:bg-slate-800/40 transition-colors <?= $hasDeath ? 'border-l-2 border-l-red-500/40 bg-red-950/5' : '' ?> <?= $fleetRole === 'fc' ? 'border-l-2 border-l-yellow-400/60' : '' ?>"
        data-deaths="<?= $deaths ?>"
        data-kills="<?= $kills ?>"
        data-kd="<?= number_format($kdRatio, 4) ?>"
        data-role-rank="<?= $roleRank ?>"
        data-hull="<?= htmlspecialchars($sortHull, ENT_QUOTES) ?>"
        data-damage="<?= $dmgDone ?>"
        data-isk="<?= $iskLost ?>">
        <!-- Pilot: portrait + ship icon overlay, name + ship name below -->
        <td class="px-1.5 py-1">
            <div class="flex items-center gap-1.5">
                <div class="relative flex-shrink-0">
                    <img src="https://images.evetech.net/characters/<?= (int) ($p['character_id'] ?? 0) ?>/portrait?size=64" alt="" class="w-8 h-8 rounded flex-shrink-0" loading="lazy">
                    <?php if ($flyingShipId > 0): ?>
                        <img src="https://images.evetech.net/types/<?= $flyingShipId ?>/icon?size=64" alt="" class="absolute -bottom-0.5 -right-0.5 w-5 h-5 rounded-sm border border-slate-900 flex-shrink-0 <?= $hasDeath ? 'ring-1 ring-red-500/50' : '' ?>" title="<?= htmlspecialchars($flyingShipName, ENT_QUOTES) ?>" loading="lazy">
                    <?php endif; ?>
                </div>
                <div class="min-w-0 flex-1">
                    <div class="flex items-center gap-1">
                        <a class="text-accent text-xs font-medium truncate leading-tight" href="/battle-intelligence/character.php?character_id=<?= (int) ($p['character_id'] ?? 0) ?>" title="<?= htmlspecialchars($charName, ENT_QUOTES) ?>">
                            <?= htmlspecialchars($charName, ENT_QUOTES) ?>
                        </a>
                        <?php if ($isSusp): ?>
                            <span class="inline-block w-1.5 h-1.5 rounded-full bg-red-400 flex-shrink-0" title="Suspicious"></span>
                        <?php endif; ?>
                        <?php if ($hasDeath): ?>
                            <span class="inline-flex items-center justify-center w-3.5 h-3.5 rounded-full bg-red-950 ring-1 ring-red-500/60 text-[8px] text-red-400 flex-shrink-0 <?= $kmSeqId > 0 ? 'cursor-pointer hover:bg-red-900 transition-colors' : '' ?>" title="<?= $deaths ?> loss(es)" <?= $kmSeqId > 0 ? 'onclick="window._scKmModal(' . $kmSeqId . ')"' : '' ?>>&#x2715;</span>
                        <?php endif; ?>
                    </div>
                    <div class="text-[10px] text-slate-500 truncate leading-tight"><?= htmlspecialchars($flyingShipName !== '' ? $flyingShipName : '—', ENT_QUOTES) ?></div>
                </div>
            </div>
        </td>
        <!-- Lost ships (icons) -->
        <td class="px-1 py-1">
            <div class="flex items-center gap-0.5 flex-wrap">
                <?php if ($lossIcons !== []): ?>
                    <?php foreach ($lossIcons as $li): ?>
                        <img class="<?= $li['pod'] ? 'w-4 h-4 opacity-60' : 'w-5 h-5' ?> flex-shrink-0 rounded-sm ring-1 ring-red-500/30 <?= $li['seq_id'] > 0 ? 'cursor-pointer hover:brightness-125 hover:ring-red-400/60 transition-all' : '' ?>"
                             src="https://images.evetech.net/types/<?= $li['type_id'] ?>/icon?size=64"
                             title="<?= htmlspecialchars($li['name'], ENT_QUOTES) ?>"
                             loading="lazy"
                             <?= $li['seq_id'] > 0 ? 'onclick="window._scKmModal(' . $li['seq_id'] . ')"' : '' ?>>
                    <?php endforeach; ?>
                <?php endif; ?>
            </div>
        </td>
        <!-- Role badge -->
        <td class="px-1 py-1">
            <span class="inline-flex items-center rounded-full px-[5px] py-[3px] text-[8px] font-semibold uppercase tracking-[0.1em] leading-none <?= fleet_function_color_class($fleetRole) ?> <?= $roleGlowCls ?>">
                <?= htmlspecialchars(fleet_function_label($fleetRole), ENT_QUOTES) ?>
            </span>
        </td>
        <!-- K/D -->
        <td class="px-1 py-1 text-right text-xs whitespace-nowrap">
            <span class="text-green-400"><?= $kills ?></span><span class="text-slate-600 mx-px">/</span><span class="<?= $deathCls ?>"><?= $deaths ?></span>
        </td>
        <!-- Damage bar + value -->
        <td class="px-1 py-1 text-right">
            <div class="flex items-center justify-end gap-1">
                <div class="w-8 h-[3px] rounded-full bg-slate-800 overflow-hidden">
                    <div class="h-full bg-blue-500/50 rounded-full" style="width: <?= number_format($dmgPct, 1) ?>%"></div>
                </div>
                <span class="text-[10px] text-slate-400"><?= _fmt_damage($dmgDone) ?></span>
            </div>
        </td>
        <!-- ISK Lost -->
        <td class="px-1 py-1 text-right">
            <?php if ($iskLost > 0): ?>
                <span class="text-[10px] text-red-400"><?= supplycore_format_isk($iskLost) ?></span>
            <?php endif; ?>
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

/**
 * Render a single structure-kill row for the participant table.
 * Structures have no pilot character, K/D, or damage — only an owning
 * corp/alliance, hull type, and ISK lost.
 */
function _render_structure_row(array $sk, array $resolvedEntities, array $shipTypeNames, array $killmailSeqLookup = []): string {
    $allianceId = (int) ($sk['victim_alliance_id'] ?? 0);
    $corpId     = (int) ($sk['victim_corporation_id'] ?? 0);
    $shipTypeId = (int) ($sk['victim_ship_type_id'] ?? 0);
    $iskLost    = (float) ($sk['isk_lost'] ?? 0);
    $skKmId     = (int) ($sk['killmail_id'] ?? 0);
    $skSeqId    = (int) ($sk['sequence_id'] ?? 0);
    // Fallback: use the battle-based lookup if the JOIN didn't resolve sequence_id
    if ($skSeqId === 0 && $skKmId > 0 && isset($killmailSeqLookup[$skKmId])) {
        $skSeqId = $killmailSeqLookup[$skKmId];
    }
    $shipName   = $shipTypeId > 0
        ? (string) ($shipTypeNames[$shipTypeId] ?? ('Structure #' . $shipTypeId))
        : 'Unknown Structure';

    // Prefer corp name, fall back to alliance name
    $orgName = '';
    if ($corpId > 0) {
        $orgName = killmail_entity_preferred_name($resolvedEntities, 'corporation', $corpId, (string) ($sk['corporation_name'] ?? ''), 'Corp');
    }
    if (($orgName === '' || str_starts_with($orgName, 'Corp #')) && $allianceId > 0) {
        $orgName = killmail_entity_preferred_name($resolvedEntities, 'alliance', $allianceId, (string) ($sk['alliance_name'] ?? ''), 'Alliance');
    }
    if ($orgName === '') {
        $orgName = 'Unknown';
    }

    ob_start();
    ?>
    <tr class="border-b border-border/50 hover:bg-slate-800/40 transition-colors border-l-2 border-l-orange-500/40 bg-orange-950/5"
        data-deaths="1"
        data-kills="0"
        data-kd="0.0000"
        data-role-rank="99"
        data-hull="zzz-structure"
        data-damage="0"
        data-isk="<?= $iskLost ?>">
        <td class="px-1.5 py-1">
            <div class="flex items-center gap-1.5">
                <div class="relative flex-shrink-0">
                    <?php if ($shipTypeId > 0): ?>
                        <img class="w-8 h-8 rounded flex-shrink-0" src="https://images.evetech.net/types/<?= $shipTypeId ?>/icon?size=64" loading="lazy">
                    <?php else: ?>
                        <span class="flex items-center justify-center w-8 h-8 rounded bg-orange-950/40 text-orange-500/70 text-sm">&#x1F3DB;</span>
                    <?php endif; ?>
                </div>
                <div class="min-w-0 flex-1">
                    <div class="flex items-center gap-1">
                        <span class="text-xs text-slate-400 truncate leading-tight" title="<?= htmlspecialchars($orgName, ENT_QUOTES) ?>">
                            <?= htmlspecialchars($orgName, ENT_QUOTES) ?>
                        </span>
                        <span class="inline-flex items-center justify-center w-3.5 h-3.5 rounded-full bg-orange-950 ring-1 ring-orange-500/60 text-[8px] text-orange-400 flex-shrink-0 <?= $skSeqId > 0 ? 'cursor-pointer hover:bg-orange-900 transition-colors' : '' ?>" title="Structure destroyed" <?= $skSeqId > 0 ? 'onclick="window._scKmModal(' . $skSeqId . ')"' : '' ?>>&#x2715;</span>
                    </div>
                    <div class="text-[10px] text-orange-300/60 truncate leading-tight"><?= htmlspecialchars($shipName, ENT_QUOTES) ?></div>
                </div>
            </div>
        </td>
        <td class="px-1 py-1"></td>
        <td class="px-1 py-1">
            <span class="inline-flex items-center rounded-full px-[5px] py-[3px] text-[8px] font-semibold uppercase tracking-[0.1em] leading-none bg-orange-950/60 border border-orange-500/30 text-orange-400">
                Structure
            </span>
        </td>
        <td class="px-1 py-1 text-right text-xs text-slate-600">&mdash;</td>
        <td class="px-1 py-1 text-right text-xs text-slate-600">&mdash;</td>
        <td class="px-1 py-1 text-right">
            <?php if ($iskLost > 0): ?>
                <span class="text-[10px] text-red-400"><?= supplycore_format_isk($iskLost) ?></span>
            <?php endif; ?>
        </td>
    </tr>
    <?php
    return ob_get_clean();
}
?>
<section class="surface-primary mt-4">
    <div class="flex items-center justify-between gap-4 flex-wrap">
        <h2 class="text-lg font-semibold text-slate-50">Participants</h2>
        <?php if ($showSideBySide): ?>
        <div class="flex gap-1 text-xs items-center flex-wrap" id="sc-coalition-tabs">
            <button type="button" class="sc-coalition-tab active px-2.5 py-1 rounded-md bg-slate-700 text-slate-100 border border-slate-600 cursor-pointer transition-colors font-medium" data-coalition="all">All</button>
            <button type="button" class="sc-coalition-tab px-2.5 py-1 rounded-md bg-slate-800/60 text-blue-300 border border-slate-700/60 cursor-pointer transition-colors hover:bg-blue-900/30 hover:border-blue-500/40" data-coalition="friendly"><?= htmlspecialchars($sideLabels['friendly'] ?? 'Friendlies', ENT_QUOTES) ?></button>
            <button type="button" class="sc-coalition-tab px-2.5 py-1 rounded-md bg-slate-800/60 text-red-300 border border-slate-700/60 cursor-pointer transition-colors hover:bg-red-900/30 hover:border-red-500/40" data-coalition="opponent"><?= htmlspecialchars($sideLabels['opponent'] ?? 'Opposition', ENT_QUOTES) ?></button>
            <?php if ($_isThreeColumnParticipants): ?>
            <button type="button" class="sc-coalition-tab px-2.5 py-1 rounded-md bg-slate-800/60 text-amber-300 border border-slate-700/60 cursor-pointer transition-colors hover:bg-amber-900/30 hover:border-amber-500/40" data-coalition="third_party"><?= htmlspecialchars($sideLabels['third_party'] ?? 'Third Party', ENT_QUOTES) ?></button>
            <?php endif; ?>
            <a href="?theater_id=<?= urlencode($theaterId) ?>&suspicious=1" class="px-2.5 py-1 rounded-md bg-slate-800/60 text-yellow-400/80 border border-slate-700/60 cursor-pointer transition-colors hover:bg-yellow-900/20 hover:border-yellow-500/40 no-underline">Suspicious</a>
        </div>
        <?php else: ?>
        <div class="flex gap-2 text-sm">
            <a href="?theater_id=<?= urlencode($theaterId) ?>" class="<?= $sideFilter === null && !$suspiciousOnly ? 'text-slate-50 font-semibold' : 'text-accent' ?>">All</a>
            <a href="?theater_id=<?= urlencode($theaterId) ?>&side=friendly" class="<?= $sideFilter === 'friendly' ? 'text-blue-300 font-semibold' : 'text-accent' ?>"><?= htmlspecialchars($sideLabels['friendly'] ?? 'Friendlies', ENT_QUOTES) ?></a>
            <a href="?theater_id=<?= urlencode($theaterId) ?>&side=opponent" class="<?= $sideFilter === 'opponent' ? 'text-red-300 font-semibold' : 'text-accent' ?>"><?= htmlspecialchars($sideLabels['opponent'] ?? 'Opposition', ENT_QUOTES) ?></a>
            <a href="?theater_id=<?= urlencode($theaterId) ?>&side=third_party" class="<?= $sideFilter === 'third_party' ? 'text-amber-300 font-semibold' : 'text-accent' ?>"><?= htmlspecialchars($sideLabels['third_party'] ?? 'Third Party', ENT_QUOTES) ?></a>
            <a href="?theater_id=<?= urlencode($theaterId) ?>&suspicious=1" class="<?= $suspiciousOnly ? 'text-yellow-300 font-semibold' : 'text-accent' ?>">Suspicious</a>
        </div>
        <?php endif; ?>
    </div>
    <p class="text-xs text-muted mt-1">Kill Involvements = killmails where pilot was an attacker. Damage Done = HP damage. ISK Lost = total value of all ships destroyed.</p>

<?php if ($showSideBySide): ?>
    <?php
        $_participantGridCols = $_isThreeColumnParticipants ? 'lg:grid-cols-3 md:grid-cols-2' : 'md:grid-cols-2';
    ?>
    <div class="mt-3 grid gap-4 <?= $_participantGridCols ?>">
        <?php
        if ($_isThreeColumnParticipants) {
            // 3-column: separate panels for each side
            $panelSets = [
                ['label' => $sideLabels['friendly'] ?? 'Friendlies', 'side' => 'friendly', 'rows' => $friendlyParticipants, 'structure_kills' => $friendlyStructureKills, 'colorClass' => 'text-blue-300', 'borderClass' => 'border-blue-500/30', 'badgeClass' => 'bg-blue-950 text-blue-400 ring-1 ring-blue-600/60', 'badgeLabel' => 'Friendly'],
                ['label' => $sideLabels['opponent'] ?? 'Opposition', 'side' => 'opponent', 'rows' => $enemyParticipants, 'structure_kills' => $enemyStructureKills, 'colorClass' => 'text-red-300', 'borderClass' => 'border-red-500/30', 'badgeClass' => 'bg-red-950 text-red-400 ring-1 ring-red-600/60', 'badgeLabel' => 'Hostile'],
                ['label' => $sideLabels['third_party'] ?? 'Third Party', 'side' => 'third_party', 'rows' => $thirdPartyParticipants, 'structure_kills' => $thirdPartyStructureKills, 'colorClass' => 'text-amber-300', 'borderClass' => 'border-amber-500/30', 'badgeClass' => 'bg-amber-950 text-amber-400 ring-1 ring-amber-600/60', 'badgeLabel' => 'Third Party'],
            ];
        } else {
            // 2-column: merge opponent + third party
            $enemyCombinedParticipants = array_merge($enemyParticipants, $thirdPartyParticipants);
            $enemyLabel = ($sideLabels['opponent'] ?? 'Opposition');
            if ($thirdPartyParticipants !== []) {
                $enemyLabel .= ' + Third Party';
            }
            $panelSets = [
                ['label' => $sideLabels['friendly'] ?? 'Friendlies', 'side' => 'friendly', 'rows' => $friendlyParticipants, 'structure_kills' => $friendlyStructureKills, 'colorClass' => 'text-blue-300', 'borderClass' => 'border-blue-500/30', 'badgeClass' => 'bg-blue-950 text-blue-400 ring-1 ring-blue-600/60', 'badgeLabel' => 'Friendly'],
                ['label' => $enemyLabel, 'side' => 'opponent', 'rows' => $enemyCombinedParticipants, 'structure_kills' => array_merge($enemyStructureKills, $thirdPartyStructureKills), 'colorClass' => 'text-red-300', 'borderClass' => 'border-red-500/30', 'badgeClass' => 'bg-red-950 text-red-400 ring-1 ring-red-600/60', 'badgeLabel' => 'Opponent'],
            ];
        }
        foreach ($panelSets as $panel):
            // Max damage for this panel
            $panelMaxDmg = 0;
            foreach ($panel['rows'] as $pr) {
                $d = (float) ($pr['damage_done'] ?? 0);
                if ($d > $panelMaxDmg) $panelMaxDmg = $d;
            }
        ?>
        <div data-panel="<?= $panel['side'] ?>" class="border-t-2 <?= $panel['borderClass'] ?> pt-2">
            <div class="flex items-center justify-between mb-2">
                <h3 class="text-sm font-semibold <?= $panel['colorClass'] ?>">
                    <?= htmlspecialchars($panel['label'], ENT_QUOTES) ?>
                    <span class="<?= $panel['badgeClass'] ?> text-[10px] rounded px-1.5 py-0.5 ml-1.5"><?= $panel['badgeLabel'] ?></span>
                    <span class="text-muted font-normal text-xs ml-1" data-panel-count>
                        (<?= count($panel['rows']) + count($panel['structure_kills'] ?? []) ?>)
                    </span>
                </h3>
            </div>
            <div class="flex items-center gap-1 mb-2 flex-wrap">
                <button type="button" class="sc-filter-btn active text-[10px] px-2 py-0.5 rounded bg-blue-600/80 text-slate-100 border border-blue-500/60 cursor-pointer transition-colors leading-tight" data-filter="all">All</button>
                <button type="button" class="sc-filter-btn text-[10px] px-2 py-0.5 rounded bg-slate-800 text-slate-400 border border-slate-700 cursor-pointer transition-colors hover:bg-slate-700 hover:text-slate-200 leading-tight" data-filter="dead">&#x2715; Deaths only</button>
                <button type="button" class="sc-filter-btn text-[10px] px-2 py-0.5 rounded bg-slate-800 text-slate-400 border border-slate-700 cursor-pointer transition-colors hover:bg-slate-700 hover:text-slate-200 leading-tight" data-filter="clean">&#10003; No losses</button>
                <select class="sc-sort-select text-[10px] bg-slate-800 border border-slate-700 rounded text-slate-400 px-2 py-0.5 ml-auto cursor-pointer focus:outline-none focus:border-blue-500/60 leading-tight">
                    <option value="role_hull" selected>Sort: FC & CAPs, then hull</option>
                    <option value="kd_ratio">Sort: K/D ratio</option>
                    <option value="kills">Sort: kills</option>
                    <option value="isk_lost">Sort: ISK lost</option>
                    <option value="damage">Sort: damage</option>
                </select>
            </div>
            <div class="border border-slate-800 rounded-md">
                <table class="table-ui w-full table-fixed">
                    <colgroup>
                        <col class="w-[40%]">
                        <col>
                        <col>
                        <col>
                        <col>
                        <col>
                    </colgroup>
                    <thead>
                        <tr class="border-b border-border/70 text-[10px] uppercase tracking-[0.1em] text-muted">
                            <th class="px-1.5 py-1 text-left">Pilot</th>
                            <th class="px-1.5 py-1 text-left">Ship</th>
                            <th class="px-1.5 py-1 text-left">Role</th>
                            <th class="px-1.5 py-1 text-right">K/D</th>
                            <th class="px-1.5 py-1 text-right">Damage</th>
                            <th class="px-1.5 py-1 text-right whitespace-nowrap">ISK Lost</th>
                        </tr>
                    </thead>
                    <tbody class="sc-tbody">
                        <?php if ($panel['rows'] === [] && ($panel['structure_kills'] ?? []) === []): ?>
                            <tr><td colspan="6" class="px-2 py-4 text-sm text-muted text-center">No participants.</td></tr>
                        <?php else: ?>
                            <?php foreach ($panel['rows'] as $p): ?>
                                <?= _render_participant_row($p, $resolvedEntities, $shipTypeNames, $panelMaxDmg, $hasMonitorOrFlagShips, $victimKmLookup ?? []) ?>
                            <?php endforeach; ?>
                            <?php foreach (($panel['structure_kills'] ?? []) as $sk): ?>
                                <?= _render_structure_row($sk, $resolvedEntities, $shipTypeNames, $killmailSeqLookup ?? []) ?>
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
                    if (mode === 'role_hull') {
                        var roleCmp = parseInt(a.dataset.roleRank || '99', 10) - parseInt(b.dataset.roleRank || '99', 10);
                        if (roleCmp !== 0) return roleCmp;
                        var hullA = (a.dataset.hull || '');
                        var hullB = (b.dataset.hull || '');
                        var hullCmp = hullA.localeCompare(hullB);
                        if (hullCmp !== 0) return hullCmp;
                        return parseFloat(b.dataset.kd) - parseFloat(a.dataset.kd);
                    }
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
                        b.className = 'sc-filter-btn text-[10px] px-2 py-0.5 rounded bg-slate-800 text-slate-400 border border-slate-700 cursor-pointer transition-colors hover:bg-slate-700 hover:text-slate-200 leading-tight';
                    });
                    var mode = btn.getAttribute('data-filter');
                    currentFilter = mode;
                    if (mode === 'all') {
                        btn.className = 'sc-filter-btn active text-[10px] px-2 py-0.5 rounded bg-blue-600/80 text-slate-100 border border-blue-500/60 cursor-pointer transition-colors leading-tight';
                    } else if (mode === 'dead') {
                        btn.className = 'sc-filter-btn active text-[10px] px-2 py-0.5 rounded bg-red-900/60 text-red-300 border border-red-600/60 cursor-pointer transition-colors leading-tight';
                    } else if (mode === 'clean') {
                        btn.className = 'sc-filter-btn active text-[10px] px-2 py-0.5 rounded bg-green-900/60 text-green-300 border border-green-600/60 cursor-pointer transition-colors leading-tight';
                    }
                    applyFilter();
                });
            });

            if (sortSelect) {
                sortSelect.addEventListener('change', function() {
                    applySort(sortSelect.value);
                });
                applySort(sortSelect.value || 'role_hull');
            }
        });
    })();

    // Coalition tab switcher — show/hide entire panels + expand to full-width
    (function() {
        var tabContainer = document.getElementById('sc-coalition-tabs');
        if (!tabContainer) return;
        var tabs = tabContainer.querySelectorAll('.sc-coalition-tab');
        var grid = tabContainer.closest('.surface-primary').querySelector('.grid[class*="grid-cols"]');
        if (!grid) return;
        var panels = grid.querySelectorAll('[data-panel]');

        tabs.forEach(function(tab) {
            tab.addEventListener('click', function() {
                var coalition = tab.getAttribute('data-coalition');

                // Reset all tab styles
                tabs.forEach(function(t) {
                    t.className = 'sc-coalition-tab px-2.5 py-1 rounded-md bg-slate-800/60 border border-slate-700/60 cursor-pointer transition-colors';
                    // Restore per-side text color
                    var c = t.getAttribute('data-coalition');
                    if (c === 'friendly') t.classList.add('text-blue-300', 'hover:bg-blue-900/30', 'hover:border-blue-500/40');
                    else if (c === 'opponent') t.classList.add('text-red-300', 'hover:bg-red-900/30', 'hover:border-red-500/40');
                    else if (c === 'third_party') t.classList.add('text-amber-300', 'hover:bg-amber-900/30', 'hover:border-amber-500/40');
                    else t.classList.add('text-slate-400', 'hover:bg-slate-700');
                });

                // Highlight active tab
                tab.className = 'sc-coalition-tab active px-2.5 py-1 rounded-md bg-slate-700 text-slate-100 border border-slate-600 cursor-pointer transition-colors font-medium';

                if (coalition === 'all') {
                    // Show all panels, restore grid
                    panels.forEach(function(p) { p.style.display = ''; p.classList.remove('col-span-full'); });
                } else {
                    // Show only selected panel at full width
                    panels.forEach(function(p) {
                        if (p.getAttribute('data-panel') === coalition) {
                            p.style.display = '';
                            p.classList.add('col-span-full');
                        } else {
                            p.style.display = 'none';
                            p.classList.remove('col-span-full');
                        }
                    });
                }
            });
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
                            $pSide = $classifyAlliance((int) ($p['alliance_id'] ?? 0), (int) ($p['corporation_id'] ?? 0));
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

                            if ($fleetRole === 'command') {
                                if (_is_monitor_or_flag_ship_name($flyingShipName2)) {
                                    $fleetRole = 'links';
                                } elseif (!$hasMonitorOrFlagShips) {
                                    $fleetRole = 'fc_links';
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
                                        <img src="https://images.evetech.net/alliances/<?= $allianceId ?>/logo?size=64" alt="" class="w-5 h-5" loading="lazy">
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
                            <?php
                                // Resolve killmail sequence_id for this participant
                                $charId2 = (int) ($p['character_id'] ?? 0);
                                $kmSeqId2 = 0;
                                if ($flyingShipId2 > 0 && $charId2 > 0 && isset($victimKmLookup[$charId2][$flyingShipId2])) {
                                    $kmSeqId2 = $victimKmLookup[$charId2][$flyingShipId2];
                                }
                                if ($kmSeqId2 === 0 && $lostShipId2 > 0 && $charId2 > 0 && isset($victimKmLookup[$charId2][$lostShipId2])) {
                                    $kmSeqId2 = $victimKmLookup[$charId2][$lostShipId2];
                                }
                                if ($kmSeqId2 === 0) {
                                    foreach ($lostDisplay2 as $ld2) {
                                        foreach ((array) ($ld2['killmail_ids'] ?? []) as $kmRef2) {
                                            $seqId2 = (int) ($kmRef2['sequence_id'] ?? 0);
                                            if ($seqId2 > 0) { $kmSeqId2 = $seqId2; break 2; }
                                        }
                                    }
                                }
                            ?>
                            <?php
                                // Resolve killmail for flying ship
                                $flyingSeqId2 = 0;
                                if ($flyingShipId2 > 0 && $charId2 > 0 && isset($victimKmLookup[$charId2][$flyingShipId2])) {
                                    $flyingSeqId2 = $victimKmLookup[$charId2][$flyingShipId2];
                                }
                                $lostSeqId2 = 0;
                                if ($lostShipId2 > 0 && $charId2 > 0 && isset($victimKmLookup[$charId2][$lostShipId2])) {
                                    $lostSeqId2 = $victimKmLookup[$charId2][$lostShipId2];
                                }
                                if ($lostSeqId2 === 0) $lostSeqId2 = $kmSeqId2;
                            ?>
                            <td class="px-3 py-2">
                                <div class="flex flex-col gap-0.5">
                                    <?php if ($flyingShipId2 > 0): ?>
                                        <div class="flex items-center gap-1 <?= $flyingSeqId2 > 0 ? 'cursor-pointer hover:brightness-125 transition-all' : 'opacity-60' ?>"<?= $flyingSeqId2 > 0 ? ' onclick="window._scKmModal(' . $flyingSeqId2 . ')"' : '' ?>>
                                            <img class="w-4 h-4" src="https://images.evetech.net/types/<?= $flyingShipId2 ?>/icon?size=32" loading="lazy">
                                            <span class="text-[11px] text-slate-400 truncate max-w-[6rem] <?= $flyingSeqId2 > 0 ? 'underline decoration-slate-500/40 underline-offset-2' : '' ?>"><?= htmlspecialchars($flyingShipName2, ENT_QUOTES) ?></span>
                                        </div>
                                    <?php endif; ?>
                                    <?php if ($lostShipId2 > 0 && !$lostSameAsFlying2): ?>
                                        <div class="flex items-center gap-1 <?= $lostSeqId2 > 0 ? 'cursor-pointer hover:brightness-125 transition-all' : '' ?>"<?= $lostSeqId2 > 0 ? ' onclick="window._scKmModal(' . $lostSeqId2 . ')"' : '' ?>>
                                            <img class="w-4 h-4" src="https://images.evetech.net/types/<?= $lostShipId2 ?>/icon?size=32" loading="lazy">
                                            <span class="text-[11px] text-red-400 truncate max-w-[5rem] <?= $lostSeqId2 > 0 ? 'underline decoration-red-500/30 underline-offset-2' : '' ?>"><?= htmlspecialchars($lostShipName2, ENT_QUOTES) ?></span>
                                            <?php if ($lostShipCount2 > 1): ?>
                                                <span class="text-[10px] text-red-500">&times;<?= $lostShipCount2 ?></span>
                                            <?php endif; ?>
                                        </div>
                                    <?php elseif ($lostSameAsFlying2 && $hasDeath): ?>
                                        <div class="flex items-center gap-1 opacity-70 <?= $lostSeqId2 > 0 ? 'cursor-pointer hover:brightness-125 transition-all' : '' ?>"<?= $lostSeqId2 > 0 ? ' onclick="window._scKmModal(' . $lostSeqId2 . ')"' : '' ?>>
                                            <span class="text-[10px] text-red-500/60 <?= $lostSeqId2 > 0 ? 'underline decoration-red-500/30 underline-offset-2' : '' ?>">&darr; lost<?= $lostShipCount2 > 1 ? ' &times;' . $lostShipCount2 : '' ?></span>
                                        </div>
                                    <?php endif; ?>
                                    <?php
                                        $podIsk2 = 0.0;
                                        $wasPodded2 = false;
                                        foreach ($lostDetail2 as $_pe2) {
                                            if (in_array((int) ($_pe2['ship_type_id'] ?? 0), [670, 33328], true)) {
                                                $podIsk2 += (float) ($_pe2['isk_lost'] ?? 0);
                                                $wasPodded2 = true;
                                            }
                                        }
                                    ?>
                                    <?php if ($wasPodded2): ?>
                                        <div class="flex items-center gap-1 opacity-80" title="Capsule also destroyed">
                                            <img class="w-3 h-3 flex-shrink-0 opacity-50" src="https://images.evetech.net/types/670/icon?size=32" loading="lazy">
                                            <span class="text-[10px] text-orange-400/80">+ Pod<?= $podIsk2 > 0 ? ' (' . supplycore_format_isk($podIsk2) . ')' : '' ?></span>
                                        </div>
                                    <?php endif; ?>
                                </div>
                            </td>
                            <td class="px-3 py-2">
                                <?php
                                    $roleGlowCls2 = $fleetRole === 'fc'
                                        ? 'border border-[rgba(204,255,0,0.2)] text-[#ccff00] shadow-[0_0_5px_2px_rgba(204,255,0,0.7)] [text-shadow:0_0_3px_rgb(204,255,0)]'
                                        : '';
                                ?>
                                <span class="inline-flex items-center rounded-full px-[6px] py-[4px] text-[8px] font-semibold uppercase tracking-[0.12em] leading-none <?= fleet_function_color_class($fleetRole) ?> <?= $roleGlowCls2 ?>">
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
                                <?php if ($kmSeqId2 > 0): ?>
                                    <a class="text-red-400 text-sm cursor-pointer hover:text-red-300 transition-colors" onclick="window._scKmModal(<?= $kmSeqId2 ?>)">Killmail</a>
                                <?php else: ?>
                                    <a class="text-accent text-sm" href="/battle-intelligence/character.php?character_id=<?= (int) ($p['character_id'] ?? 0) ?>">Intel</a>
                                <?php endif; ?>
                            </td>
                        </tr>
                    <?php endforeach; ?>
                    <?php
                        // Structure rows in single-column view — filter by side if active
                        $filteredStructures = array_values(array_filter(
                            $structureKills,
                            static function (array $sk) use ($sideFilter, $classifyAlliance): bool {
                                if ($sideFilter === null) return true;
                                return $classifyAlliance((int) ($sk['victim_alliance_id'] ?? 0), (int) ($sk['victim_corporation_id'] ?? 0)) === $sideFilter;
                            }
                        ));
                    ?>
                    <?php foreach ($filteredStructures as $sk): ?>
                        <?php
                            $skAllianceId = (int) ($sk['victim_alliance_id'] ?? 0);
                            $skCorpId     = (int) ($sk['victim_corporation_id'] ?? 0);
                            $skShipTypeId = (int) ($sk['victim_ship_type_id'] ?? 0);
                            $skIskLost    = (float) ($sk['isk_lost'] ?? 0);
                            $skKmId3      = (int) ($sk['killmail_id'] ?? 0);
                            $skSeqId3     = (int) ($sk['sequence_id'] ?? 0);
                            if ($skSeqId3 === 0 && $skKmId3 > 0 && isset($killmailSeqLookup[$skKmId3])) {
                                $skSeqId3 = $killmailSeqLookup[$skKmId3];
                            }
                            $skShipName   = $skShipTypeId > 0 ? (string) ($shipTypeNames[$skShipTypeId] ?? 'Structure #' . $skShipTypeId) : 'Unknown Structure';
                            $skSide       = $classifyAlliance($skAllianceId, $skCorpId);
                            $skSideClass  = $sideColorClass[$skSide] ?? 'text-slate-300';
                            $skOrgName    = '';
                            if ($skCorpId > 0) {
                                $skOrgName = killmail_entity_preferred_name($resolvedEntities, 'corporation', $skCorpId, (string) ($sk['corporation_name'] ?? ''), 'Corp');
                            }
                            if (($skOrgName === '' || str_starts_with($skOrgName, 'Corp #')) && $skAllianceId > 0) {
                                $skOrgName = killmail_entity_preferred_name($resolvedEntities, 'alliance', $skAllianceId, (string) ($sk['alliance_name'] ?? ''), 'Alliance');
                            }
                            if ($skOrgName === '') $skOrgName = 'Unknown';
                        ?>
                        <tr class="border-b border-border/50 border-l-2 border-l-orange-500/40 bg-orange-950/5">
                            <td class="px-3 py-2">
                                <div class="flex items-center gap-1.5">
                                    <span class="text-orange-500/70 text-sm leading-none flex-shrink-0" title="Structure">&#x1F3DB;</span>
                                    <span class="text-sm text-slate-400 truncate" title="<?= htmlspecialchars($skOrgName, ENT_QUOTES) ?>"><?= htmlspecialchars($skOrgName, ENT_QUOTES) ?></span>
                                    <span class="inline-flex items-center justify-center w-3.5 h-3.5 rounded-full bg-orange-950 ring-1 ring-orange-500/60 text-[8px] text-orange-400 flex-shrink-0" title="Structure destroyed">&#x2715;</span>
                                    <span class="inline-block rounded-full px-1.5 py-0.5 text-[9px] uppercase tracking-wider ml-1 <?= $sideBgClass[$skSide] ?? 'bg-slate-700' ?> <?= $skSideClass ?>">
                                        <?= htmlspecialchars($sideLabels[$skSide] ?? $skSide, ENT_QUOTES) ?>
                                    </span>
                                </div>
                            </td>
                            <td class="px-3 py-2 text-slate-300 text-xs">
                                <div class="flex items-center gap-1.5">
                                    <?php if ($skAllianceId > 0): ?>
                                        <img src="https://images.evetech.net/alliances/<?= $skAllianceId ?>/logo?size=64" alt="" class="w-5 h-5" loading="lazy">
                                        <span class="text-slate-100"><?= htmlspecialchars($skOrgName, ENT_QUOTES) ?></span>
                                    <?php endif; ?>
                                </div>
                            </td>
                            <td class="px-3 py-2">
                                <div class="flex items-center gap-1">
                                    <?php if ($skShipTypeId > 0): ?>
                                        <img class="w-4 h-4" src="https://images.evetech.net/types/<?= $skShipTypeId ?>/icon?size=32" loading="lazy">
                                    <?php endif; ?>
                                    <span class="text-[11px] text-orange-300/80 truncate max-w-[8rem]"><?= htmlspecialchars($skShipName, ENT_QUOTES) ?></span>
                                </div>
                            </td>
                            <td class="px-3 py-2">
                                <span class="inline-flex items-center rounded-full px-[6px] py-[4px] text-[8px] font-semibold uppercase tracking-[0.12em] leading-none bg-orange-950/60 border border-orange-500/30 text-orange-400">Structure</span>
                            </td>
                            <td class="px-3 py-2 text-right text-xs text-slate-600">&mdash;</td>
                            <td class="px-3 py-2 text-right text-xs text-slate-600">&mdash;</td>
                            <td class="px-3 py-2 text-right text-xs text-slate-600">&mdash;</td>
                            <td class="px-3 py-2 text-right text-xs <?= $skIskLost > 0 ? 'text-red-300' : 'text-slate-500' ?>"><?= $skIskLost > 0 ? supplycore_format_isk($skIskLost) : '&mdash;' ?></td>
                            <td class="px-3 py-2 text-right text-xs text-slate-600">&mdash;</td>
                            <td class="px-3 py-2 text-right">
                                <?php if ($skSeqId3 > 0): ?>
                                    <a class="text-red-400 text-sm cursor-pointer hover:text-red-300 transition-colors" onclick="window._scKmModal(<?= $skSeqId3 ?>)">Killmail</a>
                                <?php else: ?>
                                    <span class="text-xs text-slate-600">&mdash;</span>
                                <?php endif; ?>
                            </td>
                        </tr>
                    <?php endforeach; ?>
                <?php endif; ?>
            </tbody>
        </table>
    </div>
<?php endif; ?>
</section>

<!-- Killmail detail modal — appended to <body> via JS to escape stacking context -->
<template id="sc-km-modal-tpl">
<div id="sc-km-modal-overlay" class="hidden" style="position:fixed;inset:0;z-index:99999;margin:0;padding:0;border:0;">
    <div style="position:absolute;inset:0;background:rgba(0,0,0,0.7);backdrop-filter:blur(4px);" onclick="window._scKmModalClose()"></div>
    <div id="sc-km-modal-panel" style="position:absolute;right:0;top:0;bottom:0;width:28rem;max-width:100vw;background:#0b1120;overflow-y:auto;border-left:1px solid rgba(100,116,139,0.4);box-shadow:-8px 0 30px rgba(0,0,0,0.5);">
        <div style="position:sticky;top:0;z-index:10;display:flex;align-items:center;justify-content:space-between;gap:0.75rem;padding:0.75rem 1.25rem;border-bottom:1px solid rgba(100,116,139,0.4);background:rgba(11,17,32,0.97);backdrop-filter:blur(4px);">
            <h3 class="text-sm font-semibold text-slate-50 tracking-wide uppercase">Killmail Detail</h3>
            <button onclick="window._scKmModalClose()" class="text-slate-400 hover:text-slate-100 transition-colors text-lg leading-none px-1">&times;</button>
        </div>
        <div id="sc-km-modal-body" class="p-5">
            <div class="flex items-center justify-center py-12">
                <div class="w-5 h-5 border-2 border-blue-500/40 border-t-blue-400 rounded-full animate-spin"></div>
            </div>
        </div>
    </div>
</div>
</template>

<script>
(function() {
    // Move modal from <template> to <body> so it escapes all stacking contexts
    var tpl = document.getElementById('sc-km-modal-tpl');
    if (tpl) {
        document.body.appendChild(tpl.content.cloneNode(true));
        tpl.remove();
    }
    var overlay = document.getElementById('sc-km-modal-overlay');
    var body = document.getElementById('sc-km-modal-body');
    var panel = document.getElementById('sc-km-modal-panel');
    var cache = {};

    function fmtIsk(v) {
        if (v == null) return '—';
        if (v >= 1e9) return (v / 1e9).toFixed(1) + 'B';
        if (v >= 1e6) return (v / 1e6).toFixed(1) + 'M';
        if (v >= 1e3) return (v / 1e3).toFixed(0) + 'k';
        return v.toFixed(0);
    }

    function esc(s) {
        var el = document.createElement('span');
        el.textContent = s || '';
        return el.innerHTML;
    }

    function renderKm(d) {
        var ship = d.ship || {};
        var victim = d.victim || {};
        var loc = d.location || {};
        var attackers = d.top_attackers || [];
        var fb = d.final_blow;

        var html = '';

        // Ship render + name
        if (ship.render_url) {
            html += '<div class="rounded-xl overflow-hidden bg-slate-800/50 mb-4" style="max-height:12rem;">';
            html += '<img src="' + esc(ship.render_url) + '" alt="' + esc(ship.name) + '" class="w-full object-contain" style="max-height:12rem;" loading="eager">';
            html += '</div>';
        }

        // Ship info header
        html += '<div class="mb-4">';
        html += '<p class="text-xs uppercase tracking-[0.2em] text-muted">Loss</p>';
        html += '<h2 class="mt-1 text-2xl font-semibold text-slate-50">' + esc(ship.name || 'Unknown Ship') + '</h2>';
        if (ship['class']) html += '<p class="mt-1 text-sm text-slate-400">' + esc(ship['class']);
        if (ship.hull_price != null) html += ' &middot; Hull: ' + fmtIsk(ship.hull_price);
        if (ship['class']) html += '</p>';
        html += '</div>';

        // Value + Time + Location
        html += '<div class="grid grid-cols-3 gap-3 mb-4">';
        html += '<div class="surface-tertiary"><p class="text-[10px] uppercase tracking-wider text-muted">Value</p><p class="mt-1 text-sm font-semibold text-slate-50">' + esc(d.value || '—') + '</p></div>';
        html += '<div class="surface-tertiary"><p class="text-[10px] uppercase tracking-wider text-muted">Time</p><p class="mt-1 text-sm font-semibold text-slate-50">' + esc(d.killmail_time || '—') + '</p></div>';
        html += '<div class="surface-tertiary"><p class="text-[10px] uppercase tracking-wider text-muted">System</p><p class="mt-1 text-sm font-semibold text-slate-50">' + esc(loc.system || '—') + '</p><p class="text-[10px] text-muted">' + esc(loc.region || '') + '</p></div>';
        html += '</div>';

        // Victim
        html += '<div class="surface-tertiary mb-4">';
        html += '<p class="text-[10px] uppercase tracking-wider text-muted mb-2">Victim</p>';
        html += '<div class="flex items-center gap-3">';
        if (victim.character_id) {
            html += '<img src="https://images.evetech.net/characters/' + victim.character_id + '/portrait?size=64" alt="" class="w-10 h-10 rounded-lg">';
        }
        html += '<div>';
        html += '<p class="text-sm font-medium text-slate-50">' + esc(victim.character_name || 'Unknown') + '</p>';
        html += '<p class="text-xs text-slate-400">' + esc(victim.corporation_display || '') + (victim.alliance_display ? ' / ' + esc(victim.alliance_display) : '') + '</p>';
        html += '<p class="text-xs text-muted mt-0.5">Damage taken: ' + esc(victim.damage_taken || '0') + '</p>';
        html += '</div></div></div>';

        // Final blow
        if (fb) {
            html += '<div class="surface-tertiary mb-4">';
            html += '<p class="text-[10px] uppercase tracking-wider text-muted mb-2">Final blow</p>';
            html += '<div class="flex items-center gap-3">';
            if (fb.character_id) {
                html += '<img src="https://images.evetech.net/characters/' + fb.character_id + '/portrait?size=64" alt="" class="w-8 h-8 rounded-lg">';
            }
            html += '<div class="flex-1 min-w-0">';
            html += '<p class="text-sm text-slate-100">' + esc(fb.character_name || 'Unknown') + '</p>';
            html += '<p class="text-xs text-slate-400">' + esc(fb.corporation_display || '') + '</p>';
            html += '</div>';
            if (fb.ship_type_id) {
                html += '<div class="flex items-center gap-1">';
                html += '<img class="w-5 h-5" src="https://images.evetech.net/types/' + fb.ship_type_id + '/icon?size=32">';
                html += '<span class="text-xs text-slate-400">' + esc(fb.ship_display || '') + '</span>';
                html += '</div>';
            }
            html += '</div></div>';
        }

        // Top attackers
        if (attackers.length > 0) {
            html += '<div class="surface-tertiary mb-4">';
            html += '<p class="text-[10px] uppercase tracking-wider text-muted mb-2">Top attackers <span class="text-slate-500">(' + d.attacker_count + ' total)</span></p>';
            html += '<div class="space-y-1.5">';
            for (var i = 0; i < attackers.length; i++) {
                var a = attackers[i];
                html += '<div class="flex items-center gap-2 text-xs">';
                if (a.character_id) {
                    html += '<img src="https://images.evetech.net/characters/' + a.character_id + '/portrait?size=32" alt="" class="w-5 h-5 rounded-full flex-shrink-0">';
                } else {
                    html += '<span class="w-5 h-5 rounded-full bg-slate-700 flex-shrink-0"></span>';
                }
                html += '<span class="text-slate-200 truncate">' + esc(a.character_name || 'Unknown') + '</span>';
                if (a.ship_type_id) {
                    html += '<img class="w-4 h-4 flex-shrink-0 ml-auto" src="https://images.evetech.net/types/' + a.ship_type_id + '/icon?size=32">';
                }
                html += '<span class="text-slate-500 flex-shrink-0 min-w-[3rem] text-right">' + a.damage_done.toLocaleString() + '</span>';
                if (a.final_blow) html += '<span class="text-[9px] text-yellow-400 flex-shrink-0">FB</span>';
                html += '</div>';
            }
            html += '</div></div>';
        }

        // Fitted modules / items
        var items = d.items || {};
        var roleOrder = ['fitted', 'destroyed', 'dropped'];
        var roleColors = {fitted: 'text-slate-300', destroyed: 'text-red-400', dropped: 'text-emerald-400'};
        var roleBorders = {fitted: 'border-slate-600/30', destroyed: 'border-red-500/20', dropped: 'border-emerald-500/20'};
        var hasAnyItems = false;
        for (var ri = 0; ri < roleOrder.length; ri++) {
            var rk = roleOrder[ri];
            if (items[rk] && items[rk].rows && items[rk].rows.length > 0) { hasAnyItems = true; break; }
        }
        if (hasAnyItems) {
            html += '<div class="mb-4">';
            html += '<p class="text-[10px] uppercase tracking-[0.2em] text-muted mb-2">Modules & cargo</p>';
            for (var ri = 0; ri < roleOrder.length; ri++) {
                var rk = roleOrder[ri];
                var group = items[rk];
                if (!group || !group.rows || group.rows.length === 0) continue;
                var clr = roleColors[rk] || 'text-slate-300';
                var bdr = roleBorders[rk] || 'border-slate-600/30';
                html += '<details open class="mb-2 rounded-lg border ' + bdr + ' bg-black/20 overflow-hidden">';
                var groupValLabel = group.total_value != null ? ' &middot; ' + fmtIsk(group.total_value) : '';
                html += '<summary class="flex items-center justify-between gap-2 px-3 py-2 cursor-pointer select-none hover:bg-slate-800/40 transition-colors">';
                html += '<span class="text-xs font-medium ' + clr + '">' + esc(group.label || rk) + '</span>';
                html += '<span class="text-[10px] text-muted rounded-full border border-border bg-black/20 px-2 py-0.5">' + group.total + ' items' + groupValLabel + '</span>';
                html += '</summary>';
                html += '<div class="px-3 pb-2 space-y-1">';
                for (var ii = 0; ii < group.rows.length; ii++) {
                    var item = group.rows[ii];
                    html += '<div style="display:grid;grid-template-columns:1.25rem 1fr 3rem 3.5rem;align-items:center;gap:0.375rem;padding:0.2rem 0;font-size:0.75rem;">';
                    if (item.type_id > 0) {
                        html += '<img style="width:1.25rem;height:1.25rem;background:rgba(0,0,0,0.3);padding:1px;border-radius:0.25rem;" src="https://images.evetech.net/types/' + item.type_id + '/icon?size=32" loading="lazy">';
                    } else {
                        html += '<span></span>';
                    }
                    html += '<span style="color:#e2e8f0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">' + esc(item.name) + '</span>';
                    html += '<span style="font-size:0.625rem;color:#64748b;text-align:right;white-space:nowrap;">' + (item.quantity > 1 ? '&times;' + item.quantity.toLocaleString() : '') + '</span>';
                    html += '<span style="font-size:0.625rem;color:rgba(234,179,8,0.7);text-align:right;white-space:nowrap;">' + (item.total_price != null ? fmtIsk(item.total_price) : '') + '</span>';
                    html += '</div>';
                }
                html += '</div></details>';
            }
            html += '</div>';
        }

        // Links
        html += '<div class="flex gap-2 mt-4">';
        html += '<a href="' + esc(d.detail_url || '#') + '" class="flex-1 text-center text-xs px-3 py-2 rounded-lg bg-blue-600/20 border border-blue-500/30 text-blue-300 hover:bg-blue-600/30 transition-colors">Full detail</a>';
        html += '<a href="' + esc(d.zkb_url || '#') + '" target="_blank" rel="noopener" class="flex-1 text-center text-xs px-3 py-2 rounded-lg bg-slate-700/50 border border-slate-600/30 text-slate-300 hover:bg-slate-700/80 transition-colors">zKillboard</a>';
        html += '</div>';

        return html;
    }

    window._scKmModal = function(seqId) {
        overlay.classList.remove('hidden');
        document.body.style.overflow = 'hidden';

        if (cache[seqId]) {
            body.innerHTML = renderKm(cache[seqId]);
            return;
        }

        body.innerHTML = '<div class="flex items-center justify-center py-12"><div class="w-5 h-5 border-2 border-blue-500/40 border-t-blue-400 rounded-full animate-spin"></div></div>';

        fetch('/api/killmail-summary.php?sequence_id=' + seqId)
            .then(function(r) { return r.json(); })
            .then(function(d) {
                if (d.error) {
                    body.innerHTML = '<div class="text-sm text-red-400 py-8 text-center">' + esc(d.error) + '</div>';
                    return;
                }
                cache[seqId] = d;
                body.innerHTML = renderKm(d);
            })
            .catch(function(err) {
                body.innerHTML = '<div class="text-sm text-red-400 py-8 text-center">Failed to load killmail.</div>';
            });
    };

    window._scKmModalClose = function() {
        overlay.classList.add('hidden');
        document.body.style.overflow = '';
    };

    document.addEventListener('keydown', function(e) {
        if (e.key === 'Escape' && !overlay.classList.contains('hidden')) {
            window._scKmModalClose();
        }
    });
})();
</script>
