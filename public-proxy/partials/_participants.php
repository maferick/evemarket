<?php if ($participants !== []): ?>
<?php
    // Split participants by side
    $friendlyParticipants = [];
    $enemyParticipants = [];
    $thirdPartyParticipants = [];
    foreach ($participants as $p) {
        $pSide = $classifyAlliance((int) ($p['alliance_id'] ?? 0), (int) ($p['corporation_id'] ?? 0));
        if ($pSide === 'friendly') {
            $friendlyParticipants[] = $p;
        } elseif ($pSide === 'opponent') {
            $enemyParticipants[] = $p;
        } else {
            $thirdPartyParticipants[] = $p;
        }
    }

    // Detect 2 vs 3 column mode
    $_proxyHasOpponent = $enemyParticipants !== [];
    $_proxyHasThirdParty = $thirdPartyParticipants !== [];
    $_proxyIsThreeCol = $_proxyHasOpponent && $_proxyHasThirdParty;

    // Helper to get flying ship name
    $flyingShipName = static function (array $p) use ($shipTypeNames): string {
        $podIds = [670, 33328];
        $flyingId = (int) ($p['flying_ship_type_id'] ?? 0);
        if ($flyingId > 0 && !in_array($flyingId, $podIds, true)) {
            return (string) ($shipTypeNames[$flyingId] ?? '');
        }
        $shipJson = $p['ship_type_ids'] ?? null;
        if (!is_string($shipJson)) return '';
        $decoded = json_decode($shipJson, true);
        if (!is_array($decoded) || $decoded === []) return '';
        foreach ($decoded as $stid) {
            $stid = (int) $stid;
            if ($stid > 0 && !in_array($stid, $podIds, true)) {
                return (string) ($shipTypeNames[$stid] ?? '');
            }
        }
        return '';
    };

    // Build panel sets
    if ($_proxyIsThreeCol) {
        $proxyPanelSets = [
            ['label' => $sideLabels['friendly'] ?? 'Friendlies', 'rows' => $friendlyParticipants, 'colorClass' => 'text-blue-300', 'borderClass' => 'border-blue-500/30'],
            ['label' => $sideLabels['opponent'] ?? 'Opposition', 'rows' => $enemyParticipants, 'colorClass' => 'text-red-300', 'borderClass' => 'border-red-500/30'],
            ['label' => $sideLabels['third_party'] ?? 'Third Party', 'rows' => $thirdPartyParticipants, 'colorClass' => 'text-amber-300', 'borderClass' => 'border-amber-500/30'],
        ];
    } else {
        $hostileParticipants = array_merge($enemyParticipants, $thirdPartyParticipants);
        $hostileLabel = ($sideLabels['opponent'] ?? 'Opposition');
        if ($_proxyHasThirdParty) $hostileLabel .= ' + Third Party';
        $proxyPanelSets = [
            ['label' => $sideLabels['friendly'] ?? 'Friendlies', 'rows' => $friendlyParticipants, 'colorClass' => 'text-blue-300', 'borderClass' => 'border-blue-500/30'],
            ['label' => $hostileLabel, 'rows' => $hostileParticipants, 'colorClass' => 'text-red-300', 'borderClass' => 'border-red-500/30'],
        ];
    }

    $_proxyGridCols = $_proxyIsThreeCol ? 'lg:grid-cols-3 md:grid-cols-2' : 'lg:grid-cols-2';
?>

<section class="surface-primary mt-4">
    <h2 class="text-lg font-semibold text-slate-50">Participants</h2>
    <p class="text-xs text-muted mt-1"><?= count($participants) ?> unique pilots across all sides.</p>

    <div class="mt-3 grid gap-4 <?= $_proxyGridCols ?>">
        <?php foreach ($proxyPanelSets as $proxyPanel): ?>
        <div class="border-t-2 <?= $proxyPanel['borderClass'] ?> pt-2">
            <h3 class="text-sm font-semibold <?= $proxyPanel['colorClass'] ?> mb-2">
                <?= proxy_e($proxyPanel['label']) ?>
                <span class="text-muted font-normal">(<?= count($proxyPanel['rows']) ?>)</span>
            </h3>
            <?php if ($proxyPanel['rows'] !== []): ?>
                <div class="table-shell">
                    <table class="table-ui text-xs">
                        <thead>
                            <tr class="border-b border-border/70 text-muted uppercase tracking-wider text-[10px]">
                                <th class="px-2 py-1.5 text-left">Pilot</th>
                                <th class="px-2 py-1.5 text-left">Ship</th>
                                <th class="px-2 py-1.5 text-right">Damage</th>
                                <th class="px-2 py-1.5 text-right">Kills</th>
                                <th class="px-2 py-1.5 text-right">Losses</th>
                            </tr>
                        </thead>
                        <tbody>
                            <?php foreach ($proxyPanel['rows'] as $p): ?>
                                <?php $charName = (string) ($p['character_name'] ?? 'Unknown'); ?>
                                <tr class="border-b border-border/50">
                                    <td class="px-2 py-1.5 text-slate-100">
                                        <?php if (($p['character_id'] ?? 0) > 0): ?>
                                            <div class="flex items-center gap-1.5">
                                                <img src="https://images.evetech.net/characters/<?= (int) $p['character_id'] ?>/portrait?size=32" alt="" class="w-4 h-4 rounded-full" loading="lazy">
                                                <?= proxy_e($charName) ?>
                                            </div>
                                        <?php else: ?>
                                            <?= proxy_e($charName) ?>
                                        <?php endif; ?>
                                    </td>
                                    <td class="px-2 py-1.5 text-slate-300"><?= proxy_e($flyingShipName($p)) ?></td>
                                    <td class="px-2 py-1.5 text-right"><?= number_format((int) ($p['damage_done'] ?? 0)) ?></td>
                                    <td class="px-2 py-1.5 text-right"><?= (int) ($p['kills'] ?? $p['kill_count'] ?? 0) ?></td>
                                    <td class="px-2 py-1.5 text-right"><?= (int) ($p['deaths'] ?? $p['loss_count'] ?? 0) ?></td>
                                </tr>
                            <?php endforeach; ?>
                        </tbody>
                    </table>
                </div>
            <?php else: ?>
                <p class="text-xs text-muted">No participants.</p>
            <?php endif; ?>
        </div>
        <?php endforeach; ?>
    </div>
</section>
<?php endif; ?>
