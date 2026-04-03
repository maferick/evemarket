<?php
$killmails = (array) ($killmails ?? []);
if ($killmails === []) return;

// Classify killmails by side using victim's alliance/corporation
$killmailsBySide = ['friendly' => [], 'opponent' => [], 'third_party' => []];

// Build a character_id → participant lookup for side classification
$charParticipantMap = [];
foreach ($participants as $p) {
    $cid = (int) ($p['character_id'] ?? 0);
    if ($cid > 0) {
        $charParticipantMap[$cid] = $p;
    }
}

foreach ($killmails as $km) {
    $victimCharId = (int) ($km['victim_character_id'] ?? 0);
    $p = $charParticipantMap[$victimCharId] ?? null;
    if ($p !== null) {
        $side = $classifyAlliance((int) ($p['alliance_id'] ?? 0), (int) ($p['corporation_id'] ?? 0));
    } else {
        $side = 'third_party';
    }
    $killmailsBySide[$side][] = $km;
}

$totalKillmails = count($killmails);
$friendlyLosses = count($killmailsBySide['friendly']);
$hostileLosses = count($killmailsBySide['opponent']);
$thirdPartyLosses = count($killmailsBySide['third_party']);
?>
<section class="surface-primary mt-4">
    <h2 class="text-lg font-semibold text-slate-50">Killmails</h2>
    <p class="text-xs text-muted mt-1">
        <?= number_format($totalKillmails) ?> killmails &mdash;
        <span class="text-blue-300"><?= number_format($friendlyLosses) ?> friendly losses</span> ·
        <span class="text-red-300"><?= number_format($hostileLosses) ?> hostile losses</span>
        <?php if ($thirdPartyLosses > 0): ?>
            · <span class="text-amber-300"><?= number_format($thirdPartyLosses) ?> third party losses</span>
        <?php endif; ?>
    </p>

    <details class="mt-3" open>
        <summary class="cursor-pointer text-sm text-slate-100">All Killmails</summary>
        <div class="mt-2 table-shell">
            <table class="table-ui text-xs">
                <thead>
                    <tr class="border-b border-border/70 text-muted uppercase tracking-wider text-[10px]">
                        <th class="px-2 py-1.5 text-left">Victim</th>
                        <th class="px-2 py-1.5 text-left">Ship</th>
                        <th class="px-2 py-1.5 text-left">Side</th>
                        <th class="px-2 py-1.5 text-right"></th>
                    </tr>
                </thead>
                <tbody>
                    <?php foreach ($killmails as $km): ?>
                        <?php
                            $seqId = (int) ($km['sequence_id'] ?? 0);
                            $victimCharId = (int) ($km['victim_character_id'] ?? 0);
                            $victimName = (string) ($km['victim_character_name'] ?? 'Unknown');
                            $shipName = (string) ($km['victim_ship_name'] ?? 'Unknown');
                            $shipTypeId = (int) ($km['victim_ship_type_id'] ?? 0);

                            $p = $charParticipantMap[$victimCharId] ?? null;
                            $kmSide = $p !== null ? $classifyAlliance((int) ($p['alliance_id'] ?? 0), (int) ($p['corporation_id'] ?? 0)) : 'third_party';
                            $sideColor = match ($kmSide) {
                                'friendly' => 'text-blue-300',
                                'opponent' => 'text-red-300',
                                default => 'text-amber-300',
                            };
                            $sideBadge = match ($kmSide) {
                                'friendly' => ['bg-blue-900/60 text-blue-300', $sideLabels['friendly'] ?? 'Friendly'],
                                'opponent' => ['bg-red-900/60 text-red-300', $sideLabels['opponent'] ?? 'Hostile'],
                                default => ['bg-amber-900/60 text-amber-300', $sideLabels['third_party'] ?? 'Third Party'],
                            };
                        ?>
                        <tr class="border-b border-border/50">
                            <td class="px-2 py-1.5 text-slate-100">
                                <div class="flex items-center gap-1.5">
                                    <?php if ($victimCharId > 0): ?>
                                        <img src="https://images.evetech.net/characters/<?= $victimCharId ?>/portrait?size=32" alt="" class="w-4 h-4 rounded-full" loading="lazy">
                                    <?php endif; ?>
                                    <?= proxy_e($victimName) ?>
                                </div>
                            </td>
                            <td class="px-2 py-1.5 text-slate-300">
                                <div class="flex items-center gap-1.5">
                                    <?php if ($shipTypeId > 0): ?>
                                        <img src="https://images.evetech.net/types/<?= $shipTypeId ?>/render?size=32" alt="" class="w-4 h-4 rounded" loading="lazy">
                                    <?php endif; ?>
                                    <?= proxy_e($shipName) ?>
                                </div>
                            </td>
                            <td class="px-2 py-1.5">
                                <span class="inline-block rounded-full px-1.5 py-0.5 text-[9px] uppercase tracking-wider <?= $sideBadge[0] ?>"><?= proxy_e($sideBadge[1]) ?></span>
                            </td>
                            <td class="px-2 py-1.5 text-right">
                                <?php if ($seqId > 0): ?>
                                    <a href="killmail?sequence_id=<?= $seqId ?>" class="text-blue-400 hover:text-blue-300">View &rarr;</a>
                                <?php endif; ?>
                            </td>
                        </tr>
                    <?php endforeach; ?>
                </tbody>
            </table>
        </div>
    </details>
</section>
