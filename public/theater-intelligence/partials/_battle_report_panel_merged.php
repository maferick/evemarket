<div class="coalition-panel rounded-lg overflow-hidden border <?= $panelBorderColor ?>">
    <div class="<?= $panelHeaderBg ?> px-4 py-3 flex items-center justify-between border-b <?= $panelHeaderBorder ?>">
        <h3 class="text-sm font-semibold <?= $panelTextColor ?>"><?= $panelLabel ?><?= $panelHeaderExtra ?></h3>
        <span class="text-[10px] uppercase tracking-wider <?= $panelBadgeBg ?> <?= $panelTextColor ?> rounded-full px-1.5 py-0.5 ml-1"><?= $panelBadge ?></span>
    </div>
    <div class="px-4 pt-3">
        <p class="mb-3 text-[11px] uppercase tracking-[0.08em] text-muted"><?= $panelSubtitle ?></p>
    </div>

    <div class="grid grid-cols-2 divide-x divide-y divide-white/5 border-b border-white/5 text-sm">
        <div class="px-4 py-3">
            <p class="text-xs text-muted">Unique Pilots</p>
            <p class="text-slate-100 font-semibold"><?= number_format((int) ($panelData['pilots'] ?? 0)) ?></p>
        </div>
        <div class="px-4 py-3">
            <p class="text-xs text-muted">ISK Efficiency</p>
            <p class="text-slate-100 font-semibold"><?= number_format(($panelData['efficiency'] ?? 0) * 100, 1) ?>%</p>
        </div>
        <div class="px-4 py-3">
            <p class="text-xs text-muted">Final Blows / Losses</p>
            <p class="text-slate-100 font-semibold"><?= number_format((int) ($panelData['final_blows'] ?? 0)) ?> / <?= number_format((int) ($panelData['losses'] ?? 0)) ?></p>
            <p class="text-[10px] text-muted">Kill involvements: <?= number_format((int) ($panelData['kill_involvements'] ?? 0)) ?></p>
        </div>
        <div class="px-4 py-3">
            <p class="text-xs text-muted">ISK Killed / Lost</p>
            <p class="text-slate-100 font-semibold"><?= supplycore_format_isk((float) ($panelData['isk_killed'] ?? 0)) ?> / <?= supplycore_format_isk((float) ($panelData['isk_lost'] ?? 0)) ?></p>
        </div>
    </div>

    <?php $_opAlliances = $panelData['alliances'] ?? []; ?>
    <?php if ($_opAlliances): ?>
        <div class="divide-y divide-white/5">
            <p class="text-[10px] uppercase tracking-wider text-muted px-4 py-2"><?= $panelAlliancesLabel ?></p>
            <?php foreach ($_opAlliances as $allianceRow): ?>
                <?php $allianceId = (int) ($allianceRow['alliance_id'] ?? 0); ?>
                <?php $corporationId = (int) ($allianceRow['corporation_id'] ?? 0); ?>
                <div class="flex items-center gap-2.5 px-4 py-2">
                    <?php if ($allianceId > 0): ?>
                        <img src="https://images.evetech.net/alliances/<?= $allianceId ?>/logo?size=64" alt="" class="w-5 h-5 rounded-sm" loading="lazy">
                    <?php elseif ($corporationId > 0): ?>
                        <img src="https://images.evetech.net/corporations/<?= $corporationId ?>/logo?size=64" alt="" class="w-5 h-5 rounded-sm" loading="lazy">
                    <?php endif; ?>
                    <span class="text-xs text-slate-200 flex-1 truncate"><?= htmlspecialchars((string) ($allianceRow['name'] ?? ''), ENT_QUOTES) ?></span>
                    <span class="text-xs text-muted"><?= number_format((int) ($allianceRow['pilots'] ?? 0)) ?> pilots</span>
                </div>
            <?php endforeach; ?>
        </div>
    <?php endif; ?>

    <?php if (!empty($panelThirdPartyAlliances)): ?>
        <div class="divide-y divide-white/5 border-t border-slate-700/50">
            <p class="text-[10px] uppercase tracking-wider text-slate-400 px-4 py-2">Third Party</p>
            <?php foreach ($panelThirdPartyAlliances as $allianceRow): ?>
                <?php $allianceId = (int) ($allianceRow['alliance_id'] ?? 0); ?>
                <?php $corporationId = (int) ($allianceRow['corporation_id'] ?? 0); ?>
                <div class="flex items-center gap-2.5 px-4 py-2">
                    <?php if ($allianceId > 0): ?>
                        <img src="https://images.evetech.net/alliances/<?= $allianceId ?>/logo?size=64" alt="" class="w-5 h-5 rounded-sm" loading="lazy">
                    <?php elseif ($corporationId > 0): ?>
                        <img src="https://images.evetech.net/corporations/<?= $corporationId ?>/logo?size=64" alt="" class="w-5 h-5 rounded-sm" loading="lazy">
                    <?php endif; ?>
                    <span class="text-xs text-slate-400 flex-1 truncate"><?= htmlspecialchars((string) ($allianceRow['name'] ?? ''), ENT_QUOTES) ?></span>
                    <span class="text-xs text-muted"><?= number_format((int) ($allianceRow['pilots'] ?? 0)) ?> pilots</span>
                </div>
            <?php endforeach; ?>
        </div>
    <?php endif; ?>

    <?php $_panelShips = $panelData['ships'] ?? []; ?>
    <?php if ($_panelShips): ?>
        <div class="mt-3 border-t border-slate-700/50 px-4 py-2">
            <p class="text-[10px] uppercase tracking-wider text-muted mb-1">Top Hulls (by appearances)</p>
            <div class="flex flex-wrap gap-2">
                <?php foreach (array_slice($_panelShips, 0, 12) as $ship): ?>
                    <div class="flex flex-col items-center gap-1 bg-slate-800/50 border border-white/6 rounded-md px-2 py-2 min-w-[72px]">
                        <?php if (($ship['type_id'] ?? 0) > 0): ?>
                            <img src="https://images.evetech.net/types/<?= (int) $ship['type_id'] ?>/render?size=64" alt="" class="w-12 h-12 object-contain" loading="lazy">
                        <?php endif; ?>
                        <span class="text-[11px] text-slate-300 text-center leading-tight max-w-[68px] truncate"><?= htmlspecialchars((string) ($ship['name'] ?? ''), ENT_QUOTES) ?></span>
                        <span class="text-[10px] text-slate-500 font-semibold">x<?= (int) ($ship['pilots'] ?? 0) ?></span>
                    </div>
                <?php endforeach; ?>
            </div>
        </div>
    <?php endif; ?>
</div>
