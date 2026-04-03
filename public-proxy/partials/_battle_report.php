<?php if ($allianceSummary !== []): ?>
<?php
    $ourPanel = $sidePanels['friendly'] ?? [];
    $enemyPanel = $sidePanels['opponent'] ?? [];
    $thirdPartyPanel = $sidePanels['third_party'] ?? [];

    $hasOpponent = ($enemyPanel['pilots'] ?? 0) > 0;
    $hasThirdParty = ($thirdPartyPanel['pilots'] ?? 0) > 0;
    $isThreeColumn = $hasOpponent && $hasThirdParty;

    $ourTotalIsk = ($ourPanel['isk_killed'] ?? 0) + ($ourPanel['isk_lost'] ?? 0);
    $ourEfficiency = $ourTotalIsk > 0 ? ($ourPanel['isk_killed'] ?? 0) / $ourTotalIsk : 0.0;

    $enemyTotalIsk = ($enemyPanel['isk_killed'] ?? 0) + ($enemyPanel['isk_lost'] ?? 0);
    $enemyEfficiency = $enemyTotalIsk > 0 ? ($enemyPanel['isk_killed'] ?? 0) / $enemyTotalIsk : 0.0;

    $tpTotalIsk = ($thirdPartyPanel['isk_killed'] ?? 0) + ($thirdPartyPanel['isk_lost'] ?? 0);
    $tpEfficiency = $tpTotalIsk > 0 ? ($thirdPartyPanel['isk_killed'] ?? 0) / $tpTotalIsk : 0.0;

    $enemyCombinedIskLost = ($enemyPanel['isk_lost'] ?? 0) + ($thirdPartyPanel['isk_lost'] ?? 0);
    $totalIskBothSides = ($ourPanel['isk_lost'] ?? 0) + $enemyCombinedIskLost;
    $ourBarPct = $totalIskBothSides > 0 ? ($enemyCombinedIskLost / $totalIskBothSides) * 100 : 50;

    if ($isThreeColumn) {
        $totalIskDestAll = ($ourPanel['isk_killed'] ?? 0) + ($enemyPanel['isk_killed'] ?? 0) + ($thirdPartyPanel['isk_killed'] ?? 0);
        $ourBarPct = $totalIskDestAll > 0 ? (($ourPanel['isk_killed'] ?? 0) / $totalIskDestAll) * 100 : 33.3;
        $enemyBarPct = $totalIskDestAll > 0 ? (($enemyPanel['isk_killed'] ?? 0) / $totalIskDestAll) * 100 : 33.3;
        $tpBarPct = 100 - $ourBarPct - $enemyBarPct;
    } else {
        $enemyBarPct = 100 - $ourBarPct;
        $enemyEfficiency = $hasOpponent ? $enemyEfficiency : $tpEfficiency;
    }

    $gridCols = $isThreeColumn ? 'md:grid-cols-3' : 'md:grid-cols-2';
?>
<section class="surface-primary mt-4">
    <!-- View Toggle -->
    <div class="flex items-center justify-end mb-3">
        <div class="inline-flex rounded-md border border-slate-700 overflow-hidden text-xs" role="group">
            <button type="button" id="sc-br-view-summary" class="px-3 py-1.5 font-medium transition-colors bg-slate-700 text-slate-100" onclick="window._scBrToggle('summary')">
                Summary
            </button>
            <button type="button" id="sc-br-view-classic" class="px-3 py-1.5 font-medium transition-colors bg-slate-800/60 text-slate-400 hover:text-slate-200" onclick="window._scBrToggle('classic')">
                Classic
            </button>
        </div>
    </div>

    <!-- Summary view (default) -->
    <div id="sc-br-summary">

    <!-- Efficiency Bar -->
    <div class="flex items-center gap-3 mb-3">
        <span class="text-xs font-semibold text-blue-300"><?= number_format($ourEfficiency * 100, 1) ?>%</span>
        <div class="flex-1 h-3 rounded-full overflow-hidden bg-slate-800 flex shadow-inner">
            <div class="bg-blue-500 h-full transition-all" style="width: <?= number_format($ourBarPct, 1) ?>%"></div>
            <div class="bg-red-500 h-full transition-all" style="width: <?= number_format($enemyBarPct, 1) ?>%"></div>
            <?php if ($isThreeColumn): ?>
                <div class="bg-amber-500 h-full transition-all" style="width: <?= number_format($tpBarPct, 1) ?>%"></div>
            <?php endif; ?>
        </div>
        <?php if ($isThreeColumn): ?>
            <span class="text-xs font-semibold text-red-300"><?= number_format($enemyEfficiency * 100, 1) ?>%</span>
            <span class="text-xs font-semibold text-amber-300"><?= number_format($tpEfficiency * 100, 1) ?>%</span>
        <?php else: ?>
            <span class="text-xs font-semibold text-red-300"><?= number_format($enemyEfficiency * 100, 1) ?>%</span>
        <?php endif; ?>
    </div>

    <!-- Dynamic column layout -->
    <div class="grid gap-4 <?= $gridCols ?>">
        <!-- Friendly panel -->
        <?php
            $_brp_data = $ourPanel;
            $_brp_label = proxy_e($sideLabels['friendly'] ?? 'Friendlies');
            $_brp_badge = 'Friendly';
            $_brp_border = 'border-blue-500/25';
            $_brp_header_bg = 'bg-blue-900/40';
            $_brp_header_border = 'border-blue-500/20';
            $_brp_text = 'text-blue-300';
            $_brp_badge_bg = 'bg-blue-900/60';
            $_brp_subtitle = 'Friendly coalition overview';
            $_brp_alliances_label = 'Alliances';
            $_brp_alliance_text = 'text-slate-200';
            $_brp_header_extra = '';
        ?>
        <?php include __DIR__ . '/_battle_report_panel.php'; ?>

        <!-- Opponent panel -->
        <?php if (!$isThreeColumn): ?>
            <?php
                $_brp_data = [
                    'pilots' => ($enemyPanel['pilots'] ?? 0) + ($thirdPartyPanel['pilots'] ?? 0),
                    'efficiency' => $enemyEfficiency,
                    'kills' => ($enemyPanel['kills'] ?? 0) + ($thirdPartyPanel['kills'] ?? 0),
                    'final_blows' => ($enemyPanel['final_blows'] ?? 0) + ($thirdPartyPanel['final_blows'] ?? 0),
                    'losses' => ($enemyPanel['losses'] ?? 0) + ($thirdPartyPanel['losses'] ?? 0),
                    'kill_involvements' => ($enemyPanel['kill_involvements'] ?? 0) + ($thirdPartyPanel['kill_involvements'] ?? 0),
                    'isk_killed' => ($enemyPanel['isk_killed'] ?? 0) + ($thirdPartyPanel['isk_killed'] ?? 0),
                    'isk_lost' => ($enemyPanel['isk_lost'] ?? 0) + ($thirdPartyPanel['isk_lost'] ?? 0),
                    'alliances' => $enemyPanel['alliances'] ?? [],
                    'ships' => (static function() use ($enemyPanel, $thirdPartyPanel) {
                        $merged = array_merge($enemyPanel['ships'] ?? [], $thirdPartyPanel['ships'] ?? []);
                        usort($merged, static fn(array $l, array $r): int => ($r['pilots'] ?? 0) <=> ($l['pilots'] ?? 0));
                        return array_slice($merged, 0, 12);
                    })(),
                ];
                $_brp_label = proxy_e($sideLabels['opponent'] ?? 'Opposition');
                $_brp_badge = 'Hostile';
                $_brp_border = 'border-red-500/25';
                $_brp_header_bg = 'bg-red-900/40';
                $_brp_header_border = 'border-red-500/20';
                $_brp_text = 'text-red-300';
                $_brp_badge_bg = 'bg-red-900/60';
                $_brp_subtitle = 'Opposition coalition overview';
                $_brp_alliances_label = 'Opponent Alliances';
                $_brp_alliance_text = 'text-slate-200';
                $_brp_header_extra = $hasThirdParty ? '<span class="text-slate-400 text-xs font-normal ml-1">+ Third Party</span>' : '';
                $_brp_tp_alliances = $thirdPartyPanel['alliances'] ?? [];
            ?>
            <?php include __DIR__ . '/_battle_report_panel_merged.php'; ?>
        <?php else: ?>
            <?php
                $_brp_data = $enemyPanel;
                $_brp_label = proxy_e($sideLabels['opponent'] ?? 'Opposition');
                $_brp_badge = 'Hostile';
                $_brp_border = 'border-red-500/25';
                $_brp_header_bg = 'bg-red-900/40';
                $_brp_header_border = 'border-red-500/20';
                $_brp_text = 'text-red-300';
                $_brp_badge_bg = 'bg-red-900/60';
                $_brp_subtitle = 'Opposition coalition overview';
                $_brp_alliances_label = 'Opponent Alliances';
                $_brp_alliance_text = 'text-slate-200';
                $_brp_header_extra = '';
            ?>
            <?php include __DIR__ . '/_battle_report_panel.php'; ?>
        <?php endif; ?>

        <?php if ($isThreeColumn): ?>
            <!-- Third Party panel -->
            <?php
                $_brp_data = $thirdPartyPanel;
                $_brp_label = proxy_e($sideLabels['third_party'] ?? 'Third Party');
                $_brp_badge = 'Third Party';
                $_brp_border = 'border-amber-500/25';
                $_brp_header_bg = 'bg-amber-900/40';
                $_brp_header_border = 'border-amber-500/20';
                $_brp_text = 'text-amber-300';
                $_brp_badge_bg = 'bg-amber-900/60';
                $_brp_subtitle = 'Third party overview';
                $_brp_alliances_label = 'Alliances';
                $_brp_alliance_text = 'text-slate-200';
                $_brp_header_extra = '';
            ?>
            <?php include __DIR__ . '/_battle_report_panel.php'; ?>
        <?php endif; ?>
    </div>

    </div><!-- /sc-br-summary -->

    <!-- Classic view (br.evetools.org-style) -->
    <div id="sc-br-classic" style="display: none;">
        <?php include __DIR__ . '/_battle_report_classic.php'; ?>
    </div>

    <?php if ($dataQualityNotes !== []): ?>
        <div class="mt-3 pt-2 border-t border-white/5">
            <?php foreach ($dataQualityNotes as $note): ?>
                <p class="text-[10px] text-slate-500 leading-relaxed"><?= proxy_e((string) $note) ?></p>
            <?php endforeach; ?>
        </div>
    <?php endif; ?>
</section>

<script>
(function() {
    var KEY = 'sc_br_view';
    window._scBrToggle = function(view) {
        var summary = document.getElementById('sc-br-summary');
        var classic = document.getElementById('sc-br-classic');
        var btnSummary = document.getElementById('sc-br-view-summary');
        var btnClassic = document.getElementById('sc-br-view-classic');
        if (!summary || !classic) return;
        if (view === 'classic') {
            summary.style.display = 'none';
            classic.style.display = '';
            btnSummary.className = 'px-3 py-1.5 font-medium transition-colors bg-slate-800/60 text-slate-400 hover:text-slate-200';
            btnClassic.className = 'px-3 py-1.5 font-medium transition-colors bg-slate-700 text-slate-100';
        } else {
            summary.style.display = '';
            classic.style.display = 'none';
            btnSummary.className = 'px-3 py-1.5 font-medium transition-colors bg-slate-700 text-slate-100';
            btnClassic.className = 'px-3 py-1.5 font-medium transition-colors bg-slate-800/60 text-slate-400 hover:text-slate-200';
        }
        try { localStorage.setItem(KEY, view); } catch(e) {}
    };
    try {
        var saved = localStorage.getItem(KEY);
        if (saved === 'classic') window._scBrToggle('classic');
    } catch(e) {}
})();
</script>
<?php endif; ?>
