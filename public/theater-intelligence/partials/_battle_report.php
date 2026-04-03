<?php if ($allianceSummary !== []): ?>
<?php
    $ourPanel = $sidePanels['friendly'] ?? [];
    $enemyPanel = $sidePanels['opponent'] ?? [];
    $thirdPartyPanel = $sidePanels['third_party'] ?? [];

    $hasOpponent = ($enemyPanel['pilots'] ?? 0) > 0;
    $hasThirdParty = ($thirdPartyPanel['pilots'] ?? 0) > 0;
    $isThreeColumn = $hasOpponent && $hasThirdParty;

    // Compute per-panel efficiency
    $ourTotalIsk = ($ourPanel['isk_killed'] ?? 0) + ($ourPanel['isk_lost'] ?? 0);
    $ourEfficiency = $ourTotalIsk > 0 ? ($ourPanel['isk_killed'] ?? 0) / $ourTotalIsk : 0.0;

    $enemyTotalIsk = ($enemyPanel['isk_killed'] ?? 0) + ($enemyPanel['isk_lost'] ?? 0);
    $enemyEfficiency = $enemyTotalIsk > 0 ? ($enemyPanel['isk_killed'] ?? 0) / $enemyTotalIsk : 0.0;

    $tpTotalIsk = ($thirdPartyPanel['isk_killed'] ?? 0) + ($thirdPartyPanel['isk_lost'] ?? 0);
    $tpEfficiency = $tpTotalIsk > 0 ? ($thirdPartyPanel['isk_killed'] ?? 0) / $tpTotalIsk : 0.0;

    // Efficiency bar: friendly ISK destroyed vs opponent+third_party ISK destroyed (how much each side lost)
    $enemyCombinedIskLost = ($enemyPanel['isk_lost'] ?? 0) + ($thirdPartyPanel['isk_lost'] ?? 0);
    $totalIskBothSides = ($ourPanel['isk_lost'] ?? 0) + $enemyCombinedIskLost;
    $ourBarPct = $totalIskBothSides > 0 ? ($enemyCombinedIskLost / $totalIskBothSides) * 100 : 50;

    if ($isThreeColumn) {
        // 3-segment bar: friendly, opponent, third_party based on ISK destroyed (what they killed)
        $totalIskDestAll = ($ourPanel['isk_killed'] ?? 0) + ($enemyPanel['isk_killed'] ?? 0) + ($thirdPartyPanel['isk_killed'] ?? 0);
        $ourBarPct = $totalIskDestAll > 0 ? (($ourPanel['isk_killed'] ?? 0) / $totalIskDestAll) * 100 : 33.3;
        $enemyBarPct = $totalIskDestAll > 0 ? (($enemyPanel['isk_killed'] ?? 0) / $totalIskDestAll) * 100 : 33.3;
        $tpBarPct = 100 - $ourBarPct - $enemyBarPct;
    } else {
        $enemyBarPct = 100 - $ourBarPct;
        // When no opponent, the "enemy" panel is the promoted third party
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

    <!-- Summary view (default / current) -->
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
                $panelData = $ourPanel;
                $panelLabel = htmlspecialchars($sideLabels['friendly'] ?? 'Friendlies', ENT_QUOTES);
                $panelBadge = 'Friendly';
                $panelBorderColor = 'border-blue-500/25';
                $panelHeaderBg = 'bg-blue-900/40';
                $panelHeaderBorder = 'border-blue-500/20';
                $panelTextColor = 'text-blue-300';
                $panelBadgeBg = 'bg-blue-900/60';
                $panelSubtitle = 'Friendly coalition overview';
                $panelAlliancesLabel = 'Alliances';
                $panelAllianceTextColor = 'text-slate-200';
                $panelHeaderExtra = '';
            ?>
            <?php include __DIR__ . '/_battle_report_panel.php'; ?>

            <!-- Opponent panel -->
            <?php
                $panelData = $enemyPanel;
                $panelLabel = htmlspecialchars($sideLabels['opponent'] ?? 'Opposition', ENT_QUOTES);
                $panelBadge = 'Hostile';
                $panelBorderColor = 'border-red-500/25';
                $panelHeaderBg = 'bg-red-900/40';
                $panelHeaderBorder = 'border-red-500/20';
                $panelTextColor = 'text-red-300';
                $panelBadgeBg = 'bg-red-900/60';
                $panelSubtitle = 'Opposition coalition overview';
                $panelAlliancesLabel = 'Opponent Alliances';
                $panelAllianceTextColor = 'text-slate-200';
                if (!$isThreeColumn && $hasThirdParty) {
                    $panelHeaderExtra = '<span class="text-slate-400 text-xs font-normal ml-1">+ Third Party</span>';
                } else {
                    $panelHeaderExtra = '';
                }
            ?>
            <?php if (!$isThreeColumn): ?>
                <?php
                    // Merge third party into opponent for 2-column display
                    $panelData = [
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
                    // Show third party alliances as a sub-section
                    $panelThirdPartyAlliances = $thirdPartyPanel['alliances'] ?? [];
                ?>
                <?php include __DIR__ . '/_battle_report_panel_merged.php'; ?>
            <?php else: ?>
                <?php include __DIR__ . '/_battle_report_panel.php'; ?>
            <?php endif; ?>

            <?php if ($isThreeColumn): ?>
                <!-- Third Party panel -->
                <?php
                    $panelData = $thirdPartyPanel;
                    $panelLabel = htmlspecialchars($sideLabels['third_party'] ?? 'Third Party', ENT_QUOTES);
                    $panelBadge = 'Third Party';
                    $panelBorderColor = 'border-amber-500/25';
                    $panelHeaderBg = 'bg-amber-900/40';
                    $panelHeaderBorder = 'border-amber-500/20';
                    $panelTextColor = 'text-amber-300';
                    $panelBadgeBg = 'bg-amber-900/60';
                    $panelSubtitle = 'Third party overview';
                    $panelAlliancesLabel = 'Alliances';
                    $panelAllianceTextColor = 'text-slate-200';
                    $panelHeaderExtra = '';
                ?>
                <?php include __DIR__ . '/_battle_report_panel.php'; ?>
            <?php endif; ?>
        </div>
    </div>

    <!-- Classic view (br.evetools.org-style) -->
    <div id="sc-br-classic" style="display: none;">
        <?php include __DIR__ . '/_battle_report_classic.php'; ?>
    </div>

    <?php if ($dataQualityNotes !== []): ?>
        <div class="mt-3 pt-2 border-t border-white/5">
            <?php foreach ($dataQualityNotes as $note): ?>
                <p class="text-[10px] text-slate-500 leading-relaxed"><?= htmlspecialchars($note, ENT_QUOTES) ?></p>
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
    // Restore saved preference
    try {
        var saved = localStorage.getItem(KEY);
        if (saved === 'classic') window._scBrToggle('classic');
    } catch(e) {}
})();
</script>
<?php endif; ?>
