<?php

declare(strict_types=1);

require_once __DIR__ . '/../../src/bootstrap.php';

$title = 'Missing Items';
$data = missing_items_data();
$summary = $data['summary'] ?? [];
$tableColumns = [
    'module' => 'Module',
    'reference_price' => market_hub_reference_name() . ' Price',
    'daily_volume' => 'Daily Volume',
    'score' => 'Score',
    'priority' => 'Priority',
];
$highlights = $data['highlights'] ?? [];
$allRows = $data['rows'] ?? [];
$emptyMessage = 'No missing items detected against ' . market_hub_reference_name() . '.';
$pageFreshness = supplycore_page_freshness_view_model((array) ($data['freshness'] ?? []));
$liveRefreshConfig = supplycore_live_refresh_page_config('missing_items');
$modulePageSectionKey = 'missing-items-main';

// Tier filter
$tier = trim((string) ($_GET['tier'] ?? 'high'));
if (!in_array($tier, ['high', 'all', 'high_volume'], true)) {
    $tier = 'high';
}

// Classify rows into tiers
$highPriority = [];
$highVolume = [];
$standard = [];
foreach ($allRows as $row) {
    $tone = (string) ($row['row_tone'] ?? '');
    $vol = (int) ($row['daily_volume'] ?? 0);
    if ($tone === 'opp_high') {
        $highPriority[] = $row;
    } elseif ($vol >= 1000) {
        $highVolume[] = $row;
    } else {
        $standard[] = $row;
    }
}

$tableRows = match ($tier) {
    'high' => $highPriority,
    'high_volume' => $highVolume,
    default => $allRows,
};

include __DIR__ . '/../../src/views/partials/header.php';
?>
<div class="mb-6 flex items-center gap-2">
    <?php
    $tabs = [
        'high' => 'High Priority ' . count($highPriority),
        'high_volume' => 'High Volume ' . count($highVolume),
        'all' => 'All ' . count($allRows),
    ];
    foreach ($tabs as $tabKey => $tabLabel):
        $isActive = $tier === $tabKey;
        $tabTone = $isActive
            ? 'border-cyan-400/30 bg-cyan-500/12 text-cyan-100'
            : 'border-white/8 bg-white/[0.03] text-slate-400 hover:bg-white/[0.06] hover:text-slate-200';
    ?>
        <a href="<?= htmlspecialchars(current_path() . '?tier=' . $tabKey, ENT_QUOTES) ?>"
           class="rounded-full border px-4 py-1.5 text-sm font-medium transition <?= $tabTone ?>">
            <?= htmlspecialchars($tabLabel, ENT_QUOTES) ?>
        </a>
    <?php endforeach; ?>
</div>
<?php
include __DIR__ . '/../../src/views/partials/module-page.php';
include __DIR__ . '/../../src/views/partials/footer.php';
