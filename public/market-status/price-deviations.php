<?php

declare(strict_types=1);

require_once __DIR__ . '/../../src/bootstrap.php';

$title = 'Price Deviations';
$data = price_deviations_data();
$summary = $data['summary'] ?? [];
$tableColumns = [
    'module' => 'Module',
    'alliance_price' => 'Alliance Price',
    'reference_price' => market_hub_reference_name() . ' Price',
    'deviation' => 'Deviation',
    'score' => 'Risk Score',
    'severity' => 'Severity',
];
$highlights = $data['highlights'] ?? [];
$allRows = $data['rows'] ?? [];
$emptyMessage = 'No deviation alerts at this time versus ' . market_hub_reference_name() . '.';
$pageFreshness = supplycore_page_freshness_view_model((array) ($data['freshness'] ?? []));
$liveRefreshConfig = supplycore_live_refresh_page_config('price_deviations');
$modulePageSectionKey = 'price-deviations-main';

// View filter: actionable (default), all, or noise
$view = trim((string) ($_GET['view'] ?? 'actionable'));
if (!in_array($view, ['actionable', 'all', 'noise'], true)) {
    $view = 'actionable';
}

// Filter rows based on selected view
$tableRows = [];
$inActionable = false;
$inNoise = false;
foreach ($allRows as $row) {
    if ((bool) ($row['is_group_header'] ?? false)) {
        $group = strtolower(trim((string) ($row['module'] ?? '')));
        $inActionable = $group === 'actionable';
        $inNoise = $group === 'noise';
        if ($view === 'all') {
            $tableRows[] = $row;
        }
        continue;
    }
    if ($view === 'all') {
        $tableRows[] = $row;
    } elseif ($view === 'actionable' && $inActionable) {
        $tableRows[] = $row;
    } elseif ($view === 'noise' && $inNoise) {
        $tableRows[] = $row;
    }
}

// Count per group for tab badges
$actionableCount = (int) (($summary[1] ?? [])['value'] ?? 0);
$noiseCount = (int) (($summary[2] ?? [])['value'] ?? 0);
$totalCount = (int) (($summary[0] ?? [])['value'] ?? 0);

include __DIR__ . '/../../src/views/partials/header.php';
?>
<div class="mb-6 flex items-center gap-2">
    <?php
    $tabs = [
        'actionable' => 'Actionable ' . $actionableCount,
        'all' => 'All ' . $totalCount,
        'noise' => 'Noise ' . $noiseCount,
    ];
    foreach ($tabs as $tabKey => $tabLabel):
        $isActive = $view === $tabKey;
        $tabTone = $isActive
            ? 'border-cyan-400/30 bg-cyan-500/12 text-cyan-100'
            : 'border-white/8 bg-white/[0.03] text-slate-400 hover:bg-white/[0.06] hover:text-slate-200';
    ?>
        <a href="<?= htmlspecialchars(current_path() . '?view=' . $tabKey, ENT_QUOTES) ?>"
           class="rounded-full border px-4 py-1.5 text-sm font-medium transition <?= $tabTone ?>">
            <?= htmlspecialchars($tabLabel, ENT_QUOTES) ?>
        </a>
    <?php endforeach; ?>
</div>
<?php
include __DIR__ . '/../../src/views/partials/module-page.php';
include __DIR__ . '/../../src/views/partials/footer.php';
