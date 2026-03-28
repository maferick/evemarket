<?php

declare(strict_types=1);

require_once __DIR__ . '/../../src/bootstrap.php';

$title = 'Current Alliance Structure';
$data = current_alliance_market_status_data();
$summary = $data['summary'] ?? [];
$tableColumns = [
    'module' => 'Module',
    'price' => 'Alliance Price',
    'stock' => 'Stock',
    'restock_priority' => 'Restock Priority',
    'price_delta' => 'Price Delta',
    'score' => 'Score',
    'severity' => 'Severity',
    'updated_at' => 'Updated',
];
$highlights = $data['highlights'] ?? [];
$tableRows = $data['rows'] ?? [];
$tableControls = $data['pagination'] ?? [];
$emptyMessage = 'No alliance structure listings available.';
$pageFreshness = supplycore_page_freshness_view_model((array) ($data['freshness'] ?? []));
$liveRefreshConfig = supplycore_live_refresh_page_config('current_alliance');
$modulePageSectionKey = 'current-alliance-main';

// Coverage gauge data
$trackedTotal = 0;
$stockedCount = 0;
$lowStockCount = 0;
$criticalCount = 0;
foreach ($summary as $card) {
    $label = (string) ($card['label'] ?? '');
    $val = (int) ($card['value'] ?? 0);
    if ($label === 'Tracked Modules') { $trackedTotal = $val; }
    elseif ($label === 'Listings with Stock') { $stockedCount = $val; }
    elseif ($label === 'Low Stock Count') { $lowStockCount = $val; }
    elseif ($label === 'Critical Restocks') { $criticalCount = $val; }
}
$coveragePercent = $trackedTotal > 0 ? round(($stockedCount / $trackedTotal) * 100, 1) : 0;
$gapCount = max(0, $trackedTotal - $stockedCount);

include __DIR__ . '/../../src/views/partials/header.php';
?>
<!-- Coverage gauge -->
<section class="surface-secondary mb-6">
    <div class="flex flex-wrap items-center gap-6">
        <div class="flex-1">
            <div class="flex items-center justify-between gap-3">
                <p class="text-sm font-medium text-slate-200">Coverage</p>
                <p class="text-2xl font-semibold tabular-nums <?= $coveragePercent >= 80 ? 'text-emerald-300' : ($coveragePercent >= 50 ? 'text-amber-300' : 'text-rose-300') ?>"><?= $coveragePercent ?>%</p>
            </div>
            <div class="mt-2 h-2.5 rounded-full bg-white/8 overflow-hidden">
                <div class="h-full rounded-full transition-all <?= $coveragePercent >= 80 ? 'bg-emerald-400/70' : ($coveragePercent >= 50 ? 'bg-amber-400/70' : 'bg-rose-400/70') ?>" style="width: <?= min(100, max(1, $coveragePercent)) ?>%"></div>
            </div>
            <p class="mt-2 text-xs text-slate-500"><?= number_format($trackedTotal) ?> total · <?= number_format($stockedCount) ?> stocked · <?= number_format($gapCount) ?> gaps · <?= number_format($lowStockCount) ?> below minimum<?php if ($criticalCount > 0): ?> · <span class="text-rose-300"><?= number_format($criticalCount) ?> critical</span><?php endif; ?></p>
        </div>
    </div>
</section>
<?php
include __DIR__ . '/../../src/views/partials/module-page.php';
include __DIR__ . '/../../src/views/partials/footer.php';
