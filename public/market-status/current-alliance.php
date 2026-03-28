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

$suppressPageFreshness = true;
include __DIR__ . '/../../src/views/partials/header.php';
?>
<!-- Coverage gauge (above freshness — most important summary) -->
<section class="surface-secondary mb-4">
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
<?php if ($pageFreshness !== [] && ($pageFreshness['state'] ?? '') === 'stale'): ?>
    <div class="mb-4 flex items-center gap-2 text-xs text-amber-300/80">
        <svg viewBox="0 0 16 16" fill="currentColor" width="14" height="14" class="h-3.5 w-3.5 shrink-0 opacity-70"><path d="M8 1a7 7 0 1 0 0 14A7 7 0 0 0 8 1Zm0 3a.75.75 0 0 1 .75.75v3.5a.75.75 0 0 1-1.5 0v-3.5A.75.75 0 0 1 8 4Zm0 7a.75.75 0 1 0 0-1.5.75.75 0 0 0 0 1.5Z"/></svg>
        <span>Data from <?= htmlspecialchars((string) ($pageFreshness['computed_relative'] ?? 'Unknown'), ENT_QUOTES) ?></span>
    </div>
<?php elseif ($pageFreshness !== [] && ($pageFreshness['state'] ?? '') !== 'fresh'): ?>
    <p class="mb-4 text-xs text-slate-500">Data from <?= htmlspecialchars((string) ($pageFreshness['computed_relative'] ?? 'Unknown'), ENT_QUOTES) ?></p>
<?php endif; ?>
<?php
include __DIR__ . '/../../src/views/partials/module-page.php';
include __DIR__ . '/../../src/views/partials/footer.php';
