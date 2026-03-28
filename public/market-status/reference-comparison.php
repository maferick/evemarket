<?php

declare(strict_types=1);

require_once __DIR__ . '/../../src/bootstrap.php';

$referenceHubName = market_hub_reference_name();
$title = $referenceHubName . ' Comparison';
$data = reference_hub_comparison_data();
$summary = $data['summary'] ?? [];
$tableColumns = [
    'module' => 'Module',
    'alliance_price' => 'Alliance Price',
    'reference_price' => $referenceHubName . ' Price',
    'delta' => 'Delta',
    'score' => 'Score',
    'severity' => 'Tier',
];
$highlights = $data['highlights'] ?? [];
$allRows = $data['rows'] ?? [];
$emptyMessage = 'No comparison rows available for ' . $referenceHubName . '.';
$pageFreshness = supplycore_page_freshness_view_model((array) ($data['freshness'] ?? []));
$liveRefreshConfig = supplycore_live_refresh_page_config('reference_comparison');
$modulePageSectionKey = 'reference-comparison-main';

// View filter
$view = trim((string) ($_GET['view'] ?? 'all'));
if (!in_array($view, ['all', 'underpriced', 'overpriced'], true)) {
    $view = 'all';
}

// Filter rows by view
$tableRows = [];
$inUnderpriced = false;
$inOverpriced = false;
foreach ($allRows as $row) {
    if ((bool) ($row['is_group_header'] ?? false)) {
        $group = strtolower(trim((string) ($row['module'] ?? '')));
        $inUnderpriced = str_contains($group, 'underpriced');
        $inOverpriced = str_contains($group, 'overpriced');
        if ($view === 'all') {
            $tableRows[] = $row;
        }
        continue;
    }
    if ($view === 'all') {
        $tableRows[] = $row;
    } elseif ($view === 'underpriced' && $inUnderpriced) {
        $tableRows[] = $row;
    } elseif ($view === 'overpriced' && $inOverpriced) {
        $tableRows[] = $row;
    }
}

$underpricedCount = (int) (($summary[1] ?? [])['value'] ?? 0);
$overpricedCount = (int) (($summary[2] ?? [])['value'] ?? 0);
$totalCount = (int) (($summary[0] ?? [])['value'] ?? 0);

include __DIR__ . '/../../src/views/partials/header.php';
?>
<div class="mb-6 flex items-center gap-2">
    <?php
    $tabs = [
        'all' => 'All ' . $totalCount,
        'underpriced' => 'Underpriced ' . $underpricedCount,
        'overpriced' => 'Overpriced ' . $overpricedCount,
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
