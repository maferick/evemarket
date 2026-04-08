<?php $modulePageSectionKey = is_string($modulePageSectionKey ?? null) ? trim((string) $modulePageSectionKey) : ''; ?>
<?php

$summary = is_array($summary ?? null) ? $summary : [];
$tableColumns = is_array($tableColumns ?? null) ? $tableColumns : [];
$tableRows = is_array($tableRows ?? null) ? $tableRows : [];
$tableControls = is_array($tableControls ?? null) ? $tableControls : [];
$emptyMessage = is_string($emptyMessage ?? null) ? $emptyMessage : 'No records available yet.';
$highlights = is_array($highlights ?? null) ? $highlights : [];
$filterFields = is_array($filterFields ?? null) ? $filterFields : [];
$filterAction = is_string($filterAction ?? null) ? $filterAction : current_path();

$hasTableControls = $tableControls !== [];
$search = (string) ($tableControls['search'] ?? '');
$page = max(1, (int) ($tableControls['page'] ?? 1));
$totalPages = max(1, (int) ($tableControls['total_pages'] ?? 1));
$pageSize = max(1, (int) ($tableControls['page_size'] ?? 25));
$pageSizeOptions = is_array($tableControls['page_size_options'] ?? null) ? $tableControls['page_size_options'] : [25, 50, 100];
$sort = (string) ($tableControls['sort'] ?? '');
$sortOptions = is_array($tableControls['sort_options'] ?? null) ? $tableControls['sort_options'] : [];
$totalItems = max(0, (int) ($tableControls['total_items'] ?? count($tableRows)));
$showingFrom = max(0, (int) ($tableControls['showing_from'] ?? 0));
$showingTo = max(0, (int) ($tableControls['showing_to'] ?? 0));

$queryParams = $_GET;
$buildPageUrl = static function (int $targetPage) use ($queryParams): string {
    $params = $queryParams;
    $params['page'] = max(1, $targetPage);

    return current_path() . '?' . http_build_query($params);
};
?>
<?php if ($modulePageSectionKey !== ''): ?>
    <!-- ui-section:<?= htmlspecialchars($modulePageSectionKey, ENT_QUOTES) ?>:start -->
    <div data-ui-section="<?= htmlspecialchars($modulePageSectionKey, ENT_QUOTES) ?>">
<?php endif; ?>
<?php if ($filterFields !== []): ?>
    <section class="surface-tertiary mb-6">
        <form method="get" action="<?= htmlspecialchars($filterAction, ENT_QUOTES) ?>" class="grid gap-4 md:grid-cols-4 md:items-end">
            <?php foreach ($filterFields as $field): ?>
                <?php
                $key = (string) ($field['key'] ?? '');
                $label = (string) ($field['label'] ?? $key);
                $value = (string) ($field['value'] ?? '');
                $options = is_array($field['options'] ?? null) ? $field['options'] : [];
                ?>
                <?php if ($key !== ''): ?>
                    <label class="block">
                        <span class="mb-2 block field-label"><?= htmlspecialchars($label, ENT_QUOTES) ?></span>
                        <select name="<?= htmlspecialchars($key, ENT_QUOTES) ?>" class="field-select">
                            <?php foreach ($options as $optionValue => $optionLabel): ?>
                                <option value="<?= htmlspecialchars((string) $optionValue, ENT_QUOTES) ?>" <?= (string) $optionValue === $value ? 'selected' : '' ?>>
                                    <?= htmlspecialchars((string) $optionLabel, ENT_QUOTES) ?>
                                </option>
                            <?php endforeach; ?>
                        </select>
                    </label>
                <?php endif; ?>
            <?php endforeach; ?>
            <button type="submit" class="btn-primary">Apply filters</button>
        </form>
    </section>
<?php endif; ?>

<?php if ($summary !== []): ?>
    <section class="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
        <?php foreach ($summary as $card): ?>
            <article class="surface-secondary">
                <p class="eyebrow"><?= htmlspecialchars((string) ($card['label'] ?? ''), ENT_QUOTES) ?></p>
                <p class="mt-3 text-3xl metric-value"><?= htmlspecialchars((string) ($card['value'] ?? ''), ENT_QUOTES) ?></p>
                <p class="mt-2 text-sm text-slate-300"><?= htmlspecialchars((string) ($card['context'] ?? ''), ENT_QUOTES) ?></p>
            </article>
        <?php endforeach; ?>
    </section>
<?php endif; ?>

<?php if ($highlights !== []): ?>
    <section class="surface-secondary mt-6">
        <div class="section-header">
            <div>
                <p class="eyebrow">Priority signals</p>
                <h2 class="mt-2 section-title"><?= htmlspecialchars((string) ($highlights['title'] ?? 'Top Signals'), ENT_QUOTES) ?></h2>
            </div>
            <span class="badge border-sky-400/20 bg-sky-500/10 text-sky-100">Action queue</span>
        </div>
        <?php $highlightRows = is_array($highlights['rows'] ?? null) ? $highlights['rows'] : []; ?>
        <?php if ($highlightRows === []): ?>
            <p class="surface-tertiary text-sm text-slate-400">No high-priority signals yet.</p>
        <?php else: ?>
            <div class="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
                <?php foreach ($highlightRows as $signal): ?>
                    <article class="surface-tertiary">
                        <p class="text-sm font-semibold text-slate-100"><?= htmlspecialchars((string) ($signal['module'] ?? ''), ENT_QUOTES) ?></p>
                        <p class="mt-1 text-sm text-slate-400"><?= htmlspecialchars((string) ($signal['signal'] ?? ''), ENT_QUOTES) ?></p>
                        <p class="mt-3 text-xs text-slate-500">Priority score <span class="tabular-nums text-slate-200"><?= htmlspecialchars((string) ($signal['score'] ?? '0'), ENT_QUOTES) ?></span></p>
                    </article>
                <?php endforeach; ?>
            </div>
        <?php endif; ?>
    </section>
<?php endif; ?>

<section class="surface-secondary mt-8">
    <?php if ($hasTableControls): ?>
        <form method="get" action="<?= htmlspecialchars(current_path(), ENT_QUOTES) ?>" class="surface-tertiary mb-5">
            <div class="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
                <div class="grid flex-1 gap-4 md:grid-cols-[minmax(0,1fr)_auto_auto]">
                    <label class="block text-sm text-muted">
                        <span class="mb-2 block field-label">Search item</span>
                        <input type="search" name="q" value="<?= htmlspecialchars($search, ENT_QUOTES) ?>" placeholder="Filter by module name..." class="field-input">
                    </label>
                    <?php if ($sortOptions !== []): ?>
                        <label class="block md:w-44">
                            <span class="mb-2 block field-label">Sort by</span>
                            <select name="sort" class="field-select">
                                <?php foreach ($sortOptions as $sortValue => $sortLabel): ?>
                                    <option value="<?= htmlspecialchars((string) $sortValue, ENT_QUOTES) ?>" <?= (string) $sortValue === $sort ? 'selected' : '' ?>><?= htmlspecialchars((string) $sortLabel, ENT_QUOTES) ?></option>
                                <?php endforeach; ?>
                            </select>
                        </label>
                    <?php endif; ?>
                    <label class="block md:w-36">
                        <span class="mb-2 block field-label">Page size</span>
                        <select name="page_size" class="field-select">
                            <?php foreach ($pageSizeOptions as $option): ?>
                                <?php $optionValue = max(1, (int) $option); ?>
                                <option value="<?= $optionValue ?>" <?= $optionValue === $pageSize ? 'selected' : '' ?>><?= $optionValue ?></option>
                            <?php endforeach; ?>
                        </select>
                    </label>
                </div>
                <div class="flex items-center gap-3">
                    <button type="submit" class="btn-primary">Apply</button>
                    <span class="text-sm text-slate-400"><span class="tabular-nums text-slate-100"><?= number_format($totalItems) ?></span> items</span>
                </div>
            </div>
            <input type="hidden" name="page" value="1">
        </form>

        <div class="mb-3 flex flex-wrap items-center justify-between gap-3 text-sm text-slate-400">
            <p>Showing <span class="tabular-nums text-slate-200"><?= $showingFrom ?>-<?= $showingTo ?></span> of <span class="tabular-nums text-slate-200"><?= number_format($totalItems) ?></span></p>
            <div class="flex items-center gap-2">
                <span>Page <span class="tabular-nums text-slate-200"><?= $page ?></span> / <span class="tabular-nums text-slate-200"><?= $totalPages ?></span></span>
                <a href="<?= htmlspecialchars($buildPageUrl($page - 1), ENT_QUOTES) ?>" class="btn-secondary px-3 py-1.5 <?= $page <= 1 ? 'pointer-events-none opacity-40' : '' ?>">Previous</a>
                <a href="<?= htmlspecialchars($buildPageUrl($page + 1), ENT_QUOTES) ?>" class="btn-secondary px-3 py-1.5 <?= $page >= $totalPages ? 'pointer-events-none opacity-40' : '' ?>">Next</a>
            </div>
        </div>
    <?php endif; ?>

    <div class="table-shell">
        <table class="table-ui">
            <thead>
            <tr>
                <?php foreach ($tableColumns as $column): ?>
                    <?php
                    $alignRight = in_array((string) $column, ['Stock', 'Daily Volume'], true);
                    $thClass = $alignRight ? 'text-right' : '';
                    ?>
                    <th class="<?= $thClass ?> select-none"><?= htmlspecialchars((string) $column, ENT_QUOTES) ?><span class="ml-1 text-[10px] text-slate-500" data-sort-arrow></span></th>
                <?php endforeach; ?>
            </tr>
            </thead>
            <tbody>
            <?php if ($tableRows === []): ?>
                <tr>
                    <td class="px-3 py-6 text-slate-400" colspan="<?= max(1, count($tableColumns)) ?>"><?= htmlspecialchars($emptyMessage, ENT_QUOTES) ?></td>
                </tr>
            <?php else: ?>
                <?php foreach ($tableRows as $index => $row): ?>
                    <?php if ((bool) ($row['is_group_header'] ?? false)): ?>
                        <tr class="bg-white/[0.05]">
                            <td class="px-3 py-2 text-xs font-semibold uppercase tracking-[0.15em] text-slate-400" colspan="<?= max(1, count($tableColumns)) ?>"><?= htmlspecialchars((string) ($row['module'] ?? ''), ENT_QUOTES) ?></td>
                        </tr>
                        <?php continue; ?>
                    <?php endif; ?>
                    <?php
                    $rowTone = (string) ($row['row_tone'] ?? '');
                    $toneClass = $rowTone === 'risk_high' ? 'bg-rose-900/10' : ($rowTone === 'risk_medium' ? 'bg-amber-900/10' : ($rowTone === 'opp_high' ? 'bg-emerald-900/10' : ''));
                    ?>
                    <tr class="<?= $toneClass ?>">
                        <?php foreach (array_keys($tableColumns) as $key): ?>
                            <?php
                            $value = (string) ($row[$key] ?? '');
                            $cellClass = '';

                            if ($key === 'module') {
                                $cellClass .= ' font-semibold text-slate-50';
                            } elseif ($key === 'price') {
                                $cellClass .= ' font-medium text-sky-300 tabular-nums';
                            } elseif ($key === 'stock') {
                                $cellClass .= ' text-right tabular-nums';
                            } elseif ($key === 'updated_at') {
                                $cellClass .= ' text-slate-400';
                            }
                            ?>
                            <td class="<?= $cellClass ?>">
                                <?php if ($key === 'stock'): ?>
                                    <?php
                                    $stockState = (string) ($row['stock_state'] ?? 'healthy');
                                    $stockTone = $stockState === 'low' ? 'text-rose-300 bg-rose-500/10 border-rose-500/40' : 'text-emerald-300 bg-emerald-500/10 border-emerald-500/40';
                                    $stockLabel = $stockState === 'low' ? 'Low stock' : 'Healthy';
                                    ?>
                                    <div class="flex items-center justify-end gap-2">
                                        <span><?= htmlspecialchars($value, ENT_QUOTES) ?></span>
                                        <span class="rounded-full border px-2 py-0.5 text-[11px] uppercase tracking-[0.08em] <?= $stockTone ?>"><?= $stockLabel ?></span>
                                    </div>
                                <?php elseif ($key === 'severity' || $key === 'priority'): ?>
                                    <?php
                                    $sev = strtolower($value);
                                    $sevTone = match (true) {
                                        $sev === 'critical' => 'text-rose-200 bg-rose-500/20 border-rose-400/50',
                                        $sev === 'high' => 'text-rose-300 bg-rose-500/10 border-rose-500/40',
                                        $sev === 'medium' => 'text-amber-300 bg-amber-500/10 border-amber-500/40',
                                        default => 'text-slate-300 bg-slate-500/10 border-slate-500/40',
                                    };
                                    ?>
                                    <span class="rounded-full border px-2 py-0.5 text-[11px] uppercase tracking-[0.08em] <?= $sevTone ?>"><?= htmlspecialchars($value, ENT_QUOTES) ?></span>
                                <?php elseif ($key === 'updated_at'): ?>
                                    <?php
                                    $freshnessState = (string) ($row['freshness_state'] ?? 'fresh');
                                    $freshnessTone = $freshnessState === 'stale' ? 'text-amber-300 bg-amber-500/10 border-amber-500/40' : 'text-slate-300 bg-slate-500/10 border-slate-400/40';
                                    $freshnessLabel = $freshnessState === 'stale' ? 'Stale' : 'Fresh';
                                    ?>
                                    <div class="flex items-center gap-2">
                                        <span><?= htmlspecialchars($value, ENT_QUOTES) ?></span>
                                        <span class="rounded-full border px-2 py-0.5 text-[11px] uppercase tracking-[0.08em] <?= $freshnessTone ?>"><?= $freshnessLabel ?></span>
                                    </div>
                                <?php else: ?>
                                    <?= htmlspecialchars($value, ENT_QUOTES) ?>
                                <?php endif; ?>
                            </td>
                        <?php endforeach; ?>
                    </tr>
                <?php endforeach; ?>
            <?php endif; ?>
            </tbody>
        </table>
    </div>
</section>

<?php if (!$hasTableControls): ?>
    <div class="mt-3 flex items-center gap-3">
        <label class="block flex-1 max-w-xs">
            <input type="search" placeholder="Quick filter by name..." class="field-input text-sm" data-table-quick-filter>
        </label>
        <span class="text-xs text-slate-500" data-table-filter-count></span>
    </div>
<?php endif; ?>

<script>
(() => {
    // Client-side column sort on th click
    document.querySelectorAll('.table-ui').forEach(table => {
        const headers = table.querySelectorAll('thead th');
        const tbody = table.querySelector('tbody');
        if (!tbody || !headers.length) return;

        let sortCol = -1, sortAsc = true;

        headers.forEach((th, idx) => {
            th.style.cursor = 'pointer';
            th.title = 'Click to sort';
            th.addEventListener('click', () => {
                if (sortCol === idx) { sortAsc = !sortAsc; } else { sortCol = idx; sortAsc = true; }

                // Update header indicators
                headers.forEach(h => { const a = h.querySelector('[data-sort-arrow]'); if (a) a.textContent = ''; });
                const arrow = th.querySelector('[data-sort-arrow]');
                if (arrow) arrow.textContent = sortAsc ? '\u25B2' : '\u25BC';

                const rows = Array.from(tbody.querySelectorAll('tr:not([class*="bg-white"])'));
                rows.sort((a, b) => {
                    const aCell = a.cells[idx]?.textContent?.trim() ?? '';
                    const bCell = b.cells[idx]?.textContent?.trim() ?? '';
                    const aNum = parseFloat(aCell.replace(/[^0-9.\-+]/g, ''));
                    const bNum = parseFloat(bCell.replace(/[^0-9.\-+]/g, ''));
                    if (!isNaN(aNum) && !isNaN(bNum)) return sortAsc ? aNum - bNum : bNum - aNum;
                    return sortAsc ? aCell.localeCompare(bCell) : bCell.localeCompare(aCell);
                });
                rows.forEach(r => tbody.appendChild(r));
            });
        });
    });

    // Quick filter for non-paginated tables
    const filterInput = document.querySelector('[data-table-quick-filter]');
    const countEl = document.querySelector('[data-table-filter-count]');
    if (filterInput) {
        const tbody = document.querySelector('.table-ui tbody');
        if (tbody) {
            filterInput.addEventListener('input', () => {
                const q = filterInput.value.toLowerCase();
                let shown = 0;
                tbody.querySelectorAll('tr').forEach(row => {
                    if (row.classList.contains('bg-white/[0.05]')) return; // group headers
                    const text = row.textContent.toLowerCase();
                    const match = q === '' || text.includes(q);
                    row.style.display = match ? '' : 'none';
                    if (match) shown++;
                });
                if (countEl) countEl.textContent = q ? shown + ' shown' : '';
            });
        }
    }
})();
</script>

<?php if ($modulePageSectionKey !== ''): ?>
    </div>
    <!-- ui-section:<?= htmlspecialchars($modulePageSectionKey, ENT_QUOTES) ?>:end -->
<?php endif; ?>
