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
<?php if ($filterFields !== []): ?>
    <section class="mb-6 rounded-xl border border-border bg-card p-4">
        <form method="get" action="<?= htmlspecialchars($filterAction, ENT_QUOTES) ?>" class="grid gap-4 md:grid-cols-4 md:items-end">
            <?php foreach ($filterFields as $field): ?>
                <?php
                $key = (string) ($field['key'] ?? '');
                $label = (string) ($field['label'] ?? $key);
                $value = (string) ($field['value'] ?? '');
                $options = is_array($field['options'] ?? null) ? $field['options'] : [];
                ?>
                <?php if ($key !== ''): ?>
                    <label class="block text-sm text-muted">
                        <span class="mb-1 block text-xs uppercase tracking-[0.15em]"><?= htmlspecialchars($label, ENT_QUOTES) ?></span>
                        <select name="<?= htmlspecialchars($key, ENT_QUOTES) ?>" class="w-full rounded-lg border border-border bg-black/20 px-3 py-2 text-slate-100">
                            <?php foreach ($options as $optionValue => $optionLabel): ?>
                                <option value="<?= htmlspecialchars((string) $optionValue, ENT_QUOTES) ?>" <?= (string) $optionValue === $value ? 'selected' : '' ?>>
                                    <?= htmlspecialchars((string) $optionLabel, ENT_QUOTES) ?>
                                </option>
                            <?php endforeach; ?>
                        </select>
                    </label>
                <?php endif; ?>
            <?php endforeach; ?>
            <button type="submit" class="rounded-lg border border-border bg-accent/30 px-4 py-2 text-sm text-white hover:bg-accent/50">Apply filters</button>
        </form>
    </section>
<?php endif; ?>

<?php if ($summary !== []): ?>
    <section class="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
        <?php foreach ($summary as $card): ?>
            <article class="rounded-xl border border-border/90 bg-gradient-to-b from-card to-black/40 p-5 shadow-lg shadow-black/20 ring-1 ring-white/5">
                <p class="text-xs uppercase tracking-[0.2em] text-muted"><?= htmlspecialchars((string) ($card['label'] ?? ''), ENT_QUOTES) ?></p>
                <p class="mt-2 text-2xl font-semibold text-slate-50"><?= htmlspecialchars((string) ($card['value'] ?? ''), ENT_QUOTES) ?></p>
                <p class="mt-2 text-sm text-muted"><?= htmlspecialchars((string) ($card['context'] ?? ''), ENT_QUOTES) ?></p>
            </article>
        <?php endforeach; ?>
    </section>
<?php endif; ?>

<?php if ($highlights !== []): ?>
    <section class="mt-6 rounded-xl border border-border bg-card p-5">
        <div class="mb-3 flex items-center justify-between gap-3">
            <h2 class="text-base font-medium"><?= htmlspecialchars((string) ($highlights['title'] ?? 'Top Signals'), ENT_QUOTES) ?></h2>
            <span class="text-xs text-muted">Action queue</span>
        </div>
        <?php $highlightRows = is_array($highlights['rows'] ?? null) ? $highlights['rows'] : []; ?>
        <?php if ($highlightRows === []): ?>
            <p class="rounded-lg border border-dashed border-border bg-black/20 p-3 text-sm text-muted">No high-priority signals yet.</p>
        <?php else: ?>
            <div class="grid gap-2 md:grid-cols-2 xl:grid-cols-3">
                <?php foreach ($highlightRows as $signal): ?>
                    <article class="rounded-lg border border-border bg-black/20 px-3 py-2">
                        <p class="text-sm font-medium"><?= htmlspecialchars((string) ($signal['module'] ?? ''), ENT_QUOTES) ?></p>
                        <p class="mt-1 text-xs text-muted"><?= htmlspecialchars((string) ($signal['signal'] ?? ''), ENT_QUOTES) ?></p>
                        <p class="mt-1 text-[11px] uppercase tracking-[0.08em] text-muted">Score: <?= htmlspecialchars((string) ($signal['score'] ?? '0'), ENT_QUOTES) ?></p>
                    </article>
                <?php endforeach; ?>
            </div>
        <?php endif; ?>
    </section>
<?php endif; ?>

<section class="mt-8 rounded-xl border border-border bg-card p-5 shadow-lg shadow-black/20">
    <?php if ($hasTableControls): ?>
        <form method="get" action="<?= htmlspecialchars(current_path(), ENT_QUOTES) ?>" class="mb-5 rounded-xl border border-border/80 bg-black/20 p-4">
            <div class="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
                <div class="grid flex-1 gap-4 md:grid-cols-[minmax(0,1fr)_auto]">
                    <label class="block text-sm text-muted">
                        <span class="mb-1 block text-xs uppercase tracking-[0.18em]">Search item</span>
                        <input type="search" name="q" value="<?= htmlspecialchars($search, ENT_QUOTES) ?>" placeholder="Filter by module name..." class="w-full rounded-lg border border-border bg-black/30 px-3 py-2 text-slate-100 placeholder:text-muted focus:border-accent/70 focus:outline-none">
                    </label>
                    <?php if ($sortOptions !== []): ?>
                        <label class="block text-sm text-muted md:w-44">
                            <span class="mb-1 block text-xs uppercase tracking-[0.18em]">Sort by</span>
                            <select name="sort" class="w-full rounded-lg border border-border bg-black/30 px-3 py-2 text-slate-100 focus:border-accent/70 focus:outline-none">
                                <?php foreach ($sortOptions as $sortValue => $sortLabel): ?>
                                    <option value="<?= htmlspecialchars((string) $sortValue, ENT_QUOTES) ?>" <?= (string) $sortValue === $sort ? 'selected' : '' ?>><?= htmlspecialchars((string) $sortLabel, ENT_QUOTES) ?></option>
                                <?php endforeach; ?>
                            </select>
                        </label>
                    <?php endif; ?>
                    <label class="block text-sm text-muted md:w-36">
                        <span class="mb-1 block text-xs uppercase tracking-[0.18em]">Page size</span>
                        <select name="page_size" class="w-full rounded-lg border border-border bg-black/30 px-3 py-2 text-slate-100 focus:border-accent/70 focus:outline-none">
                            <?php foreach ($pageSizeOptions as $option): ?>
                                <?php $optionValue = max(1, (int) $option); ?>
                                <option value="<?= $optionValue ?>" <?= $optionValue === $pageSize ? 'selected' : '' ?>><?= $optionValue ?></option>
                            <?php endforeach; ?>
                        </select>
                    </label>
                </div>
                <div class="flex items-center gap-3">
                    <button type="submit" class="rounded-lg border border-border bg-accent/40 px-4 py-2 text-sm font-medium text-white transition hover:bg-accent/60">Apply</button>
                    <span class="text-sm text-muted"><?= number_format($totalItems) ?> items</span>
                </div>
            </div>
            <input type="hidden" name="page" value="1">
        </form>

        <div class="mb-3 flex flex-wrap items-center justify-between gap-3 text-sm text-muted">
            <p>Showing <?= $showingFrom ?>-<?= $showingTo ?> of <?= number_format($totalItems) ?> results</p>
            <div class="flex items-center gap-2">
                <span>Page <?= $page ?> / <?= $totalPages ?></span>
                <a href="<?= htmlspecialchars($buildPageUrl($page - 1), ENT_QUOTES) ?>" class="rounded-md border border-border px-3 py-1.5 text-slate-200 transition hover:bg-white/5 <?= $page <= 1 ? 'pointer-events-none opacity-40' : '' ?>">Previous</a>
                <a href="<?= htmlspecialchars($buildPageUrl($page + 1), ENT_QUOTES) ?>" class="rounded-md border border-border px-3 py-1.5 text-slate-200 transition hover:bg-white/5 <?= $page >= $totalPages ? 'pointer-events-none opacity-40' : '' ?>">Next</a>
            </div>
        </div>
    <?php endif; ?>

    <div class="overflow-x-auto rounded-lg border border-border/80">
        <table class="min-w-full text-sm">
            <thead>
            <tr class="border-b border-border/80 bg-white/[0.03] text-left text-xs uppercase tracking-[0.15em] text-muted">
                <?php foreach ($tableColumns as $column): ?>
                    <?php
                    $alignRight = in_array((string) $column, ['Stock', 'Daily Volume'], true);
                    $thClass = $alignRight ? 'px-3 py-2 font-medium text-right' : 'px-3 py-2 font-medium';
                    ?>
                    <th class="<?= $thClass ?>"><?= htmlspecialchars((string) $column, ENT_QUOTES) ?></th>
                <?php endforeach; ?>
            </tr>
            </thead>
            <tbody>
            <?php if ($tableRows === []): ?>
                <tr>
                    <td class="px-3 py-6 text-muted" colspan="<?= max(1, count($tableColumns)) ?>"><?= htmlspecialchars($emptyMessage, ENT_QUOTES) ?></td>
                </tr>
            <?php else: ?>
                <?php foreach ($tableRows as $index => $row): ?>
                    <?php if ((bool) ($row['is_group_header'] ?? false)): ?>
                        <tr class="border-b border-border/80 bg-white/[0.05]">
                            <td class="px-3 py-2 text-xs font-semibold uppercase tracking-[0.15em] text-muted" colspan="<?= max(1, count($tableColumns)) ?>"><?= htmlspecialchars((string) ($row['module'] ?? ''), ENT_QUOTES) ?></td>
                        </tr>
                        <?php continue; ?>
                    <?php endif; ?>
                    <?php
                    $rowTone = (string) ($row['row_tone'] ?? '');
                    $toneClass = $rowTone === 'risk_high' ? 'bg-rose-900/10' : ($rowTone === 'risk_medium' ? 'bg-amber-900/10' : ($rowTone === 'opp_high' ? 'bg-emerald-900/10' : ''));
                    ?>
                    <tr class="border-b border-border/60 text-slate-200 transition hover:bg-accent/10 <?= $index % 2 === 1 ? 'bg-white/[0.01]' : '' ?> <?= $toneClass ?>">
                        <?php foreach (array_keys($tableColumns) as $key): ?>
                            <?php
                            $value = (string) ($row[$key] ?? '');
                            $cellClass = 'px-3 py-3';

                            if ($key === 'module') {
                                $cellClass .= ' font-semibold text-slate-50';
                            } elseif ($key === 'price') {
                                $cellClass .= ' font-medium text-sky-300';
                            } elseif ($key === 'stock') {
                                $cellClass .= ' text-right tabular-nums';
                            } elseif ($key === 'updated_at') {
                                $cellClass .= ' text-muted';
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
                                    $sevTone = $sev === 'high' ? 'text-rose-300 bg-rose-500/10 border-rose-500/40' : ($sev === 'medium' ? 'text-amber-300 bg-amber-500/10 border-amber-500/40' : 'text-slate-300 bg-slate-500/10 border-slate-500/40');
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
