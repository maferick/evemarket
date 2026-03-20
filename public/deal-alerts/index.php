<?php

declare(strict_types=1);

require_once __DIR__ . '/../../src/bootstrap.php';

if ($_SERVER['REQUEST_METHOD'] === 'POST') {
    if (!validate_csrf($_POST['_token'] ?? null)) {
        http_response_code(419);
        exit('Invalid CSRF token');
    }

    $action = trim((string) ($_POST['alert_action'] ?? ''));
    $returnTo = deal_alert_safe_return_path((string) ($_POST['return_to'] ?? '/deal-alerts'));

    if ($action === 'dismiss') {
        $saved = deal_alert_dismiss(
            trim((string) ($_POST['alert_key'] ?? '')),
            max(1, (int) ($_POST['severity_rank'] ?? 1))
        );
        flash('success', $saved ? 'Deal alert dismissed temporarily.' : 'Unable to dismiss that deal alert.');
    }

    header('Location: ' . $returnTo);
    exit;
}

$title = 'Deal Alerts';
$pageData = deal_alerts_page_data($_GET);
$pageFreshness = [
    'freshness_state' => ($pageData['summary']['last_seen_at'] ?? null) !== null ? 'fresh' : 'stale',
    'freshness_label' => ($pageData['summary']['last_seen_at'] ?? null) !== null ? 'Fresh' : 'Awaiting scan',
    'computed_at' => $pageData['summary']['last_seen_at'] ?? null,
    'reason' => 'market-deal-alerts',
];

$filters = $pageData['filters'];
$summary = $pageData['summary'];
$rows = $pageData['rows'];
$pageCount = (int) ($pageData['page_count'] ?? 1);

include __DIR__ . '/../../src/views/partials/header.php';
?>
<section class="grid gap-4 xl:grid-cols-4">
    <article class="kpi-card xl:col-span-1">
        <p class="eyebrow">Active windows</p>
        <p class="mt-3 metric-value text-[2.35rem] text-rose-100"><?= htmlspecialchars((string) ($summary['active_count'] ?? 0), ENT_QUOTES) ?></p>
        <p class="mt-2 text-sm text-slate-300">Current suspicious listings still active in the scan window.</p>
    </article>
    <article class="kpi-card xl:col-span-1">
        <p class="eyebrow">Critical now</p>
        <p class="mt-3 metric-value text-[2.35rem] text-orange-100"><?= htmlspecialchars((string) ($summary['critical_count'] ?? 0), ENT_QUOTES) ?></p>
        <p class="mt-2 text-sm text-slate-300">Listings at or below the critical misprice threshold.</p>
    </article>
    <article class="kpi-card xl:col-span-2">
        <p class="eyebrow">Detection model</p>
        <h2 class="mt-3 text-lg font-semibold text-white">Historical median + weighted average baseline</h2>
        <p class="mt-2 text-sm text-slate-300">SupplyCore compares the current cheapest sell listing against its own recent history, then ranks anomalies by percent-of-normal, severity tier, quantity, and history depth.</p>
        <p class="mt-3 text-xs text-slate-400">Last seen <?= htmlspecialchars((string) ($summary['last_seen_relative'] ?? 'Never'), ENT_QUOTES) ?> · <?= htmlspecialchars((string) ($summary['last_seen_label'] ?? 'Unavailable'), ENT_QUOTES) ?></p>
    </article>
</section>

<section class="surface-primary mt-8">
    <div class="section-header border-b border-white/8 pb-4">
        <div>
            <p class="eyebrow">Actionable market intelligence</p>
            <h2 class="mt-2 section-title">Mispriced listings watchfloor</h2>
            <p class="mt-2 section-copy">Separate from the regular market pages, this view stays focused on listings that are accidentally cheap enough to warrant immediate attention.</p>
        </div>
        <a href="/settings?section=deal-alerts" class="rounded-full border border-white/10 bg-white/5 px-4 py-2 text-sm font-medium text-slate-100 hover:bg-white/10">Tune thresholds</a>
    </div>

    <form method="get" class="mt-5 grid gap-3 rounded-[1.35rem] border border-white/8 bg-black/20 p-4 lg:grid-cols-[minmax(0,1.2fr)_repeat(4,minmax(0,0.5fr))]">
        <label class="block space-y-2">
            <span class="text-xs font-semibold uppercase tracking-[0.16em] text-muted">Search</span>
            <input type="search" name="search" value="<?= htmlspecialchars((string) ($filters['search'] ?? ''), ENT_QUOTES) ?>" placeholder="Type name or item ID" class="w-full field-input" />
        </label>
        <label class="block space-y-2">
            <span class="text-xs font-semibold uppercase tracking-[0.16em] text-muted">Market</span>
            <select name="market" class="w-full field-input">
                <?php foreach (deal_alert_market_filter_options() as $value => $label): ?>
                    <option value="<?= htmlspecialchars($value, ENT_QUOTES) ?>" <?= ($filters['market'] ?? '') === $value ? 'selected' : '' ?>><?= htmlspecialchars($label, ENT_QUOTES) ?></option>
                <?php endforeach; ?>
            </select>
        </label>
        <label class="block space-y-2">
            <span class="text-xs font-semibold uppercase tracking-[0.16em] text-muted">Threshold</span>
            <select name="min_severity" class="w-full field-input">
                <?php foreach (deal_alert_minimum_severity_options() as $value => $label): ?>
                    <option value="<?= (int) $value ?>" <?= (int) ($filters['minimum_severity_rank'] ?? 0) === (int) $value ? 'selected' : '' ?>><?= htmlspecialchars($label, ENT_QUOTES) ?></option>
                <?php endforeach; ?>
            </select>
        </label>
        <label class="block space-y-2">
            <span class="text-xs font-semibold uppercase tracking-[0.16em] text-muted">Sort</span>
            <select name="sort" class="w-full field-input">
                <?php foreach (deal_alert_sort_options() as $value => $label): ?>
                    <option value="<?= htmlspecialchars($value, ENT_QUOTES) ?>" <?= ($filters['sort'] ?? 'severity') === $value ? 'selected' : '' ?>><?= htmlspecialchars($label, ENT_QUOTES) ?></option>
                <?php endforeach; ?>
            </select>
        </label>
        <div class="flex items-end gap-2">
            <button class="btn-primary w-full">Apply filters</button>
            <a href="/deal-alerts" class="rounded-lg border border-white/10 px-4 py-2 text-sm font-medium text-slate-100 hover:bg-white/5">Reset</a>
        </div>
    </form>

    <div class="mt-5 overflow-hidden rounded-[1.35rem] border border-white/8">
        <div class="overflow-x-auto">
            <table class="min-w-full divide-y divide-white/6 text-sm">
                <thead class="bg-white/[0.03] text-left text-xs uppercase tracking-[0.18em] text-slate-400">
                    <tr>
                        <th class="px-4 py-3">Item</th>
                        <th class="px-4 py-3">Severity</th>
                        <th class="px-4 py-3">Current</th>
                        <th class="px-4 py-3">Normal</th>
                        <th class="px-4 py-3">% of normal</th>
                        <th class="px-4 py-3">Score</th>
                        <th class="px-4 py-3">Market</th>
                        <th class="px-4 py-3">Qty / listings</th>
                        <th class="px-4 py-3">Freshness</th>
                        <th class="px-4 py-3">Action</th>
                    </tr>
                </thead>
                <tbody class="divide-y divide-white/6 bg-black/10">
                    <?php foreach ($rows as $row): ?>
                        <tr class="align-top hover:bg-white/[0.025]">
                            <td class="px-4 py-4">
                                <p class="font-semibold text-slate-100"><?= htmlspecialchars((string) ($row['display_name'] ?? ''), ENT_QUOTES) ?></p>
                                <p class="mt-1 text-xs text-slate-500">Type ID <?= (int) ($row['item_type_id'] ?? 0) ?> · Baseline <?= htmlspecialchars((string) ($row['baseline_model'] ?? 'median_weighted_blend'), ENT_QUOTES) ?></p>
                            </td>
                            <td class="px-4 py-4">
                                <span class="inline-flex rounded-full border px-2.5 py-1 text-[11px] font-semibold uppercase tracking-[0.14em] <?= htmlspecialchars((string) ($row['severity_tone'] ?? ''), ENT_QUOTES) ?>">
                                    <?= htmlspecialchars((string) ($row['severity_label'] ?? ''), ENT_QUOTES) ?>
                                </span>
                            </td>
                            <td class="px-4 py-4 text-slate-100"><?= htmlspecialchars((string) ($row['current_price_label'] ?? '—'), ENT_QUOTES) ?></td>
                            <td class="px-4 py-4 text-slate-100"><?= htmlspecialchars((string) ($row['normal_price_label'] ?? '—'), ENT_QUOTES) ?></td>
                            <td class="px-4 py-4 text-rose-100"><?= htmlspecialchars((string) ($row['percent_of_normal_label'] ?? '—'), ENT_QUOTES) ?></td>
                            <td class="px-4 py-4 text-slate-100"><?= htmlspecialchars(number_format((float) ($row['anomaly_score'] ?? 0.0), 2, '.', ','), ENT_QUOTES) ?></td>
                            <td class="px-4 py-4">
                                <p class="text-slate-100"><?= htmlspecialchars((string) ($row['market_label'] ?? ''), ENT_QUOTES) ?></p>
                                <p class="mt-1 text-xs text-slate-500"><?= htmlspecialchars((string) ($row['source_name'] ?? ''), ENT_QUOTES) ?></p>
                            </td>
                            <td class="px-4 py-4">
                                <p class="text-slate-100"><?= number_format((int) ($row['quantity_available'] ?? 0)) ?></p>
                                <p class="mt-1 text-xs text-slate-500"><?= number_format((int) ($row['listing_count'] ?? 0)) ?> listing(s)</p>
                            </td>
                            <td class="px-4 py-4">
                                <p class="text-slate-100"><?= htmlspecialchars((string) ($row['freshness_relative'] ?? 'Unknown'), ENT_QUOTES) ?></p>
                                <p class="mt-1 text-xs text-slate-500"><?= htmlspecialchars((string) ($row['freshness_at'] ?? 'Unavailable'), ENT_QUOTES) ?></p>
                            </td>
                            <td class="px-4 py-4">
                                <form method="post" class="space-y-2">
                                    <input type="hidden" name="_token" value="<?= htmlspecialchars(csrf_token(), ENT_QUOTES) ?>">
                                    <input type="hidden" name="alert_action" value="dismiss">
                                    <input type="hidden" name="alert_key" value="<?= htmlspecialchars((string) ($row['alert_key'] ?? ''), ENT_QUOTES) ?>">
                                    <input type="hidden" name="severity_rank" value="<?= (int) ($row['severity_rank'] ?? 1) ?>">
                                    <input type="hidden" name="return_to" value="<?= htmlspecialchars((string) ($_SERVER['REQUEST_URI'] ?? '/deal-alerts'), ENT_QUOTES) ?>">
                                    <button type="submit" class="rounded-lg border border-white/10 px-3 py-2 text-xs font-medium text-slate-100 hover:bg-white/5">Dismiss popup</button>
                                </form>
                                <?php if (!empty($row['best_order_id'])): ?>
                                    <button type="button" class="mt-2 rounded-lg border border-white/10 px-3 py-2 text-xs font-medium text-slate-100 hover:bg-white/5" onclick="navigator.clipboard.writeText('<?= htmlspecialchars((string) ($row['display_name'] ?? ''), ENT_QUOTES) ?>')">Copy item name</button>
                                <?php endif; ?>
                            </td>
                        </tr>
                    <?php endforeach; ?>
                    <?php if ($rows === []): ?>
                        <tr>
                            <td colspan="10" class="px-4 py-10 text-center text-sm text-slate-400">No active deal alerts matched the current filters. Try widening the market or threshold filters, or wait for the next background scan.</td>
                        </tr>
                    <?php endif; ?>
                </tbody>
            </table>
        </div>
    </div>

    <div class="mt-5 flex flex-wrap items-center justify-between gap-3 text-sm text-slate-400">
        <p>Showing page <?= (int) ($filters['page'] ?? 1) ?> of <?= $pageCount ?> · <?= (int) ($pageData['total'] ?? 0) ?> active alert(s).</p>
        <div class="flex items-center gap-2">
            <?php
            $baseQuery = $filters;
            unset($baseQuery['page'], $baseQuery['per_page'], $baseQuery['minimum_severity_rank']);
            $baseQuery['min_severity'] = $filters['minimum_severity_rank'] ?? 0;
            ?>
            <?php if ((int) ($filters['page'] ?? 1) > 1): ?>
                <?php $prevQuery = http_build_query($baseQuery + ['page' => (int) $filters['page'] - 1]); ?>
                <a href="/deal-alerts?<?= htmlspecialchars($prevQuery, ENT_QUOTES) ?>" class="rounded-lg border border-white/10 px-3 py-2 text-slate-100 hover:bg-white/5">Previous</a>
            <?php endif; ?>
            <?php if ((int) ($filters['page'] ?? 1) < $pageCount): ?>
                <?php $nextQuery = http_build_query($baseQuery + ['page' => (int) $filters['page'] + 1]); ?>
                <a href="/deal-alerts?<?= htmlspecialchars($nextQuery, ENT_QUOTES) ?>" class="rounded-lg border border-white/10 px-3 py-2 text-slate-100 hover:bg-white/5">Next</a>
            <?php endif; ?>
        </div>
    </div>
</section>

<?php include __DIR__ . '/../../src/views/partials/footer.php'; ?>
