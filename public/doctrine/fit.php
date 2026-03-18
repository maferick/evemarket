<?php

declare(strict_types=1);

require_once __DIR__ . '/../../src/bootstrap.php';

$fitId = max(0, (int) ($_GET['fit_id'] ?? 0));
$data = doctrine_fit_detail_view_model($fitId);
$fit = $data['fit'] ?? null;
$categories = $data['categories'] ?? [];
$summary = $data['summary'] ?? [];
$title = $fit !== null ? ((string) ($fit['fit_name'] ?? 'Doctrine Fit')) : 'Doctrine Fit';

$statusTone = static function (string $status): string {
    return match ($status) {
        'ok' => 'border-emerald-400/20 bg-emerald-500/10 text-emerald-100',
        'low' => 'border-amber-400/20 bg-amber-500/10 text-amber-100',
        default => 'border-rose-400/20 bg-rose-500/10 text-rose-200',
    };
};

include __DIR__ . '/../../src/views/partials/header.php';
?>
<?php if ($fit === null): ?>
    <section class="surface-secondary">
        <div class="section-header">
            <div>
                <p class="eyebrow">Doctrine fit</p>
                <h2 class="mt-2 section-title">Doctrine fit not found</h2>
            </div>
            <a href="/doctrine" class="btn-secondary">Back to groups</a>
        </div>
        <p class="text-sm text-slate-400">The requested doctrine fit does not exist yet or the database is unavailable.</p>
    </section>
<?php else: ?>
    <section class="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
        <?php foreach ($summary as $card): ?>
            <article class="surface-secondary">
                <p class="eyebrow"><?= htmlspecialchars((string) ($card['label'] ?? ''), ENT_QUOTES) ?></p>
                <p class="mt-3 text-3xl metric-value"><?= htmlspecialchars((string) ($card['value'] ?? ''), ENT_QUOTES) ?></p>
                <p class="mt-2 text-sm text-slate-300"><?= htmlspecialchars((string) ($card['context'] ?? ''), ENT_QUOTES) ?></p>
            </article>
        <?php endforeach; ?>
    </section>

    <section class="mt-8 grid gap-6 xl:grid-cols-[minmax(280px,360px)_minmax(0,1fr)]">
        <aside class="surface-secondary">
            <div class="section-header">
                <div>
                    <p class="eyebrow">Hull profile</p>
                    <h2 class="mt-2 section-title"><?= htmlspecialchars((string) ($fit['ship_name'] ?? ''), ENT_QUOTES) ?></h2>
                    <p class="mt-2 text-sm text-slate-400"><?= htmlspecialchars((string) ($fit['fit_name'] ?? ''), ENT_QUOTES) ?></p>
                </div>
                <span class="badge border-cyan-400/18 bg-cyan-500/10 text-cyan-100"><?= htmlspecialchars((string) strtoupper((string) ($fit['source_format'] ?? 'buyall')), ENT_QUOTES) ?></span>
            </div>
            <div class="overflow-hidden rounded-[1.35rem] border border-white/8 bg-slate-950/70 p-6">
                <?php if (!empty($fit['ship_image_url'])): ?>
                    <img src="<?= htmlspecialchars((string) $fit['ship_image_url'], ENT_QUOTES) ?>" alt="<?= htmlspecialchars((string) ($fit['ship_name'] ?? ''), ENT_QUOTES) ?>" class="mx-auto h-48 w-48 object-contain">
                <?php else: ?>
                    <div class="flex h-48 items-center justify-center text-sm text-slate-500">Ship image unavailable</div>
                <?php endif; ?>
            </div>
            <div class="mt-4 space-y-3 text-sm text-slate-300">
                <div class="surface-tertiary">
                    <p class="text-xs uppercase tracking-[0.16em] text-slate-500">Doctrine group</p>
                    <p class="mt-2 font-semibold text-slate-100"><?= htmlspecialchars((string) ($fit['group_name'] ?? ''), ENT_QUOTES) ?></p>
                </div>
                <div class="surface-tertiary">
                    <p class="text-xs uppercase tracking-[0.16em] text-slate-500">Purpose</p>
                    <p class="mt-2 text-slate-400">Baseline data for supply gap detection, restock generation, and hauling suggestions.</p>
                </div>
                <div class="flex gap-3">
                    <a href="/doctrine/group?group_id=<?= (int) ($fit['doctrine_group_id'] ?? 0) ?>" class="btn-secondary">Back to group</a>
                    <a href="/doctrine/import" class="btn-primary">Import fit</a>
                </div>
            </div>
        </aside>

        <article class="surface-secondary">
            <div class="section-header">
                <div>
                    <p class="eyebrow">Market mapping</p>
                    <h2 class="mt-2 section-title">Required items by category</h2>
                    <p class="mt-2 text-sm text-slate-400">Each line compares required fit quantity against alliance stock plus the reference hub price floor.</p>
                </div>
                <span class="badge border-sky-400/18 bg-sky-500/10 text-sky-100">Green / Yellow / Red</span>
            </div>

            <?php if ($categories === []): ?>
                <div class="surface-tertiary text-sm text-slate-400">No doctrine items were stored for this fit.</div>
            <?php else: ?>
                <div class="space-y-6">
                    <?php foreach ($categories as $category => $rows): ?>
                        <section>
                            <div class="mb-3 flex items-center justify-between gap-3">
                                <h3 class="text-base font-semibold text-slate-100"><?= htmlspecialchars((string) $category, ENT_QUOTES) ?></h3>
                                <span class="text-xs uppercase tracking-[0.16em] text-slate-500"><?= doctrine_format_quantity(count($rows)) ?> lines</span>
                            </div>
                            <div class="overflow-hidden rounded-[1.25rem] border border-white/8">
                                <table class="table-ui">
                                    <thead>
                                        <tr>
                                            <th>Item</th>
                                            <th class="text-right">Required Qty</th>
                                            <th class="text-right">Local Qty</th>
                                            <th class="text-right">Missing Qty</th>
                                            <th class="text-right">Hub Price</th>
                                            <th class="text-right">Local Price</th>
                                            <th>Status</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        <?php foreach ($rows as $row): ?>
                                            <tr>
                                                <td class="font-semibold text-slate-50"><?= htmlspecialchars((string) ($row['item_name'] ?? ''), ENT_QUOTES) ?></td>
                                                <td class="text-right tabular-nums"><?= htmlspecialchars((string) ($row['required_qty_label'] ?? '0'), ENT_QUOTES) ?></td>
                                                <td class="text-right tabular-nums"><?= htmlspecialchars((string) ($row['local_available_qty_label'] ?? '0'), ENT_QUOTES) ?></td>
                                                <td class="text-right tabular-nums"><?= htmlspecialchars((string) ($row['missing_qty_label'] ?? '0'), ENT_QUOTES) ?></td>
                                                <td class="text-right tabular-nums text-sky-300"><?= htmlspecialchars((string) ($row['hub_price_label'] ?? '—'), ENT_QUOTES) ?></td>
                                                <td class="text-right tabular-nums text-sky-300"><?= htmlspecialchars((string) ($row['local_price_label'] ?? '—'), ENT_QUOTES) ?></td>
                                                <td>
                                                    <span class="rounded-full border px-2 py-0.5 text-[11px] uppercase tracking-[0.08em] <?= $statusTone((string) ($row['market_status'] ?? 'missing')) ?>">
                                                        <?= htmlspecialchars((string) ($row['market_label'] ?? 'Missing'), ENT_QUOTES) ?>
                                                    </span>
                                                </td>
                                            </tr>
                                        <?php endforeach; ?>
                                    </tbody>
                                </table>
                            </div>
                        </section>
                    <?php endforeach; ?>
                </div>
            <?php endif; ?>
        </article>
    </section>
<?php endif; ?>
<?php include __DIR__ . '/../../src/views/partials/footer.php'; ?>
