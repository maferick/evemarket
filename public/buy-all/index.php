<?php
/**
 * Auto buy-all — deterministic list driven by `auto_doctrines` + observed
 * loss rates × runway. No modes, no sorts, no manual filters: the Python
 * job (`compute_auto_buyall`) precomputes the ranked item set.
 */

declare(strict_types=1);

require_once __DIR__ . '/../../src/bootstrap.php';

$title = 'Buy All';
$latest = auto_buyall_latest();
$summary = $latest['summary'] ?? null;
$items = $latest['items'] ?? [];
$settings = auto_doctrine_settings();

$pageHeaderBadge = 'Automatic';
if ($summary === null) {
    $pageHeaderSummary = 'No buy-all has been computed yet. Once doctrines are detected and the buy-all job runs, the list will appear here.';
    $pageHeaderMeta = [];
} else {
    $pageHeaderSummary = sprintf(
        'Deterministic from %d active doctrines × runway. Computed %s.',
        (int) ($summary['doctrine_count'] ?? 0),
        htmlspecialchars((string) ($summary['computed_at'] ?? 'unknown'))
    );
    $pageHeaderMeta = [
        ['label' => 'Line items', 'value' => (string) (int) ($summary['total_items'] ?? 0), 'caption' => 'distinct types'],
        ['label' => 'Total ISK', 'value' => number_format((float) ($summary['total_isk'] ?? 0), 0), 'caption' => 'at hub sell'],
        ['label' => 'Total volume', 'value' => number_format((float) ($summary['total_volume'] ?? 0), 0) . ' m³', 'caption' => 'packaged'],
        ['label' => 'Hub snapshot', 'value' => htmlspecialchars((string) ($summary['hub_snapshot_at'] ?? '—')), 'caption' => 'market_hub'],
    ];
}

include __DIR__ . '/../../src/views/partials/header.php';
?>
<!-- ui-section:buyall-overview:start -->
<?php if ($summary === null): ?>
    <section class="surface-primary" data-ui-section="buyall-empty">
        <div class="rounded border border-white/8 bg-white/3 px-6 py-10 text-center text-sm text-white/60">
            <p class="text-base text-white/80 font-semibold">No buy-all computed yet.</p>
            <p class="mt-2">Run <code class="rounded bg-black/40 px-1 py-0.5">bin/python_compute_auto_doctrines.py</code> to detect doctrines and generate the buy list.</p>
            <p class="mt-2"><a href="/doctrines/" class="text-cyan-300 hover:text-cyan-200">Go to doctrines →</a></p>
        </div>
    </section>
<?php else: ?>
    <section class="surface-primary" data-ui-section="buyall-table">
        <div class="section-header flex items-center justify-between border-b border-white/8 pb-4">
            <div>
                <h2 class="text-lg font-semibold">Buy list</h2>
                <p class="text-sm text-white/60">Ordered by line cost. Quantities are already net of current alliance stock.</p>
            </div>
            <div class="flex items-center gap-3">
                <a href="/doctrines/" class="text-sm text-cyan-300 hover:text-cyan-200">Doctrines →</a>
                <button type="button" id="buyall-copy" data-target="#buyall-clipboard"
                        class="rounded bg-cyan-500/20 px-3 py-1 text-sm text-cyan-100 hover:bg-cyan-500/30">
                    Copy multi-buy
                </button>
            </div>
        </div>

        <?php if (empty($items)): ?>
            <div class="mt-4 rounded border border-white/8 bg-white/3 px-4 py-6 text-sm text-white/60">
                Alliance stock covers every active doctrine — nothing to buy right now.
            </div>
        <?php else: ?>
        <div class="mt-4 overflow-x-auto">
            <table class="w-full text-sm">
                <thead class="text-white/60">
                    <tr class="border-b border-white/8">
                        <th class="px-3 py-2 text-left">Type</th>
                        <th class="px-3 py-2 text-right">Demand</th>
                        <th class="px-3 py-2 text-right">Alliance stock</th>
                        <th class="px-3 py-2 text-right">Buy qty</th>
                        <th class="px-3 py-2 text-right">Unit cost</th>
                        <th class="px-3 py-2 text-right">Line cost</th>
                        <th class="px-3 py-2 text-right">Volume</th>
                        <th class="px-3 py-2 text-left">Doctrines</th>
                    </tr>
                </thead>
                <tbody>
                <?php foreach ($items as $item): ?>
                    <tr class="border-b border-white/5">
                        <td class="px-3 py-2">
                            <div class="flex items-center gap-2">
                                <img src="https://images.evetech.net/types/<?= (int) $item['type_id'] ?>/icon?size=24" width="24" height="24" alt="" loading="lazy" class="rounded">
                                <span><?= htmlspecialchars((string) $item['type_name']) ?></span>
                            </div>
                        </td>
                        <td class="px-3 py-2 text-right tabular-nums"><?= number_format((int) $item['demand_qty']) ?></td>
                        <td class="px-3 py-2 text-right tabular-nums text-white/60"><?= number_format((int) $item['alliance_stock_qty']) ?></td>
                        <td class="px-3 py-2 text-right tabular-nums font-semibold text-white"><?= number_format((int) $item['buy_qty']) ?></td>
                        <td class="px-3 py-2 text-right tabular-nums"><?= number_format((float) $item['unit_cost'], 2) ?></td>
                        <td class="px-3 py-2 text-right tabular-nums"><?= number_format((float) $item['line_cost'], 0) ?></td>
                        <td class="px-3 py-2 text-right tabular-nums text-white/60"><?= number_format((float) $item['line_volume'], 1) ?></td>
                        <td class="px-3 py-2 text-xs text-white/60">
                            <?= (int) $item['contributing_fit_count'] ?> fits across <?= count((array) $item['contributing_doctrine_ids']) ?> doctrine(s)
                        </td>
                    </tr>
                <?php endforeach; ?>
                </tbody>
                <tfoot class="text-white/70">
                    <tr class="border-t border-white/10">
                        <td class="px-3 py-2 font-semibold">Totals</td>
                        <td colspan="4"></td>
                        <td class="px-3 py-2 text-right tabular-nums font-semibold"><?= number_format((float) ($summary['total_isk'] ?? 0), 0) ?></td>
                        <td class="px-3 py-2 text-right tabular-nums"><?= number_format((float) ($summary['total_volume'] ?? 0), 1) ?></td>
                        <td></td>
                    </tr>
                </tfoot>
            </table>
        </div>

        <!-- ui-section:buyall-clipboard:start -->
        <div class="mt-6">
            <label class="mb-2 block text-xs font-semibold uppercase tracking-wide text-white/50" for="buyall-clipboard">
                In-game multi-buy (Name \t Qty)
            </label>
            <textarea id="buyall-clipboard" readonly rows="8"
                      class="w-full rounded border border-white/10 bg-black/40 px-3 py-2 font-mono text-xs text-white/80"><?php
                foreach ($items as $item) {
                    echo htmlspecialchars((string) $item['type_name']) . "\t" . (int) $item['buy_qty'] . "\n";
                }
            ?></textarea>
        </div>
        <!-- ui-section:buyall-clipboard:end -->
        <?php endif; ?>
    </section>
<?php endif; ?>
<!-- ui-section:buyall-overview:end -->

<script>
(function () {
    var btn = document.getElementById('buyall-copy');
    if (!btn) { return; }
    btn.addEventListener('click', function () {
        var target = document.querySelector(btn.getAttribute('data-target'));
        if (!target) { return; }
        target.select();
        try {
            navigator.clipboard.writeText(target.value).then(function () {
                var original = btn.textContent;
                btn.textContent = 'Copied';
                window.setTimeout(function () { btn.textContent = original; }, 1200);
            });
        } catch (e) {
            document.execCommand('copy');
        }
    });
})();
</script>
<?php include __DIR__ . '/../../src/views/partials/footer.php'; ?>
