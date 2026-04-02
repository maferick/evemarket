<?php if ($timeline !== []): ?>
<section class="surface-primary mt-4">
    <h2 class="text-lg font-semibold text-slate-50">Timeline</h2>
    <p class="text-xs text-muted mt-1"><?= count($timeline) ?> buckets (1-minute intervals). Momentum: positive = <span class="text-blue-300"><?= proxy_e($sideLabels['friendly'] ?? 'Friendlies') ?></span> winning, negative = <span class="text-red-300"><?= proxy_e($sideLabels['opponent'] ?? 'Opposition') ?></span> winning.</p>

    <?php if ($turningPoints !== []): ?>
        <div class="mt-2">
            <p class="text-xs uppercase tracking-widest text-muted mb-1">Turning Points</p>
            <?php foreach ($turningPoints as $tp): ?>
                <?php
                    $tpDir = (string) ($tp['direction'] ?? '');
                    $tpSide = str_contains($tpDir, 'friendly') ? 'friendly' : 'opponent';
                    $tpColor = $sideColorClass[$tpSide] ?? 'text-slate-300';
                    $tpSideLabel = $sideLabels[$tpSide] ?? $tpSide;
                ?>
                <p class="text-xs text-slate-300">
                    <span class="<?= $tpColor ?>"><?= proxy_e((string) ($tp['turning_point_at'] ?? '')) ?></span>
                    &mdash; <span class="<?= $tpColor ?>"><?= proxy_e($tpSideLabel) ?></span> <?= proxy_e((string) ($tp['description'] ?? '')) ?>
                    (magnitude: <?= number_format((float) ($tp['magnitude'] ?? 0), 3) ?>)
                </p>
            <?php endforeach; ?>
        </div>
    <?php endif; ?>

    <details class="mt-3">
        <summary class="cursor-pointer text-sm text-slate-100">Show timeline data</summary>
        <div class="mt-2 table-shell">
            <table class="table-ui">
                <thead>
                    <tr class="border-b border-border/70 text-xs uppercase tracking-wider text-muted">
                        <th class="px-3 py-2 text-left">Time</th>
                        <th class="px-3 py-2 text-right">Killmails</th>
                        <th class="px-3 py-2 text-right">ISK</th>
                        <th class="px-3 py-2 text-right text-blue-300"><?= proxy_e($sideLabels['friendly'] ?? 'Friendlies') ?></th>
                        <th class="px-3 py-2 text-right text-red-300"><?= proxy_e($sideLabels['opponent'] ?? 'Opposition') ?></th>
                        <th class="px-3 py-2 text-right">Momentum</th>
                    </tr>
                </thead>
                <tbody>
                    <?php foreach ($timeline as $t): ?>
                        <?php $mom = (float) ($t['momentum_score'] ?? 0); ?>
                        <tr class="border-b border-border/50">
                            <td class="px-3 py-2 text-xs text-slate-300"><?= proxy_e((string) ($t['bucket_time'] ?? '')) ?></td>
                            <td class="px-3 py-2 text-right"><?= (int) ($t['kills'] ?? 0) ?></td>
                            <td class="px-3 py-2 text-right"><?= proxy_format_isk((float) ($t['isk_destroyed'] ?? 0)) ?></td>
                            <td class="px-3 py-2 text-right text-blue-300"><?= (int) ($t['side_a_kills'] ?? 0) ?></td>
                            <td class="px-3 py-2 text-right text-red-300"><?= (int) ($t['side_b_kills'] ?? 0) ?></td>
                            <td class="px-3 py-2 text-right <?= $mom > 0 ? 'text-blue-400' : ($mom < 0 ? 'text-red-400' : 'text-slate-300') ?>">
                                <?= number_format($mom, 3) ?>
                            </td>
                        </tr>
                    <?php endforeach; ?>
                </tbody>
            </table>
        </div>
    </details>
</section>
<?php endif; ?>
