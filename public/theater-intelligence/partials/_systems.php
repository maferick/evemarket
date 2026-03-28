<?php if ($systems !== []): ?>
<section class="surface-primary mt-4">
    <h2 class="text-lg font-semibold text-slate-50">Systems</h2>
    <div class="mt-3 grid gap-3 md:grid-cols-4">
        <?php foreach ($systems as $sys): ?>
            <?php $sysWeight = (float) ($sys['weight'] ?? 0); ?>
            <div class="surface-tertiary">
                <p class="text-sm font-semibold text-slate-100"><?= htmlspecialchars((string) ($sys['system_name'] ?? 'Unknown'), ENT_QUOTES) ?></p>
                <p class="text-xs text-muted mt-1">
                    Participants: <?= number_format((int) ($sys['participant_count'] ?? 0)) ?>
                    &middot; Weight: <?= number_format($sysWeight, 2) ?>
                </p>
            </div>
        <?php endforeach; ?>
    </div>
</section>
<?php endif; ?>
