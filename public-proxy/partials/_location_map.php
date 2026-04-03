<?php
$mapSvg = (string) ($mapSvg ?? '');
if ($mapSvg === '') return;
?>
<section class="surface-primary mt-4">
    <h2 class="text-lg font-semibold text-slate-50">
        Battle Location
        <?php if (count($systems) === 1 && isset($systems[0])): ?>
            <span class="text-muted text-sm font-normal ml-1">&mdash; <?= proxy_e((string) ($systems[0]['system_name'] ?? 'Unknown')) ?></span>
        <?php endif; ?>
    </h2>

    <div class="mt-3 rounded-lg overflow-hidden border border-slate-700/50">
        <?= $mapSvg ?>
    </div>

    <?php if (count($systems) > 1): ?>
        <div class="mt-3 grid gap-2 md:grid-cols-4">
            <?php foreach ($systems as $sys): ?>
                <div class="surface-tertiary">
                    <p class="text-sm font-semibold text-slate-100"><?= proxy_e((string) ($sys['system_name'] ?? 'Unknown')) ?></p>
                    <p class="text-xs text-muted mt-1">
                        Participants: <?= number_format((int) ($sys['participant_count'] ?? 0)) ?>
                        &middot; Weight: <?= number_format((float) ($sys['weight'] ?? 0), 2) ?>
                    </p>
                </div>
            <?php endforeach; ?>
        </div>
    <?php endif; ?>
</section>
