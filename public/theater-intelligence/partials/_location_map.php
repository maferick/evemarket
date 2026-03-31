<?php if ($systems !== []): ?>
<?php
    $mapSystemIds = array_values(array_filter(
        array_map(static fn(array $s): int => (int) ($s['system_id'] ?? 0), $systems),
        static fn(int $id): bool => $id > 0
    ));
    $mapSvgUrl = $mapSystemIds !== [] ? supplycore_theater_map_svg($theaterId, $mapSystemIds, 1) : null;
    $primarySystem = $systems[0] ?? null;
?>
<section class="surface-primary mt-4">
    <h2 class="text-lg font-semibold text-slate-50">
        Battle Location
        <?php if (count($systems) === 1 && $primarySystem !== null): ?>
            <span class="text-muted text-sm font-normal ml-1">&mdash; <?= htmlspecialchars((string) ($primarySystem['system_name'] ?? 'Unknown'), ENT_QUOTES) ?></span>
        <?php endif; ?>
    </h2>

    <?php if ($mapSvgUrl !== null): ?>
        <div class="mt-3 rounded-lg overflow-hidden border border-slate-700/50">
            <img src="<?= htmlspecialchars($mapSvgUrl, ENT_QUOTES) ?>" alt="Battle location map" class="w-full" loading="lazy">
        </div>
    <?php endif; ?>

    <?php if (count($systems) > 1): ?>
        <div class="mt-3 grid gap-2 md:grid-cols-4">
            <?php foreach ($systems as $sys): ?>
                <div class="surface-tertiary">
                    <p class="text-sm font-semibold text-slate-100"><?= htmlspecialchars((string) ($sys['system_name'] ?? 'Unknown'), ENT_QUOTES) ?></p>
                    <p class="text-xs text-muted mt-1">
                        Participants: <?= number_format((int) ($sys['participant_count'] ?? 0)) ?>
                        &middot; Weight: <?= number_format((float) ($sys['weight'] ?? 0), 2) ?>
                    </p>
                </div>
            <?php endforeach; ?>
        </div>
    <?php endif; ?>
</section>
<?php endif; ?>
