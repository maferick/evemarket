<section class="surface-primary">
    <a href="./" class="text-sm text-blue-400 hover:text-blue-300">&larr; Back to Theater Overview</a>
    <div class="mt-3 text-center">
        <p class="text-xs uppercase tracking-widest text-muted">Battle Report — Theater Detail</p>
        <h1 class="mt-1 text-2xl font-semibold text-slate-50">
            <?= proxy_e((string) ($theater['primary_system_name'] ?? 'Unknown')) ?>
            <?php if ((int) ($theater['system_count'] ?? 0) > 1): ?>
                <span class="text-sm text-muted font-normal">+<?= (int) ($theater['system_count'] ?? 0) - 1 ?> systems</span>
            <?php endif; ?>
        </h1>
        <div class="mt-2 flex flex-wrap items-center justify-center gap-2 text-base">
            <span class="text-blue-300 font-semibold"><?= proxy_e($sideLabels['friendly'] ?? 'Friendlies') ?></span>
            <span class="text-[10px] uppercase tracking-wider bg-blue-900/60 text-blue-300 rounded-full px-1.5 py-0.5">Friendly</span>
            <span class="text-slate-500">vs</span>
            <span class="text-red-300 font-semibold"><?= proxy_e($sideLabels['opponent'] ?? 'Opposition') ?></span>
            <span class="text-[10px] uppercase tracking-wider bg-red-900/60 text-red-300 rounded-full px-1.5 py-0.5">Hostile</span>
            <?php $thirdPartyCount = count($sideAlliancesByPilots['third_party'] ?? []); ?>
            <?php if ($thirdPartyCount > 0): ?>
                <span class="text-slate-500">vs</span>
                <span class="text-amber-300 font-semibold"><?= proxy_e($sideLabels['third_party'] ?? 'Third Party') ?></span>
                <span class="text-[10px] uppercase tracking-wider bg-amber-900/60 text-amber-300 rounded-full px-1.5 py-0.5">Third Party</span>
            <?php endif; ?>
        </div>
        <div class="mt-2 flex flex-wrap justify-center gap-x-4 gap-y-1 text-xs text-muted">
            <span><?= proxy_e((string) ($theater['region_name'] ?? '')) ?></span>
            <span><?= proxy_e($theaterStartActual) ?> — <?= proxy_e($theaterEndActual) ?></span>
            <span>Friendly: <?= number_format(count($sideAlliancesByPilots['friendly'] ?? [])) ?> alliances</span>
            <span>Hostile: <?= number_format(count($sideAlliancesByPilots['opponent'] ?? [])) ?> alliances</span>
            <?php if (count($sideAlliancesByPilots['third_party'] ?? []) > 0): ?>
                <span>Third Party: <?= number_format(count($sideAlliancesByPilots['third_party'])) ?> alliances</span>
            <?php endif; ?>
        </div>
    </div>

    <div class="mt-3 grid gap-3 sm:grid-cols-2 md:grid-cols-3 xl:grid-cols-6">
        <div class="surface-tertiary">
            <p class="text-xs text-muted">Battles</p>
            <p class="text-lg text-slate-50 font-semibold"><?= (int) ($theater['battle_count'] ?? 0) ?></p>
        </div>
        <div class="surface-tertiary">
            <p class="text-xs text-muted">Systems</p>
            <p class="text-lg text-slate-50 font-semibold"><?= (int) ($theater['system_count'] ?? 0) ?></p>
        </div>
        <div class="surface-tertiary">
            <p class="text-xs text-muted">Unique Pilots</p>
            <p class="text-lg text-slate-50 font-semibold"><?= number_format((int) ($theater['participant_count'] ?? 0)) ?></p>
        </div>
        <div class="surface-tertiary">
            <p class="text-xs text-muted">Distinct Killmails</p>
            <p class="text-lg text-slate-50 font-semibold"><?= number_format($displayKillTotal) ?></p>
            <?php if ($reportedKillTotal !== $observedKillTotal): ?>
                <p class="mt-1 text-[10px] text-muted">Stored: <?= number_format($reportedKillTotal) ?> · Observed: <?= number_format($observedKillTotal) ?></p>
            <?php endif; ?>
        </div>
        <div class="surface-tertiary">
            <p class="text-xs text-muted">Duration</p>
            <p class="text-lg text-slate-50 font-semibold"><?= proxy_e($durationLabel) ?></p>
        </div>
        <div class="surface-tertiary">
            <p class="text-xs text-muted">ISK Destroyed</p>
            <p class="text-lg text-slate-50 font-semibold"><?= proxy_format_isk($totalIskDestroyed) ?></p>
        </div>
    </div>
</section>
