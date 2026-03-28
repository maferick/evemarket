<section class="surface-primary">
    <a href="/theater-intelligence" class="text-sm text-accent">&#8592; Back to theaters</a>

    <div class="mt-3 flex items-center justify-between gap-4">
        <div>
            <p class="text-xs uppercase tracking-[0.16em] text-muted">Theater Intelligence</p>
            <h1 class="mt-1 text-2xl font-semibold text-slate-50">
                <?= htmlspecialchars((string) ($theater['primary_system_name'] ?? 'Unknown'), ENT_QUOTES) ?>
                <?php if ((int) ($theater['system_count'] ?? 0) > 1): ?>
                    <span class="text-sm text-muted">+<?= (int) ($theater['system_count'] ?? 0) - 1 ?> systems</span>
                <?php endif; ?>
            </h1>
            <p class="mt-1 text-base text-slate-200">
                <span class="<?= $sideColorClass[$ourSide ?? 'side_a'] ?? 'text-blue-300' ?> font-semibold"><?= htmlspecialchars($sideLabels[$ourSide ?? 'side_a'] ?? 'Side A', ENT_QUOTES) ?></span>
                <?php if ($ourSide !== null): ?>
                    <span class="text-[10px] uppercase tracking-wider bg-blue-900/60 text-blue-300 rounded-full px-1.5 py-0.5 ml-1">Tracked</span>
                <?php endif; ?>
                <span class="text-slate-500 mx-2">vs</span>
                <span class="<?= $sideColorClass[$enemySide] ?? 'text-red-300' ?> font-semibold"><?= htmlspecialchars($sideLabels[$enemySide] ?? 'Side B', ENT_QUOTES) ?></span>
            </p>
            <p class="mt-1 text-sm text-slate-300">
                <?= htmlspecialchars((string) ($theater['region_name'] ?? ''), ENT_QUOTES) ?>
                &middot; <?= htmlspecialchars($theaterStartActual, ENT_QUOTES) ?>
                &mdash; <?= htmlspecialchars($theaterEndActual, ENT_QUOTES) ?>
            </p>
        </div>
    </div>

    <div class="mt-3 grid gap-3 md:grid-cols-6">
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
            <p class="text-lg text-slate-50 font-semibold"><?= $durationLabel ?></p>
        </div>
        <div class="surface-tertiary">
            <p class="text-xs text-muted">ISK Destroyed</p>
            <p class="text-lg text-slate-50 font-semibold"><?= supplycore_format_isk($totalIskDestroyed) ?></p>
        </div>
    </div>

    <?php if ($dataQualityNotes !== []): ?>
        <div class="mt-3 rounded-lg border border-amber-400/30 bg-amber-500/10 p-3">
            <p class="text-xs uppercase tracking-[0.15em] text-amber-300">Data quality notes</p>
            <ul class="mt-1 list-disc space-y-1 pl-5 text-sm text-amber-100/90">
                <?php foreach ($dataQualityNotes as $note): ?>
                    <li><?= htmlspecialchars($note, ENT_QUOTES) ?></li>
                <?php endforeach; ?>
            </ul>
        </div>
    <?php endif; ?>
</section>
