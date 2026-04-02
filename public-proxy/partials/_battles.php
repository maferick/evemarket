<?php if ($battles !== []): ?>
<section class="surface-primary mt-4">
    <h2 class="text-lg font-semibold text-slate-50">Constituent Battles</h2>
    <?php
        $maxParticipants = 0;
        foreach ($battles as $battleRow) {
            $maxParticipants = max($maxParticipants, (int) ($battleRow['participant_count'] ?? 0));
        }
        $maxParticipants = max($maxParticipants, 1);
        $sizeClasses = [
            'MICRO' => 'bg-slate-900/70 text-slate-300 border border-slate-500/70',
            'SMALL' => 'bg-indigo-900/60 text-indigo-200 border border-indigo-400/60',
            'MEDIUM'=> 'bg-cyan-900/60 text-cyan-200 border border-cyan-400/60',
            'LARGE' => 'bg-yellow-900/60 text-yellow-200 border border-yellow-400/70',
            'MEGA'  => 'bg-orange-900/60 text-orange-200 border border-orange-400/70',
            'ULTRA' => 'bg-red-900/60 text-red-200 border border-red-400/70',
        ];
    ?>
    <div class="mt-3 table-shell">
        <table class="table-ui">
            <thead>
                <tr class="border-b border-border/70 text-xs uppercase tracking-wider text-muted">
                    <th class="px-3 py-2 text-left">System</th>
                    <th class="px-3 py-2 text-right">Kills</th>
                    <th class="px-3 py-2 text-right">Pilots</th>
                    <th class="px-3 py-2 text-left">Size</th>
                    <th class="px-3 py-2 text-left">Start</th>
                    <th class="px-3 py-2 text-left">End</th>
                </tr>
            </thead>
            <tbody>
                <?php foreach ($battles as $b): ?>
                    <?php
                        $bParticipants = (int) ($b['participant_count'] ?? 0);
                        $killCount = (int) ($b['kill_count'] ?? 0);
                        $participantPct = (int) round(($bParticipants / $maxParticipants) * 100);
                        $battleSize = strtoupper((string) ($b['battle_size_class'] ?? 'MICRO'));
                        $sizeClass = $sizeClasses[$battleSize] ?? $sizeClasses['MICRO'];
                    ?>
                    <tr class="border-b border-border/50">
                        <td class="px-3 py-2 text-slate-100"><?= proxy_e((string) ($b['system_name'] ?? '-')) ?></td>
                        <td class="px-3 py-2 text-right"><?= number_format($killCount) ?></td>
                        <td class="px-3 py-2 text-right">
                            <div class="flex items-center justify-end gap-2">
                                <div class="w-16 h-1.5 rounded-full bg-slate-700 overflow-hidden">
                                    <div class="h-full rounded-full bg-blue-500/70" style="width: <?= $participantPct ?>%"></div>
                                </div>
                                <span><?= number_format($bParticipants) ?></span>
                            </div>
                        </td>
                        <td class="px-3 py-2">
                            <span class="inline-block rounded-full px-2.5 py-0.5 text-[10px] font-semibold uppercase tracking-wider <?= $sizeClass ?>">
                                <?= proxy_e((string) ($b['battle_size_class'] ?? '')) ?>
                            </span>
                        </td>
                        <td class="px-3 py-2 text-slate-300 text-xs"><?= proxy_e((string) ($b['started_at'] ?? '')) ?></td>
                        <td class="px-3 py-2 text-slate-300 text-xs"><?= proxy_e((string) ($b['ended_at'] ?? '')) ?></td>
                    </tr>
                <?php endforeach; ?>
            </tbody>
        </table>
    </div>
</section>
<?php endif; ?>
