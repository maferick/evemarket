<section class="surface-primary mt-4">
    <h2 class="text-lg font-semibold text-slate-50">Constituent Battles</h2>
    <?php
        $maxParticipants = 0;
        foreach ($battles as $battleRow) {
            $maxParticipants = max($maxParticipants, (int) ($battleRow['participant_count'] ?? 0));
        }
        $maxParticipants = max($maxParticipants, 1);
        $sizeClasses = [
            'MICRO' => 'bg-slate-900/70 text-slate-300 border border-slate-500/70 ring-1 ring-slate-500/30',
            'SMALL' => 'bg-indigo-900/60 text-indigo-200 border border-indigo-400/60 ring-1 ring-indigo-400/30',
            'MEDIUM'=> 'bg-cyan-900/60 text-cyan-200 border border-cyan-400/60 ring-1 ring-cyan-400/30',
            'LARGE' => 'bg-yellow-900/60 text-yellow-200 border border-yellow-400/70 ring-1 ring-yellow-400/30',
            'MEGA'  => 'bg-orange-900/60 text-orange-200 border border-orange-400/70 ring-1 ring-orange-400/30',
            'ULTRA' => 'bg-red-900/60 text-red-200 border border-red-400/70 ring-1 ring-red-400/30',
        ];
    ?>
    <div class="mt-3 table-shell">
        <table id="battles-table" class="table-ui">
            <thead>
                <tr class="border-b border-border/70 text-xs uppercase tracking-[0.15em] text-muted">
                    <th data-sort class="px-3 py-2 text-left cursor-pointer select-none hover:text-slate-200">System <span class="sort-indicator">↕</span></th>
                    <th data-sort class="px-3 py-2 text-right cursor-pointer select-none hover:text-slate-200">Participants <span class="sort-indicator">↕</span></th>
                    <th data-sort class="px-3 py-2 text-left cursor-pointer select-none hover:text-slate-200">Size <span class="sort-indicator">↕</span></th>
                    <th data-sort class="px-3 py-2 text-left cursor-pointer select-none hover:text-slate-200">Start <span class="sort-indicator">↕</span></th>
                    <th data-sort class="px-3 py-2 text-left cursor-pointer select-none hover:text-slate-200">End <span class="sort-indicator">↕</span></th>
                    <th data-sort class="px-3 py-2 text-right cursor-pointer select-none hover:text-slate-200">Weight <span class="sort-indicator">↕</span></th>
                    <th class="px-3 py-2 text-right"></th>
                </tr>
            </thead>
            <tbody>
                <?php if ($battles === []): ?>
                    <tr><td colspan="7" class="px-3 py-6 text-sm text-muted">No battles linked.</td></tr>
                <?php else: ?>
                    <?php foreach ($battles as $b): ?>
                        <?php
                            $participants = (int) ($b['participant_count'] ?? 0);
                            $participantPct = (int) round(($participants / $maxParticipants) * 100);
                            $battleSize = strtoupper((string) ($b['battle_size_class'] ?? 'MICRO'));
                            $sizeClass = $sizeClasses[$battleSize] ?? $sizeClasses['MICRO'];
                            $startAt = (string) ($b['started_at'] ?? '');
                            $endAt = (string) ($b['ended_at'] ?? '');
                        ?>
                        <tr class="border-b border-border/50">
                            <td class="px-3 py-2 text-slate-100" data-val="<?= htmlspecialchars((string) ($b['system_name'] ?? '-'), ENT_QUOTES) ?>"><?= htmlspecialchars((string) ($b['system_name'] ?? '-'), ENT_QUOTES) ?></td>
                            <td class="px-3 py-2 text-right" data-val="<?= $participants ?>">
                                <div class="flex items-center justify-end gap-2">
                                    <div class="w-16 h-1.5 rounded-full bg-slate-700 overflow-hidden">
                                        <div class="h-full rounded-full bg-blue-500/70" style="width: <?= $participantPct ?>%"></div>
                                    </div>
                                    <span><?= number_format($participants) ?></span>
                                </div>
                            </td>
                            <td class="px-3 py-2" data-val="<?= htmlspecialchars($battleSize, ENT_QUOTES) ?>">
                                <span class="inline-block rounded-full px-2.5 py-0.5 text-[10px] font-semibold uppercase tracking-[0.14em] <?= $sizeClass ?>">
                                    <?= htmlspecialchars((string) ($b['battle_size_class'] ?? ''), ENT_QUOTES) ?>
                                </span>
                            </td>
                            <td class="px-3 py-2 text-slate-300 text-xs" data-val="<?= strtotime($startAt) ?: 0 ?>"><?= htmlspecialchars($startAt, ENT_QUOTES) ?></td>
                            <td class="px-3 py-2 text-slate-300 text-xs" data-val="<?= strtotime($endAt) ?: 0 ?>"><?= htmlspecialchars($endAt, ENT_QUOTES) ?></td>
                            <td class="px-3 py-2 text-right" data-val="<?= (float) ($b['weight'] ?? 0) ?>"><?= number_format((float) ($b['weight'] ?? 0), 2) ?></td>
                            <td class="px-3 py-2 text-right">
                                <a class="text-accent text-sm" href="/battle-intelligence/battle.php?battle_id=<?= urlencode((string) ($b['battle_id'] ?? '')) ?>">Detail</a>
                            </td>
                        </tr>
                    <?php endforeach; ?>
                <?php endif; ?>
            </tbody>
        </table>
    </div>
</section>
<script>
(() => {
    const table = document.getElementById('battles-table');
    if (!table) return;
    let sortCol = -1;
    let sortDir = 1;
    table.querySelectorAll('thead th[data-sort]').forEach((th, i) => {
        th.addEventListener('click', () => {
            sortDir = (sortCol === i) ? -sortDir : 1;
            sortCol = i;
            const tbody = table.querySelector('tbody');
            const rows = Array.from(tbody.querySelectorAll('tr'));
            rows.sort((a, b) => {
                const va = a.cells[i].dataset.val ?? a.cells[i].textContent.trim();
                const vb = b.cells[i].dataset.val ?? b.cells[i].textContent.trim();
                return (isNaN(va) ? va.localeCompare(vb) : va - vb) * sortDir;
            });
            rows.forEach((row) => tbody.appendChild(row));
            table.querySelectorAll('thead th').forEach((header) => {
                header.classList.remove('sort-asc', 'sort-desc');
                const indicator = header.querySelector('.sort-indicator');
                if (indicator) indicator.textContent = '↕';
            });
            th.classList.add(sortDir === 1 ? 'sort-asc' : 'sort-desc');
            const activeIndicator = th.querySelector('.sort-indicator');
            if (activeIndicator) activeIndicator.textContent = sortDir === 1 ? '▲' : '▼';
        });
    });
})();
</script>
