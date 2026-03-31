<?php

declare(strict_types=1);
require_once __DIR__ . '/../../src/bootstrap.php';

// Default to Amamake (30002537) as the example system
$systemId = isset($_GET['system_id']) ? (int) $_GET['system_id'] : 30002537;
$systemId = max(1, $systemId);
$hops = isset($_GET['hops']) ? (int) $_GET['hops'] : 2;
$hops = max(1, min(3, $hops));

// Load system info for the page title
$systemRows = db_ref_systems_by_ids([$systemId]);
$systemName = (string) $systemId;
foreach ($systemRows as $row) {
    if ((int) ($row['system_id'] ?? 0) === $systemId) {
        $systemName = (string) ($row['system_name'] ?? $systemName);
        break;
    }
}

$mapPath = supplycore_system_area_svg($systemId, $hops);

$title = 'System Map — ' . $systemName;
$pageHeaderSummary = 'Radial neighbourhood map showing stargate connections around a focal system.';

include __DIR__ . '/../../src/views/partials/header.php';
?>

<section class="surface-primary">
    <div class="flex items-center justify-between gap-4">
        <div>
            <p class="text-xs uppercase tracking-[0.16em] text-muted">Battle Intelligence</p>
            <h1 class="mt-1 text-2xl font-semibold text-slate-50">System Map — <?= htmlspecialchars($systemName, ENT_QUOTES) ?></h1>
            <p class="mt-2 text-sm text-muted">
                Surrounding stargate connections up to <?= $hops ?> jump<?= $hops !== 1 ? 's' : '' ?> from
                <span class="font-semibold text-slate-200"><?= htmlspecialchars($systemName, ENT_QUOTES) ?></span>.
                Node ring colour = security status. Node fill colour = threat level.
            </p>
        </div>
        <div class="flex gap-2">
            <a href="/threat-corridors" class="btn-secondary">Threat Corridors</a>
            <a href="/theater-map" class="btn-secondary">Theater Map</a>
        </div>
    </div>
</section>

<section class="surface-primary mt-4">
    <form method="GET" class="flex gap-3 items-end flex-wrap">
        <input type="hidden" name="system_id" value="<?= $systemId ?>">
        <div>
            <label class="text-xs text-muted block mb-1">Surrounding Jumps</label>
            <select name="hops" class="w-40 rounded bg-slate-800 border border-border px-2 py-1.5 text-sm text-slate-100">
                <?php for ($i = 1; $i <= 3; $i++): ?>
                    <option value="<?= $i ?>" <?= $hops === $i ? 'selected' : '' ?>><?= $i ?> jump<?= $i !== 1 ? 's' : '' ?></option>
                <?php endfor; ?>
            </select>
        </div>
        <button type="submit" class="btn-secondary h-fit">Update</button>
    </form>
</section>

<section class="surface-primary mt-4">
    <?php if (is_string($mapPath) && $mapPath !== ''): ?>
        <?php $dialogId = 'system-map-dialog-' . $systemId; ?>
        <div class="rounded-lg border border-border/50 bg-slate-950 overflow-hidden">
            <div class="flex items-center justify-between gap-2 px-3 py-2 border-b border-border/30 bg-slate-900/60">
                <p class="text-[10px] uppercase tracking-[0.15em] text-slate-500">
                    <?= htmlspecialchars($systemName, ENT_QUOTES) ?> &mdash; <?= $hops ?>-jump neighbourhood
                </p>
                <button type="button"
                        data-dialog-open="<?= htmlspecialchars($dialogId, ENT_QUOTES) ?>"
                        class="flex items-center gap-1 text-[10px] text-slate-400 hover:text-slate-100 transition-colors">
                    <svg width="10" height="10" viewBox="0 0 10 10" fill="none">
                        <path d="M1 9L9 1M9 1H5M9 1V5" stroke="currentColor" stroke-width="1.4" stroke-linecap="round" stroke-linejoin="round"/>
                    </svg>
                    Expand
                </button>
            </div>
            <img src="<?= htmlspecialchars($mapPath, ENT_QUOTES) ?>"
                 alt="Neighbourhood map for <?= htmlspecialchars($systemName, ENT_QUOTES) ?>"
                 class="w-full cursor-zoom-in"
                 data-dialog-open="<?= htmlspecialchars($dialogId, ENT_QUOTES) ?>"
                 loading="lazy">
        </div>

        <div class="mt-3 flex flex-wrap items-center gap-x-5 gap-y-1 text-[10px] text-slate-500">
            <span class="flex items-center gap-1.5">
                <span class="inline-block w-2.5 h-2.5 rounded-full border" style="border-color:#10b981"></span>High-sec (≥0.5)
            </span>
            <span class="flex items-center gap-1.5">
                <span class="inline-block w-2.5 h-2.5 rounded-full border" style="border-color:#f59e0b"></span>Low-sec (0–0.5)
            </span>
            <span class="flex items-center gap-1.5">
                <span class="inline-block w-2.5 h-2.5 rounded-full border" style="border-color:#ef4444"></span>Null-sec (≤0)
            </span>
            <span class="flex items-center gap-1.5">
                <span class="inline-block w-2.5 h-2.5 rounded-full" style="background:#ef4444"></span>Critical threat
            </span>
            <span class="flex items-center gap-1.5">
                <span class="inline-block w-2.5 h-2.5 rounded-full" style="background:#f97316"></span>High threat
            </span>
            <span class="flex items-center gap-1.5">
                <span class="inline-block w-2.5 h-2.5 rounded-full" style="background:#94a3b8"></span>No data
            </span>
            <span class="flex items-center gap-1.5">
                <span class="inline-block w-5 h-px" style="background:#2d5a9e"></span>Direct connection
            </span>
            <span class="flex items-center gap-1.5">
                <span class="inline-block w-5 h-px" style="background:#1e3a5f"></span>Surrounding connection
            </span>
        </div>

        <dialog id="<?= htmlspecialchars($dialogId, ENT_QUOTES) ?>"
                class="rounded-xl border border-white/8 bg-slate-950 p-0 text-slate-100 shadow-2xl backdrop:bg-black/80 backdrop:backdrop-blur-sm w-[min(95vw,1000px)] max-h-[92vh]">
            <div class="flex flex-col">
                <div class="flex items-center justify-between gap-3 px-4 py-2.5 border-b border-white/8 bg-slate-900/70 shrink-0">
                    <div class="min-w-0">
                        <p class="text-[10px] uppercase tracking-[0.15em] text-slate-500">System Map</p>
                        <p class="mt-0.5 text-xs text-slate-200 font-medium">
                            <?= htmlspecialchars($systemName, ENT_QUOTES) ?> — <?= $hops ?>-jump neighbourhood
                        </p>
                    </div>
                    <button type="button"
                            data-dialog-close="<?= htmlspecialchars($dialogId, ENT_QUOTES) ?>"
                            class="flex shrink-0 items-center justify-center w-7 h-7 rounded border border-white/10 text-slate-400 hover:text-slate-100 hover:bg-slate-700/60 transition-colors">
                        <svg width="11" height="11" viewBox="0 0 11 11" fill="none">
                            <path d="M1 1l9 9M10 1L1 10" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"/>
                        </svg>
                    </button>
                </div>
                <div class="overflow-auto bg-[#04080f]">
                    <img src="<?= htmlspecialchars($mapPath, ENT_QUOTES) ?>"
                         alt="Expanded neighbourhood map for <?= htmlspecialchars($systemName, ENT_QUOTES) ?>"
                         class="w-full"
                         loading="lazy">
                </div>
            </div>
        </dialog>
    <?php else: ?>
        <p class="text-muted py-8 text-center">No map data available for system <?= $systemId ?>. Ensure <code>ref_systems</code> and <code>ref_stargates</code> are populated.</p>
    <?php endif; ?>
</section>

<?php include __DIR__ . '/../../src/views/partials/footer.php'; ?>
