<?php
$notice = flash('success');
$dealAlertPopupRows = deal_alert_popup_view_model();
$dealAlertDismissMinutes = (int) (deal_alert_settings()['deal_alert_popup_dismiss_minutes'] ?? 120);
$pageHeaderSummary = trim((string) ($pageHeaderSummary ?? ''));
$pageHeaderBadge = trim((string) ($pageHeaderBadge ?? ''));
$pageHeaderBadgeTone = trim((string) ($pageHeaderBadgeTone ?? 'border-cyan/20 bg-cyan/10 text-cyan-100'));
$liveRefreshSummary = supplycore_live_refresh_summary($liveRefreshConfig ?? null);
$pageFreshness = is_array($pageFreshness ?? null) ? $pageFreshness : [];
$pageFreshnessLine = $pageFreshness !== []
    ? trim((string) (($pageFreshness['label'] ?? 'Freshness') . ' · ' . ($pageFreshness['computed_relative'] ?? 'Unknown') . ' · ' . ($pageFreshness['computed_at'] ?? 'Unavailable')))
    : trim((string) ('Live refresh · ' . ($liveRefreshSummary['last_refresh_relative'] ?? 'Unknown')));
?>
<!doctype html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="application-name" content="<?= htmlspecialchars(app_name(), ENT_QUOTES) ?>">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta name="description" content="SupplyCore is an alliance-scale supply intelligence platform for logistics, pricing, sync health, and market coverage.">
    <?php $pageTitle = trim((string) ($title ?? app_name())); ?>
    <title><?= htmlspecialchars($pageTitle, ENT_QUOTES) ?><?= $pageTitle !== app_name() ? " | " . htmlspecialchars(app_name(), ENT_QUOTES) : "" ?></title>
    <link rel="icon" type="image/svg+xml" href="<?= htmlspecialchars(brand_favicon_path(), ENT_QUOTES) ?>">
    <link rel="shortcut icon" href="<?= htmlspecialchars(brand_favicon_path(), ENT_QUOTES) ?>">
    <link rel="stylesheet" href="/assets/css/app.css">
</head>
<body<?= supplycore_live_refresh_page_data_attributes($liveRefreshConfig ?? null) ?>>
<div class="app-shell">
    <?php include __DIR__ . '/sidebar.php'; ?>
    <main class="relative flex-1 px-5 py-6 sm:px-6 lg:px-8 xl:px-10 xl:py-8">
        <header class="page-header mb-8">
            <div class="relative z-10 grid gap-4 xl:grid-cols-[minmax(0,1fr)_auto] xl:items-start">
                <div class="max-w-3xl">
                    <p class="eyebrow text-cyan/80"><?= htmlspecialchars(brand_console_label(), ENT_QUOTES) ?></p>
                    <div class="mt-3 flex flex-wrap items-center gap-3">
                        <h1 class="text-3xl font-semibold tracking-tight text-white sm:text-[2.1rem]"><?= htmlspecialchars($title ?? 'Dashboard', ENT_QUOTES) ?></h1>
                        <?php if ($pageHeaderBadge !== ''): ?>
                            <span class="status-chip <?= htmlspecialchars($pageHeaderBadgeTone, ENT_QUOTES) ?>">
                                <span class="h-2 w-2 rounded-full bg-cyan shadow-[0_0_12px_rgba(34,211,238,0.65)]"></span>
                                <?= htmlspecialchars($pageHeaderBadge, ENT_QUOTES) ?>
                            </span>
                        <?php endif; ?>
                    </div>
                    <?php if ($pageHeaderSummary !== ''): ?>
                        <p class="mt-3 max-w-2xl text-sm leading-6 text-slate-300/88"><?= htmlspecialchars($pageHeaderSummary, ENT_QUOTES) ?></p>
                    <?php endif; ?>
                    <?php if ($pageFreshness === [] && $pageFreshnessLine !== ''): ?>
                        <p class="mt-3 text-sm text-slate-300" data-ui-freshness-target="page-freshness"><?= htmlspecialchars($pageFreshnessLine, ENT_QUOTES) ?></p>
                    <?php endif; ?>
                </div>
                <div class="flex flex-wrap items-start justify-end gap-2">
                    <?php if ($pageFreshness !== []): ?>
                        <span class="badge <?= htmlspecialchars((string) ($pageFreshness['tone'] ?? 'border-amber-400/20 bg-amber-500/10 text-amber-100'), ENT_QUOTES) ?>">
                            <?= htmlspecialchars((string) ($pageFreshness['label'] ?? 'Unknown'), ENT_QUOTES) ?>
                        </span>
                    <?php endif; ?>
                    <span class="badge <?= htmlspecialchars((string) ($liveRefreshSummary['health_tone'] ?? 'border-slate-400/20 bg-slate-500/10 text-slate-100'), ENT_QUOTES) ?>">
                        <?= htmlspecialchars((string) ($liveRefreshSummary['mode_label'] ?? 'Live updates unavailable'), ENT_QUOTES) ?>
                    </span>
                </div>
            </div>
        </header>
        <?php if ($notice): ?>
            <div class="mb-6 rounded-2xl border border-emerald-500/30 bg-emerald-500/12 px-4 py-3 text-sm text-emerald-100 shadow-[0_0_24px_rgba(34,197,94,0.08)]"><?= htmlspecialchars($notice, ENT_QUOTES) ?></div>
        <?php endif; ?>
        <?php if ($pageFreshness !== [] && empty($suppressPageFreshness)): ?>
            <?php
            $freshnessState = (string) ($pageFreshness['state'] ?? 'stale');
            $freshnessRelative = (string) ($pageFreshness['computed_relative'] ?? 'Unknown');
            $freshnessAt = (string) ($pageFreshness['computed_at'] ?? '');
            ?>
            <!-- ui-section:page-freshness:start -->
            <?php if ($freshnessState === 'fresh'): ?>
                <!-- Fresh data: no banner shown -->
            <?php elseif ($freshnessState === 'updating'): ?>
                <p class="mb-4 text-xs text-slate-500" data-ui-section="page-freshness" data-ui-freshness-target="page-freshness">Refreshing data... · Last snapshot <?= htmlspecialchars($freshnessRelative, ENT_QUOTES) ?></p>
            <?php elseif ($freshnessState === 'stale'): ?>
                <div class="mb-4 flex items-center gap-2 text-xs text-amber-300/80" data-ui-section="page-freshness">
                    <svg viewBox="0 0 16 16" fill="currentColor" width="14" height="14" class="h-3.5 w-3.5 shrink-0 opacity-70"><path d="M8 1a7 7 0 1 0 0 14A7 7 0 0 0 8 1Zm0 3a.75.75 0 0 1 .75.75v3.5a.75.75 0 0 1-1.5 0v-3.5A.75.75 0 0 1 8 4Zm0 7a.75.75 0 1 0 0-1.5.75.75 0 0 0 0 1.5Z"/></svg>
                    <span data-ui-freshness-target="page-freshness">Data from <?= htmlspecialchars($freshnessRelative, ENT_QUOTES) ?><?= $freshnessAt !== '' ? ' · ' . htmlspecialchars($freshnessAt, ENT_QUOTES) : '' ?></span>
                </div>
            <?php else: ?>
                <p class="mb-4 text-xs text-slate-500" data-ui-section="page-freshness" data-ui-freshness-target="page-freshness">Data from <?= htmlspecialchars($freshnessRelative, ENT_QUOTES) ?></p>
            <?php endif; ?>
            <!-- ui-section:page-freshness:end -->
        <?php endif; ?>
        <?php if ($dealAlertPopupRows !== []): ?>
            <!-- Notification bell trigger (fixed top-right) -->
            <button type="button" class="fixed right-5 top-5 z-50 flex h-10 w-10 items-center justify-center rounded-full border border-rose-400/30 bg-[#1a0c14]/90 text-rose-100 shadow-[0_4px_24px_rgba(244,63,94,0.25)] backdrop-blur-xl transition hover:bg-rose-500/20" data-alert-bell-toggle aria-label="Open anomaly alerts">
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" class="h-5 w-5"><path stroke-linecap="round" stroke-linejoin="round" d="M18 8A6 6 0 0 0 6 8c0 7-3 9-3 9h18s-3-2-3-9"/><path stroke-linecap="round" stroke-linejoin="round" d="M13.73 21a2 2 0 0 1-3.46 0"/></svg>
                <span class="absolute -right-1 -top-1 flex h-5 min-w-5 items-center justify-center rounded-full bg-rose-500 px-1 text-[10px] font-bold text-white" data-alert-bell-count><?= count($dealAlertPopupRows) ?></span>
            </button>

            <!-- Slide-out alert drawer -->
            <div class="fixed inset-0 z-[60] hidden" data-alert-drawer>
                <div class="absolute inset-0 bg-black/50 backdrop-blur-sm" data-alert-drawer-backdrop></div>
                <aside class="absolute bottom-0 right-0 top-0 flex w-full max-w-md flex-col border-l border-rose-400/20 bg-gradient-to-b from-[#1a0c14] via-[#120811] to-[#0b0610] shadow-[0_0_60px_rgba(244,63,94,0.15)]">
                    <div class="flex items-center justify-between border-b border-white/8 px-5 py-4">
                        <div>
                            <p class="text-[0.68rem] font-semibold uppercase tracking-[0.24em] text-rose-200/80">Market anomalies</p>
                            <p class="mt-1 text-sm text-rose-100/70"><span data-alert-drawer-count><?= count($dealAlertPopupRows) ?></span> active alerts</p>
                        </div>
                        <div class="flex items-center gap-2">
                            <a href="/deal-alerts" class="rounded-full border border-rose-200/20 bg-white/8 px-3 py-1 text-xs font-medium text-white hover:bg-white/12">View all</a>
                            <button type="button" class="rounded-full border border-rose-200/20 bg-white/8 px-3 py-1 text-xs font-medium text-white hover:bg-white/12" data-alert-dismiss-all>Dismiss all</button>
                            <button type="button" class="rounded-full border border-white/20 bg-black/20 px-2.5 py-1 text-xs font-semibold uppercase tracking-[0.14em] text-white hover:bg-black/35" data-alert-drawer-close>Close</button>
                        </div>
                    </div>
                    <div class="flex-1 overflow-y-auto px-5 py-4 space-y-3" data-alert-drawer-list>
                        <?php foreach ($dealAlertPopupRows as $popupRow): ?>
                            <article class="rounded-2xl border border-white/10 bg-black/20 p-4" data-alert-item data-alert-id="<?= htmlspecialchars((string) ($popupRow['alert_key'] ?? ''), ENT_QUOTES) ?>">
                                <div class="flex flex-wrap items-start justify-between gap-3">
                                    <div class="min-w-0 flex-1">
                                        <div class="flex flex-wrap items-center gap-2">
                                            <span class="inline-flex items-center rounded-full border px-2.5 py-1 text-[11px] font-semibold uppercase tracking-[0.16em] <?= htmlspecialchars((string) ($popupRow['severity_tone'] ?? 'border-rose-400/35 bg-rose-500/12 text-rose-100'), ENT_QUOTES) ?>">
                                                <?= htmlspecialchars((string) ($popupRow['severity_label'] ?? 'Deal alert'), ENT_QUOTES) ?>
                                            </span>
                                            <span class="text-xs text-rose-100/70"><?= htmlspecialchars((string) ($popupRow['market_label'] ?? ''), ENT_QUOTES) ?></span>
                                        </div>
                                        <p class="mt-2 text-base font-semibold text-white"><?= htmlspecialchars((string) ($popupRow['display_name'] ?? ''), ENT_QUOTES) ?></p>
                                        <p class="mt-1 text-xs text-rose-100/75">
                                            Current <?= htmlspecialchars((string) ($popupRow['current_price_label'] ?? '—'), ENT_QUOTES) ?>
                                            · Normal <?= htmlspecialchars((string) ($popupRow['normal_price_label'] ?? '—'), ENT_QUOTES) ?>
                                            · <?= htmlspecialchars((string) ($popupRow['percent_of_normal_label'] ?? '—'), ENT_QUOTES) ?> of normal
                                        </p>
                                        <p class="mt-1 text-xs text-rose-100/60">
                                            <?= htmlspecialchars((string) ($popupRow['source_name'] ?? ''), ENT_QUOTES) ?> · Fresh <?= htmlspecialchars((string) ($popupRow['freshness_relative'] ?? 'Unknown'), ENT_QUOTES) ?>
                                        </p>
                                    </div>
                                    <button type="button" class="shrink-0 rounded-full border border-white/12 bg-white/6 px-3 py-1.5 text-xs font-medium text-white hover:bg-white/12" data-alert-dismiss="<?= htmlspecialchars((string) ($popupRow['alert_key'] ?? ''), ENT_QUOTES) ?>">
                                        Dismiss
                                    </button>
                                </div>
                            </article>
                        <?php endforeach; ?>
                    </div>
                </aside>
            </div>
            <script>
                (() => {
                    const bell = document.querySelector('[data-alert-bell-toggle]');
                    const drawer = document.querySelector('[data-alert-drawer]');
                    if (!bell || !drawer) return;
                    const backdrop = drawer.querySelector('[data-alert-drawer-backdrop]');
                    const closeBtn = drawer.querySelector('[data-alert-drawer-close]');
                    const bellCount = bell.querySelector('[data-alert-bell-count]');
                    const drawerCount = drawer.querySelector('[data-alert-drawer-count]');

                    function filterDismissed() {
                        let visible = 0;
                        drawer.querySelectorAll('[data-alert-item]').forEach(item => {
                            const id = item.getAttribute('data-alert-id');
                            try {
                                const ts = localStorage.getItem('dismissed_alert_' + id);
                                if (ts && Date.now() < Number(ts)) { item.remove(); return; }
                                if (ts) localStorage.removeItem('dismissed_alert_' + id);
                            } catch (e) {}
                            visible++;
                        });
                        updateCount(visible);
                        if (visible === 0) { bell.classList.add('hidden'); }
                    }

                    function updateCount(n) {
                        if (bellCount) bellCount.textContent = n;
                        if (drawerCount) drawerCount.textContent = n;
                    }

                    function openDrawer() { drawer.classList.remove('hidden'); document.body.style.overflow = 'hidden'; }
                    function closeDrawer() { drawer.classList.add('hidden'); document.body.style.overflow = ''; }

                    bell.addEventListener('click', openDrawer);
                    if (backdrop) backdrop.addEventListener('click', closeDrawer);
                    if (closeBtn) closeBtn.addEventListener('click', closeDrawer);
                    document.addEventListener('keydown', e => { if (e.key === 'Escape') closeDrawer(); });

                    const dismissAllBtn = drawer.querySelector('[data-alert-dismiss-all]');
                    if (dismissAllBtn) {
                        dismissAllBtn.addEventListener('click', () => {
                            const ttl = <?= $dealAlertDismissMinutes ?> * 60 * 1000;
                            drawer.querySelectorAll('[data-alert-item]').forEach(item => {
                                const id = item.getAttribute('data-alert-id');
                                try { localStorage.setItem('dismissed_alert_' + id, String(Date.now() + ttl)); } catch (e) {}
                                item.remove();
                            });
                            updateCount(0);
                            closeDrawer();
                            bell.classList.add('hidden');
                        });
                    }

                    drawer.querySelectorAll('[data-alert-dismiss]').forEach(btn => {
                        btn.addEventListener('click', () => {
                            const id = btn.getAttribute('data-alert-dismiss');
                            const ttl = 2 * 60 * 60 * 1000; // 2 hours
                            try { localStorage.setItem('dismissed_alert_' + id, String(Date.now() + ttl)); } catch (e) {}
                            const item = btn.closest('[data-alert-item]');
                            if (item) item.remove();
                            const remaining = drawer.querySelectorAll('[data-alert-item]').length;
                            updateCount(remaining);
                            if (remaining === 0) { closeDrawer(); bell.classList.add('hidden'); }
                        });
                    });

                    filterDismissed();
                })();
            </script>
        <?php endif; ?>
