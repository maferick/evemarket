<?php
$notice = flash('success');
$dealAlertPopupRows = deal_alert_popup_view_model();
$dealAlertDismissMinutes = (int) (deal_alert_settings()['deal_alert_popup_dismiss_minutes'] ?? 120);
$pageHeaderSummary = trim((string) ($pageHeaderSummary ?? brand_tagline()));
$pageHeaderBadge = trim((string) ($pageHeaderBadge ?? 'Operations aligned'));
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
                        <span class="status-chip <?= htmlspecialchars($pageHeaderBadgeTone, ENT_QUOTES) ?>">
                            <span class="h-2 w-2 rounded-full bg-cyan shadow-[0_0_12px_rgba(34,211,238,0.65)]"></span>
                            <?= htmlspecialchars($pageHeaderBadge, ENT_QUOTES) ?>
                        </span>
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
        <?php if ($pageFreshness !== []): ?>
            <!-- ui-section:page-freshness:start -->
            <div class="mb-6 flex flex-wrap items-center justify-between gap-3 rounded-2xl border px-4 py-2 text-sm shadow-[0_0_24px_rgba(15,23,42,0.14)] <?= htmlspecialchars((string) ($pageFreshness['tone'] ?? 'border-amber-400/20 bg-amber-500/10 text-amber-100'), ENT_QUOTES) ?>" data-ui-section="page-freshness">
                <div>
                    <p class="font-medium"><?= htmlspecialchars((string) ($pageFreshness['message'] ?? 'Summary freshness unavailable.'), ENT_QUOTES) ?></p>
                    <p class="mt-1 text-xs opacity-80" data-ui-freshness-target="page-freshness">Last computed <?= htmlspecialchars((string) ($pageFreshness['computed_relative'] ?? 'Never'), ENT_QUOTES) ?> · <?= htmlspecialchars((string) ($pageFreshness['computed_at'] ?? 'Unavailable'), ENT_QUOTES) ?></p>
                </div>
                <span class="rounded-full border border-current/20 px-3 py-1 text-[11px] uppercase tracking-[0.14em]"><?= htmlspecialchars((string) ($pageFreshness['label'] ?? 'Unknown'), ENT_QUOTES) ?></span>
            </div>
            <!-- ui-section:page-freshness:end -->
        <?php endif; ?>
        <?php if ($dealAlertPopupRows !== []): ?>
            <?php
            $dealAlertPopupSignature = sha1(json_encode(array_values(array_map(static fn (array $row): string => (string) ($row['alert_key'] ?? ''), $dealAlertPopupRows)), JSON_UNESCAPED_SLASHES | JSON_INVALID_UTF8_SUBSTITUTE) ?: '');
            ?>
            <div class="fixed inset-x-4 top-4 z-50 ml-auto max-w-xl" data-deal-alert-popup data-deal-alert-signature="<?= htmlspecialchars($dealAlertPopupSignature, ENT_QUOTES) ?>" data-deal-alert-dismiss-minutes="<?= max(5, $dealAlertDismissMinutes) ?>">
                <section class="rounded-[1.6rem] border border-rose-400/35 bg-gradient-to-br from-[#2a0911]/96 via-[#1a0c14]/96 to-[#120811]/96 px-5 py-4 text-rose-50 shadow-[0_28px_90px_rgba(244,63,94,0.28)] backdrop-blur-xl">
                    <div class="flex items-start justify-between gap-4">
                        <div>
                            <p class="text-[0.68rem] font-semibold uppercase tracking-[0.24em] text-rose-200/80">Immediate market anomaly</p>
                            <h2 class="mt-2 text-lg font-semibold text-white">Critical / high-confidence deal alerts are active</h2>
                            <p class="mt-1 text-sm text-rose-100/90">SupplyCore detected listings far below their local historical baseline in the alliance market or reference hub.</p>
                        </div>
                        <div class="flex items-center gap-2">
                            <a href="/deal-alerts" class="rounded-full border border-rose-200/20 bg-white/8 px-3 py-1 text-xs font-medium text-white hover:bg-white/12">Open deals page</a>
                            <button type="button" class="rounded-full border border-white/20 bg-black/20 px-2.5 py-1 text-xs font-semibold uppercase tracking-[0.14em] text-white hover:bg-black/35" data-deal-alert-close>Close</button>
                        </div>
                    </div>

                    <div class="mt-4 space-y-3">
                        <?php foreach ($dealAlertPopupRows as $popupRow): ?>
                            <article class="rounded-2xl border border-white/10 bg-black/20 p-4">
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
                                    <form method="post" action="/deal-alerts" class="shrink-0">
                                        <input type="hidden" name="_token" value="<?= htmlspecialchars(csrf_token(), ENT_QUOTES) ?>">
                                        <input type="hidden" name="alert_action" value="dismiss">
                                        <input type="hidden" name="alert_key" value="<?= htmlspecialchars((string) ($popupRow['alert_key'] ?? ''), ENT_QUOTES) ?>">
                                        <input type="hidden" name="severity_rank" value="<?= (int) ($popupRow['severity_rank'] ?? 1) ?>">
                                        <input type="hidden" name="return_to" value="<?= htmlspecialchars((string) ($_SERVER['REQUEST_URI'] ?? '/deal-alerts'), ENT_QUOTES) ?>">
                                        <button type="submit" class="rounded-full border border-white/12 bg-white/6 px-3 py-1.5 text-xs font-medium text-white hover:bg-white/12">
                                            Dismiss <?= $dealAlertDismissMinutes ?>m
                                        </button>
                                    </form>
                                </div>
                            </article>
                        <?php endforeach; ?>
                    </div>
                </section>
            </div>
            <script>
                (() => {
                    const popup = document.querySelector('[data-deal-alert-popup]');
                    if (!popup) return;
                    const signature = popup.getAttribute('data-deal-alert-signature') || '';
                    const ttlMinutes = Number.parseInt(popup.getAttribute('data-deal-alert-dismiss-minutes') || '120', 10);
                    const key = `deal-alert-popup:${signature}`;
                    try {
                        const raw = window.sessionStorage.getItem(key);
                        if (raw) {
                            const expiresAt = Number.parseInt(raw, 10);
                            if (Number.isFinite(expiresAt) && Date.now() < expiresAt) {
                                popup.remove();
                                return;
                            }
                            window.sessionStorage.removeItem(key);
                        }
                    } catch (error) {
                        // ignore storage access failures and still allow close interaction
                    }
                    const closeButton = popup.querySelector('[data-deal-alert-close]');
                    if (!closeButton) return;
                    closeButton.addEventListener('click', () => {
                        try {
                            const minutes = Number.isFinite(ttlMinutes) ? Math.max(5, ttlMinutes) : 120;
                            const expiresAt = Date.now() + (minutes * 60 * 1000);
                            window.sessionStorage.setItem(key, String(expiresAt));
                        } catch (error) {
                            // no-op
                        }
                        popup.remove();
                    });
                })();
            </script>
        <?php endif; ?>
