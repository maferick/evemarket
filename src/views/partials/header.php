<?php
$notice = flash('success');
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
    <script src="https://cdn.jsdelivr.net/npm/@tailwindcss/browser@4"></script>
    <style type="text/tailwindcss">
        @theme {
            --color-background: #0b0f14;
            --color-background-elevated: #101722;
            --color-panel: #111927;
            --color-panel-strong: #0f1724;
            --color-panel-muted: #0d141d;
            --color-border: rgba(120, 168, 255, 0.16);
            --color-border-strong: rgba(120, 190, 255, 0.28);
            --color-muted: #8fa2bd;
            --color-muted-strong: #b6c4d9;
            --color-primary: #dbeafe;
            --color-accent: #3b82f6;
            --color-cyan: #22d3ee;
            --color-success: #22c55e;
            --color-warning: #f59e0b;
            --color-danger: #ef4444;
            --radius-xl: 1rem;
            --radius-2xl: 1.25rem;
            --shadow-panel: 0 18px 60px rgba(3, 8, 20, 0.45);
            --shadow-glow: 0 0 0 1px rgba(96, 165, 250, 0.08), 0 0 40px rgba(34, 211, 238, 0.08);
        }

        @layer base {
            html {
                color-scheme: dark;
            }

            body {
                @apply min-h-screen bg-background text-slate-100 antialiased;
                background-image:
                    radial-gradient(circle at top, rgba(59, 130, 246, 0.14), transparent 30%),
                    radial-gradient(circle at 85% 15%, rgba(34, 211, 238, 0.10), transparent 20%),
                    linear-gradient(180deg, #0b0f14 0%, #0a0f16 100%);
            }

            ::selection {
                background: rgba(56, 189, 248, 0.28);
                color: #eff6ff;
            }
        }

        @layer components {
            .app-shell {
                @apply flex min-h-screen;
            }

            .sidebar-shell {
                @apply hidden w-80 shrink-0 border-r border-white/5 bg-slate-950/55 px-5 py-6 backdrop-blur-xl lg:block;
                box-shadow: inset -1px 0 0 rgba(96, 165, 250, 0.08);
            }

            .brand-lockup {
                @apply relative mb-8 overflow-hidden rounded-2xl border border-white/8 bg-gradient-to-br from-slate-900 via-slate-900 to-slate-950 px-4 py-4 shadow-[0_20px_40px_rgba(2,6,23,0.45)] transition duration-200 hover:border-sky-400/25 hover:shadow-[0_22px_50px_rgba(6,182,212,0.12)];
            }

            .brand-lockup::after {
                content: '';
                position: absolute;
                inset: 0;
                pointer-events: none;
                background: linear-gradient(135deg, rgba(59,130,246,0.10), transparent 45%, rgba(34,211,238,0.06));
            }

            .nav-group {
                @apply space-y-2 rounded-2xl border border-white/6 bg-white/[0.03] p-2.5 shadow-[inset_0_1px_0_rgba(255,255,255,0.02)];
            }

            .nav-item {
                @apply relative flex items-center gap-3 rounded-xl border border-transparent px-3 py-2.5 text-sm font-medium text-slate-300 transition duration-200 hover:border-white/8 hover:bg-white/[0.04] hover:text-white focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-sky-400/60;
            }

            .nav-item-active {
                @apply border-sky-400/20 bg-sky-400/10 text-white shadow-[0_0_24px_rgba(59,130,246,0.16)];
            }

            .nav-item-active::before {
                content: '';
                position: absolute;
                left: -0.35rem;
                top: 0.65rem;
                bottom: 0.65rem;
                width: 3px;
                border-radius: 999px;
                background: linear-gradient(180deg, rgba(34,211,238,0.95), rgba(59,130,246,0.95));
                box-shadow: 0 0 18px rgba(34,211,238,0.6);
            }

            .subnav-item {
                @apply block rounded-xl border border-transparent px-3 py-2 text-sm text-slate-400 transition duration-200 hover:border-white/6 hover:bg-white/[0.04] hover:text-slate-100 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-sky-400/60;
            }

            .subnav-item-active {
                @apply border-sky-400/16 bg-slate-900/70 text-sky-100;
            }

            .page-header {
                @apply relative overflow-hidden rounded-2xl border border-white/8 bg-gradient-to-br from-slate-900 via-slate-900/95 to-slate-950 px-6 py-6 shadow-[0_24px_60px_rgba(2,6,23,0.42)];
            }

            .page-header::after {
                content: '';
                position: absolute;
                inset: 0;
                pointer-events: none;
                background: radial-gradient(circle at top right, rgba(34,211,238,0.10), transparent 24%), linear-gradient(135deg, rgba(59,130,246,0.10), transparent 50%);
            }

            .section-header {
                @apply mb-4 flex items-start justify-between gap-3;
            }

            .panel {
                @apply rounded-2xl border border-white/8 bg-gradient-to-b from-panel via-panel to-panel-strong p-6 shadow-[0_18px_60px_rgba(3,8,20,0.42)] transition duration-200 hover:border-sky-300/16 hover:shadow-[0_22px_70px_rgba(6,182,212,0.12)];
                box-shadow: inset 0 1px 0 rgba(255,255,255,0.03), 0 18px 60px rgba(3,8,20,0.42);
            }

            .tertiary-panel {
                @apply rounded-2xl border border-dashed border-white/10 bg-slate-950/45 p-4;
            }

            .kpi-card {
                @apply relative overflow-hidden rounded-2xl border border-white/8 bg-gradient-to-br from-panel via-panel to-slate-950 p-5 shadow-[0_18px_55px_rgba(3,8,20,0.45)] transition duration-200 hover:-translate-y-0.5 hover:border-sky-300/18 hover:shadow-[0_24px_70px_rgba(59,130,246,0.15)] focus-within:border-sky-300/25;
                box-shadow: inset 0 1px 0 rgba(255,255,255,0.03), 0 18px 55px rgba(3,8,20,0.45);
            }

            .list-row {
                @apply flex items-start gap-4 rounded-xl border border-white/7 bg-slate-950/45 px-4 py-3.5 transition duration-200 hover:border-sky-300/18 hover:bg-slate-900/75 hover:shadow-[0_12px_30px_rgba(8,47,73,0.18)];
            }

            .badge {
                @apply inline-flex items-center rounded-full border px-2.5 py-1 text-[0.65rem] font-semibold uppercase tracking-[0.18em];
            }

            .status-chip {
                @apply inline-flex items-center gap-2 rounded-full border px-3 py-1 text-xs font-semibold;
            }

            .muted-meta {
                @apply text-sm text-muted;
            }
        }
    </style>
</head>
<body>
<div class="app-shell">
    <?php include __DIR__ . '/sidebar.php'; ?>
    <main class="flex-1 px-5 py-6 sm:px-6 lg:px-8 xl:px-10 xl:py-8">
        <header class="page-header mb-8">
            <div class="relative z-10 flex flex-wrap items-start justify-between gap-5">
                <div class="max-w-3xl">
                    <p class="text-[0.68rem] font-semibold uppercase tracking-[0.28em] text-cyan/80"><?= htmlspecialchars(brand_console_label(), ENT_QUOTES) ?></p>
                    <div class="mt-3 flex flex-wrap items-center gap-3">
                        <h1 class="text-3xl font-semibold tracking-tight text-white sm:text-[2.1rem]"><?= htmlspecialchars($title ?? 'Dashboard', ENT_QUOTES) ?></h1>
                        <span class="status-chip border-cyan/20 bg-cyan/10 text-cyan-100">
                            <span class="h-2 w-2 rounded-full bg-cyan shadow-[0_0_12px_rgba(34,211,238,0.85)]"></span>
                            Alliance-scale supply intelligence
                        </span>
                    </div>
                    <p class="mt-3 max-w-2xl text-sm leading-6 text-slate-300/88"><?= htmlspecialchars(brand_tagline(), ENT_QUOTES) ?> for coverage monitoring, operational risk, and market sync readiness.</p>
                </div>
                <div class="relative z-10 flex flex-col items-start gap-3 rounded-2xl border border-white/8 bg-slate-950/45 px-4 py-3 text-sm text-slate-300 shadow-[inset_0_1px_0_rgba(255,255,255,0.02)] sm:items-end">
                    <span class="text-[0.7rem] font-semibold uppercase tracking-[0.22em] text-slate-500">Deployment profile</span>
                    <span class="font-medium text-slate-100">PHP 8 · MySQL · Apache2</span>
                    <span class="text-xs text-slate-400">Production-usable dark command layer</span>
                </div>
            </div>
        </header>
        <?php if ($notice): ?>
            <div class="mb-6 rounded-2xl border border-emerald-500/30 bg-emerald-500/12 px-4 py-3 text-sm text-emerald-100 shadow-[0_0_30px_rgba(34,197,94,0.10)]"><?= htmlspecialchars($notice, ENT_QUOTES) ?></div>
        <?php endif; ?>
