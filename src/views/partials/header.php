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
    <link rel="stylesheet" href="/assets/css/app.css">
</head>
<body>
<div class="app-shell">
    <?php include __DIR__ . '/sidebar.php'; ?>
    <main class="relative flex-1 px-5 py-6 sm:px-6 lg:px-8 xl:px-10 xl:py-8">
        <header class="page-header mb-8">
            <div class="relative z-10 flex flex-wrap items-start justify-between gap-5">
                <div class="max-w-3xl">
                    <p class="eyebrow text-cyan/80"><?= htmlspecialchars(brand_console_label(), ENT_QUOTES) ?></p>
                    <div class="mt-3 flex flex-wrap items-center gap-3">
                        <h1 class="text-3xl font-semibold tracking-tight text-white sm:text-[2.1rem]"><?= htmlspecialchars($title ?? 'Dashboard', ENT_QUOTES) ?></h1>
                        <span class="status-chip border-cyan/20 bg-cyan/10 text-cyan-100">
                            <span class="h-2 w-2 rounded-full bg-cyan shadow-[0_0_12px_rgba(34,211,238,0.65)]"></span>
                            Alliance logistics intelligence
                        </span>
                    </div>
                    <p class="mt-3 max-w-2xl text-sm leading-6 text-slate-300/88">Alliance logistics intelligence for coverage, risk, and market readiness.</p>
                </div>
                <div class="surface-tertiary relative z-10 flex flex-col items-start gap-3 px-4 py-3 text-sm text-slate-300 sm:items-end">
                    <span class="eyebrow">Deployment profile</span>
                    <span class="font-medium text-slate-100">PHP 8 · MySQL · Apache2</span>
                    <span class="text-xs text-slate-500">Premium operational command layer</span>
                </div>
            </div>
        </header>
        <?php if ($notice): ?>
            <div class="mb-6 rounded-2xl border border-emerald-500/30 bg-emerald-500/12 px-4 py-3 text-sm text-emerald-100 shadow-[0_0_24px_rgba(34,197,94,0.08)]"><?= htmlspecialchars($notice, ENT_QUOTES) ?></div>
        <?php endif; ?>
