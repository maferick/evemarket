<?php
$notice = flash('success');
?>
<!doctype html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title><?= htmlspecialchars($title ?? app_name(), ENT_QUOTES) ?> | <?= htmlspecialchars(app_name(), ENT_QUOTES) ?></title>
    <script src="https://cdn.jsdelivr.net/npm/@tailwindcss/browser@4"></script>
    <style type="text/tailwindcss">
        @theme {
            --color-background: #09090b;
            --color-card: #111113;
            --color-border: #27272a;
            --color-muted: #a1a1aa;
            --color-primary: #e2e8f0;
            --color-accent: #2563eb;
            --radius-xl: 1rem;
        }
    </style>
</head>
<body class="bg-background text-slate-100 min-h-screen antialiased">
<div class="flex min-h-screen">
    <?php include __DIR__ . '/sidebar.php'; ?>
    <main class="flex-1 px-6 py-8 lg:px-10">
        <header class="mb-8 flex flex-wrap items-center justify-between gap-4">
            <div>
                <p class="text-xs uppercase tracking-[0.2em] text-muted">EveMarket Console</p>
                <h1 class="text-2xl font-semibold"><?= htmlspecialchars($title ?? 'Dashboard', ENT_QUOTES) ?></h1>
            </div>
            <div class="rounded-xl border border-border bg-card px-4 py-2 text-sm text-muted">Built for PHP + MySQL + Apache2</div>
        </header>
        <?php if ($notice): ?>
            <div class="mb-6 rounded-xl border border-emerald-500/40 bg-emerald-500/10 px-4 py-3 text-sm text-emerald-200"><?= htmlspecialchars($notice, ENT_QUOTES) ?></div>
        <?php endif; ?>
