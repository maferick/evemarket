        <footer class="mt-10 border-t border-white/8 pt-5 text-sm text-slate-400">
            <div class="flex flex-wrap items-center justify-between gap-3">
                <p class="font-medium text-slate-300"><?= htmlspecialchars(app_name(), ENT_QUOTES) ?> · <?= htmlspecialchars(brand_tagline(), ENT_QUOTES) ?></p>
                <p class="text-xs uppercase tracking-[0.18em] text-slate-500">Brand family: <?= htmlspecialchars(brand_family_name(), ENT_QUOTES) ?></p>
            </div>
        </footer>
    </main>
</div>
<?php if (supplycore_live_refresh_should_include($liveRefreshConfig ?? null)): ?>
    <script src="/assets/js/ui-live-refresh.js"></script>
<?php endif; ?>
</body>
</html>
