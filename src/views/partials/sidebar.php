<aside class="sidebar-shell">
    <a href="/" class="brand-lockup block">
        <div class="relative z-10 flex items-center gap-3">
            <img src="<?= htmlspecialchars(brand_logo_path(), ENT_QUOTES) ?>" alt="<?= htmlspecialchars(app_name(), ENT_QUOTES) ?> logo" class="h-12 w-12 rounded-2xl border border-white/10 bg-slate-950/70 p-2 shadow-[0_0_30px_rgba(59,130,246,0.18)]">
            <div>
                <p class="text-[0.68rem] font-semibold uppercase tracking-[0.26em] text-cyan/75"><?= htmlspecialchars(brand_family_name(), ENT_QUOTES) ?></p>
                <p class="mt-1 text-xl font-semibold tracking-tight text-white"><?= htmlspecialchars(app_name(), ENT_QUOTES) ?></p>
                <p class="mt-1 text-xs text-slate-400">Supply intelligence command layer</p>
            </div>
        </div>
    </a>

    <div class="mb-4 px-1">
        <p class="text-[0.7rem] font-semibold uppercase tracking-[0.22em] text-slate-500">Navigation</p>
    </div>
    <nav class="space-y-4">
        <?php foreach (nav_items() as $item): ?>
            <?php
                $requestUri = (string) ($_SERVER['REQUEST_URI'] ?? '/');
                $requestPath = current_path();
                $requestQuery = (string) parse_url($requestUri, PHP_URL_QUERY);
                parse_str($requestQuery, $queryParams);

                $childStates = [];
                foreach ($item['children'] as $index => $child) {
                    $childPath = (string) parse_url($child['path'], PHP_URL_PATH);
                    $childQuery = (string) parse_url($child['path'], PHP_URL_QUERY);
                    parse_str($childQuery, $childQueryParams);

                    $isChildRouteMatch = $requestPath === $childPath || str_starts_with($requestPath, rtrim($childPath, '/') . '/');
                    $isChildQueryMatch = true;
                    foreach ($childQueryParams as $key => $value) {
                        if (!array_key_exists($key, $queryParams) || (string) $queryParams[$key] !== (string) $value) {
                            $isChildQueryMatch = false;
                            break;
                        }
                    }

                    $childStates[$index] = $isChildRouteMatch && $isChildQueryMatch;
                }

                if ($item['path'] === '/') {
                    $isParentActive = $requestPath === '/';
                } else {
                    $isParentActive = $requestPath === $item['path'] || str_starts_with($requestPath, $item['path'] . '/');
                }

                if (in_array(true, $childStates, true)) {
                    $isParentActive = true;
                }
            ?>
            <div class="nav-group">
                <a href="<?= htmlspecialchars($item['path'], ENT_QUOTES) ?>"
                   class="nav-item <?= $isParentActive ? 'nav-item-active' : '' ?>">
                    <span class="flex h-8 w-8 items-center justify-center rounded-xl border border-white/8 bg-slate-950/70 text-base shadow-[inset_0_1px_0_rgba(255,255,255,0.03)]"><?= $item['icon'] ?></span>
                    <span class="flex-1"><?= htmlspecialchars($item['label'], ENT_QUOTES) ?></span>
                    <?php if ($item['children'] !== []): ?>
                        <span class="text-xs text-slate-500"><?= count($item['children']) ?></span>
                    <?php endif; ?>
                </a>
                <?php if ($item['children'] !== []): ?>
                    <div class="mt-2 space-y-1 border-t border-white/6 pt-2">
                        <?php foreach ($item['children'] as $index => $child): ?>
                            <?php $isChildActive = $childStates[$index] ?? false; ?>
                            <a href="<?= htmlspecialchars($child['path'], ENT_QUOTES) ?>"
                               class="subnav-item <?= $isChildActive ? 'subnav-item-active' : '' ?>">
                                <?= htmlspecialchars($child['label'], ENT_QUOTES) ?>
                            </a>
                        <?php endforeach; ?>
                    </div>
                <?php endif; ?>
            </div>
        <?php endforeach; ?>
    </nav>
</aside>
