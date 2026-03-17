<aside class="hidden w-80 shrink-0 border-r border-border bg-black/30 p-6 lg:block">
    <a href="/" class="mb-8 block rounded-xl border border-border bg-card px-4 py-3">
        <p class="text-xs uppercase tracking-[0.2em] text-muted">Project</p>
        <p class="text-lg font-semibold"><?= htmlspecialchars(app_name(), ENT_QUOTES) ?></p>
    </a>

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
            <div class="rounded-xl border border-border bg-card p-2">
                <a href="<?= htmlspecialchars($item['path'], ENT_QUOTES) ?>"
                   class="flex items-center gap-2 rounded-lg px-3 py-2 text-sm transition <?= $isParentActive ? 'bg-accent/20 text-white' : 'text-slate-200 hover:bg-white/5' ?>">
                    <span><?= $item['icon'] ?></span>
                    <span><?= htmlspecialchars($item['label'], ENT_QUOTES) ?></span>
                </a>
                <?php if ($item['children'] !== []): ?>
                    <div class="mt-1 space-y-1 border-t border-border pt-2">
                        <?php foreach ($item['children'] as $index => $child): ?>
                            <?php $isChildActive = $childStates[$index] ?? false; ?>
                            <a href="<?= htmlspecialchars($child['path'], ENT_QUOTES) ?>"
                               class="block rounded-lg px-3 py-2 text-sm <?= $isChildActive ? 'bg-accent/20 text-white' : 'text-muted hover:bg-white/5 hover:text-slate-200' ?>">
                                <?= htmlspecialchars($child['label'], ENT_QUOTES) ?>
                            </a>
                        <?php endforeach; ?>
                    </div>
                <?php endif; ?>
            </div>
        <?php endforeach; ?>
    </nav>
</aside>
