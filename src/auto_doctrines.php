<?php
/**
 * Automatic doctrines and automatic buy-all — PHP side.
 *
 * This file hosts the public API for pages under `/public/doctrines/` and
 * `/public/buy-all/`, plus the thin DB layer that reads from the
 * `auto_doctrines`, `auto_doctrine_modules`, `auto_doctrine_fit_demand_1d`,
 * `auto_buyall_summary`, and `auto_buyall_items` tables.
 *
 * The data is produced by the Python jobs:
 *   - python/orchestrator/jobs/compute_auto_doctrines.py
 *   - python/orchestrator/jobs/compute_auto_buyall.py
 *
 * This file is required by src/functions.php on bootstrap so the symbols
 * are always available on both public pages and CLI scripts.
 */

declare(strict_types=1);

// ─── DB layer ──────────────────────────────────────────────────────────────

/** Fetch every auto-doctrine row joined with today's demand rollup. */
function db_auto_doctrine_fetch_all(bool $includeHidden = false): array
{
    $pdo = db();
    $where = $includeHidden ? '1 = 1' : 'ad.is_hidden = 0';
    $sql = "SELECT ad.id, ad.hull_type_id, ad.fingerprint_hash, ad.canonical_name,
                   ad.first_seen_at, ad.last_seen_at,
                   ad.loss_count_window, ad.loss_count_total,
                   ad.window_days, ad.min_losses_threshold,
                   ad.is_active, ad.is_hidden, ad.is_pinned,
                   ad.runway_days_override, ad.notes,
                   COALESCE(rit.type_name, CONCAT('Type #', ad.hull_type_id)) AS hull_name,
                   demand.loss_count         AS demand_loss_count,
                   demand.daily_loss_rate    AS demand_daily_loss_rate,
                   demand.target_fits        AS demand_target_fits,
                   demand.priority_score     AS demand_priority_score
              FROM auto_doctrines ad
         LEFT JOIN ref_item_types rit ON rit.type_id = ad.hull_type_id
         LEFT JOIN auto_doctrine_fit_demand_1d demand
                ON demand.doctrine_id = ad.id
               AND demand.bucket_start = (
                   SELECT MAX(bucket_start) FROM auto_doctrine_fit_demand_1d
                    WHERE doctrine_id = ad.id
               )
             WHERE {$where}
             ORDER BY ad.is_pinned DESC,
                      demand.priority_score DESC,
                      ad.loss_count_window DESC,
                      ad.id ASC";
    $stmt = $pdo->query($sql);
    return $stmt ? $stmt->fetchAll(PDO::FETCH_ASSOC) : [];
}

function db_auto_doctrine_fetch(int $id): ?array
{
    if ($id <= 0) {
        return null;
    }
    $pdo = db();
    $stmt = $pdo->prepare(
        "SELECT ad.*, COALESCE(rit.type_name, CONCAT('Type #', ad.hull_type_id)) AS hull_name
           FROM auto_doctrines ad
      LEFT JOIN ref_item_types rit ON rit.type_id = ad.hull_type_id
          WHERE ad.id = :id"
    );
    $stmt->execute([':id' => $id]);
    $row = $stmt->fetch(PDO::FETCH_ASSOC);
    return $row ?: null;
}

function db_auto_doctrine_modules(int $doctrineId): array
{
    if ($doctrineId <= 0) {
        return [];
    }
    $pdo = db();
    $stmt = $pdo->prepare(
        "SELECT adm.type_id, adm.flag_category, adm.quantity, adm.observation_frequency,
                COALESCE(rit.type_name, CONCAT('Type #', adm.type_id)) AS type_name,
                COALESCE(rit.volume, 0) AS unit_volume
           FROM auto_doctrine_modules adm
      LEFT JOIN ref_item_types rit ON rit.type_id = adm.type_id
          WHERE adm.doctrine_id = :id
          ORDER BY adm.flag_category ASC, adm.quantity DESC, adm.type_id ASC"
    );
    $stmt->execute([':id' => $doctrineId]);
    return $stmt->fetchAll(PDO::FETCH_ASSOC);
}

/**
 * Update a subset of editable flags (hidden/pinned/runway_days_override/notes).
 * Unknown keys are silently ignored — callers can pass a partial array.
 */
function db_auto_doctrine_update_flags(int $id, array $fields): bool
{
    if ($id <= 0 || $fields === []) {
        return false;
    }
    $allowed = [
        'is_hidden'            => PDO::PARAM_INT,
        'is_pinned'            => PDO::PARAM_INT,
        'runway_days_override' => PDO::PARAM_INT,
        'notes'                => PDO::PARAM_STR,
    ];
    $sets = [];
    $params = [':id' => $id];
    foreach ($fields as $key => $value) {
        if (!array_key_exists($key, $allowed)) {
            continue;
        }
        $sets[] = "{$key} = :{$key}";
        if ($key === 'runway_days_override' && ($value === null || $value === '')) {
            $params[":{$key}"] = null;
        } elseif ($key === 'notes') {
            $params[":{$key}"] = $value === null ? null : substr((string) $value, 0, 500);
        } else {
            $params[":{$key}"] = (int) $value;
        }
    }
    if ($sets === []) {
        return false;
    }
    $sql = 'UPDATE auto_doctrines SET ' . implode(', ', $sets) . ', updated_at = CURRENT_TIMESTAMP WHERE id = :id';
    $pdo = db();
    $stmt = $pdo->prepare($sql);
    foreach ($params as $key => $value) {
        if ($value === null) {
            $stmt->bindValue($key, null, PDO::PARAM_NULL);
        } elseif ($key === ':id' || isset($allowed[ltrim($key, ':')]) && $allowed[ltrim($key, ':')] === PDO::PARAM_INT) {
            $stmt->bindValue($key, (int) $value, PDO::PARAM_INT);
        } else {
            $stmt->bindValue($key, (string) $value, PDO::PARAM_STR);
        }
    }
    return $stmt->execute();
}

function db_auto_buyall_latest_summary(): ?array
{
    $pdo = db();
    $stmt = $pdo->query(
        "SELECT id, computed_at, doctrine_count, total_items, total_isk, total_volume,
                hub_snapshot_at, alliance_snapshot_at, payload_json
           FROM auto_buyall_summary
          ORDER BY computed_at DESC
          LIMIT 1"
    );
    $row = $stmt ? $stmt->fetch(PDO::FETCH_ASSOC) : null;
    return $row ?: null;
}

function db_auto_buyall_items(int $summaryId): array
{
    if ($summaryId <= 0) {
        return [];
    }
    $pdo = db();
    $stmt = $pdo->prepare(
        "SELECT summary_id, type_id, type_name,
                demand_qty, alliance_stock_qty, buy_qty,
                hub_best_sell, alliance_best_sell, unit_cost, unit_volume,
                line_cost, line_volume,
                contributing_doctrine_ids, contributing_fit_count
           FROM auto_buyall_items
          WHERE summary_id = :id
          ORDER BY line_cost DESC, type_id ASC"
    );
    $stmt->execute([':id' => $summaryId]);
    return $stmt->fetchAll(PDO::FETCH_ASSOC);
}

function db_auto_doctrine_fit_demand_latest(): array
{
    $pdo = db();
    $stmt = $pdo->query(
        "SELECT d.*
           FROM auto_doctrine_fit_demand_1d d
           JOIN (
               SELECT doctrine_id, MAX(bucket_start) AS latest_bucket
                 FROM auto_doctrine_fit_demand_1d
                GROUP BY doctrine_id
           ) latest
             ON latest.doctrine_id = d.doctrine_id
            AND latest.latest_bucket = d.bucket_start"
    );
    return $stmt ? $stmt->fetchAll(PDO::FETCH_ASSOC) : [];
}

// ─── Settings helpers ──────────────────────────────────────────────────────

function auto_doctrine_settings(): array
{
    static $cache = null;
    if ($cache !== null) {
        return $cache;
    }
    $pdo = db();
    $stmt = $pdo->query(
        "SELECT setting_key, setting_value FROM settings
          WHERE setting_key IN (
              'auto_doctrines.window_days',
              'auto_doctrines.min_losses_threshold',
              'auto_doctrines.default_runway_days',
              'auto_doctrines.jaccard_threshold'
          )"
    );
    $rows = $stmt ? $stmt->fetchAll(PDO::FETCH_ASSOC) : [];
    $byKey = [];
    foreach ($rows as $row) {
        $byKey[(string) $row['setting_key']] = (string) $row['setting_value'];
    }
    return $cache = [
        'window_days'         => (int) ($byKey['auto_doctrines.window_days'] ?? 30),
        'min_losses_threshold' => (int) ($byKey['auto_doctrines.min_losses_threshold'] ?? 5),
        'default_runway_days' => (int) ($byKey['auto_doctrines.default_runway_days'] ?? 14),
        'jaccard_threshold'   => (float) ($byKey['auto_doctrines.jaccard_threshold'] ?? 0.80),
    ];
}

// ─── Public API consumed by pages & dashboard ──────────────────────────────

/**
 * Return a denormalized list of auto-doctrines ready for the index page.
 *
 * Each row carries the hull metadata, window stats, and the latest
 * demand rollup. Module lists are NOT included here — call
 * `auto_doctrine_detail($id)` for that.
 */
function auto_doctrine_list(array $opts = []): array
{
    $includeHidden = (bool) ($opts['include_hidden'] ?? false);
    $rows = db_auto_doctrine_fetch_all($includeHidden);
    $settings = auto_doctrine_settings();
    $defaultRunway = (int) $settings['default_runway_days'];

    $out = [];
    foreach ($rows as $row) {
        $doctrineId = (int) $row['id'];
        $runwayDays = (int) ($row['runway_days_override'] ?? 0);
        if ($runwayDays <= 0) {
            $runwayDays = $defaultRunway;
        }
        $out[] = [
            'id'                   => $doctrineId,
            'hull_type_id'         => (int) $row['hull_type_id'],
            'hull_name'            => (string) ($row['hull_name'] ?? ''),
            'canonical_name'       => (string) ($row['canonical_name'] ?? ''),
            'first_seen_at'        => $row['first_seen_at'] ?? null,
            'last_seen_at'         => $row['last_seen_at'] ?? null,
            'loss_count_window'    => (int) ($row['loss_count_window'] ?? 0),
            'loss_count_total'     => (int) ($row['loss_count_total'] ?? 0),
            'window_days'          => (int) ($row['window_days'] ?? $settings['window_days']),
            'min_losses_threshold' => (int) ($row['min_losses_threshold'] ?? $settings['min_losses_threshold']),
            'is_active'            => (int) ($row['is_active'] ?? 0) === 1,
            'is_hidden'            => (int) ($row['is_hidden'] ?? 0) === 1,
            'is_pinned'            => (int) ($row['is_pinned'] ?? 0) === 1,
            'runway_days_override' => isset($row['runway_days_override']) && $row['runway_days_override'] !== null
                ? (int) $row['runway_days_override']
                : null,
            'runway_days_effective' => $runwayDays,
            'notes'                => $row['notes'] ?? null,
            'daily_loss_rate'      => (float) ($row['demand_daily_loss_rate'] ?? 0),
            'target_fits'          => (int) ($row['demand_target_fits'] ?? 0),
            'priority_score'       => (float) ($row['demand_priority_score'] ?? 0),
            'demand_loss_count'    => (int) ($row['demand_loss_count'] ?? 0),
        ];
    }
    return $out;
}

function auto_doctrine_detail(int $id): ?array
{
    $row = db_auto_doctrine_fetch($id);
    if ($row === null) {
        return null;
    }
    $modules = db_auto_doctrine_modules($id);
    $row['modules'] = $modules;
    return $row;
}

function auto_doctrine_set_hidden(int $id, bool $hidden): bool
{
    return db_auto_doctrine_update_flags($id, ['is_hidden' => $hidden ? 1 : 0]);
}

function auto_doctrine_set_pinned(int $id, bool $pinned): bool
{
    return db_auto_doctrine_update_flags($id, ['is_pinned' => $pinned ? 1 : 0]);
}

function auto_doctrine_set_runway(int $id, ?int $days): bool
{
    if ($days !== null && $days <= 0) {
        $days = null;
    }
    return db_auto_doctrine_update_flags($id, ['runway_days_override' => $days]);
}

/**
 * Return the latest precomputed buy-all (summary + items) with a 30s
 * request-level cache. Callers never touch the DB tables directly.
 */
function auto_buyall_latest(): array
{
    static $cache = null;
    static $cachedAt = 0.0;
    $now = microtime(true);
    if ($cache !== null && ($now - $cachedAt) < 30.0) {
        return $cache;
    }
    $cache = auto_buyall_latest_uncached();
    $cachedAt = $now;
    return $cache;
}

function auto_buyall_latest_uncached(): array
{
    $summary = db_auto_buyall_latest_summary();
    if ($summary === null) {
        return [
            'summary' => null,
            'items'   => [],
            'empty'   => true,
        ];
    }
    $items = db_auto_buyall_items((int) $summary['id']);
    $payload = null;
    $rawPayload = $summary['payload_json'] ?? null;
    if (is_string($rawPayload) && $rawPayload !== '') {
        $decoded = json_decode($rawPayload, true);
        if (is_array($decoded)) {
            $payload = $decoded;
        }
    }
    return [
        'summary' => [
            'id'                   => (int) $summary['id'],
            'computed_at'          => $summary['computed_at'] ?? null,
            'doctrine_count'       => (int) ($summary['doctrine_count'] ?? 0),
            'total_items'          => (int) ($summary['total_items'] ?? 0),
            'total_isk'            => (float) ($summary['total_isk'] ?? 0),
            'total_volume'         => (float) ($summary['total_volume'] ?? 0),
            'hub_snapshot_at'      => $summary['hub_snapshot_at'] ?? null,
            'alliance_snapshot_at' => $summary['alliance_snapshot_at'] ?? null,
            'payload'              => $payload,
        ],
        'items' => array_map(static function (array $row): array {
            return [
                'type_id'                   => (int) $row['type_id'],
                'type_name'                 => (string) ($row['type_name'] ?? ''),
                'demand_qty'                => (int) ($row['demand_qty'] ?? 0),
                'alliance_stock_qty'        => (int) ($row['alliance_stock_qty'] ?? 0),
                'buy_qty'                   => (int) ($row['buy_qty'] ?? 0),
                'hub_best_sell'             => isset($row['hub_best_sell']) ? (float) $row['hub_best_sell'] : null,
                'alliance_best_sell'        => isset($row['alliance_best_sell']) ? (float) $row['alliance_best_sell'] : null,
                'unit_cost'                 => isset($row['unit_cost']) ? (float) $row['unit_cost'] : 0.0,
                'unit_volume'               => isset($row['unit_volume']) ? (float) $row['unit_volume'] : 0.0,
                'line_cost'                 => (float) ($row['line_cost'] ?? 0),
                'line_volume'               => (float) ($row['line_volume'] ?? 0),
                'contributing_doctrine_ids' => json_decode((string) ($row['contributing_doctrine_ids'] ?? '[]'), true) ?: [],
                'contributing_fit_count'    => (int) ($row['contributing_fit_count'] ?? 0),
            ];
        }, $items),
        'empty' => false,
    ];
}

/**
 * Dashboard summary — replaces the legacy `buy_all_dashboard_summary()`.
 * Returns a compact KPI-friendly shape so partials can drop it in without
 * knowing about the full buy-all payload.
 */
function auto_buyall_dashboard_summary(): array
{
    $latest = auto_buyall_latest();
    $summary = $latest['summary'] ?? null;
    $items = $latest['items'] ?? [];

    if ($summary === null) {
        return [
            'available'            => false,
            'computed_at'          => null,
            'doctrine_count'       => 0,
            'total_items'          => 0,
            'total_isk'            => 0.0,
            'total_volume'         => 0.0,
            'hub_snapshot_at'      => null,
            'alliance_snapshot_at' => null,
            'top_items'            => [],
        ];
    }

    $top = [];
    foreach ($items as $item) {
        if (count($top) >= 5) {
            break;
        }
        $top[] = [
            'type_id'   => (int) $item['type_id'],
            'type_name' => (string) $item['type_name'],
            'buy_qty'   => (int) $item['buy_qty'],
            'line_cost' => (float) $item['line_cost'],
        ];
    }

    return [
        'available'            => true,
        'computed_at'          => $summary['computed_at'] ?? null,
        'doctrine_count'       => (int) ($summary['doctrine_count'] ?? 0),
        'total_items'          => (int) ($summary['total_items'] ?? 0),
        'total_isk'            => (float) ($summary['total_isk'] ?? 0),
        'total_volume'         => (float) ($summary['total_volume'] ?? 0),
        'hub_snapshot_at'      => $summary['hub_snapshot_at'] ?? null,
        'alliance_snapshot_at' => $summary['alliance_snapshot_at'] ?? null,
        'top_items'            => $top,
        // ── Legacy keys the dashboard view still reads. Kept here so the
        // dashboard template doesn't have to change. These stand in for
        // the retired multi-mode planner concepts.
        'planner_href'            => '/buy-all/',
        'blended_href'            => '/buy-all/',
        'recommended_mode_label'  => 'Automatic',
        'pages'                   => 1,
        'total_planned_volume'    => (float) ($summary['total_volume'] ?? 0),
        'expected_net_profit'     => 0.0,
        'doctrine_critical_count' => (int) ($summary['doctrine_count'] ?? 0),
        'top_reason_theme'        => 'Loss-rate × runway',
    ];
}

/**
 * Lightweight doctrine overview for the dashboard partial. The legacy
 * shape had ``not_ready_fits``, ``groups``, ``top_bottlenecks``,
 * ``highest_priority_restock_items``, ``top_missing_items`` — those
 * concepts are retired, so the new shape returns empty arrays for them
 * while exposing the handful of keys the new dashboard panel will want
 * going forward (doctrine list + pinned count).
 */
function auto_doctrine_dashboard_overview(): array
{
    $list = auto_doctrine_list(['include_hidden' => false]);
    $active = array_values(array_filter($list, static fn (array $d): bool => (bool) $d['is_active']));
    $pinned = array_values(array_filter($list, static fn (array $d): bool => (bool) $d['is_pinned']));

    $topPriority = $active;
    usort($topPriority, static fn (array $a, array $b): int => (int) round(((float) $b['priority_score']) * 100) - (int) round(((float) $a['priority_score']) * 100));

    return [
        'doctrines'       => $list,
        'active_count'    => count($active),
        'pinned_count'    => count($pinned),
        'top_priority'    => array_slice($topPriority, 0, 6),
        // Legacy compat — view reads these keys directly.
        'not_ready_fits'                  => [],
        'groups'                          => [],
        'top_bottlenecks'                 => [],
        'highest_priority_restock_items'  => [],
        'top_missing_items'               => [],
    ];
}
