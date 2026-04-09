<?php

declare(strict_types=1);

/**
 * Shared helper functions for character intelligence display.
 * Used by both character.php and pilot-lookup.php.
 */

/** Check whether a name is an unresolved placeholder (e.g. "Character #12345"). */
function ci_is_placeholder_name(string $name): bool
{
    return str_starts_with($name, 'Character #') || str_starts_with($name, 'Alliance #') || str_starts_with($name, 'Corp #');
}

/** Return a risk-level label and Tailwind color classes based on review priority score. */
function ci_risk_level(float $score): array
{
    if ($score >= 0.75) {
        return ['label' => 'High', 'bg' => 'bg-red-900/60', 'text' => 'text-red-300', 'bar' => 'bg-red-500'];
    }
    if ($score >= 0.45) {
        return ['label' => 'Medium', 'bg' => 'bg-yellow-900/60', 'text' => 'text-yellow-300', 'bar' => 'bg-yellow-500'];
    }
    if ($score >= 0.20) {
        return ['label' => 'Low', 'bg' => 'bg-blue-900/60', 'text' => 'text-blue-300', 'bar' => 'bg-blue-500'];
    }
    return ['label' => 'Minimal', 'bg' => 'bg-green-900/60', 'text' => 'text-green-300', 'bar' => 'bg-green-500'];
}

/** Return color classes for a 0-1 metric (higher = more suspicious). */
function ci_metric_color(float $value, float $cautionAt = 0.4, float $dangerAt = 0.7): string
{
    if ($value >= $dangerAt) {
        return 'text-red-400';
    }
    if ($value >= $cautionAt) {
        return 'text-yellow-400';
    }
    return 'text-green-400';
}

/** Return color for lift values (>1 means enemy survives more than expected). */
function ci_lift_color(float $lift): string
{
    if ($lift >= 1.5) {
        return 'text-red-400';
    }
    if ($lift >= 1.15) {
        return 'text-yellow-400';
    }
    return 'text-green-400';
}

/** Color for overperformance scores. */
function ci_overperf_color(float $score): string
{
    if ($score >= 1.3) {
        return 'text-red-400';
    }
    if ($score >= 1.1) {
        return 'text-yellow-400';
    }
    if ($score >= 0.9) {
        return 'text-slate-100';
    }
    return 'text-blue-400';
}

/** Return a human-readable label for an anomaly class. */
function ci_anomaly_label(string $class): array
{
    return match ($class) {
        'high_enemy_overperformance' => ['label' => 'Suspicious', 'bg' => 'bg-red-900/60', 'text' => 'text-red-300'],
        'underperforming' => ['label' => 'Under-performing', 'bg' => 'bg-blue-900/60', 'text' => 'text-blue-300'],
        default => ['label' => 'Normal', 'bg' => 'bg-slate-700', 'text' => 'text-slate-300'],
    };
}

/** Convert snake_case evidence keys to human-readable labels. */
function ci_evidence_label(string $key): string
{
    $map = [
        'anomalous_battle_presence_count' => 'Anomalous battle appearances',
        'enemy_same_hull_survival_lift_detail' => 'Enemy hull survival lift',
        'enemy_sustain_lift' => 'Enemy sustain lift',
        'graph_copresence_cluster_proximity' => 'Network clustering',
        'repeatability_across_battles_windows' => 'Pattern repeatability',
        'anomalous_presence_rate' => 'Anomalous presence rate',
        'presence_rate_delta' => 'Presence rate delta',
        'org_history_movement_180d' => 'Corp movement (180 days)',
        'neo4j_cross_side_overlap' => 'Cross-side corp overlap',
        'neo4j_recent_defector' => 'Recent defector',
        'neo4j_hostile_adjacency' => 'Hostile corp adjacency',
        'footprint_expansion' => 'Footprint expansion',
        'footprint_contraction' => 'Footprint contraction',
        'new_area_entry' => 'New area entry',
        'hostile_overlap_change' => 'Hostile overlap change',
        'active_hour_shift' => 'Active hour shift',
        'weekday_profile_shift' => 'Weekday profile shift',
        'cadence_burstiness' => 'Cadence burstiness',
        'reactivation_after_dormancy' => 'Reactivation after dormancy',
    ];
    return $map[$key] ?? ucwords(str_replace('_', ' ', $key));
}

/** Render a slim progress bar (0-1 range). */
function ci_progress_bar(float $value, string $barColor = 'bg-cyan-500', float $max = 1.0): string
{
    $pct = min(100, max(0, ($value / $max) * 100));
    return '<div class="mt-1 h-1.5 w-full rounded-full bg-slate-700/60 overflow-hidden">'
         . '<div class="h-full rounded-full ' . $barColor . '" style="width:' . number_format($pct, 1) . '%"></div>'
         . '</div>';
}

/** Render an evidence path from structured node data, using resolved names. */
function ci_render_evidence_path(string $nodesJson, string $edgesJson, array $nameMap): string
{
    $nodes = json_decode($nodesJson, true);
    if (!is_array($nodes) || $nodes === []) {
        return '<span class="text-muted">No path data</span>';
    }

    $edges = json_decode($edgesJson, true);
    if (!is_array($edges)) {
        $edges = [];
    }

    $html = '';
    foreach ($nodes as $i => $node) {
        $type = (string) ($node['type'] ?? 'Unknown');
        $id = $node['id'] ?? null;
        $rawName = (string) ($node['name'] ?? '');
        $flagged = !empty($node['flagged']);

        // Try resolved name first, fall back to node name, then short ID
        if ($type === 'Character' && is_numeric($id) && isset($nameMap[(int) $id])) {
            $displayName = $nameMap[(int) $id];
        } elseif ($rawName !== '' && !is_numeric($rawName)) {
            $displayName = $rawName;
        } elseif (is_numeric($id)) {
            $displayName = $type . ' #' . $id;
        } else {
            $displayName = '?';
        }

        // Edge label between nodes
        if ($i > 0 && isset($edges[$i - 1])) {
            $edgeType = (string) ($edges[$i - 1]['type'] ?? '?');
            $readableEdge = strtolower(str_replace('_', ' ', $edgeType));
            $html .= ' <span class="text-muted">&rarr;</span> <span class="text-[11px] text-slate-500 italic">' . htmlspecialchars($readableEdge, ENT_QUOTES) . '</span> <span class="text-muted">&rarr;</span> ';
        }

        // Node display
        $nameClass = $flagged ? 'text-red-400 font-semibold' : ($i === 0 ? 'text-cyan-400' : 'text-slate-100');
        if ($type === 'Character' && is_numeric($id)) {
            $html .= '<a href="/battle-intelligence/character.php?character_id=' . (int) $id . '" class="' . $nameClass . ' hover:underline">' . htmlspecialchars($displayName, ENT_QUOTES) . '</a>';
        } else {
            $html .= '<span class="' . $nameClass . '">' . htmlspecialchars($displayName, ENT_QUOTES) . '</span>';
        }

        if ($flagged) {
            $html .= ' <span class="text-[10px] rounded-full bg-red-900/60 text-red-300 px-1.5 py-0.5">flagged</span>';
        }
    }

    return $html;
}

/** Format a date string like "2022/01/05 04:25" into a human-friendly relative duration. */
function ci_corp_duration(string $startDate, ?string $endDate): string
{
    $start = strtotime(str_replace('/', '-', $startDate));
    $end = $endDate !== null ? strtotime(str_replace('/', '-', $endDate)) : time();
    if ($start === false || $end === false || $end <= $start) {
        return '';
    }
    $days = (int) round(($end - $start) / 86400);
    if ($days < 1) {
        return '< 1 day';
    }
    if ($days < 30) {
        return $days . 'd';
    }
    $months = (int) round($days / 30.44);
    if ($months < 12) {
        return $months . 'mo';
    }
    $years = round($days / 365.25, 1);
    return rtrim(rtrim(number_format($years, 1), '0'), '.') . 'y';
}

/** Render a plain-English one-line verdict based on key metrics. */
function ci_verdict_summary(array $char): string
{
    $score = (float) ($char['review_priority_score'] ?? 0);
    $pctile = (float) ($char['percentile_rank'] ?? 0) * 100;
    $lift = (float) ($char['enemy_sustain_lift'] ?? 0);
    $repeat = (float) ($char['repeatability_score'] ?? 0);
    $hops = (float) ($char['corp_hop_frequency_180d'] ?? 0);

    $parts = [];

    if ($score >= 0.75) {
        $parts[] = 'This character has a <strong class="text-red-400">high</strong> review priority and warrants close investigation.';
    } elseif ($score >= 0.45) {
        $parts[] = 'This character shows <strong class="text-yellow-400">moderate</strong> suspicious signals.';
    } elseif ($score >= 0.20) {
        $parts[] = 'This character has <strong class="text-blue-400">low-level</strong> flags &mdash; likely not a priority.';
    } else {
        $parts[] = 'This character shows <strong class="text-green-400">minimal</strong> suspicious activity.';
    }

    if ($lift >= 1.15) {
        $parts[] = 'Enemies survive <strong class="text-yellow-400">' . number_format(($lift - 1) * 100, 0) . '% more</strong> than expected when this pilot is on the field.';
    }

    if ($repeat >= 0.5) {
        $parts[] = 'Suspicious patterns repeat across multiple time windows.';
    }

    if ($hops >= 3) {
        $parts[] = 'Frequent corp-hopping detected (' . number_format($hops, 0) . ' changes in 180 days).';
    }

    if ($pctile >= 95) {
        $parts[] = 'Ranks in the <strong>top ' . number_format(100 - $pctile, 1) . '%</strong> of all tracked characters.';
    }

    return implode(' ', $parts);
}

/**
 * Generate a complete pilot assessment for the executive summary.
 * Returns: ['summary' => string, 'action' => string, 'concerns' => string[], 'mitigating' => string[],
 *           'activity_level' => string, 'fleet_pattern' => string, 'risk_label' => string]
 */
function ci_pilot_assessment(
    array $profile,
    ?array $blended,
    ?array $behavStats,
    array $temporalMetrics,
    ?array $ciCharacter
): array {
    $stats = $profile['stats'] ?? [];
    $theaterBattles = (int) ($stats['total_battles'] ?? 0);
    $totalTheaters  = (int) ($stats['theater_count'] ?? 0);

    $bldScore = $blended !== null ? (float) ($blended['blended_score'] ?? 0) : null;
    $bldConf  = $blended !== null ? (float) ($blended['blended_confidence'] ?? 0) : null;

    $bsRisk = (float) ($behavStats['behavioral_risk_score'] ?? 0);
    $bsFleetAbsence = (float) ($behavStats['fleet_absence_ratio'] ?? 0);
    $bsCompanion = (float) ($behavStats['companion_consistency_score'] ?? 0);
    $bsCrossSide = (float) ($behavStats['cross_side_small_rate'] ?? 0);
    $bsContinuation = (float) ($behavStats['post_engagement_continuation_rate'] ?? 0);
    $bsAsymmetry = (float) ($behavStats['asymmetry_preference'] ?? 0);
    $bsGeoConc = (float) ($behavStats['geographic_concentration_score'] ?? 0);
    $bsTempReg = (float) ($behavStats['temporal_regularity_score'] ?? 0);
    $kmTotal = (int) ($behavStats['total_kill_count'] ?? 0);
    $kmSolo  = (int) ($behavStats['solo_kill_count'] ?? 0);
    $kmGang  = (int) ($behavStats['gang_kill_count'] ?? 0);
    $kmLarge = (int) ($behavStats['large_battle_count'] ?? 0);

    $ciLift    = (float) ($ciCharacter['enemy_sustain_lift'] ?? 0);
    $ciHops    = (float) ($ciCharacter['corp_hop_frequency_180d'] ?? 0);
    $ciBridge  = (float) ($ciCharacter['graph_bridge_score'] ?? 0);
    $ciRepeat  = (float) ($ciCharacter['repeatability_score'] ?? 0);

    // Determine the effective risk score
    $riskScore = $bldScore ?? $bsRisk;

    // Temporal analysis
    $recent7d = null;
    $recent90d = null;
    foreach ($temporalMetrics as $tm) {
        if (($tm['window_label'] ?? '') === '7d') $recent7d = $tm;
        if (($tm['window_label'] ?? '') === '90d') $recent90d = $tm;
    }
    $battles7d  = (int) ($recent7d['battles_present'] ?? 0);
    $battles90d = (int) ($recent90d['battles_present'] ?? 0);

    // ── Activity level ──────────────────────────────────
    $activityLevel = 'Unknown';
    if ($battles7d >= 10) $activityLevel = 'Very active';
    elseif ($battles7d >= 3) $activityLevel = 'Active';
    elseif ($battles7d >= 1) $activityLevel = 'Low activity';
    elseif ($battles90d >= 1) $activityLevel = 'Recently active';
    elseif ($theaterBattles > 0 || $kmTotal > 0) $activityLevel = 'Dormant';
    else $activityLevel = 'No tracked activity';

    // ── Fleet pattern ───────────────────────────────────
    $fleetPattern = 'Unknown';
    if ($kmTotal === 0 && $theaterBattles === 0) {
        $fleetPattern = 'No combat data';
    } elseif ($bsFleetAbsence >= 0.9 && $kmTotal > 0) {
        $fleetPattern = 'Small-gang only';
    } elseif ($bsFleetAbsence >= 0.6) {
        $fleetPattern = 'Mostly small-gang';
    } elseif ($kmLarge >= 10) {
        $fleetPattern = 'Heavy fleet participant';
    } elseif ($kmLarge >= 3) {
        $fleetPattern = 'Mixed fleet / small-gang';
    } elseif ($totalTheaters > 0) {
        $fleetPattern = 'Regular fleet participant';
    } else {
        $fleetPattern = 'Light engagement';
    }

    // ── Risk label ──────────────────────────────────────
    if ($riskScore >= 0.75) {
        $riskLabel = 'High concern';
    } elseif ($riskScore >= 0.45) {
        $riskLabel = 'Elevated';
    } elseif ($riskScore >= 0.20) {
        $riskLabel = 'Low concern';
    } else {
        $riskLabel = 'No concern';
    }

    // ── Concerns ────────────────────────────────────────
    $concerns = [];
    $mitigating = [];

    // Dormancy reactivation
    $suspicion = $profile['suspicion'] ?? null;
    if (is_array($ciCharacter) && $battles7d > 0 && $battles90d <= 2) {
        $concerns[] = 'Returned after a long inactive period';
    }

    // Cross-side activity
    if ($bsCrossSide >= 0.3) {
        $concerns[] = 'Significant cross-side kills in small engagements (' . number_format($bsCrossSide * 100, 0) . '%)';
    } elseif ($bsCrossSide >= 0.1) {
        $concerns[] = 'Some cross-side activity detected';
    }

    // Enemy uplift
    if ($ciLift >= 1.3) {
        $concerns[] = 'Enemies survive significantly more when present (+' . number_format(($ciLift - 1) * 100, 0) . '%)';
    } elseif ($ciLift >= 1.15) {
        $concerns[] = 'Mild enemy survival uplift detected';
    }

    // Corp hopping
    if ($ciHops >= 3) {
        $concerns[] = 'Frequent corp changes (' . number_format($ciHops, 0) . ' in 180 days)';
    }

    // High burstiness
    if ($bsTempReg >= 0.8) {
        $concerns[] = 'Very bursty activity pattern';
    }

    // Low continuation
    if ($bsContinuation > 0 && $bsContinuation <= 0.15) {
        $concerns[] = 'Appears briefly then disappears';
    }

    // High bridge score
    if ($ciBridge >= 20) {
        $concerns[] = 'Acts as a connector between separate groups';
    }

    // Pattern repeatability
    if ($ciRepeat >= 0.5) {
        $concerns[] = 'Suspicious patterns repeat across time windows';
    }

    // ── Mitigating ──────────────────────────────────────
    if ($bsCrossSide < 0.05) {
        $mitigating[] = 'No meaningful cross-side activity';
    }
    if ($ciLift > 0 && $ciLift < 1.1) {
        $mitigating[] = 'No enemy over-performance pattern';
    }
    if ($ciHops < 1) {
        $mitigating[] = 'Stable corp history';
    }
    if ($bsCompanion >= 0.5) {
        $mitigating[] = 'Strong same-group fleet alignment';
    }
    if ($kmLarge >= 5) {
        $mitigating[] = 'Regular large-fleet participation';
    }
    if ($bsFleetAbsence < 0.3 && $kmTotal > 0) {
        $mitigating[] = 'Healthy mix of fleet and small-gang activity';
    }

    // ── Summary sentence ────────────────────────────────
    $charName = htmlspecialchars((string) ($profile['character']['character_name'] ?? 'This pilot'), ENT_QUOTES);
    $fleetFunc = (string) ($profile['fleet_function'] ?? '');

    if ($riskScore >= 0.75) {
        $summary = 'High-priority review. Multiple hostile-behavior indicators detected that warrant manual investigation.';
    } elseif ($riskScore >= 0.45) {
        $summary = 'Elevated signals detected. Some behavioral indicators warrant a closer look, but no definitive hostile pattern confirmed.';
    } elseif ($riskScore >= 0.20) {
        $summary = 'Low current risk. ' . ($totalTheaters > 0 ? 'Regular fleet participation with ' : '') . ($bsCompanion >= 0.5 ? 'a stable same-group network. ' : 'moderate network consistency. ') . ($concerns !== [] ? ucfirst($concerns[0]) . '.' : 'No significant hostile-performance anomalies detected.');
    } else {
        $summary = 'No concern. ' . ($totalTheaters > 5 ? 'Highly regular fleet participation' : ($kmTotal > 0 ? 'Normal engagement patterns' : 'Limited tracked activity')) . ' with no hostile-performance anomalies detected.';
    }

    // ── Action ──────────────────────────────────────────
    if ($riskScore >= 0.75) {
        $action = 'Escalate for manual review. Cross-reference with operational intelligence.';
    } elseif ($riskScore >= 0.45) {
        $action = 'Monitor closely. Re-evaluate if cross-side activity or enemy-performance anomalies increase.';
    } elseif ($riskScore >= 0.20) {
        $action = 'Monitor only. No escalation needed unless cross-side activity or enemy-performance anomalies increase.';
    } else {
        $action = 'No action required. Routine monitoring.';
    }

    return [
        'summary' => $summary,
        'action' => $action,
        'concerns' => $concerns,
        'mitigating' => $mitigating,
        'activity_level' => $activityLevel,
        'fleet_pattern' => $fleetPattern,
        'risk_label' => $riskLabel,
    ];
}

/**
 * Human-friendly evidence label (operator language, not model language).
 * Falls back to the original ci_evidence_label() for unknown keys.
 */
function ci_evidence_label_human(string $key): string
{
    $map = [
        'anomalous_battle_presence_count' => 'Suspicious battle appearances',
        'enemy_same_hull_survival_lift_detail' => 'Enemies survive more (same hull)',
        'enemy_sustain_lift' => 'Enemies survive more when present',
        'graph_copresence_cluster_proximity' => 'Network connections',
        'repeatability_across_battles_windows' => 'Pattern consistency',
        'anomalous_presence_rate' => 'Presence in flagged battles',
        'presence_rate_delta' => 'Presence pattern change',
        'org_history_movement_180d' => 'Corp loyalty (180 days)',
        'neo4j_cross_side_overlap' => 'Shared corp history with enemies',
        'neo4j_recent_defector' => 'Recent defector',
        'neo4j_hostile_adjacency' => 'Hostile pilots in same corp',
        'footprint_expansion' => 'Operating in more areas',
        'footprint_contraction' => 'Operating in fewer areas',
        'new_area_entry' => 'Entered new regions',
        'hostile_overlap_change' => 'More overlap with hostile areas',
        'active_hour_shift' => 'Changed active hours',
        'weekday_profile_shift' => 'Changed active days',
        'cadence_burstiness' => 'Activity irregularity',
        'reactivation_after_dormancy' => 'Returned after long inactivity',
    ];
    // Handle "bv2_" prefixed keys
    if (str_starts_with($key, 'bv2_') || str_starts_with($key, 'Bv2 ')) {
        $clean = preg_replace('/^(bv2_|Bv2 )/i', '', $key);
        $clean = str_replace('_', ' ', $clean);
        return ucfirst(trim($clean));
    }
    return $map[$key] ?? ucwords(str_replace('_', ' ', $key));
}

/** Map a risk score to a human-readable risk band label. */
function ci_risk_band(float $score): string
{
    if ($score >= 0.75) return 'Critical';
    if ($score >= 0.45) return 'Watchlist';
    if ($score >= 0.20) return 'Normal range';
    return 'Clear';
}

/** Format a confidence value for display, handling the 0% edge case. */
function ci_confidence_display(float $confidence, int $evidenceCount = 0): string
{
    if ($confidence < 0.05 && $evidenceCount < 3) {
        return 'Insufficient data';
    }
    if ($confidence < 0.2) {
        return 'Low confidence';
    }
    if ($confidence < 0.5) {
        return 'Moderate confidence';
    }
    if ($confidence < 0.8) {
        return 'Good confidence';
    }
    return 'High confidence';
}

/** Format enemy boost for display, replacing confusing "-100%" with plain English. */
function ci_enemy_boost_display(float $overperf): string
{
    if ($overperf <= 0 || abs($overperf) < 0.01) {
        return 'None detected';
    }
    if ($overperf < 0.9) {
        return 'Enemies under-performed';
    }
    if ($overperf < 1.1) {
        return 'Normal range';
    }
    $pct = number_format(($overperf - 1) * 100, 0);
    return '+' . $pct . '% above expected';
}

/** Format engagement rate for display, replacing confusing >100% values. */
function ci_engagement_display(float $rate): string
{
    if ($rate < 0.01) return 'None';
    return number_format($rate, 1) . ' kills/battle';
}
