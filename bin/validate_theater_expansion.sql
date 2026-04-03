-- ============================================================================
-- Theater Engagement Expansion — Live Validation Queries
-- ============================================================================
-- Run these BEFORE and AFTER re-running theater_analysis to compare the
-- impact of the engagement expansion changes.
--
-- Usage:
--   1. Run these queries BEFORE deploying the new code (baseline).
--   2. Deploy the new code and re-run theater_clustering + theater_analysis.
--   3. Run these queries AGAIN (post-expansion).
--   4. Compare the two result sets.
-- ============================================================================

-- ─── 1. Per-theater killmail counts (before/after comparison) ───────────────
-- Compare with post-expansion to see how many killmails were added.
SELECT
    t.theater_id,
    t.primary_system_id,
    rs.system_name,
    t.start_time,
    t.end_time,
    t.total_kills,
    t.total_isk,
    t.participant_count,
    (SELECT COUNT(DISTINCT ke.killmail_id)
     FROM killmail_events ke
     WHERE ke.battle_id IN (SELECT tb.battle_id FROM theater_battles tb WHERE tb.theater_id = t.theater_id)
    ) AS killmails_via_battles
FROM theaters t
LEFT JOIN ref_systems rs ON rs.system_id = t.primary_system_id
ORDER BY t.start_time DESC
LIMIT 20;


-- ─── 2. Candidate expansion killmails for a specific theater ────────────────
-- Replace THEATER_ID_HERE with an actual theater_id.
-- Shows killmails in theater systems during the theater window that are NOT
-- already in the theater's battle set — these are what expansion would add.
SET @theater_id = 'THEATER_ID_HERE';
SET @margin_seconds = 300;

SELECT
    ke.killmail_id,
    ke.solar_system_id,
    ke.effective_killmail_at,
    ke.battle_id,
    ke.mail_type,
    ke.victim_alliance_id,
    ke.victim_corporation_id,
    COALESCE(ke.zkb_total_value, 0) AS total_value,
    CASE
        WHEN ke.battle_id IN (SELECT tb.battle_id FROM theater_battles tb WHERE tb.theater_id = @theater_id)
        THEN 'ALREADY_IN_THEATER'
        ELSE 'CANDIDATE_FOR_EXPANSION'
    END AS status
FROM killmail_events ke
WHERE ke.solar_system_id IN (
    SELECT ts.system_id FROM theater_systems ts WHERE ts.theater_id = @theater_id
)
AND ke.effective_killmail_at BETWEEN
    (SELECT DATE_SUB(t.start_time, INTERVAL @margin_seconds SECOND) FROM theaters t WHERE t.theater_id = @theater_id)
    AND
    (SELECT DATE_ADD(t.end_time, INTERVAL @margin_seconds SECOND) FROM theaters t WHERE t.theater_id = @theater_id)
ORDER BY ke.effective_killmail_at ASC;


-- ─── 3. Sub-threshold battles near theater systems ──────────────────────────
-- Shows battles with < 10 participants in the same systems as a theater
-- within its time window — these are candidates for absorption.
SET @theater_id = 'THEATER_ID_HERE';
SET @margin_seconds = 300;

SELECT
    br.battle_id,
    br.system_id,
    rs.system_name,
    br.started_at,
    br.ended_at,
    br.participant_count,
    br.battle_size_class,
    CASE
        WHEN br.battle_id IN (SELECT tb.battle_id FROM theater_battles tb WHERE tb.theater_id = @theater_id)
        THEN 'ALREADY_IN_THEATER'
        ELSE 'CANDIDATE_FOR_ABSORPTION'
    END AS status
FROM battle_rollups br
INNER JOIN ref_systems rs ON rs.system_id = br.system_id
WHERE br.system_id IN (
    SELECT ts.system_id FROM theater_systems ts WHERE ts.theater_id = @theater_id
)
AND br.participant_count > 0
AND br.participant_count < 10
AND br.started_at <= (SELECT DATE_ADD(t.end_time, INTERVAL @margin_seconds SECOND) FROM theaters t WHERE t.theater_id = @theater_id)
AND br.ended_at >= (SELECT DATE_SUB(t.start_time, INTERVAL @margin_seconds SECOND) FROM theaters t WHERE t.theater_id = @theater_id)
ORDER BY br.started_at ASC;


-- ─── 4. ISK reconciliation per theater ──────────────────────────────────────
-- Shows per-side ISK breakdown from the alliance summary.
-- After expansion, total_isk_lost across all sides should be >= prior value.
SELECT
    t.theater_id,
    rs.system_name,
    t.total_isk AS theater_total_isk,
    SUM(CASE WHEN tas.side = 'friendly' THEN tas.total_isk_lost ELSE 0 END) AS friendly_isk_lost,
    SUM(CASE WHEN tas.side = 'opponent' THEN tas.total_isk_lost ELSE 0 END) AS enemy_isk_lost,
    SUM(CASE WHEN tas.side = 'third_party' THEN tas.total_isk_lost ELSE 0 END) AS third_party_isk_lost,
    SUM(tas.total_isk_lost) AS sum_all_losses,
    t.total_isk - SUM(tas.total_isk_lost) AS isk_gap
FROM theaters t
LEFT JOIN theater_alliance_summary tas ON tas.theater_id = t.theater_id
LEFT JOIN ref_systems rs ON rs.system_id = t.primary_system_id
GROUP BY t.theater_id, rs.system_name, t.total_isk
HAVING isk_gap <> 0 OR third_party_isk_lost > 0
ORDER BY t.start_time DESC
LIMIT 20;


-- ─── 5. Expansion diagnostics from job logs ─────────────────────────────────
-- If theater_analysis logs to a JSON log file, grep for diagnostics:
-- grep "theater_analysis.killmails_loaded" /path/to/theater_analysis.log | tail -20
-- Look for:
--   expansion_passed_overlap > 0  → killmails were added
--   expansion_rejected_no_overlap > 0  → noise was filtered out
--   base_killmail_ids vs expanded_killmail_ids  → coverage ratio


-- ─── 6. Before/after comparison template ────────────────────────────────────
-- Save the output of query 1 and 4 before deployment.
-- Re-run after deployment and compare:
--   - Did total_kills increase? (sub-threshold battles absorbed)
--   - Did total_isk increase? (expanded killmails captured)
--   - Did third_party_isk_lost appear/increase?
--   - Did isk_gap shrink toward 0? (timeline total matches loss total)
--   - Did participant_count increase? (new entities captured)
