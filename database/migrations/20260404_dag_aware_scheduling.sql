-- Migration: DAG-aware job scheduling
-- Adds dependency tracking columns to sync_schedules and a scheduler_dag_log
-- table for auditing scheduling decisions.
--
-- The old lock_group column is preserved for backwards compatibility but the
-- new concurrency_group + depends_on_json columns are used by the DAG scheduler.

-- 1. Add dependency and concurrency metadata to sync_schedules
ALTER TABLE sync_schedules
    ADD COLUMN IF NOT EXISTS depends_on_json JSON DEFAULT NULL
        COMMENT 'JSON array of upstream job_keys that must complete before this job runs'
        AFTER locked_until,
    ADD COLUMN IF NOT EXISTS concurrency_group VARCHAR(80) DEFAULT ''
        COMMENT 'Jobs in the same concurrency group never overlap (resource-level exclusion)'
        AFTER depends_on_json,
    ADD COLUMN IF NOT EXISTS dag_tier TINYINT UNSIGNED DEFAULT NULL
        COMMENT 'Computed execution tier from topological sort (0 = root, higher = deeper)'
        AFTER concurrency_group;

-- 2. Index for concurrency group lookups during claim
ALTER TABLE sync_schedules
    ADD INDEX IF NOT EXISTS idx_concurrency_group (concurrency_group);

-- 3. Scheduler DAG decision log — records why each job was dispatched, blocked, or deferred
CREATE TABLE IF NOT EXISTS scheduler_dag_log (
    id              BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    cycle_ts        DATETIME        NOT NULL DEFAULT (UTC_TIMESTAMP())
        COMMENT 'Timestamp of the scheduling cycle',
    worker_id       VARCHAR(200)    NOT NULL DEFAULT '',
    job_key         VARCHAR(120)    NOT NULL,
    decision        ENUM('dispatched', 'blocked_dependency', 'blocked_concurrency', 'deferred', 'skipped') NOT NULL,
    tier            TINYINT UNSIGNED DEFAULT NULL,
    blocked_by_json JSON            DEFAULT NULL
        COMMENT 'JSON array of job_keys that blocked this job',
    reason_text     VARCHAR(500)    DEFAULT NULL,
    created_at      DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,

    INDEX idx_dag_log_job_key  (job_key, created_at),
    INDEX idx_dag_log_cycle    (cycle_ts),
    INDEX idx_dag_log_decision (decision, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='Audit log for DAG scheduler dispatch decisions';

-- 4. Backfill concurrency_group from existing lock_group values
UPDATE sync_schedules
   SET concurrency_group = COALESCE(
       (SELECT JSON_UNQUOTE(JSON_EXTRACT(
           (SELECT payload_json FROM worker_jobs wj WHERE wj.job_key = sync_schedules.job_key ORDER BY id DESC LIMIT 1),
           '$.lock_group'
       )), ''),
       ''
   )
 WHERE concurrency_group = '' OR concurrency_group IS NULL;
