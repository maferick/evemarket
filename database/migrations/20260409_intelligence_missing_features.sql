-- Intelligence expansion: battle type classification, escalation detection,
-- shell corp indicators, staging system candidates, and pre-op join defector flags.

-- 1. Battle type classification (roam, defense, timer, camp, third_party, skirmish)
CREATE TABLE IF NOT EXISTS battle_type_classifications (
    battle_id           CHAR(64)        NOT NULL PRIMARY KEY,
    battle_type         VARCHAR(30)     NOT NULL DEFAULT 'skirmish',
    confidence          DECIMAL(5,4)    NOT NULL DEFAULT 0.0000,
    features_json       JSON            NOT NULL,
    computed_at         DATETIME        NOT NULL,
    created_at          TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP       DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_battle_type_class_type (battle_type, computed_at),
    KEY idx_battle_type_class_computed (computed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 2. Escalation sequences — groups of related battles showing increasing commitment.
CREATE TABLE IF NOT EXISTS escalation_sequences (
    sequence_id         CHAR(64)        NOT NULL,
    battle_id           CHAR(64)        NOT NULL,
    ordinal             INT UNSIGNED    NOT NULL DEFAULT 0,
    system_id           INT UNSIGNED    NOT NULL,
    participant_count   INT UNSIGNED    NOT NULL DEFAULT 0,
    capital_count       INT UNSIGNED    NOT NULL DEFAULT 0,
    isk_destroyed       DECIMAL(20,2)   NOT NULL DEFAULT 0.00,
    started_at          DATETIME        NOT NULL,
    computed_at         DATETIME        NOT NULL,
    PRIMARY KEY (sequence_id, battle_id),
    KEY idx_escalation_seq_battle (battle_id),
    KEY idx_escalation_seq_system (system_id),
    KEY idx_escalation_seq_computed (computed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS escalation_sequence_summary (
    sequence_id         CHAR(64)        NOT NULL PRIMARY KEY,
    region_id           INT UNSIGNED    DEFAULT NULL,
    constellation_id    INT UNSIGNED    DEFAULT NULL,
    battle_count        INT UNSIGNED    NOT NULL DEFAULT 0,
    peak_participants   INT UNSIGNED    NOT NULL DEFAULT 0,
    peak_capitals       INT UNSIGNED    NOT NULL DEFAULT 0,
    total_isk_destroyed DECIMAL(20,2)   NOT NULL DEFAULT 0.00,
    escalation_grade    VARCHAR(20)     NOT NULL DEFAULT 'minor',
    primary_aggressor_alliance_id BIGINT UNSIGNED DEFAULT NULL,
    primary_defender_alliance_id  BIGINT UNSIGNED DEFAULT NULL,
    first_battle_at     DATETIME        NOT NULL,
    last_battle_at      DATETIME        NOT NULL,
    computed_at         DATETIME        NOT NULL,
    created_at          TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP       DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_escalation_summary_grade (escalation_grade),
    KEY idx_escalation_summary_region (region_id),
    KEY idx_escalation_summary_computed (computed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 3. Shell corp indicators — corps with suspicious structural patterns.
CREATE TABLE IF NOT EXISTS shell_corp_indicators (
    corporation_id      BIGINT UNSIGNED NOT NULL PRIMARY KEY,
    alliance_id         BIGINT UNSIGNED DEFAULT NULL,
    member_count        INT UNSIGNED    NOT NULL DEFAULT 0,
    age_days            INT UNSIGNED    NOT NULL DEFAULT 0,
    turnover_ratio_90d  DECIMAL(8,4)   NOT NULL DEFAULT 0.0000,
    killmail_count_90d  INT UNSIGNED    NOT NULL DEFAULT 0,
    unique_members_90d  INT UNSIGNED    NOT NULL DEFAULT 0,
    avg_member_tenure_days INT UNSIGNED NOT NULL DEFAULT 0,
    shell_score         DECIMAL(5,4)    NOT NULL DEFAULT 0.0000,
    flags_json          JSON            NOT NULL,
    computed_at         DATETIME        NOT NULL,
    created_at          TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP       DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_shell_corp_alliance (alliance_id),
    KEY idx_shell_corp_score (shell_score DESC),
    KEY idx_shell_corp_computed (computed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 4. Staging system candidates — systems where forces concentrate before ops.
CREATE TABLE IF NOT EXISTS staging_system_candidates (
    system_id           INT UNSIGNED    NOT NULL,
    alliance_id         BIGINT UNSIGNED NOT NULL,
    staging_score       DECIMAL(8,4)    NOT NULL DEFAULT 0.0000,
    unique_pilots_7d    INT UNSIGNED    NOT NULL DEFAULT 0,
    battles_within_2j   INT UNSIGNED    NOT NULL DEFAULT 0,
    avg_jump_to_battle  DECIMAL(5,2)    NOT NULL DEFAULT 0.00,
    pre_battle_appearances INT UNSIGNED NOT NULL DEFAULT 0,
    features_json       JSON            NOT NULL,
    computed_at         DATETIME        NOT NULL,
    created_at          TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP       DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (system_id, alliance_id),
    KEY idx_staging_sys_alliance (alliance_id, staging_score DESC),
    KEY idx_staging_sys_score (staging_score DESC),
    KEY idx_staging_sys_computed (computed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 5. Pre-op join defector signal uses existing character_counterintel_evidence
--    with evidence_key = 'pre_op_join'. No schema changes needed — the evidence
--    table already supports arbitrary signal types via the (character_id,
--    evidence_key, window_label) PK.
