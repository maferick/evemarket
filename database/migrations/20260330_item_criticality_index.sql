-- Item Criticality Index: graph-based scoring with trend, market stress, and priority signals

CREATE TABLE IF NOT EXISTS item_criticality_index (
    type_id             INT UNSIGNED    PRIMARY KEY,
    -- Graph structural signals
    doctrine_count      INT UNSIGNED    NOT NULL DEFAULT 0,
    fit_count           INT UNSIGNED    NOT NULL DEFAULT 0,
    dependency_score    DECIMAL(10,2)   NOT NULL DEFAULT 0,
    critical_edge_ratio DECIMAL(8,4)    NOT NULL DEFAULT 0,
    substitute_count    INT UNSIGNED    NOT NULL DEFAULT 0,
    substitutability    DECIMAL(8,4)    NOT NULL DEFAULT 1.0,
    dependency_depth    INT UNSIGNED    NOT NULL DEFAULT 0,
    spof_flag           TINYINT         NOT NULL DEFAULT 0,
    spof_impact_score   DECIMAL(10,2)   NOT NULL DEFAULT 0,
    criticality_score   DECIMAL(10,4)   NOT NULL DEFAULT 0,
    -- Trend signals (from InfluxDB time-series)
    price_avg_7d        DECIMAL(20,2)   DEFAULT NULL,
    price_avg_30d       DECIMAL(20,2)   DEFAULT NULL,
    volume_avg_7d       DECIMAL(20,2)   DEFAULT NULL,
    volume_avg_30d      DECIMAL(20,2)   DEFAULT NULL,
    price_velocity      DECIMAL(10,4)   DEFAULT NULL,
    volume_velocity     DECIMAL(10,4)   DEFAULT NULL,
    price_volatility    DECIMAL(10,4)   DEFAULT NULL,
    trend_regime        VARCHAR(32)     DEFAULT 'stable',
    trend_score         DECIMAL(10,4)   NOT NULL DEFAULT 0,
    -- Market stress signals
    market_spread_pct   DECIMAL(8,4)    DEFAULT NULL,
    liquidity_score     DECIMAL(8,4)    DEFAULT NULL,
    stock_days_remaining DECIMAL(8,2)   DEFAULT NULL,
    market_stress_score DECIMAL(10,4)   NOT NULL DEFAULT 0,
    -- Confidence
    data_freshness_hrs  DECIMAL(8,2)    DEFAULT NULL,
    coverage_ratio      DECIMAL(8,4)    DEFAULT NULL,
    confidence_score    DECIMAL(8,4)    NOT NULL DEFAULT 0.5,
    -- Unified priority index
    priority_index      DECIMAL(10,4)   NOT NULL DEFAULT 0,
    priority_rank       INT UNSIGNED    DEFAULT NULL,
    computed_at         DATETIME        NOT NULL,
    KEY idx_priority (priority_index DESC),
    KEY idx_criticality (criticality_score DESC),
    KEY idx_spof (spof_flag, spof_impact_score DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
