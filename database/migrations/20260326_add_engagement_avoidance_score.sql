-- Add engagement_avoidance_score to character_graph_intelligence.
-- This column stores a Neo4j-derived metric that detects characters who
-- repeatedly encounter a specific alliance on the opposing side but
-- disproportionately appear in high-sustain (non-engagement) battles
-- against that alliance compared to others.

ALTER TABLE character_graph_intelligence
    ADD COLUMN engagement_avoidance_score DECIMAL(14,6) NOT NULL DEFAULT 0.000000
    AFTER bridge_between_clusters_score;

CREATE INDEX idx_character_graph_intelligence_avoidance
    ON character_graph_intelligence (engagement_avoidance_score, computed_at);
