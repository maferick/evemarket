// Neo4j indexes for SupplyCore EveWho Intelligence Graph
// Run once on Neo4j startup: cat neo4j_indexes.cypher | cypher-shell -u neo4j -p <password>

CREATE INDEX char_id IF NOT EXISTS FOR (c:Character) ON (c.character_id);
CREATE INDEX corp_id IF NOT EXISTS FOR (c:Corporation) ON (c.corporation_id);
CREATE INDEX alliance_id IF NOT EXISTS FOR (a:Alliance) ON (a.alliance_id);
CREATE INDEX battle_id IF NOT EXISTS FOR (b:Battle) ON (b.battle_id);
CREATE INDEX member_from IF NOT EXISTS FOR ()-[r:MEMBER_OF]-() ON (r.from);
