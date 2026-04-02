// Neo4j constraints and indexes for SupplyCore Intelligence Graph
// Canonical schema — see docs/NEO4J_STANDARD.md for full specification.
// Run once on Neo4j startup: cat neo4j_indexes.cypher | cypher-shell -u neo4j -p <password>

// ── Unique constraints ──────────────────────────────────────────────────────
CREATE CONSTRAINT character_character_id IF NOT EXISTS FOR (n:Character) REQUIRE n.character_id IS UNIQUE;
CREATE CONSTRAINT battle_battle_id IF NOT EXISTS FOR (n:Battle) REQUIRE n.battle_id IS UNIQUE;
CREATE CONSTRAINT killmail_killmail_id IF NOT EXISTS FOR (n:Killmail) REQUIRE n.killmail_id IS UNIQUE;
CREATE CONSTRAINT alliance_alliance_id IF NOT EXISTS FOR (n:Alliance) REQUIRE n.alliance_id IS UNIQUE;
CREATE CONSTRAINT corp_corporation_id IF NOT EXISTS FOR (n:Corporation) REQUIRE n.corporation_id IS UNIQUE;
CREATE CONSTRAINT doctrine_doctrine_id IF NOT EXISTS FOR (n:Doctrine) REQUIRE n.doctrine_id IS UNIQUE;
CREATE CONSTRAINT fit_fit_id IF NOT EXISTS FOR (n:Fit) REQUIRE n.fit_id IS UNIQUE;
CREATE CONSTRAINT item_type_id IF NOT EXISTS FOR (n:Item) REQUIRE n.type_id IS UNIQUE;
CREATE CONSTRAINT side_side_uid IF NOT EXISTS FOR (n:BattleSide) REQUIRE n.side_uid IS UNIQUE;
CREATE CONSTRAINT ship_type_id IF NOT EXISTS FOR (n:ShipType) REQUIRE n.type_id IS UNIQUE;
CREATE CONSTRAINT system_system_id IF NOT EXISTS FOR (n:System) REQUIRE n.system_id IS UNIQUE;
CREATE CONSTRAINT constellation_constellation_id IF NOT EXISTS FOR (n:Constellation) REQUIRE n.constellation_id IS UNIQUE;
CREATE CONSTRAINT region_region_id IF NOT EXISTS FOR (n:Region) REQUIRE n.region_id IS UNIQUE;
CREATE CONSTRAINT shipclass_ship_class_id IF NOT EXISTS FOR (n:ShipClass) REQUIRE n.ship_class_id IS UNIQUE;
CREATE CONSTRAINT checkpoint_run_id IF NOT EXISTS FOR (n:ComputeCheckpoint) REQUIRE n.run_id IS UNIQUE;

// ── Node property indexes ───────────────────────────────────────────────────
CREATE INDEX character_lookup IF NOT EXISTS FOR (n:Character) ON (n.character_id);
CREATE INDEX character_tracked IF NOT EXISTS FOR (n:Character) ON (n.tracked);
CREATE INDEX battle_lookup IF NOT EXISTS FOR (n:Battle) ON (n.battle_id);
CREATE INDEX battle_started_lookup IF NOT EXISTS FOR (n:Battle) ON (n.started_at);
CREATE INDEX doctrine_lookup IF NOT EXISTS FOR (n:Doctrine) ON (n.doctrine_id);
CREATE INDEX fit_lookup IF NOT EXISTS FOR (n:Fit) ON (n.fit_id);
CREATE INDEX item_lookup IF NOT EXISTS FOR (n:Item) ON (n.type_id);
CREATE INDEX side_key_lookup IF NOT EXISTS FOR (n:BattleSide) ON (n.side_key);
CREATE INDEX system_security IF NOT EXISTS FOR (n:System) ON (n.security);
CREATE INDEX system_constellation IF NOT EXISTS FOR (n:System) ON (n.constellation_id);
CREATE INDEX killmail_battle_id IF NOT EXISTS FOR (n:Killmail) ON (n.battle_id);
CREATE INDEX corp_id IF NOT EXISTS FOR (n:Corporation) ON (n.corporation_id);
CREATE INDEX alliance_id IF NOT EXISTS FOR (n:Alliance) ON (n.alliance_id);

// ── Relationship property indexes ───────────────────────────────────────────
CREATE INDEX member_from IF NOT EXISTS FOR ()-[r:MEMBER_OF]-() ON (r.from);
CREATE INDEX part_of_as_of IF NOT EXISTS FOR ()-[r:PART_OF]-() ON (r.as_of);
CREATE INDEX current_corp_as_of IF NOT EXISTS FOR ()-[r:CURRENT_CORP]-() ON (r.as_of);

// ── Alliance relationship graph (computed from killmail co-occurrence) ─────
CREATE INDEX allied_with_weight IF NOT EXISTS FOR ()-[r:ALLIED_WITH]-() ON (r.weight_30d);
CREATE INDEX hostile_to_weight IF NOT EXISTS FOR ()-[r:HOSTILE_TO]-() ON (r.weight_30d);
CREATE INDEX allied_with_computed IF NOT EXISTS FOR ()-[r:ALLIED_WITH]-() ON (r.computed_at);
CREATE INDEX hostile_to_computed IF NOT EXISTS FOR ()-[r:HOSTILE_TO]-() ON (r.computed_at);
CREATE INDEX allied_with_killmails IF NOT EXISTS FOR ()-[r:ALLIED_WITH]-() ON (r.shared_killmails);
CREATE INDEX hostile_to_engagements IF NOT EXISTS FOR ()-[r:HOSTILE_TO]-() ON (r.engagements);
CREATE INDEX ceasefire_region IF NOT EXISTS FOR ()-[r:CEASEFIRE_WITH]-() ON (r.region_id);
CREATE INDEX ceasefire_computed IF NOT EXISTS FOR ()-[r:CEASEFIRE_WITH]-() ON (r.computed_at);
