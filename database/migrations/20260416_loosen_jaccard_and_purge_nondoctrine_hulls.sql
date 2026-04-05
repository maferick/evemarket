-- Loosen the auto-doctrine clustering threshold and purge non-doctrine
-- hulls that leaked into the ``auto_doctrines`` table before the
-- hull-group filter was added.

-- ── Lower default Jaccard clustering threshold from 0.80 → 0.65 ─────────
-- 0.80 was inherited from the opponent-kill economic warfare clusterer
-- and is too strict for our own losses: incomplete killmails, mid-fight
-- refits, and meta variance fragment what are really the same doctrine
-- operationally. 0.65 merges clusters sharing roughly two-thirds of
-- their modules, which empirically matches real fleet composition
-- overlap. Only update if the operator hasn't already hand-tuned it.
UPDATE app_settings
   SET setting_value = '0.65'
 WHERE setting_key = 'auto_doctrines.jaccard_threshold'
   AND setting_value = '0.80';

-- ── Purge stale rows for hulls that should never have been detected ────
-- The detector now filters out rookie ships, shuttles, capsules,
-- haulers, freighters, mining barges, exhumers, mining frigates
-- (Venture et al), industrial command ships (Orca/Porpoise), and
-- Rorquals. Any rows already in auto_doctrines for those hulls
-- (matched via ref_item_types.group_id) are removed here.
--
-- auto_doctrine_modules and auto_doctrine_fit_demand_1d both cascade
-- on doctrine_id via their FKs, so a single DELETE sweeps their
-- children.
DELETE ad
  FROM auto_doctrines ad
  JOIN ref_item_types rit ON rit.type_id = ad.hull_type_id
 WHERE rit.group_id IN (
     29, 31, 237,
     28, 380, 513, 1022,
     463, 543, 1283, 941, 902
 );
