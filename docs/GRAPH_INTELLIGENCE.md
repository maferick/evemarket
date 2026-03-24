# Optional Graph Intelligence Layer

This layer is intentionally selective and **not** a source of truth.

## Scope

Use graph traversal only for:

1. Doctrine dependency graph (`Doctrine -> Fit -> Item`).
2. Character intelligence graph (`Character -> Alliance`, `Character -> System`).

## Read/Write rules

- Python sync jobs write graph nodes/edges from MariaDB source datasets.
- Python can query the graph for multi-hop analysis.
- Python writes resulting insights back into MariaDB precomputed tables.
- PHP does **not** query graph directly in the initial rollout.

## Minimal schema sketch

### Doctrine dependency

- Node labels: `Doctrine`, `Fit`, `Item`
- Relationship types: `HAS_FIT`, `REQUIRES_ITEM`

### Character intelligence

- Node labels: `Character`, `Alliance`, `System`
- Relationship types: `MEMBER_OF`, `SEEN_IN`

## Sync cadence

- Run after the MariaDB/Influx precompute jobs complete.
- Keep graph updates incremental where possible (changed doctrines, changed sightings).
