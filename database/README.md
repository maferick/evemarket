# Database exports for Codex

Files:
- `export-schema.sql` — schema-only dump
- `table-counts.txt` — approximate row counts per table
- `sample_ref.sql` — reference/static tables
- `sample_app.sql` — sampled application-owned operational tables
- `sample_heavy_recent.sql` — sampled recent rows from heavy killmail/market/history tables
- `OPTIMIZATION_AUDIT.md` — architecture map, hot-path findings, implemented DB optimizations, and phased follow-up plan

Notes:
- These files are intended for schema review, query planning, and DB optimization.
- Samples are intentionally limited and do not represent full production volume.
- Secrets/tokens are intentionally excluded from these exports.
