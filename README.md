
---

### Root-level `README.md`

```markdown
# Data-Warehouse-Pipeline (learning series)

A personal playground to practise end-to-end data-engineering patterns on my laptop
using Docker, PostgreSQL, and Python.  
I’m building it in **phases** so I can focus on one concept at a time.

| Phase |           Folder         |                                         What it covers                                                         |
|-------|--------------------------|----------------------------------------------------------------------------------------------------------------|
| 1     | [`DE/phase1`](DE/phase1) | SQL-only mini warehouse: raw → staging → curated, data-quality checks, rerunnable scripts, star-schema basics. |
| 2     | [`DE/phase2`](DE/phase2) | Ingest external data (Open-Meteo API) into raw tables, keep an audit log, schedule a daily job on Windows.     |


---

## Quick start (Phase 1 & 2)

```bash
# Start (or resume) the Postgres container
docker start de-postgres

# Run Phase 1 rebuild (Windows CMD)
DE\phase1\run_phase1.cmd

python DE\phase2\scripts\load_openmeteo_hourly.py --lat 41.8781 --lon -87.6298 --name "Chicago, IL"
```
Check the per-phase README files for deeper docs.

## Why this repo?

I wanted something more realistic than copying StackOverflow snippets,
but still small enough to run on a laptop.
Everything here is done with plain SQL + Python first, then gradually automated.

