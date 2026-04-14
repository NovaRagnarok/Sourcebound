# Sourcebound

Sourcebound is a source-aware worldbuilding workbench. It ingests source
material, normalizes it into text units, extracts candidate claims, routes
those claims through human review, stores approved canon behind a truth-store
port, and serves that canon through an API, persisted background jobs, and a
writer-first workspace UI.

Sourcebound already includes:

- a FastAPI backend for intake, ingestion, review, research, Bible workspace,
  export, query, jobs, and health flows
- a Typer CLI for bootstrap, runtime checks, seeding, intake, and benchmark
  tasks
- a browser UI at `/workspace/`, with `/operator/` available as the advanced
  utilities alias
- persisted background jobs with an in-process worker enabled by default
- Postgres-backed app state and Postgres-backed canon as the default stack
- optional Zotero, GraphRAG, Qdrant, and Wikibase integrations behind explicit
  ports

## What To Try First

If you want the fastest newcomer path against the default local stack:

```bash
make bootstrap
docker compose up -d postgres qdrant
.venv/bin/saw status
.venv/bin/saw seed-dev-data
.venv/bin/saw serve --reload
```

Expected result: Postgres is ready, Qdrant is ready, and the seeded sample
project is visible in the operator UI.

Then open:

- operator view: `http://localhost:8000/operator/` (advanced alias)
- writer workspace: `http://localhost:8000/workspace/` (writer-first alias)
- API docs: `http://localhost:8000/docs`

Required for the default path: Python with a local `.venv`, Postgres, and
Qdrant.

Optional for first run: Zotero, Wikibase, and GraphRAG or other
LLM-backed extraction.

## How It Works

The main source-to-canon flow looks like this:

```text
Zotero or manual intake
  -> source documents
  -> normalized text units
  -> candidate claims + evidence
  -> human review
  -> approved claims in truth store
  -> optional Qdrant projection
  -> query + lore export surfaces
```

There is also a research-first path:

```text
research brief
  -> web scout / curated inputs
  -> persisted background job
  -> accepted findings
  -> staged sources + documents
  -> normalization
  -> extraction
  -> normal review queue
```

Important current behavior:

- `APP_STATE_BACKEND` defaults to `postgres`
- `APP_TRUTH_BACKEND` defaults to `postgres`
- `APP_JOB_WORKER_ENABLED` defaults to `true`
- `APP_UI_ENABLED` defaults to `true`
- extraction uses GraphRAG when enabled, otherwise the heuristic adapter
- Qdrant is a projection and retrieval layer, not the source of truth
- long-running research, Bible composition, regeneration, and export work run
  as persisted background jobs
- the writer workspace is already shipped, not planned

## Quick Start

For the default local setup, `.env.example` is already aligned with the main
Postgres-first path.

1. Bootstrap the environment:

```bash
make bootstrap
```

2. Start the local services:

```bash
docker compose up -d postgres qdrant
```

3. Check runtime readiness:

```bash
.venv/bin/saw status
```

4. Seed the sample project and serve the app:

```bash
.venv/bin/saw seed-dev-data
.venv/bin/saw serve --reload
```

The seed dataset includes a sample project, research run, and Bible sections so
the UI is useful immediately. After seeding, both `/workspace/` and
`/operator/` should load against the same sample project.

## Local Modes

### Postgres-first local stack

This is the default and recommended path. It gives you Postgres-backed review
state, Postgres-backed canon, the browser UI, the background worker, and Qdrant
for better retrieval relevance.

See [Author Stack](docs/AUTHOR_STACK.md) for the recommended full local config.

### Zero-infra local mode

Use this when you want to work without Postgres, Qdrant, or Zotero:

```bash
APP_STATE_BACKEND=file
APP_TRUTH_BACKEND=file
APP_JOB_WORKER_ENABLED=true
GRAPH_RAG_ENABLED=false
QDRANT_ENABLED=false
RESEARCH_SEMANTIC_ENABLED=false
```

Then reseed:

```bash
APP_STATE_BACKEND=file APP_TRUTH_BACKEND=file .venv/bin/saw seed-dev-data
```

Data will live in `data/dev/`.

### SQLite app-state mode

SQLite is available for app state, but truth storage still uses the configured
truth backend:

```bash
APP_STATE_BACKEND=sqlite
APP_SQLITE_PATH=runtime/sourcebound.db
```

## Common Commands

The CLI is mainly for runtime checks, setup, seed data, and ingestion:

- `saw serve`
- `saw seed-dev-data`
- `saw status`
- `saw qdrant-init`
- `saw qdrant-rebuild`
- `saw zotero-check`
- `saw intake-text`
- `saw intake-url`
- `saw intake-file`

Useful examples:

```bash
.venv/bin/saw status --json-output
.venv/bin/saw qdrant-rebuild --json-output
.venv/bin/saw zotero-check --json-output
.venv/bin/saw intake-text "Field Notes" "Observed three shrine rituals at dusk."
```

## API And UI

Use `http://localhost:8000/docs` for the full OpenAPI surface.

The main route groups are:

- `/health` and `/health/runtime`
- `/v1/intake/*` and `/v1/ingest/*`
- `/v1/candidates/*` and `/v1/claims/*`
- `/v1/research/*`
- `/v1/bible/*`
- `/v1/jobs/*`
- `/v1/exports/lore-packet`

Long-running research, Bible, and export routes return persisted job records.
Poll `/v1/jobs/{job_id}` until completion, then read the authoritative result
from its normal route.

## Solo Author Flow

The intended daily writing loop is:

1. run a research brief
2. stage and extract accepted findings
3. review candidates into approved canon
4. compose Bible sections
5. inspect provenance for paragraphs you want to trust
6. keep drafting in the manual section text without fear of regeneration
   overwriting it

## Repository Guide

```text
src/source_aware_worldbuilding/
  api/           FastAPI app and route wiring
  adapters/      Postgres, SQLite, file, Zotero, Qdrant, Wikibase, GraphRAG,
                 and research adapters
  domain/        models, enums, errors, normalization rules
  services/      ingestion, intake, review, research, Bible workspace, jobs,
                 query, lore export, status
  storage/       low-level JSON and relational state helpers
  settings.py    environment-driven runtime configuration
  cli.py         developer/operator CLI

frontend/operator-ui/
  static operator console for research, sources, runs, review, claims, and
  query flows

docs/
  ARCHITECTURE.md
  AUTHOR_STACK.md
  ROADMAP.md
  adrs/
  ontology/
  research/
  schemas/
```

## Tests And Contributing

Normal test run:

```bash
make test
```

Live integrations are opt-in:

```bash
.venv/bin/pytest -m live_zotero tests/test_live_integrations.py
.venv/bin/pytest -m live_qdrant tests/test_live_integrations.py
.venv/bin/pytest -m live_wikibase tests/test_live_integrations.py
```

Before opening a PR, run:

```bash
.venv/bin/ruff check src tests
make test
```

If you change runtime behavior or setup expectations, update `README.md` and
the most relevant doc in `docs/` in the same change.

## Related Docs

- [Architecture](docs/ARCHITECTURE.md)
- [Author Stack](docs/AUTHOR_STACK.md)
- [Roadmap](docs/ROADMAP.md)
- [Default Research Program](docs/research/default_program.md)
- [ADR 0001: Modular Monolith](docs/adrs/0001-modular-monolith.md)

## License

Sourcebound is licensed under the Apache License, Version 2.0. See
[LICENSE](LICENSE).
