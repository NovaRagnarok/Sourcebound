# Sourcebound

Sourcebound is a source-aware worldbuilding workbench. It ingests source
material, normalizes it into text units, extracts candidate claims, routes
those claims through human review, stores approved canon behind a truth-store
port, and serves that canon through an API, persisted background jobs, and an
writer-first workspace UI.

Today the repo is past the original seed stage. It already ships:

- a FastAPI backend with intake, ingestion, research, review, Bible workspace,
  export, query, jobs, and health routes
- a Typer CLI for local development, runtime checks, seeding, intake, and benchmarking
- a browser-based writer workspace at `/workspace/` with advanced utilities still available at `/operator/`
- persisted background jobs with an in-process worker enabled by default
- Postgres-backed app state and Postgres-backed canon as the default stack
- file-backed and SQLite-backed state adapters for lighter local workflows
- optional Zotero, GraphRAG, Qdrant, and Wikibase integrations behind explicit ports

## Current Shape

The implemented end-to-end flow looks like this:

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

There is also a second path for broad-topic research:

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

Important reality checks for the current repo:

- `APP_STATE_BACKEND` defaults to `postgres`
- `APP_TRUTH_BACKEND` defaults to `postgres`
- `APP_JOB_WORKER_ENABLED` defaults to `true`
- `APP_UI_ENABLED` defaults to `true`
- extraction uses GraphRAG when enabled, otherwise the heuristic adapter
- the writer workspace is already shipped, not planned
- lore packet export is implemented
- live integration tests exist for Zotero, Qdrant, and Wikibase but are opt-in
- long-running research and Bible composition routes now queue persisted
  background jobs and are polled by the workspace UI

## Quick Start

### 1. Bootstrap

```bash
make bootstrap
```

### 2. Configure local env

The app reads `.env` automatically. The repo already includes `.env.example`.

For the default local stack:

```bash
cp .env.example .env
docker compose up -d postgres qdrant
```

`.env.example` matches the default Postgres-first path and includes:

- `APP_STATE_BACKEND=postgres`
- `APP_TRUTH_BACKEND=postgres`
- `APP_JOB_WORKER_ENABLED=true`
- `APP_UI_ENABLED=true`
- `QDRANT_ENABLED=true`

### 3. Check runtime readiness

```bash
.venv/bin/saw status
```

This shows which services are configured, ready, disabled, or stubbed in your current environment.
If you plan to rely on semantic retrieval in a live runtime, initialize Qdrant before serving:

```bash
.venv/bin/saw qdrant-init
.venv/bin/saw qdrant-rebuild
```

### 4. Seed development data

```bash
.venv/bin/saw seed-dev-data
```

### 5. Run the app

```bash
.venv/bin/saw serve --reload
```

Then open:

- API docs: `http://localhost:8000/docs`
- writer workspace: `http://localhost:8000/workspace/`
- advanced utilities alias: `http://localhost:8000/operator/`
- runtime health: `http://localhost:8000/health/runtime`

## Development Modes

### Postgres-first local stack

This is the default and matches the repo’s main integration path.

```bash
APP_STATE_BACKEND=postgres
APP_TRUTH_BACKEND=postgres
APP_POSTGRES_DSN=postgresql://saw:saw@localhost:5432/saw
APP_POSTGRES_SCHEMA=sourcebound
APP_JOB_WORKER_ENABLED=true
APP_UI_ENABLED=true
QDRANT_ENABLED=true
QDRANT_URL=http://localhost:6333
APP_STRICT_STARTUP_CHECKS=true
```

Use this when you want the full writer workstation flow, Postgres-backed review
state, and Postgres-backed canon.
Qdrant is the recommended local retrieval path, but it remains optional.

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
Long-running work still queues as persisted jobs in this mode. The in-process
worker simply executes them against file-backed state.

### SQLite app-state mode

SQLite exists for app state, but truth storage still uses the configured truth backend:

```bash
APP_STATE_BACKEND=sqlite
APP_SQLITE_PATH=runtime/sourcebound.db
```

## CLI Surface

Main commands:

- `saw serve`
- `saw seed-dev-data`
- `saw status`
- `saw qdrant-init`
- `saw qdrant-rebuild`
- `saw zotero-check`
- `saw intake-text`
- `saw intake-url`
- `saw intake-file`
- `saw normalize-documents`
- `saw benchmark-2003-dj`

The CLI focuses on runtime checks, seed data, and ingestion. Long-running
research, Bible composition, regeneration, and export work are mainly driven
through the API and workspace UI and tracked as persisted jobs.

Useful examples:

```bash
.venv/bin/saw status --json-output
.venv/bin/saw qdrant-init --json-output
.venv/bin/saw qdrant-rebuild --json-output
.venv/bin/saw zotero-check --json-output
.venv/bin/saw intake-text "Field Notes" "Observed three shrine rituals at dusk."
.venv/bin/saw benchmark-2003-dj --json-output
```

## API Surface

Implemented routes:

- `GET /health`
- `GET /health/runtime`
- `POST /v1/ingest/zotero/pull`
- `POST /v1/ingest/normalize-documents`
- `POST /v1/ingest/extract-candidates`
- `POST /v1/intake/text`
- `POST /v1/intake/url`
- `POST /v1/intake/file`
- `GET /v1/sources`
- `GET /v1/sources/{source_id}`
- `GET /v1/extraction-runs`
- `GET /v1/candidates`
- `GET /v1/candidates/{candidate_id}`
- `POST /v1/candidates/{candidate_id}/review`
- `GET /v1/claims`
- `GET /v1/claims/{claim_id}`
- `GET /v1/claims/{claim_id}/relationships`
- `POST /v1/claims/{claim_id}/relationships`
- `POST /v1/query`
- `POST /v1/exports/lore-packet`
- `POST /v1/research/runs`
- `GET /v1/research/runs`
- `GET /v1/research/runs/{run_id}`
- `POST /v1/research/runs/{run_id}/stage`
- `POST /v1/research/runs/{run_id}/extract`
- `POST /v1/research/programs`
- `GET /v1/research/programs`
- `GET /v1/bible/profiles`
- `GET /v1/bible/profiles/{project_id}`
- `PUT /v1/bible/profiles/{project_id}`
- `GET /v1/bible/sections?project_id=...`
- `POST /v1/bible/sections`
- `GET /v1/bible/sections/{section_id}`
- `PUT /v1/bible/sections/{section_id}`
- `POST /v1/bible/sections/{section_id}/regenerate`
- `GET /v1/bible/sections/{section_id}/provenance`
- `GET /v1/bible/exports/{project_id}`
- `POST /v1/bible/exports/{project_id}`
- `GET /v1/jobs`
- `GET /v1/jobs/{job_id}`
- `POST /v1/jobs/{job_id}/cancel`
- `POST /v1/jobs/{job_id}/retry`

Long-running `POST /v1/research/...` and Bible composition, regeneration, and
export routes return `202 Accepted` with a persisted job record. Poll
`GET /v1/jobs/{job_id}` until the job reaches `completed`, then read the
authoritative research run, Bible section, or export bundle from its normal
route.

Bible export now has two paths:

- `POST /v1/bible/exports/{project_id}` queues a persisted export job
- `GET /v1/bible/exports/{project_id}` returns the latest completed export
  bundle when one exists, otherwise falls back to direct bundle generation for
  tooling and verification
- `POST /v1/jobs/{job_id}/cancel` requests best-effort cancellation
- `POST /v1/jobs/{job_id}/retry` creates a fresh retry attempt for failed retryable jobs

## Recommended Author Stack

For the smoothest solo-author workflow, use:

- `APP_STATE_BACKEND=postgres`
- `APP_TRUTH_BACKEND=postgres`
- `APP_JOB_WORKER_ENABLED=true`
- `APP_UI_ENABLED=true`
- `QDRANT_ENABLED=true`
- `RESEARCH_SEMANTIC_ENABLED=true`
- `APP_STRICT_STARTUP_CHECKS=true`
- `GRAPH_RAG_ENABLED=false` unless you are actively exercising extraction work
  beyond the heuristic path

That combination gives you persisted Bible state, background research/Bible/
export jobs, semantic research dedupe, and a stable local operator experience
without making Qdrant a trust dependency.
With strict startup checks enabled, the app refuses to boot if Qdrant is
configured but the projection collection is missing or degraded. Use
`saw qdrant-rebuild` during deploy/bootstrap to initialize and backfill it.

## Solo Author Flow

The intended daily-writing loop is:

1. run a research brief
2. stage and extract accepted findings
3. review candidates into approved canon
4. compose Bible sections
5. inspect “Why this section says this” for any paragraph you want to trust
6. keep drafting in the manual section text without fear of regeneration overwriting it

The seed dataset already includes a sample project, research run, and Bible
sections so the workspace UI is useful immediately after `saw seed-dev-data`.

## Minimal Deployment Notes

If you run Sourcebound outside local dev, keep the deployment guidance intentionally small:

- use a persistent state backend, not ephemeral file storage
- keep the background worker enabled wherever long-running jobs are expected
- treat Qdrant as recommended for relevance, not required for correctness
- keep a routine export/backup habit for Bible projects and app-state storage

## Repository Guide

```text
src/source_aware_worldbuilding/
  api/           FastAPI app and route wiring
  adapters/      Postgres, SQLite, file, Zotero, Qdrant, Wikibase, GraphRAG, research adapters
  domain/        models, enums, errors, normalization rules
  services/      ingestion, intake, review, research, Bible workspace, jobs,
                 query, lore export, status
  storage/       low-level JSON and relational state helpers
  settings.py    environment-driven runtime configuration
  cli.py         developer/operator CLI

frontend/operator-ui/
  static operator console for research, sources, runs, review, claims, and query flows

docs/
  ARCHITECTURE.md
  ROADMAP.md
  adrs/
  ontology/
  research/
  schemas/
```

## Tests

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

The live Wikibase test creates test entities and should only be run against a disposable instance.

## Notes On Integrations

- Zotero is the main corpus adapter for real-source workflows and powers both
  pull-based ingest and write-based intake.
- GraphRAG is optional at runtime. If it is not ready, the app falls back to heuristic extraction.
- Qdrant is treated as a projection layer, not the source of truth.
- Wikibase remains optional and is selected only when `APP_TRUTH_BACKEND=wikibase`.

## Contributing

Before opening a PR, run:

```bash
.venv/bin/ruff check src tests
make test
```

If you change runtime behavior or setup expectations, update `README.md` and
the most relevant doc in `docs/` in the same change.

## Related Docs

- [Architecture](docs/ARCHITECTURE.md)
- [Roadmap](docs/ROADMAP.md)
- [Author Stack](docs/AUTHOR_STACK.md)
- [Default Research Program](docs/research/default_program.md)
- [ADR 0001: Modular Monolith](docs/adrs/0001-modular-monolith.md)

## License

Sourcebound is licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE).
