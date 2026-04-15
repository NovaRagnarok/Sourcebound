# Sourcebound

[![CI](https://github.com/NovaRagnarok/Sourcebound/actions/workflows/ci.yml/badge.svg)](https://github.com/NovaRagnarok/Sourcebound/actions/workflows/ci.yml)

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
- Postgres-backed app state, Postgres-backed canon, and Qdrant-backed retrieval
  as the default local stack
- optional Zotero, GraphRAG, research semantics, and Wikibase integrations
  behind explicit flags or backend selection

## What To Try First

For the default fresh-clone local path:

```bash
cp .env.example .env
make bootstrap
docker compose up -d postgres
docker compose up -d qdrant
.venv/bin/saw status
.venv/bin/saw seed-dev-data
.venv/bin/saw serve --reload
```

Expected result: Postgres and Qdrant are ready, the Qdrant projection is
initialized during seeding, and the seeded sample project is visible in the UI.

Then open:

- writer workspace: `http://localhost:8000/workspace/`
- operator view: `http://localhost:8000/operator/`
- API docs: `http://localhost:8000/docs`

Required for the default path: Python 3.11 or 3.12, a local `.venv`, and both
Postgres and Qdrant from `docker compose`.

Optional and disabled by default: Zotero, GraphRAG, research semantics, and
Wikibase.

## How It Works

The main source-to-canon flow looks like this:

```text
Zotero or manual intake
  -> source documents
  -> normalized text units
  -> candidate claims + evidence
  -> human review
  -> approved claims in truth store
  -> Qdrant projection
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
- extraction defaults to the heuristic adapter until GraphRAG is explicitly enabled
- Qdrant projection is part of the default local stack
- research semantics stay disabled until explicitly enabled
- Qdrant is a projection and retrieval layer, not the source of truth
- long-running research, Bible composition, regeneration, and export work run
  as persisted background jobs
- the writer workspace is already shipped, not planned

## Quick Start

1. Create your local env file:

```bash
cp .env.example .env
```

2. Bootstrap the environment:

```bash
make bootstrap
```

3. Start the default local dependencies:

```bash
docker compose up -d postgres qdrant
```

4. Check runtime readiness:

```bash
.venv/bin/saw status
```

You should see Postgres-backed app state and truth storage as ready, plus
Qdrant projection either ready or marked for initialization before seeding.
Zotero, GraphRAG, research semantics, and Wikibase should show up as optional
or disabled, not as startup blockers.

5. Seed the sample project and serve the app:

```bash
.venv/bin/saw seed-dev-data
.venv/bin/saw serve --reload
```

The seed dataset includes a sample project, research run, and Bible sections so
the UI is useful immediately. After seeding, both `/workspace/` and
`/operator/` should load against the same sample project.

## Local Modes

### Default local stack

This is the recommended newcomer path. It gives you:

- Postgres-backed workflow state
- Postgres-backed canon
- Qdrant-backed retrieval
- the browser UI
- the in-process job worker
- heuristic extraction with no GraphRAG setup required

### Zero-infra local mode

Use this when you want to work without Postgres, Qdrant, Zotero, or Wikibase:

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

### Optional Integrations And Deliberate Deviations

Enable these only when you want them, or disable them when you intentionally
want a lighter non-default local mode:

- GraphRAG:
  Run `make bootstrap-graphrag`, set `GRAPH_RAG_ENABLED=true`, and finish the
  GraphRAG workspace or artifact setup.
- Qdrant projection:
  This is enabled in the default local stack. If you disable it for a lighter
  mode, re-enable it with `QDRANT_ENABLED=true`, start Qdrant with
  `docker compose up -d qdrant`, then run `.venv/bin/saw seed-dev-data` or
  `.venv/bin/saw qdrant-rebuild`.
- Research semantics:
  Set `RESEARCH_SEMANTIC_ENABLED=true` and point it at the same Qdrant instance.
- Zotero:
  Fill in the Zotero variables in `.env` when you want live library pulls or
  write-back.
- Wikibase:
  Set `APP_TRUTH_BACKEND=wikibase` and fill in the Wikibase variables before
  starting the app.

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
- `saw evaluate-extraction`

Useful examples:

```bash
.venv/bin/saw status --json-output
.venv/bin/saw qdrant-rebuild --json-output
.venv/bin/saw zotero-check --json-output
.venv/bin/saw intake-text "Field Notes" "Observed three shrine rituals at dusk."
.venv/bin/saw demo-corpus-run wheatley-london-bread --json-output
.venv/bin/saw evaluate-extraction --dataset wheatley-london-bread --json-output
```

For one reproducible end-to-end real-corpus walkthrough, see
[Demo Corpus Workflow](docs/DEMO_CORPUS_WORKFLOW.md). For the extraction
quality benchmark and current baseline targets, see
[Extraction Evaluation](docs/EXTRACTION_EVAL.md).

## Quality Checks

Run the same fast checks locally that the default GitHub Actions workflow uses:

```bash
make check
```

Or run each command individually:

```bash
.venv/bin/ruff check src tests
.venv/bin/python -m mypy src
.venv/bin/pytest -q
```

## First-Run Troubleshooting

- `make bootstrap` fails while installing GraphRAG:
  The default path no longer installs GraphRAG. Run `make bootstrap` for the
  base setup, then `make bootstrap-graphrag` only if you plan to enable it.
- `saw status` says Postgres is not reachable:
  Run `docker compose up -d postgres` and confirm `APP_POSTGRES_DSN` still
  matches `.env.example`.
- `saw status` says Qdrant is not reachable:
  Run `docker compose up -d qdrant` and confirm `QDRANT_URL` still matches
  `.env.example`.
- `saw serve` exits immediately with a configuration error:
  Read the listed env var names closely. Startup now only fails for the backend
  or integration you explicitly selected, and the error message includes the
  exact fix.
- `saw seed-dev-data` fails before seeding:
  This usually means the active backend is not reachable yet. For the default
  path, start Postgres and Qdrant first.
- `health/runtime` looks unfamiliar:
  Optional services such as Zotero, research semantics, and GraphRAG can show as
  disabled or optional while the app is still fully ready for the default local path.

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
```
