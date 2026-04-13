# Source-Aware Worldbuilding

A research-first knowledge system for creators.

This repository is a research-first MVP for building a source-grounded lore bible that keeps the following layers separate:

- verified facts
- probable interpretations
- contested claims
- rumor and legend
- author choices

It is designed around the architecture we chose earlier:

- **Zotero** as the corpus/source library
- **GraphRAG** as the extraction engine
- **Postgres** as the default canonical claim store for now
- **Qdrant** as the retrieval projection/index
- **FastAPI** as the application/control plane
- a thin operator UI on top

The codebase starts as a **modular monolith** with explicit ports and adapters. That keeps the system easy to evolve with Codex while preserving the seams needed to split parts later.

---

## What this repo gives you

- a sane repository layout
- a backend MVP with domain models, ports, services, CLI commands, and HTTP routes
- JSON schemas for the most important records
- architecture docs and ADRs so the shape does not drift
- a Postgres-first app-state setup with SQLite and file-backed fallbacks
- a shipped operator UI for sources, extraction runs, review, claims, and querying
- live-capable Zotero, optional Wikibase, and Qdrant adapters with safe local fallbacks

This seed intentionally keeps approved claims in Postgres by default for the normal stack. File-backed truth remains available for zero-infra development, and Wikibase remains an optional adapter behind a `TruthStorePort`.

---

## Current project state

The project is currently between the architecture-seed phase and the first real integration phase:

- the end-to-end ingest -> extract -> review -> claim -> query path is implemented and tested
- the operator UI is already present at `/operator/`
- Postgres-backed app state is implemented and covered by integration tests
- live Zotero, Wikibase, and Qdrant integration tests exist, but they are opt-in and require local or remote services plus credentials
- extraction is still heuristic by default, not a full GraphRAG pipeline yet

Use the runtime status command to see what is live versus stubbed in your current environment:

```bash
.venv/bin/saw status
```

---

## Core architectural principles

### 1. Truth has layers

Do not flatten everything into one bucket.

The system distinguishes:

- `verified`
- `probable`
- `contested`
- `rumor`
- `legend`
- `author_choice`

### 2. Extraction is not canon

GraphRAG produces **candidate claims** only.

Nothing extracted by an LLM becomes canonical until it passes through review and is written to the truth store.

### 3. Approved claims live behind a truth-store port

Approved claims use Postgres by default in the normal stack, with a file-backed truth store still available for no-infra local work. If we bring Wikibase back later, it should remain an adapter instead of leaking into domain logic.

### 4. Qdrant is a projection, not a source of truth

Qdrant exists to support filtered semantic retrieval across approved claims and evidence. It can be rebuilt from canonical data.

### 5. Keep source provenance at evidence-snippet level

Every approved claim should be traceable back to:

- a source record
- a locator (page, section, chapter, URL fragment, timestamp, etc.)
- a supporting text span or excerpt

### 6. Start as a modular monolith

Do not start with microservices.

Start with one Python codebase that contains bounded modules with explicit interfaces. Split only when there is real operational pressure.

---

## System overview

```text
Zotero
  ↓
Normalization / Source Intake
  ↓
GraphRAG Extraction
  ↓
Candidate Claims Queue
  ↓ Human Review
Approved Claims
  ↓
Postgres truth store
  ↓
Qdrant projection
  ↓
Query / Answering API
  ↓
Future thin UI
```

---

## Bounded contexts

### Corpus
Responsible for source records, attachments, extracted text, and locators.

### Extraction
Responsible for chunking, running GraphRAG, and producing candidate claims.

### Review
Responsible for approval, rejection, merge, split, and status overrides.

### Canon
Responsible for approved claims, evidence links, author choices, and viewpoints.

### Retrieval
Responsible for query projections and filtered search over approved data.

### Application
Responsible for HTTP API, future UI, auth, workflow orchestration, and jobs.

---

## Repository layout

```text
source-aware-worldbuilding/
├── README.md
├── Makefile
├── pyproject.toml
├── .env.example
├── docker-compose.yml
├── data/
│   └── dev/
│       ├── candidates.json
│       ├── claims.json
│       ├── evidence.json
│       ├── extraction_runs.json
│       ├── review_events.json
│       └── sources.json
├── docs/
│   ├── ARCHITECTURE.md
│   ├── ROADMAP.md
│   ├── ontology/
│   │   └── initial-taxonomy.md
│   ├── schemas/
│   │   ├── candidate-claim.schema.json
│   │   ├── claim.schema.json
│   │   └── evidence.schema.json
│   └── adrs/
│       ├── 0001-modular-monolith.md
│       ├── 0002-wikibase-canonical-store.md
│       ├── 0003-human-review-gate.md
│       └── 0004-qdrant-is-a-projection.md
├── infra/
│   └── wikibase/
│       └── README.md
├── src/
│   └── source_aware_worldbuilding/
│       ├── __init__.py
│       ├── settings.py
│       ├── cli.py
│       ├── ports.py
│       ├── domain/
│       │   ├── enums.py
│       │   └── models.py
│       ├── storage/
│       │   ├── json_store.py
│       │   ├── postgres_app_state.py
│       │   └── sqlite_app_state.py
│       ├── adapters/
│       │   ├── file_backed.py
│       │   ├── graphrag_adapter.py
│       │   ├── postgres_backed.py
│       │   ├── qdrant_adapter.py
│       │   ├── sqlite_backed.py
│       │   ├── wikibase_adapter.py
│       │   └── zotero_adapter.py
│       ├── services/
│       │   ├── ingestion.py
│       │   ├── query.py
│       │   ├── review.py
│       │   └── status.py
│       └── api/
│           ├── dependencies.py
│           ├── main.py
│           └── routes/
│               ├── candidates.py
│               ├── claims.py
│               ├── health.py
│               ├── ingest.py
│               ├── query.py
│               ├── runs.py
│               └── sources.py
└── tests/
    ├── test_api_integration.py
    ├── test_health.py
    ├── test_postgres_integration.py
    ├── test_query_service.py
    └── test_review_flow.py
```

---

## Quick start

### 1. Bootstrap a virtual environment

```bash
make bootstrap
```

### 2. Copy environment file

```bash
cp .env.example .env
```

### 3. Start local infra

```bash
docker compose up -d postgres qdrant
```

Postgres is the default app-state backend. Qdrant is optional, but starting it now lets you exercise projection-backed retrieval later.

### 4. Check runtime readiness

```bash
.venv/bin/saw status
```

This reports which parts of the stack are live, disabled, stubbed, or missing configuration.

### 5. Seed dev data

```bash
.venv/bin/saw seed-dev-data
```

### 6. Run the API

```bash
.venv/bin/saw serve --reload
```

API docs will be available at `http://localhost:8000/docs`.
The operator console will be available at `http://localhost:8000/operator/`.

### 7. File-backed workflow state

```bash
APP_STATE_BACKEND=file APP_TRUTH_BACKEND=file .venv/bin/saw seed-dev-data
```

Use file-backed mode when you want zero external services while shaping models, workflows, or prompts.
Approved-claim routes work locally and write to `data/dev/claims.json`.

### 8. Optional live integration checks

```bash
.venv/bin/pytest -m live_zotero tests/test_live_integrations.py
.venv/bin/pytest -m live_qdrant tests/test_live_integrations.py
.venv/bin/pytest -m live_wikibase tests/test_live_integrations.py
```

Each live test skips automatically unless its real service is configured. Run the Wikibase live test only against a disposable or non-production instance, because it creates test items.

---

## Development modes

### Mode A — file-backed dev mode

Use this when you are shaping models, workflows, and prompts.

- no Zotero required
- no Wikibase required
- no Qdrant required
- set `APP_STATE_BACKEND=file`
- set `APP_TRUTH_BACKEND=file`
- works from `data/dev/*.json`
- approved claims are stored in `data/dev/claims.json`

### Mode B — real corpus mode

Use this when integrating a live Zotero library.

- source intake from Zotero API
- extracted text normalization from source metadata, notes, and attachment metadata
- sentence-level candidate claims and evidence from the extraction pipeline
- review and approved-claim storage can run against Postgres without adding Wikibase

### Mode C — full stack mode

Use this once the domain model is stable.

- app/workflow state persisted to Postgres
- approved claims persisted to Postgres by default
- retrieval projection stored in Qdrant
- query modes backed by approved claims and evidence

### Postgres app state and canon

Default settings:

```bash
APP_STATE_BACKEND=postgres
APP_TRUTH_BACKEND=postgres
APP_POSTGRES_DSN=postgresql://saw:saw@localhost:5432/saw
APP_POSTGRES_SCHEMA=sourcebound
```

This stores sources, text units, extraction runs, candidates, evidence, review events, and approved claims in Postgres while preserving the same service layer and API routes.

The canon side now uses dedicated epistemic tables instead of one generic approved-claims blob:

- `claims`
- `claim_evidence`
- `claim_reviews`
- `claim_versions`
- `claim_relationships`
- `author_decisions`
- `source_documents`
- `source_chunks`

If you want the old no-infra path, switch back explicitly:

```bash
APP_STATE_BACKEND=file
APP_TRUTH_BACKEND=file
```

### Optional Wikibase sync

If you explicitly set `APP_TRUTH_BACKEND=wikibase`, approved-claim reads and writes require:

```bash
WIKIBASE_API_URL=https://your-wikibase.example/w/api.php
WIKIBASE_USERNAME=...
WIKIBASE_PASSWORD=...
WIKIBASE_PROPERTY_MAP='{"main_value":"P1","predicate":"P2","status":"P3","claim_kind":"P4","place":"P5","time_start":"P6","time_end":"P7","viewpoint_scope":"P8","notes":"P9","app_claim_id":"P10","source_id":"P11","locator":"P12","evidence_text":"P13","evidence_id":"P14"}'
```

The default is `APP_TRUTH_BACKEND=postgres`. Use `APP_TRUTH_BACKEND=file` only when you explicitly want the zero-infra local path.

---

## The minimal claim lifecycle

1. Pull source metadata and attachments from Zotero.
2. Normalize content into text units with locators.
3. Run extraction to produce **candidate claims**.
4. Review candidates.
5. Write approved claims and evidence references to the truth store.
6. Project approved claims into Qdrant.
7. Answer user questions only from approved claims and evidence.

---

## API shape in the seed

### Health
- `GET /health`
- `GET /health/runtime`

### Ingestion
- `POST /v1/ingest/zotero/pull`
- `POST /v1/ingest/extract-candidates`

### Sources
- `GET /v1/sources`
- `GET /v1/sources/{source_id}`

### Extraction runs
- `GET /v1/extraction-runs`

### Candidates
- `GET /v1/candidates`
- `GET /v1/candidates/{candidate_id}`
- `POST /v1/candidates/{candidate_id}/review`

### Claims
- `GET /v1/claims`
- `GET /v1/claims/{claim_id}`

### Query
- `POST /v1/query`

---

## Query modes

The domain model already assumes separate answer modes:

- `strict_facts`
- `contested_views`
- `rumor_and_legend`
- `character_knowledge`
- `open_exploration`

That matters because the same corpus should answer different kinds of questions differently.

---

## Suggested near-term milestones

### Milestone 0
Done: repo scaffolding, schemas, Postgres-backed app state, file-backed fallback, review flow, and operator UI.

### Milestone 1
Next: connect a real Zotero pilot corpus and replace stub ingest as the normal operating path.

### Milestone 2
Next: replace or augment the heuristic extractor with the real GraphRAG or LLM-backed extraction path.

### Milestone 3
Next: harden the local approved-claim workflow and only bring back Wikibase if it earns its complexity.

### Milestone 4
Next: enable Qdrant-backed retrieval in normal local development and harden answer quality around provenance and viewpoint filters.

---

## Non-goals for the first iteration

- manuscript generation
- collaborative editing permissions
- giant ontology design
- fully automated claim approval
- multi-period / multi-domain abstraction before one pilot works
- replacing Zotero

---

## Codex-friendly rules for this repo

When you use Codex on this repository, keep these rules in place:

1. Do not let adapters import from each other.
2. Do not let domain models depend on framework code.
3. Do not write directly to Qdrant from extraction.
4. Do not bypass review when creating approved claims.
5. Do not let adapter-specific payloads leak into domain models.
6. Prefer additive ADRs over silent architecture drift.
7. Keep query answers structured and provenance-aware.

---

## External system notes

### GraphRAG
This project expects GraphRAG to be used as an extraction and retrieval building block, not as the source of truth.

### Zotero
Zotero remains the source library for research metadata and attachment management.

### Wikibase
Wikibase is optional for later canonical-sync work, not the default local truth store.

### Qdrant
Qdrant is used for fast filtered retrieval over approved claims and evidence projections.

---

## Highest-leverage next work

- configure a real Zotero collection and use it as the default pilot corpus
- replace or augment the heuristic extractor with the real GraphRAG pipeline
- harden the local approved-claim workflow and only add Wikibase back when it earns its complexity
- run Qdrant locally in the normal dev loop and tune projection-backed query ranking
- add richer character-knowledge and viewpoint handling once the real corpus is flowing
