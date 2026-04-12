# Source-Aware Worldbuilding

A research-first knowledge system for creators.

This repository is a **repo seed** for building a source-grounded lore bible that keeps the following layers separate:

- verified facts
- probable interpretations
- contested claims
- rumor and legend
- author choices

It is designed around the architecture we chose earlier:

- **Zotero** as the corpus/source library
- **GraphRAG** as the extraction engine
- **Wikibase** as the canonical claim store
- **Qdrant** as the retrieval projection/index
- **FastAPI** as the application/control plane
- a future thin web UI on top

The seed starts as a **modular monolith** with explicit ports and adapters. That keeps the early system easy to work on with Codex while preserving the seams needed to split parts later.

---

## What this repo gives you

- a sane repository layout
- a backend seed with domain models, ports, services, and HTTP routes
- JSON schemas for the most important records
- architecture docs and ADRs so the shape does not drift
- a file-backed development mode so you can start immediately
- placeholders for the real Zotero / GraphRAG / Wikibase / Qdrant adapters

This seed intentionally does **not** try to fully containerize Wikibase for local development. Wikibase should be treated as an external system behind a `TruthStorePort`, because local Wikibase setup is heavier than the rest of the MVP and should not block early domain work.

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

### 3. Wikibase is the canonical claim store

Approved claims live in Wikibase because the model supports statements with qualifiers, references, and ranks.

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
Wikibase (canonical truth)
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
│       │   └── json_store.py
│       ├── adapters/
│       │   ├── file_backed.py
│       │   ├── graphrag_adapter.py
│       │   ├── qdrant_adapter.py
│       │   ├── wikibase_adapter.py
│       │   └── zotero_adapter.py
│       ├── services/
│       │   ├── ingestion.py
│       │   ├── query.py
│       │   └── review.py
│       └── api/
│           ├── dependencies.py
│           ├── main.py
│           └── routes/
│               ├── candidates.py
│               ├── claims.py
│               ├── health.py
│               ├── ingest.py
│               └── query.py
└── tests/
    ├── test_health.py
    └── test_review_flow.py
```

---

## Quick start

### 1. Create a virtual environment

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e .[dev]
```

### 2. Copy environment file

```bash
cp .env.example .env
```

### 3. Seed dev data

```bash
saw seed-dev-data
```

### 4. Run the API

```bash
saw serve --reload
```

API docs will be available at `http://localhost:8000/docs`.

### 5. Optional local infra

```bash
docker compose up -d postgres qdrant
```

The seed uses file-backed stores by default, so Docker is optional on day one.

---

## Development modes

### Mode A — file-backed dev mode

Use this when you are shaping models, workflows, and prompts.

- no Zotero required
- no Wikibase required
- no Qdrant required
- works from `data/dev/*.json`

### Mode B — real corpus mode

Use this when integrating a live Zotero library.

- source intake from Zotero API
- extracted text normalization
- candidate claims from GraphRAG
- review still local or API-based

### Mode C — full stack mode

Use this once the domain model is stable.

- canonical claims persisted to Wikibase
- retrieval projection stored in Qdrant
- query modes backed by approved claims and evidence

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

### Ingestion
- `POST /v1/ingest/zotero/pull`
- `POST /v1/ingest/extract-candidates`

### Candidates
- `GET /v1/candidates`
- `POST /v1/candidates/{candidate_id}/review`

### Claims
- `GET /v1/claims`

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
Repo scaffolding, schemas, file-backed stores, and review flow.

### Milestone 1
Real Zotero intake and GraphRAG candidate extraction for a narrow pilot corpus.

### Milestone 2
Wikibase write path for approved claims and evidence.

### Milestone 3
Qdrant projection and filtered retrieval.

### Milestone 4
Thin review/query web UI.

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
5. Do not let Wikibase JSON leak into domain models.
6. Prefer additive ADRs over silent architecture drift.
7. Keep query answers structured and provenance-aware.

---

## External system notes

### GraphRAG
This project expects GraphRAG to be used as an extraction and retrieval building block, not as the source of truth.

### Zotero
Zotero remains the source library for research metadata and attachment management.

### Wikibase
Wikibase is used for canonical approved claims because it models statements with qualifiers and references well.

### Qdrant
Qdrant is used for fast filtered retrieval over approved claims and evidence projections.

---

## First things to build inside this seed

- replace the stub Zotero adapter with real pull + normalize logic
- replace the stub GraphRAG adapter with a candidate extraction pipeline
- implement a real Wikibase truth-store adapter
- implement a real Qdrant projection adapter
- add auth once a real UI exists

That is enough to turn this from an architecture seed into a working product skeleton.
