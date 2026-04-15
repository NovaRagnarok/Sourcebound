# Architecture

## Decision Summary

Sourcebound is still a modular monolith with explicit ports and adapters, but it is no longer just an architecture seed. The current repo already implements the main operational loop:

- intake and source ingestion
- document normalization
- candidate extraction
- human review
- approved canon storage
- query and export surfaces
- writer-facing research runs with advanced utility controls

The design goal remains the same: keep the workflow in one Python codebase while preserving clean seams around external systems.

## Current Runtime Topology

### Core workflow

```text
corpus intake
  -> source records + source documents
  -> text units
  -> candidate claims + evidence
  -> review decisions
  -> approved claims in truth store
  -> Qdrant projection index
  -> query / export consumers
```

### Research workflow

```text
research brief
  -> persisted job
  -> scout adapter
  -> accepted findings
  -> staged source records + source documents
  -> normalization
  -> extraction
  -> review queue
```

## Layering

### Domain

Pure models, enums, and errors. No framework imports.

### Ports

Interfaces for:

- corpus access and intake
- candidate storage
- review persistence
- truth storage
- projection / semantic retrieval
- research run and finding storage
- persisted background job storage

### Services

Use-case orchestration. The main shipped services are:

- `IngestionService`
- `IntakeService`
- `NormalizationService`
- `ReviewService`
- `ResearchService`
- `BibleWorkspaceService`
- `JobService`
- `QueryService`
- `LorePacketService`
- runtime status reporting

### Adapters

Concrete implementations behind the ports:

- file-backed stores for zero-infra workflows
- SQLite app-state stores
- Postgres app-state and truth stores
- Zotero corpus adapter
- GraphRAG extraction adapter
- heuristic extraction fallback
- Qdrant projection and research-semantic adapters
- optional Wikibase truth-store adapter
- web and curated-input research scouts

### API and UI

- FastAPI exposes the workflow and utility routes
- the writer workspace is a shipped static frontend mounted at `/workspace/`, with `/operator/` kept as an advanced alias

## Storage Model

### App state

App state covers operational records such as:

- sources
- source documents
- text units
- extraction runs
- candidates
- evidence
- review events
- research runs
- research findings
- research programs
- background jobs
- Bible project profiles
- Bible sections and paragraph provenance

`APP_STATE_BACKEND` selects the implementation:

- `postgres` is the default
- `sqlite` is supported
- `file` is the zero-infra fallback

### Truth store

Approved canon is behind a separate truth-store port:

- `postgres` is the default
- `file` is supported for local-only work
- `wikibase` is optional

This separation is intentional: app state and canon are related, but not the same responsibility.

### Projection layer

Qdrant is used as a rebuildable projection:

- approved-claim retrieval for query flows
- research finding semantic dedupe / novelty checks

It is not the source of truth.
It may improve ranking for query and Bible composition, but rendered output must still resolve back to approved claims and linked evidence.

## Postgres Canon Schema

The Postgres truth-store path now uses explicit canon tables rather than a single generic blob store.

- `claims`
- `claim_evidence`
- `claim_reviews`
- `claim_versions`
- `claim_relationships`
- `author_decisions`
- `source_documents`
- `source_chunks`

This supports provenance, review history, relationship tracking, and versioned canon writes without leaking persistence details into service logic.

## Key Trust Boundaries

### Extraction is not canon

Neither heuristic extraction nor GraphRAG output becomes canon automatically. Both only produce candidate claims.

### Research is not canon

Research findings are staging material. They must still be normalized, extracted, and reviewed.

### Review is the trust boundary

The only path to approved canon is explicit review.

### Projection is downstream only

Qdrant search results can help rank or retrieve, but canonical answers must still resolve back to approved claims and linked evidence.

### Jobs are orchestration only

Persisted jobs track long-running work, but the authoritative results still live in the existing research-run and Bible-section stores.

## Current Default Stack

For normal local development, the repo expects:

- Postgres for state
- Postgres for canon
- Qdrant for projection-backed retrieval
- Zotero when exercising real corpus workflows
- GraphRAG when configured and ready, otherwise heuristic fallback

## Bounded Contexts

The codebase is still organized around a few natural seams:

- corpus
- normalization
- extraction
- review
- canon
- research
- retrieval / query
- application surface

These seams are useful now for testability and adapter swapping, and later if any part deserves extraction into a separate worker or service.

## Near-Term Split Candidates

No split is required yet, but the first likely candidates remain:

1. extraction worker
2. research worker
3. dedicated frontend deployment
4. retrieval service if Qdrant-backed query traffic grows
