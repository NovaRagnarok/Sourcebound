# Roadmap

## Current status

- Phase 0 is complete.
- Large parts of Phase 4 already exist as an operator MVP.
- The main work now is making the core integrations real: live Zotero intake, non-heuristic extraction, Wikibase sync in a configured environment, and Qdrant-backed retrieval in the normal dev loop.

## Phase 0 — architecture seed

- domain models
- schemas
- file-backed stores
- API skeleton
- review flow
- operator MVP
- Postgres and SQLite app-state adapters

## Phase 1 — pilot corpus

- connect to a narrow Zotero collection as the normal ingest path
- normalize attached sources into text units
- replace the default heuristic extractor with GraphRAG or another LLM-backed extraction path
- manually review candidates against a real pilot corpus

## Phase 2 — canonical truth

- run approved-claim sync against a configured Wikibase instance
- write references and qualifiers with a project-specific property map
- handle competing claims cleanly

## Phase 3 — retrieval

- make Qdrant-backed projection part of the normal local dev loop
- support filtered semantic search
- enable query modes by certainty and viewpoint

## Phase 4 — product surface

- review queue UI
- source browsing UI
- ask-the-bible UI
- runtime readiness and environment status reporting

## Phase 5 — advanced features

- character knowledge filters
- author decisions as separate layer
- contradiction and drift detection
- reusable prompt packs for domain-specific extraction
