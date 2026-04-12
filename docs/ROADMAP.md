# Roadmap

## Phase 0 — architecture seed

- domain models
- schemas
- file-backed stores
- API skeleton
- review flow

## Phase 1 — pilot corpus

- connect to a narrow Zotero collection
- normalize attached sources into text units
- produce candidate claims via GraphRAG
- manually review candidates

## Phase 2 — canonical truth

- write approved claims to Wikibase
- write references and qualifiers
- handle competing claims cleanly

## Phase 3 — retrieval

- project approved claims and evidence into Qdrant
- support filtered semantic search
- enable query modes by certainty and viewpoint

## Phase 4 — product surface

- review queue UI
- source browsing UI
- ask-the-bible UI

## Phase 5 — advanced features

- character knowledge filters
- author decisions as separate layer
- contradiction and drift detection
- reusable prompt packs for domain-specific extraction
