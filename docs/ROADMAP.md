# Roadmap

## Current Status

The repo is well beyond the initial scaffold.

Already shipped:

- operator UI mounted at `/operator/`
- ingestion, intake, review, query, export, and runtime-health APIs
- research runs with staging and extract handoff
- Postgres-backed app state
- Postgres-backed canon as the default truth-store path
- lore packet export
- live integration coverage for Zotero, Qdrant, and Wikibase

Still notably incomplete or provisional:

- GraphRAG is not yet the always-on default in normal local use
- real Zotero-backed workflows still depend on local credentials and collection setup
- Qdrant-backed retrieval is available but not yet the default everyday loop for all development
- auth and production deployment concerns are still light

## Phase 1 — Stabilize The Default Stack

Focus: make the documented Postgres-first stack feel boring and dependable.

- tighten `.env.example` around the current defaults and optional integrations
- keep operator flow smooth in the Postgres-backed path
- harden runtime status and setup diagnostics
- continue closing gaps between file-backed tests and Postgres-backed behavior

## Phase 2 — Real Corpus Workflow

Focus: make Zotero-backed intake and normalization the standard source workflow.

- improve source-document discovery and attachment handling
- make `zotero-check` and intake tooling better at surfacing config or API issues
- reduce friction between operator intake and pull-based ingest
- validate the loop on a narrow but real pilot corpus

## Phase 3 — Extraction Upgrade

Focus: move from “heuristic fallback works” to “GraphRAG is practical”.

- harden GraphRAG runtime readiness checks
- improve artifact import and in-process GraphRAG paths
- compare heuristic and GraphRAG output quality on benchmark runs
- improve evidence span quality and candidate usefulness for review

## Phase 4 — Retrieval And Research Quality

Focus: turn Qdrant-backed search and research semantics into routine tools.

- make projection-backed retrieval a normal local workflow
- improve semantic reranking and duplicate handling for research findings
- expose clearer query modes by certainty, relationship, and viewpoint
- strengthen provenance-aware answer assembly from approved claims and Bible paragraphs

## Phase 5 — Canon Modeling

Focus: deepen what approved canon can express.

- expand relationship handling across claims
- refine author-decision support as a first-class canon layer
- improve contradiction, drift, and supersession workflows
- keep file, Postgres, and optional Wikibase behavior aligned

## Phase 6 — Product Hardening

Focus: make the tool more usable outside a dev-only loop.

- extend the shipped persisted-job model if local-first async work grows beyond the in-process worker
- auth and access control decisions for the operator surface
- deployment guidance beyond local docker compose
- stronger observability around retries, degraded modes, and partial completion

## Phase 7 — Benchmarking And Evaluation

Focus: make progress measurable.

- keep expanding benchmark scenarios beyond the current `benchmark-2003-dj` preset
- track quality, coverage, and reviewability of extracted candidates
- compare retrieval quality across projection settings
- make benchmark artifacts easy to inspect and share
