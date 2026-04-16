# Roadmap

## Current Status

The repo is well beyond the initial scaffold. The current work is mostly about
hardening, reducing setup friction, and tightening the external story around
what is already implemented.

For a concrete execution order tied to the current readiness review, see
[Implementation Backlog](IMPLEMENTATION_BACKLOG.md).

### Shipped now

- writer workspace mounted at `/workspace/` with `/operator/` retained as an
  advanced alias
- ingestion, intake, review, query, export, and runtime-health APIs
- research runs with staging and extract handoff
- Postgres-backed app state
- Postgres-backed canon as the default truth-store path
- lore packet export
- persisted background jobs for research, Bible, and export work
- live integration coverage for Zotero, Qdrant, and Wikibase

### Enabled by default but still maturing

- Qdrant-backed retrieval and projection in the default local stack

### Optional or provisional

- GraphRAG as an alternate extraction path
- real Zotero-backed workflows
- research semantics on top of Qdrant
- Wikibase as an alternate truth store

### Not yet productized

- auth and multi-user access control
- production deployment beyond minimal self-host guidance
- broad benchmark coverage beyond the current narrow presets

## Current Product Boundary

Sourcebound is currently a local-first or trusted-operator tool. It is usable
today for self-hosted technical users, but it does not yet ship a user auth
layer or a polished public multi-user deployment story. The near-term roadmap
focus is to keep that boundary explicit while making the shipped stack more
dependable. The next auth step is for a self-hosted small trusted team, not a
public sign-up or multi-tenant product expansion.

## Phase 1 — Trustworthy Default Stack

Focus: make the documented Postgres-first stack feel boring, dependable, and
externally legible.

- keep the writer-first flow smooth in the Postgres-backed path
- harden runtime status and setup diagnostics
- keep docs, runtime wording, and quick-start instructions aligned
- continue closing gaps between file-backed tests and Postgres-backed behavior

## Phase 2 — Auth And Access Design

Focus: make the next security milestone decision-complete before shipping any
partial gate.

- implement against the explicit trusted-operator boundary instead of implying
  public multi-user support
- define the first protected split between writer-facing and operator-only
  surfaces
- decide session, identity, and deployment assumptions for a self-hosted small
  trusted team before implementation
- avoid landing a half-finished auth layer that misrepresents the product

## Phase 3 — Real Corpus Workflow

Focus: make Zotero-backed intake and normalization feel routine instead of
merely supported.

- improve source-document discovery and attachment handling
- make `zotero-check` and intake tooling better at surfacing config or API issues
- reduce friction between manual intake and pull-based ingest
- validate the loop on a narrow but real pilot corpus

## Phase 4 — Retrieval And Extraction Quality

Focus: improve the maturing quality layers without pretending they are already
the default gold path.

- make the Qdrant-backed retrieval path more dependable in everyday local use
- improve semantic reranking and duplicate handling for research findings
- move GraphRAG from “implemented but setup-heavy” toward “practical when chosen”
- improve evidence span quality and candidate usefulness for review

## Phase 5 — Product Hardening

Focus: make self-hosted operation safer and easier before widening the product
surface.

- strengthen deployment guidance beyond local docker compose
- improve observability around retries, degraded modes, and partial completion
- extend the shipped persisted-job model if the in-process worker stops being enough
- keep file, Postgres, and optional Wikibase behavior aligned

## Phase 6 — Benchmarking And Evaluation

Focus: make progress measurable without overstating current benchmark breadth.

- keep expanding benchmark scenarios beyond the current `benchmark-2003-dj` preset
- track quality, coverage, and reviewability of extracted candidates
- compare retrieval quality across projection settings
- make benchmark artifacts easy to inspect and share
