# ADR 0004: Qdrant is a projection, not a source of truth

## Status
Accepted

## Context
The project needs fast filtered retrieval, but vector stores should not become canonical data stores.

## Decision
Store approved claims canonically elsewhere and rebuild Qdrant projections as needed.

## Consequences
- easier reindexing
- cleaner disaster recovery
- fewer hidden mutations in retrieval state
