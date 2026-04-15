# Operator Stack

This is the canonical runtime guide for external technical users evaluating
Sourcebound today.

## Status Taxonomy

### Shipped now

- Postgres-backed app state
- Postgres-backed canon
- browser UI at `/workspace/` and `/operator/`
- persisted background jobs with the in-process worker

### Enabled by default but still maturing

- Qdrant-backed retrieval and projection in the recommended local stack

### Optional or provisional

- live Zotero workflows
- GraphRAG-backed extraction
- research semantics
- Wikibase truth-store path

### Not yet productized

- user auth
- multi-user access control
- polished production deployment beyond minimal self-host guidance

## Recommended Local Or Trusted-Operator Configuration

```bash
APP_STATE_BACKEND=postgres
APP_TRUTH_BACKEND=postgres
APP_POSTGRES_DSN=postgresql://saw:saw@localhost:5432/saw
APP_POSTGRES_SCHEMA=sourcebound
APP_UI_ENABLED=true
APP_JOB_WORKER_ENABLED=true
APP_STRICT_STARTUP_CHECKS=true
QDRANT_ENABLED=true
QDRANT_URL=http://localhost:6333
RESEARCH_SEMANTIC_ENABLED=false
GRAPH_RAG_ENABLED=false
```

This is the recommended runtime because it matches the current shipped surface
without forcing external integrations on first run.

## Why This Stack

This keeps the daily writing and operator loop dependable:

1. research runs happen in persisted background jobs
2. stage and extract work can be cancelled or retried
3. Bible composition, regeneration, and export do not require waiting on one
   request
4. export bundles come back through persisted job results
5. Postgres holds the authoritative workflow state and canon
6. Qdrant improves ranking, but approved canon remains the trust boundary

## Degraded Modes To Expect

- If Qdrant is unavailable, Sourcebound falls back to in-memory ranking and
  surfaces the downgrade in query and Bible diagnostics.
- If Qdrant is enabled but uninitialized, strict startup checks can block boot
  until you run `saw seed-dev-data` or `saw qdrant-rebuild`.
- If the job worker is disabled, long-running routes can queue work but will
  not make progress automatically.
- If Zotero is unconfigured, intake still works through manual and seeded
  flows, but live corpus pull/write paths are unavailable.
- If GraphRAG is disabled, heuristic extraction remains the default and keeps
  startup dependency-light.

## Smallest Supported Deployment Shape

If you run Sourcebound outside local development, the smallest supported shape
is:

- one app process
- worker enabled in that same process
- persistent Postgres
- optional but recommended Qdrant

This is not yet a public multi-user deployment product. Treat it as a
self-hosted trusted-operator stack.

## Operational Checks

Before relying on long-running jobs or retrieval:

- run `.venv/bin/saw status`
- verify `GET /health/runtime`
- initialize or rebuild Qdrant when the default retrieval path is not ready
- keep backups of the Postgres schema and important export bundles

For the minimal self-host checklist, environment variables, and unsupported
areas, see [Deployment Guide](DEPLOYMENT.md).
