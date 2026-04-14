# Author Stack

Sourcebound’s recommended solo-author setup is intentionally small:

- Postgres-backed app state
- Postgres-backed canon
- background job worker enabled
- optional Qdrant enabled for better retrieval relevance

## Recommended Local Configuration

```bash
APP_STATE_BACKEND=postgres
APP_TRUTH_BACKEND=postgres
APP_POSTGRES_DSN=postgresql://saw:saw@localhost:5432/saw
APP_POSTGRES_SCHEMA=sourcebound
APP_JOB_WORKER_ENABLED=true
QDRANT_ENABLED=true
QDRANT_URL=http://localhost:6333
GRAPH_RAG_ENABLED=false
```

This keeps the daily writing loop dependable:

1. research runs happen in persisted background jobs
2. stage/extract work can be cancelled or retried
3. Bible composition and regeneration do not require waiting on one request
4. export bundles come back through persisted job results
5. Qdrant improves ranking, but approved canon remains the trust boundary

## Reliability Notes

- If Qdrant is unavailable, Sourcebound falls back to in-memory ranking and surfaces the downgrade in query/Bible diagnostics.
- Manual Bible edits remain the author’s writable layer and are preserved during regeneration.
- Provenance explains generated paragraphs only; it does not attempt to justify arbitrary manual rewrites.
- Keep regular backups of the Postgres schema or completed Bible export bundles if you are using the tool for active manuscript work.

## Minimal Deployment Guidance

If you run Sourcebound outside local development:

- use a persistent database-backed state backend
- keep the background worker enabled in the same deployment or as a paired worker process
- treat Qdrant as recommended infrastructure, not required truth infrastructure
- verify `/health/runtime` before expecting long-running research or export jobs to complete
