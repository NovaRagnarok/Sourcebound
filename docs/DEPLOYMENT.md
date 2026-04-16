# Deployment Guide

Sourcebound does not yet ship a polished production deployment story. This page
documents the smallest supported self-host shape for technical operators who
want a trustworthy runtime boundary today.

## Status Taxonomy

### Shipped now

- Postgres-backed app state and canon
- browser UI served by the app process
- persisted background jobs with the in-process worker

### Enabled by default but still maturing

- Qdrant-backed retrieval and projection in the recommended runtime stack

### Optional or provisional

- live Zotero workflows
- GraphRAG-backed extraction
- research semantics
- Wikibase truth-store path

### Not yet productized

- user auth
- multi-user access control
- public sign-up or tenant provisioning
- polished production deployment beyond minimal self-host guidance

## Current Product Boundary

Sourcebound should currently be treated as a local-first or trusted-operator
application:

- there is no shipped user auth or access-control layer
- the recommended deployment shape assumes a small trusted team or single
  operator in one self-hosted deployment
- the intended next auth boundary is writer-facing workspace use versus
  operator-only setup, recovery, runtime, and other mutation-heavy actions
- no public sign-up, tenant provisioning, or public multi-tenant hosting is
  supported
- Qdrant is recommended for the default retrieval path, but it is still a
  rebuildable projection rather than the source of truth
- Postgres-backed app state and canon are the stable core runtime path

## Recommended Runtime Shape

Smallest supported self-host deployment:

- one app process serving the FastAPI API and static UI
- the in-process job worker enabled in that same process
- persistent Postgres for app state and canon
- Qdrant for the default retrieval path

This is intentionally not a multi-service production blueprint. It is the
minimum self-host shape that matches the current docs and runtime assumptions.

## Baseline Environment

Recommended baseline:

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

Optional integrations remain opt-in:

- Zotero for live library pull and write-back
- research semantics when you want Qdrant-backed dedupe and reranking
- GraphRAG when you are ready to manage its dependency and artifact setup
- Wikibase only if you intentionally want it as the truth-store backend

## Startup Checklist

Before expecting the app to be healthy:

1. bring up Postgres
2. bring up Qdrant
3. run `.venv/bin/saw status`
4. expect `.venv/bin/saw status` to report a `degraded` runtime with
   `qdrant:uninitialized` before seeding
5. run `.venv/bin/saw seed-dev-data`
6. verify `/health/runtime` reports `ready`

`APP_STRICT_STARTUP_CHECKS=true` is recommended when you want boot to fail
instead of silently accepting an uninitialized default retrieval path.

## Health And Verification

Use these checks as the operator truth source:

- `.venv/bin/saw status`
- `.venv/bin/saw status --json-output`
- `GET /health`
- `GET /health/runtime`

Healthy default-stack signals:

- Postgres app state is ready
- Postgres truth store is ready
- job worker is ready
- projection is ready, or clearly marked as needing initialization
- optional integrations appear as optional or disabled, not as blockers

## Backups And Recovery

Use this order when you need to recover the trusted-operator stack:

1. back up Postgres and any export bundles you want to keep
2. restore Postgres first
3. rebuild Qdrant if the default projection is stale or uninitialized
4. reseed only when you intentionally want the sample corpus back
5. verify readiness with `.venv/bin/saw status` and `GET /health/runtime`

### Back Up

Back up the parts that carry authority:

- the Postgres schema used for app state and canon
- exported Bible bundles if you are using the tool for active writing work

A practical local backup uses a custom-format Postgres dump:

```bash
mkdir -p backups
pg_dump --format=custom --file backups/sourcebound-$(date +%F).dump "$APP_POSTGRES_DSN"
```

### Restore

Restore the Postgres backup before you try to repair retrieval or reseed data.
Use this order:

1. stop the Sourcebound app process so no new writes land during recovery
2. restore the Postgres backup
3. rerun `.venv/bin/saw status` to confirm the app-state and canon paths are
   back in shape
4. rebuild Qdrant only after Postgres is healthy again

If you used a custom-format dump, restore it with `pg_restore`:

```bash
pg_restore --clean --if-exists --dbname "$APP_POSTGRES_DSN" backups/sourcebound-YYYY-MM-DD.dump
```

If you have a plain SQL dump instead, restore it with `psql`:

```bash
psql "$APP_POSTGRES_DSN" < backups/sourcebound-YYYY-MM-DD.sql
```

After restore, run `.venv/bin/saw status` to confirm the app-state and canon
paths are back in shape before you touch Qdrant or reseed.

### Qdrant Rebuild

Qdrant does not need to be treated as the authoritative data source. It is a
rebuildable projection that can be restored from canon plus evidence.

If the projection exists but is stale or uninitialized:

1. bring Qdrant back up if needed with `docker compose up -d qdrant`
2. run `.venv/bin/saw qdrant-rebuild`
3. rerun `.venv/bin/saw status` and confirm the projection is ready

If you want to repopulate the seeded sample corpus after a local reset, run
`.venv/bin/saw seed-dev-data` after Postgres and Qdrant are available.

### Reseed

Use reseed when you intentionally want the demo corpus back on a fresh local
stack or after a local wipe:

```bash
docker compose up -d postgres qdrant
.venv/bin/saw seed-dev-data
```

`seed-dev-data` repopulates the sample data and also initializes or refreshes
the Qdrant projection when the default retrieval path is enabled.

### Upgrade

For a routine local upgrade:

1. take a Postgres backup first
2. update the checked-out code and any dependency pins
3. rerun `make bootstrap` if the dependency set changed
4. run `make check`
5. restart the app and verify with `.venv/bin/saw status`

### Rollback

If an upgrade needs to be rolled back:

1. stop the app process
2. restore the previous code checkout or release artifact
3. restore the Postgres backup taken before the upgrade if schema or data
   changed
4. rerun `.venv/bin/saw qdrant-rebuild` if the projection needs to be brought
   back into sync
5. confirm readiness with `.venv/bin/saw status`

## Unsupported Or Not Yet Productized

The following should not be implied by the current deployment guidance:

- built-in user auth
- built-in multi-user access control
- public sign-up or tenant provisioning
- hardened public-internet exposure guidance
- autoscaled worker architecture
- broad observability and SLO guidance
- a production-grade benchmark suite covering all major workflows

Those are roadmap items, not shipped operational guarantees.
