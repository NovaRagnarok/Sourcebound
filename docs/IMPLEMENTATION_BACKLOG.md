# Implementation Backlog

This backlog turns the current repo assessment into an execution order aimed at
making Sourcebound feel materially more usable for a technical operator.

It is intentionally biased toward the documented default stack:

- Postgres-backed app state
- Postgres-backed canon
- Qdrant-backed projection
- static workspace UI
- in-process job worker

It is not a public-product roadmap. Auth, multi-user access control, and
polished internet-facing deployment stay out of the first execution slice.

## Current Readiness Read

Sourcebound is already beyond scaffold stage and has a credible operator-grade
alpha surface, but the next big step toward usability is confidence in the
documented default path rather than net-new features.

Verified signals from the repo:

- `ruff` passes
- `mypy` passes
- runtime status reports `ready` on a configured local stack
- the app, workspace routes, jobs, Bible workspace, query, and Postgres flows
  have meaningful integration coverage
- the repo already ships a newcomer smoke script for the default stack

Verified gaps:

- full `pytest` is not green because the Zotero write-path contract currently
  conflicts with a unit test
- the newcomer path is vulnerable to environment collisions like port `8000`
  already being occupied
- the repo has good runtime status reporting, but it still asks operators to
  infer too much during first-run troubleshooting
- the default-stack smoke path is not continuously enforced in CI

## Prioritization Rubric

Each ticket is ordered using three factors:

- Impact: how much it improves day-one trust and operator success
- Effort: rough implementation size
- Leverage: how much future work gets easier or safer afterward

Effort scale:

- `S`: less than 1 day
- `M`: 1 to 3 days
- `L`: 3 to 7 days

## Release Targets

### Target A: Trustworthy Default Stack

Definition of "one big step closer to usable":

- the documented quick start works reliably on a fresh machine
- CI proves the default operator path instead of only unit and integration slices
- startup failures tell the operator exactly what is wrong and how to fix it
- the workspace setup state mirrors those same diagnostics

### Target B: Operator Beta

Definition of "push us further along":

- a technical team can run Sourcebound for real ongoing work with much lower
  support burden
- the self-host path has explicit recovery, backup, and release discipline
- the product boundary is clearer around auth, deployment shape, and supported
  operating modes
- benchmark and retrieval confidence are good enough to trust changes over time

### Target C: Bounded Multi-User Release

Definition of "push it further still":

- Sourcebound can be responsibly used by a small trusted team, not only a single
  operator
- authentication and authorization are real shipped features, not implied future work
- the deployment story includes one intentionally supported internet-facing shape
- release, upgrade, and incident handling are documented enough that usage does
  not depend on direct maintainer involvement

## Review Protocol

This backlog is sliced for review-driven implementation rather than for
single-pass execution. The unit of planning, implementation, and sign-off is a
`chunk`, not a whole release target or whole track.

### Structure

- A `track` is a coherent workstream with one outcome and one review narrative.
- A `chunk` is the smallest implementation slice that can be reviewed without
  confusing scope.
- A chunk should usually touch one concern cluster only:
  CLI, runtime status, workspace UX, retrieval quality, auth, deployment, or
  release operations.

### Consensus Gate

Every chunk must pass consensus review before the next chunk in the same track
is considered signed off.

- Default reviewer count: `3`
- Review mode: read-only
- Review sources:
  - one chunk implementation plan
  - one execution checklist
  - one chunk task brief
  - only the files claimed by that chunk
- Pass rule:
  - `3/3` reviewers approve, or
  - `2/3` approve with the third producing only non-blocking comments and the
    blocking concerns resolved in a short re-review
- Failure rule:
  - any unresolved protocol, boundary, regression, or scope violation sends the
    chunk back into revision

### Revision Loop

The review cycle is expected to repeat as needed.

- implement chunk
- run local verification
- send chunk for 3-reviewer consensus
- resolve findings
- rerun verification
- resubmit the same chunk
- mark chunk `signed off` only after consensus

Implementation is considered under a dual-review process only when:

- the chunk passes local engineering verification
- the chunk passes consensus review

### Chunk Packet

Every chunk should eventually have a review packet with:

- chunk id
- objective
- authority docs
- allowed files
- acceptance criteria
- required verification commands
- known non-goals

## Track Map

The rest of this backlog is sliced into tracks and chunks. `SB-*` ids remain as
cross-references, but sign-off happens at the chunk level.

## Track 1: Trustworthy Default Stack

Target: `Target A`

This track gets Sourcebound from promising alpha to boring, dependable
technical-operator startup.

### Chunk 1.0: Green Baseline

- Backlog ids: `SB-001`
- Effort: `S`
- Objective:
  Restore a fully green baseline for the documented repo.
- Scope:
  Fix the Zotero write-path contract/test mismatch without weakening real write
  safety.
- Acceptance criteria:
  - `pytest` passes locally from a clean env
  - GitHub Actions passes on Python 3.11 and 3.12
  - Zotero write-path behavior is explicitly tested for both credentialed and
    mocked paths
- Likely files:
  - `src/source_aware_worldbuilding/adapters/zotero_adapter.py`
  - `tests/test_adapters.py`
  - possibly `tests/test_cli_intake.py`

### Chunk 1.1: Serve Preflight Diagnostics

- Backlog ids: `SB-002`
- Effort: `M`
- Objective:
  Make `saw serve` fail clearly instead of mysteriously.
- Scope:
  Add preflight checks for ports, Postgres, Qdrant, worker expectations, and
  contradictory config.
- Acceptance criteria:
  - occupied-port detection is explicit
  - missing-service guidance is copy-pasteable
  - at least one port-collision and one missing-service test exists
- Likely files:
  - `src/source_aware_worldbuilding/cli.py`
  - `src/source_aware_worldbuilding/services/status.py`
  - `tests/test_health.py`
  - new CLI-focused tests

### Chunk 1.2: Newcomer Smoke Robustness

- Backlog ids: `SB-003`
- Effort: `M`
- Objective:
  Make the newcomer smoke path robust enough to trust locally and in CI.
- Scope:
  Remove fragile assumptions around ports, process identity, and cleanup.
- Acceptance criteria:
  - configurable or conflict-aware app port
  - process validation before URL probing
  - fast clear failure on environment collisions
- Likely files:
  - `scripts/newcomer_smoke.sh`
  - `.env.example`
  - `src/source_aware_worldbuilding/settings.py`
  - possibly `Makefile`

### Chunk 1.3: Default-Path CI Gate

- Backlog ids: `SB-004`
- Effort: `M`
- Objective:
  Continuously prove the documented quick-start path in CI.
- Scope:
  Add a smoke gate that boots Postgres and Qdrant, seeds the app, serves it,
  and validates main HTTP surfaces.
- Acceptance criteria:
  - CI runs default-stack smoke on every PR
  - failure logs cleanly separate infra from app failure
- Likely files:
  - `.github/workflows/ci.yml`
  - `scripts/newcomer_smoke.sh`
  - `docker-compose.yml`

### Chunk 1.4: Setup Guidance Unification

- Backlog ids: `SB-005`, `SB-007`
- Effort: `M`
- Objective:
  Make CLI, runtime health, and verify commands speak the same setup language.
- Scope:
  Unify troubleshooting vocabulary and add a canonical `verify-default-stack`
  style command.
- Acceptance criteria:
  - shared terminology across CLI and `/health/runtime`
  - one verification command exits `0` only for a truly usable default stack
- Likely files:
  - `src/source_aware_worldbuilding/cli.py`
  - `src/source_aware_worldbuilding/services/status.py`
  - `tests/test_health.py`

### Chunk 1.5: First-Run Workspace UX

- Backlog ids: `SB-006`
- Effort: `M`
- Objective:
  Make setup and degraded states understandable from the workspace alone.
- Scope:
  Tighten copy, dependency badges, and required-vs-optional distinction.
- Acceptance criteria:
  - first-run operators can see what is blocking them without reading docs
  - optional integrations do not look like blockers
- Likely files:
  - `frontend/operator-ui/app.js`
  - `frontend/operator-ui/styles.css`
  - `src/source_aware_worldbuilding/api/routes/workspace.py`

## Track 2: Retrieval And Corpus Quality

Target: `Target A` moving into `Target B`

### Chunk 2.0: Retrieval Guard Rails

- Backlog ids: `SB-008`
- Effort: `L`
- Objective:
  Protect the default Qdrant path from silent retrieval regressions.
- Scope:
  Add coverage for projection freshness, fallback behavior, and metadata
  correctness.
- Acceptance criteria:
  - retrieval backend and quality tier are verified
  - degraded-Qdrant fallback behavior is tested
- Likely files:
  - `src/source_aware_worldbuilding/services/query.py`
  - `src/source_aware_worldbuilding/adapters/qdrant_adapter.py`
  - `tests/test_query_service.py`
  - `tests/test_postgres_integration.py`

### Chunk 2.1: Zotero Intake Hardening

- Backlog ids: `SB-009`
- Effort: `L`
- Objective:
  Make real corpus intake feel routine rather than merely supported.
- Scope:
  Improve config diagnostics, write-path clarity, and document discovery.
- Acceptance criteria:
  - `zotero-check` gives concrete fixes
  - attachment and note failures are easier to interpret
- Likely files:
  - `src/source_aware_worldbuilding/adapters/zotero_adapter.py`
  - `src/source_aware_worldbuilding/cli.py`
  - `tests/test_live_integrations.py`

### Chunk 2.2: Benchmark Breadth

- Backlog ids: `SB-010`
- Effort: `L`
- Objective:
  Broaden evaluation so progress is measurable across more than the current
  narrow presets.
- Scope:
  Add more benchmark fixtures and inspectable summary outputs.
- Acceptance criteria:
  - at least 2 to 3 additional scenarios
  - benchmark outputs are easy to compare over time
- Likely files:
  - `src/source_aware_worldbuilding/extraction_eval.py`
  - `src/source_aware_worldbuilding/cli.py`
  - `data/evals/`
  - `docs/EXTRACTION_EVAL.md`

## Track 3: Operator Runbook And Observability

Target: `Target B`

### Chunk 3.0: Recovery Runbook

- Backlog ids: `SB-011`
- Effort: `M`
- Objective:
  Give operators a practical backup, restore, rebuild, reseed, and upgrade
  workflow.
- Scope:
  Convert deployment guidance into an actual operating runbook.
- Acceptance criteria:
  - backup and restore steps are explicit
  - Qdrant rebuild flow is documented
  - upgrade and rollback checklist exists
- Likely files:
  - `docs/DEPLOYMENT.md`
  - `docs/AUTHOR_STACK.md`
  - `README.md`
  - `src/source_aware_worldbuilding/cli.py`

### Chunk 3.1: Job And Degraded-Mode Visibility

- Backlog ids: `SB-012`
- Effort: `M`
- Objective:
  Make long-running work and degraded modes visible enough for daily operation.
- Scope:
  Improve job telemetry, failure surfacing, and degraded-mode summaries.
- Acceptance criteria:
  - partial, retry, and failure states are legible in CLI and UI
  - at least one end-to-end partial or retry scenario is covered
- Likely files:
  - `src/source_aware_worldbuilding/services/jobs.py`
  - `src/source_aware_worldbuilding/api/routes/jobs.py`
  - `src/source_aware_worldbuilding/services/status.py`
  - `frontend/operator-ui/app.js`
  - `tests/test_job_service.py`
  - `tests/test_api_integration.py`

### Chunk 3.2: Retrieval Regression Gate

- Backlog ids: `SB-013`
- Effort: `M`
- Objective:
  Promote retrieval quality to a repeatable gate rather than an ad hoc check.
- Scope:
  Make benchmark artifacts diffable and integrate at least one retrieval gate
  into CI or a release checklist.
- Acceptance criteria:
  - stable benchmark summary output
  - retrieval regression visibility in automation
- Likely files:
  - `src/source_aware_worldbuilding/services/query.py`
  - `src/source_aware_worldbuilding/extraction_eval.py`
  - `.github/workflows/ci.yml`
  - `docs/EXTRACTION_EVAL.md`

## Track 4: Boundary, Deployment, And Support Policy

Target: `Target B`

### Chunk 4.0: Auth Boundary ADR

- Backlog ids: `SB-014`
- Effort: `M`
- Objective:
  Decide the product boundary before implementing auth.
- Scope:
  Land an ADR-level decision for identity, roles, and protected surfaces.
- Acceptance criteria:
  - a new ADR captures the boundary
  - docs stop implying unsupported modes
- Likely files:
  - `docs/adrs/`
  - `docs/ROADMAP.md`
  - `docs/DEPLOYMENT.md`
  - `README.md`

### Chunk 4.1: Supported Single-Host Deployment

- Backlog ids: `SB-015`
- Effort: `L`
- Objective:
  Define one tested self-host deployment shape.
- Scope:
  Validate a concrete app + worker + Postgres + Qdrant + reverse proxy path.
- Acceptance criteria:
  - one deployment guide is real and tested
  - health and verification tooling match the guide
- Likely files:
  - `docker-compose.yml`
  - `docs/DEPLOYMENT.md`
  - `README.md`
  - `infra/`
  - `.github/workflows/ci.yml`

### Chunk 4.2: Optional Integration Productization

- Backlog ids: `SB-016`
- Effort: `L`
- Objective:
  Move the single most valuable optional integration from “supported” to
  “routine.”
- Scope:
  Choose one:
  Zotero, research semantics, or GraphRAG.
- Acceptance criteria:
  - setup is documented and tested
  - operator diagnostics are explicit
  - a smoke or benchmark path exists

### Chunk 4.3: Support Matrix And Version Policy

- Backlog ids: `SB-017`
- Effort: `S`
- Objective:
  Make supported versions and runtime modes explicit.
- Scope:
  Document supported Python versions, backend combinations, and support levels.
- Acceptance criteria:
  - supported vs experimental vs provisional modes are explicit
- Likely files:
  - `README.md`
  - `docs/DEPLOYMENT.md`
  - `docs/AUTHOR_STACK.md`

## Track 5: Auth, Authorization, And Safe Team Usage

Target: `Target C`

### Chunk 5.0: Minimal Auth Implementation

- Backlog ids: `SB-018`
- Effort: `L`
- Objective:
  Implement the smallest real auth layer that matches the chosen boundary.
- Scope:
  Add authentication to protect non-public surfaces.
- Acceptance criteria:
  - anonymous write access is gone
  - unauthenticated and unauthorized responses are explicit
- Likely files:
  - `src/source_aware_worldbuilding/api/`
  - `src/source_aware_worldbuilding/settings.py`
  - `src/source_aware_worldbuilding/domain/`
  - `tests/test_api_integration.py`

### Chunk 5.1: High-Risk Authorization Boundaries

- Backlog ids: `SB-019`
- Effort: `M`
- Objective:
  Protect mutation-heavy and operator-only actions with explicit role rules.
- Scope:
  Lock down review, canon mutation, intake, export, and runtime operations.
- Acceptance criteria:
  - high-risk routes require the intended role
  - authorization failures are reflected in API and UI behavior
- Likely files:
  - `src/source_aware_worldbuilding/api/routes/`
  - `src/source_aware_worldbuilding/api/dependencies.py`
  - `frontend/operator-ui/app.js`
  - `tests/test_api_integration.py`

### Chunk 5.2: Migration And Upgrade Safety

- Backlog ids: `SB-021`
- Effort: `M`
- Objective:
  Make schema and projection evolution safer across supported versions.
- Scope:
  Add upgrade workflow, migration or compatibility discipline, and projection
  rebuild verification.
- Acceptance criteria:
  - upgrade steps are documented
  - at least one upgrade-path verification exists
- Likely files:
  - `src/source_aware_worldbuilding/storage/`
  - `src/source_aware_worldbuilding/adapters/postgres_backed.py`
  - `src/source_aware_worldbuilding/cli.py`
  - `docs/DEPLOYMENT.md`

### Chunk 5.3: Hardened Internet-Facing Deployment

- Backlog ids: `SB-020`
- Effort: `L`
- Objective:
  Validate one supported internet-facing deployment shape for a small trusted
  team.
- Scope:
  Add front-door, secret-management, persistence, and auth-aligned deployment
  guidance.
- Acceptance criteria:
  - deployment guide is explicit about TLS, auth, and secret handling
  - the supported deployment is exercised end to end at least once
- Likely files:
  - `docs/DEPLOYMENT.md`
  - `infra/`
  - `docker-compose.yml`
  - `.github/workflows/`

## Track 6: Team Workflow And Release Operations

Target: `Target C`

### Chunk 6.0: Canon Auditability

- Backlog ids: `SB-022`
- Effort: `M`
- Objective:
  Add actor-aware auditability for canon and review actions.
- Scope:
  Record who approved, rejected, exported, edited, or regenerated important
  trust-boundary data.
- Acceptance criteria:
  - actor identity is attached where auth is enabled
  - audit history is queryable for core events
- Likely files:
  - `src/source_aware_worldbuilding/domain/models.py`
  - `src/source_aware_worldbuilding/services/review.py`
  - `src/source_aware_worldbuilding/services/bible.py`
  - `src/source_aware_worldbuilding/adapters/postgres_backed.py`

### Chunk 6.1: Durable Collaboration Workflow

- Backlog ids: `SB-023`
- Effort: `L`
- Objective:
  Support one clean team workflow end to end.
- Scope:
  Choose one collaboration loop and harden docs, permissions, UI, and tests
  around it.
- Acceptance criteria:
  - workflow is documented step by step
  - permissions align with the workflow
  - one end-to-end scenario exists
- Likely files:
  - `frontend/operator-ui/`
  - `src/source_aware_worldbuilding/api/routes/`
  - `tests/test_api_integration.py`
  - workflow docs

### Chunk 6.2: Release Operations Basics

- Backlog ids: `SB-024`
- Effort: `M`
- Objective:
  Add minimal release and rollback discipline for ongoing product use.
- Scope:
  Create release checklist, rollback guidance, and issue triage standards.
- Acceptance criteria:
  - release checklist exists
  - rollback guidance exists
  - support matrix stays version-aligned
- Likely files:
  - `docs/`
  - `.github/`
  - release process docs

## Deferred Until Boundary Decision

These can move into active implementation only after the auth and deployment
boundary is chosen explicitly:

- built-in auth implementation
- multi-user access control implementation
- public internet exposure guidance
- service decomposition beyond the current app-plus-worker shape

## Still Intentionally Out Of Scope

Even with this extended backlog, these are beyond the bounded-team release path
and should not be implied as near-term commitments:

- large-scale multi-tenant SaaS operation
- autoscaled worker fleet or service mesh decomposition
- enterprise compliance programs
- full public sign-up and self-serve tenant provisioning
- exhaustive productization of every optional integration

## Suggested Review Order

The cleanest way to run this plan is to send chunks for consensus review in
this order:

1. Track 1, Chunk 1.0 through Chunk 1.5
2. Track 2, Chunk 2.0 through Chunk 2.2
3. Track 3, Chunk 3.0 through Chunk 3.2
4. Track 4, Chunk 4.0 through Chunk 4.3
5. Track 5, Chunk 5.0 through Chunk 5.3
6. Track 6, Chunk 6.0 through Chunk 6.2

Within a track:

1. implement one chunk only
2. verify locally
3. send that chunk alone for 3-reviewer consensus
4. resolve findings
5. resubmit the same chunk until signed off
6. only then open the next chunk

This keeps the review payload small enough for consensus review without mixing
multiple concerns into one approval cycle.

## Success Metrics

The first slice should be considered successful when:

- a fresh technical user can follow the README quick start without guessing
- a broken setup produces specific recovery instructions in both terminal and UI
- the default-stack smoke path is green in CI
- the repo is back to a clean baseline on lint, types, and tests

Target B should be considered successful when:

- Sourcebound has a trustworthy operator runbook instead of only setup docs
- recovery, rebuild, and verification flows are documented and tested
- retrieval regressions are visible before they reach users
- the auth and deployment boundary is explicit enough to guide the next product step
- one supported self-host shape is concrete rather than implied

Target C should be considered successful when:

- a small trusted team can use Sourcebound without anonymous write access
- high-risk actions are protected by explicit authorization rules
- canon-changing activity is attributable to authenticated actors
- one supported internet-facing deployment shape is documented and validated
- upgrade, rollback, and incident handling are disciplined enough for routine use
