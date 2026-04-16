# Release Operations

This runbook is the minimal release, rollback, and issue-triage guide for the
current Sourcebound product boundary. It is intentionally scoped to the
trusted-operator deployment shapes documented in
[Deployment Guide](DEPLOYMENT.md).

## Support Boundary For Release Decisions

Treat the support matrix in [Deployment Guide](DEPLOYMENT.md) as the source of
truth when deciding whether a change is routine-ready.

Routine release decisions should stay inside the currently supported boundary:

- Python `3.11` and `3.12`
- `APP_STATE_BACKEND=postgres` with `APP_TRUTH_BACKEND=postgres`
- local developer mode, the supported single-host Compose path, or the
  supported internet-facing Compose overlay
- the trusted-operator runtime with the in-process worker enabled

Experimental and provisional modes can still be tested intentionally, but they
should not redefine the default release gate for the supported stack.

## Release Checklist

Use this checklist for a routine trusted-operator release:

1. Confirm the target environment stays inside the supported matrix in
   [Deployment Guide](DEPLOYMENT.md).
2. Capture the release identifier you are about to deploy:
   commit SHA, tag, or release artifact.
3. Take a Postgres backup before changing code or dependencies.
4. If operators need a user-facing snapshot before rollout, export any
   time-sensitive project artifacts you want to preserve separately.
5. Update the checked-out code or release artifact.
6. Rerun `make bootstrap` if dependency pins changed.
7. Run `.venv/bin/saw upgrade-check --json-output` and confirm the Postgres
   schema is compatible with the target code.
8. If the default retrieval path is enabled, run
   `.venv/bin/saw qdrant-rebuild --json-output` so the projection catches up to
   the current canon state.
9. Run `make check`.
10. Restart the app stack and confirm readiness with both:
    `.venv/bin/saw status` and `/health/runtime`.
11. Verify the intended operator entrypoint still works for the chosen
    deployment shape:
    `/workspace/`, `/operator/`, and one protected mutation using the expected
    shared writer or operator token.
12. Record the deployed release identifier and any operator notes needed for
    rollback.

## Rollback Guide

Use rollback when a release breaks the supported runtime path or introduces
behavior the trusted team cannot safely operate around.

1. Stop the app process or Compose stack that was just updated.
2. Restore the previous code checkout or release artifact.
3. Restore the Postgres backup taken before the rollout if schema or data
   changed.
4. Run `.venv/bin/saw upgrade-check --json-output` against the restored code
   and restored Postgres schema.
5. If the retrieval projection is stale or inconsistent, run
   `.venv/bin/saw qdrant-rebuild --json-output`.
6. Restart the restored stack and confirm readiness with
   `.venv/bin/saw status` and `/health/runtime`.
7. Recheck the main operator entrypoints:
   `/workspace/`, `/operator/`, and the protected mutation route that matters
   for the affected workflow.
8. Record the rollback point, what failed, and whether follow-up triage or a
   patched re-release is required.

For the lower-level backup, restore, reseed, and deployment details behind
these steps, use the matching sections in [Deployment Guide](DEPLOYMENT.md).

## Issue Triage Standard

Use this standard for release blockers, regressions, and operator support
issues:

1. Classify the report against the current support matrix:
   supported, experimental, or provisional.
2. Record the exact deployment shape:
   local developer mode, supported single-host Compose path, or supported
   internet-facing Compose overlay.
3. Record the exact runtime boundary:
   Python version, backend combination, whether the in-process worker was
   enabled, and whether Qdrant was enabled.
4. Record the release identifier:
   current commit SHA/tag and the last known good version if one exists.
5. Decide impact:
   release blocker, rollback candidate, degraded-but-usable issue, or
   unsupported configuration drift.
6. Collect proof:
   `make check` output if relevant, `.venv/bin/saw status`, `/health/runtime`,
   and any operator-facing failure symptom.
7. Choose the next action:
   fix forward, roll back, or document as unsupported/provisional rather than
   misclassifying it as a supported regression.

The paired GitHub issue template in `.github/ISSUE_TEMPLATE/bug-report.md`
exists to capture this minimum triage payload consistently.
