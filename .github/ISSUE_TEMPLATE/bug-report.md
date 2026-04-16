---
name: Release or support issue
about: Report a trusted-operator release blocker, regression, or support issue
title: "[Release Ops]: "
---

## Support Matrix Check

- Python version:
- Backend combination:
  `APP_STATE_BACKEND=` / `APP_TRUTH_BACKEND=`
- Deployment shape:
  local developer mode / supported single-host Compose path / supported
  internet-facing Compose overlay / other
- In-process worker enabled:
- Qdrant enabled:
- Optional integrations involved:

## Release Context

- Current commit SHA, tag, or release artifact:
- Last known good commit SHA, tag, or release artifact:
- Does this block the planned release?
- Does this require rollback or make rollback likely?

## Observed Behavior

- What happened?
- What did you expect instead?
- Which operator-facing workflow is affected?

## Verification Evidence

- `.venv/bin/saw status`:
- `/health/runtime`:
- `make check` or targeted verification:
- Any relevant logs or screenshots:

## Triage Notes

- Is this inside the supported, experimental, or provisional boundary?
- What is the next action: fix forward, roll back, or classify as unsupported?
