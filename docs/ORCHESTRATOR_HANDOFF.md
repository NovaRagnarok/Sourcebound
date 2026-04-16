# Orchestrator Handoff

This handoff is for an implementation orchestrator that will execute the
Sourcebound backlog using parallel subagents and a chunk-level consensus review
process.

Primary authority:

- [Implementation Backlog](IMPLEMENTATION_BACKLOG.md)

Supporting context:

- [Roadmap](ROADMAP.md)
- [Architecture](ARCHITECTURE.md)
- [Deployment Guide](DEPLOYMENT.md)

## Mission

Implement the backlog by progressing track by track and chunk by chunk, without
skipping chunk sign-off gates. The unit of approval is the chunk, not the
entire track.

The orchestrator should optimize for:

- tight chunk boundaries
- continuous forward progress
- high-confidence local verification
- consensus review before advancing
- no widening of scope across chunk boundaries

## Required Operating Model

### 1. Work only from chunks

Do not ask for sign-off on a whole track. Each review request must be for one
chunk only.

Chunk order is defined in [Implementation Backlog](IMPLEMENTATION_BACKLOG.md).
Start at:

- `Track 1, Chunk 1.0`

Do not open the next chunk in a track until the current chunk is signed off.

### 2. Keep at least 3 active subagents at any given time

Minimum concurrency rule:

- at least `3` subagents must be active while a chunk is being implemented,
  verified, or prepared for review

Default three-agent topology:

1. `Implementation Agent`
   Owns the current chunk’s primary code or doc changes.
2. `Verification Agent`
   Owns tests, smoke checks, and regression validation for the same chunk.
3. `Review Prep Agent`
   Owns chunk packet quality, checklist alignment, changed-file boundaries, and
   reviewer handoff preparation.

When the chunk is large enough, expand beyond 3:

- additional implementation agent for a disjoint file set
- UX/docs alignment agent
- risk audit or regression-focused agent

If work is too small for broad parallel code edits, keep the minimum 3 active by
splitting responsibilities rather than forcing overlapping writes.

### 3. Enforce chunk ownership

Each active subagent must have:

- a named chunk
- a bounded responsibility
- a disjoint or clearly coordinated file scope
- a concrete output expected in the current cycle

No subagent should “look around generally” without a chunk-bounded reason.

## Required Chunk Packet

Before implementation begins on a chunk, the orchestrator should prepare a
chunk packet. Every packet should include:

- `track id`
- `chunk id`
- `chunk title`
- `objective`
- `authority docs`
- `allowed files`
- `non-goals`
- `acceptance criteria`
- `verification commands`
- `reviewer notes`

Recommended file set per chunk packet:

- `docs/orchestration/<date>-track-<n>-chunk-<m>-implementation-plan.md`
- `docs/orchestration/<date>-track-<n>-chunk-<m>-execution-checklist.md`
- `docs/orchestration/<date>-track-<n>-chunk-<m>-task-brief.md`

These become the authority docs for reviewers.

## Review Protocol

Each chunk must pass a 3-reviewer consensus cycle.

### Reviewer count

- default reviewer count: `3`

### Reviewer mode

- read-only
- no file edits
- no git commands
- no delegation
- no reviewer orchestration scripts unless explicitly allowed in that review system

### Reviewer scope

Each reviewer should receive:

- the exact chunk packet docs
- the exact allowed file list
- the acceptance criteria
- the verification evidence or command outputs summary

Reviewers should not be asked to judge the whole track.

### Pass condition

Treat a chunk as signed off only when:

- local engineering verification is complete
- all blocking review findings are resolved
- the chunk passes consensus review

Recommended approval rule:

- `3/3` approve, or
- `2/3` approve with the third containing only non-blocking comments, followed
  by a short re-review after any required fixes

### Revision loop

Use this loop until the chunk is signed off:

1. implement the current chunk
2. run chunk-scoped verification
3. prepare reviewer packet
4. send to 3 reviewers
5. collect findings
6. fix only the approved chunk scope
7. rerun verification
8. resubmit the same chunk
9. mark signed off only after consensus

Do not fold the next chunk into the revision cycle.

## Reviewer Prompt Shape

Reviewers should get a strict prompt structure like this:

- identify the reviewer number
- identify track and chunk
- declare review as read-only
- forbid delegation
- forbid file edits
- forbid git commands
- list the authority docs
- list the allowed files
- define the compliance boundary

Your existing reviewer pattern is a good fit for this repo. Reuse it.

## Track Execution Order

Execute chunks in this order unless a dependency forces a documented exception:

1. `Track 1, Chunk 1.0` through `Track 1, Chunk 1.5`
2. `Track 2, Chunk 2.0` through `Track 2, Chunk 2.2`
3. `Track 3, Chunk 3.0` through `Track 3, Chunk 3.2`
4. `Track 4, Chunk 4.0` through `Track 4, Chunk 4.3`
5. `Track 5, Chunk 5.0` through `Track 5, Chunk 5.3`
6. `Track 6, Chunk 6.0` through `Track 6, Chunk 6.2`

## Initial Assignment

Begin with:

- `Track 1, Chunk 1.0: Green Baseline`

Suggested initial subagent split:

1. `Implementation Agent`
   Scope:
   `src/source_aware_worldbuilding/adapters/zotero_adapter.py`
2. `Verification Agent`
   Scope:
   `tests/test_adapters.py` and any targeted verification commands for the
   failing Zotero path
3. `Review Prep Agent`
   Scope:
   create the chunk packet, capture acceptance criteria, list allowed files,
   and prepare the 3-reviewer prompt payloads

Do not widen into `Chunk 1.1` until `Chunk 1.0` is signed off.

## Repo Safety Rules

- Never revert unrelated user changes.
- Do not mix unrelated files into a chunk commit.
- Keep each chunk branch or commit message identifiable by track and chunk.
- Prefer one chunk per commit when practical.
- If a change crosses chunk boundaries, stop and re-slice before continuing.

## Definition Of Success

The orchestrator is succeeding when:

- at least 3 subagents are active during chunk work
- chunk packets remain small and reviewable
- each chunk passes local verification before review
- each chunk reaches consensus sign-off before the next chunk starts
- the implementation history remains traceable by track and chunk
