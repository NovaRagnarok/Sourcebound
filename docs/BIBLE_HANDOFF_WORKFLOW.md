# Bible Handoff Workflow

This is the one supported collaboration loop for `Track 6 / Chunk 6.1`.

Sourcebound is still a trusted writer/operator tool, not a broad multi-user
 workflow platform. The clean team handoff today is the Bible workflow:

1. the writer sets the project frame
2. the writer composes and edits a Bible section
3. the operator regenerates or exports when the section is ready for handoff
4. the writer keeps drafting with the preserved manual text and visible
   provenance

## Role Boundary

- Writer token:
  - save the Bible project profile
  - compose a section
  - edit the manual section text
- Operator token:
  - regenerate a section
  - queue a Bible export

The operator actions are intentionally narrower and higher-risk. They refresh
the generated draft or produce an export bundle, while preserving the writer's
manual text.

## Step-By-Step Loop

### 1. Writer sets the shared frame

- Open `/workspace/`
- Save the Bible profile with project name, era, geography, and narrative
  focus
- Confirm the workspace now points at one active project

Why this matters:
- both writer and operator need the same project frame before the section
  workflow is trustworthy

### 2. Writer composes a section

- Use approved canon to compose the first Bible section
- Treat the generated draft as the canon-backed baseline, not final prose
- Inspect provenance and coverage before editing

Why this matters:
- the generated section gives the team a shared canon-backed starting point

### 3. Writer shapes the manual text

- Edit the working text directly in the section editor
- Save manual edits once the section reflects the writer's intended shape
- Keep provenance visible so the team can still explain where the generated
  baseline came from

Expected state:
- manual text is active
- the generated draft remains available
- the section is ready for operator handoff

### 4. Operator refreshes or exports

- Open `/operator/` for the operator step
- Use an operator token for `Regenerate section` when the canon-backed draft
  needs refreshing
- Use an operator token for `Queue export` when the project is ready for a
  shareable bundle

Expected behavior:
- regeneration refreshes the generated draft
- manual text stays intact
- export uses the current project and section state

### 5. Writer continues from the preserved text

- Re-open the section after regeneration or export
- Compare the preserved manual text against the refreshed generated draft
- Continue drafting without losing the writer's version of the section

## Verification Scenario

The end-to-end proof for this loop should show:

1. writer saves profile
2. writer composes section
3. writer saves manual edits
4. writer is blocked from regenerate/export
5. workspace summary points to operator handoff
6. operator regenerates
7. operator queues export
8. manual text remains intact throughout
