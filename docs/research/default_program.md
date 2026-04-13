# Generic Research Program

This default program is meant to guide bounded autonomous research runs for any broad subject and era.

## Goals

- Build broad factual coverage, not exhaustive scraping.
- Prefer source-backed specificity over generic summaries.
- Surface coverage gaps instead of inventing certainty.
- Keep provenance attached to every staged finding.
- Respect robots and explicit domain policy before fetching.
- Stay within bounded time and host budgets.

## Coverage Strategy

- Expand each brief into reusable facets such as people, places, institutions, practices, events, objects/technology, language/slang, economics/commercial context, media/culture, and regional context.
- Capture enough accepted findings to give each facet at least one strong lead when possible.
- Stop once the run reaches configured limits or facet targets.
- Use `web_open` for discovery on the open web.
- Use `curated_inputs` when the operator wants a no-search run that works only from supplied text or URLs.

## Source Preferences

- Prefer archives, educational sources, government/public institutions, established reporting, and domain magazines.
- Accept broader web sources when they add unique, era-specific detail.
- De-prioritize social posts, forums, and shopping pages unless they are the only plausible lead for a niche facet.
- Source-class exclusions are hard filters.
- Source-class preferences affect ranking only.

## Evidence Rules

- Stage short excerpts with clear provenance instead of full crawls by default.
- Preserve title, URL, publisher, publication date when available, query used, and the facet that motivated the find.
- Reject duplicate or near-duplicate findings once the same angle is already represented.
- Canonicalize URLs before dedupe and per-host accounting.
- Normalize titles before duplicate checks.
- In `curated_inputs` mode, text inputs are already-fetched evidence and URL inputs may still fetch if policy and adapter capabilities allow it.

## Quality Rules

- Favor findings that mention concrete dates, locations, participants, technology, costs, habits, or institutions.
- Favor findings that are close to the requested era rather than retrospective summaries.
- Prefer findings that improve coverage of an underrepresented facet.
- Default execution policy:
  - total fetch time budget: 90 seconds
  - per-host fetch cap: 3
  - retry attempts: 3 with bounded exponential backoff
  - robots respected by default
- Policy precedence:
  - domain deny / allow policy
  - robots policy
  - source-class exclusion
  - duplicate rejection
  - scoring adjustments
  - quality-threshold acceptance
- Explicit deny domains apply before robots checks.
- Allow domains narrow eligible hosts but do not override robots.

## Trust Boundary

- Research findings are not canon.
- Staged findings are only raw material for normalization and candidate extraction.
- Human review remains mandatory before any extracted claim becomes approved canon.

## Operational Notes

- Runs may finish as `completed_partial` when limits are hit after useful work was done.
- Runs may finish as `degraded_fallback` when the scout had to continue with a recorded fallback, such as unavailable robots metadata.
- Telemetry should be treated as the primary debugging surface for retries, blocked hosts, fetch failures, and dedupe activity.
