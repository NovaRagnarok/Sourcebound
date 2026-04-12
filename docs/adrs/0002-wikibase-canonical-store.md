# ADR 0002: Wikibase is the canonical approved-claim store

## Status
Accepted

## Context
Approved claims need qualifiers, references, and support for multiple competing statements.

## Decision
Persist approved claims to Wikibase through a truth-store adapter.

## Consequences
- domain models remain independent of Wikibase JSON
- the truth-store adapter handles translation
- the rest of the system reasons about claims, evidence, and statuses instead of raw Wikibase structures
