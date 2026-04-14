# ADR 0002: Wikibase as a future approved-claim adapter

## Status
Superseded for the current MVP

## Context
Approved claims may eventually need qualifiers, references, and support for multiple competing statements, but MVP development is moving faster with Postgres as the default truth store and files as the no-infra fallback.

## Decision
Do not require Wikibase in the default development loop. Keep Wikibase as an optional truth-store adapter that can be reintroduced later if product needs justify the added complexity.

## Consequences
- domain models remain independent of Wikibase JSON
- the truth-store port stays intact
- the default approved-claim path stays local and easy to run
- any future Wikibase integration must prove it is worth the operational cost
