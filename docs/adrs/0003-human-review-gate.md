# ADR 0003: Human review is the trust boundary

## Status
Accepted

## Context
Extraction systems are useful but not trustworthy enough to write directly into canon.

## Decision
All extracted claims enter the system as candidate claims. Only explicit review decisions can create approved claims.

## Consequences
- better provenance hygiene
- less silent hallucination
- slower ingestion, but much higher trust
