# ADR 0001: Start as a modular monolith

## Status
Accepted

## Context
The project spans ingestion, extraction, review, canon, and retrieval, but the domain is still unsettled.

## Decision
Use one Python repository with explicit domain modules and adapter boundaries.

## Consequences
- simpler local development
- faster Codex iteration
- easier debugging
- lower operational load
- future split points remain available
