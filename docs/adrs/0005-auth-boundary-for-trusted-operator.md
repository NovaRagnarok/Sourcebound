# ADR 0005: Trusted-operator auth boundary

## Status
Accepted

## Context
Sourcebound is already documented as a local-first or trusted-operator tool,
but the repo does not yet have an accepted decision that fixes the near-term
auth boundary before implementation begins. Without that boundary, future auth
work could drift into unsupported public multi-user assumptions, over-promise
internet-facing deployment support, or protect the wrong surfaces first.

## Decision
- Sourcebound remains a self-hosted, local-first product for a single trusted
  deployment operated by one operator or a small trusted team. It is not a
  public multi-tenant or self-serve sign-up product.
- The next auth milestone is authenticated access for that trusted deployment,
  not public internet account management, tenant isolation, or broad role
  hierarchy design.
- The near-term role boundary is:
  - `writer`: writer-facing workspace and read-mostly authoring flows
  - `operator`: setup, recovery, runtime controls, advanced utilities, and
    mutation-heavy administrative actions
- When auth lands, the first protected surfaces are:
  - `/operator/` and comparable advanced operator utilities
  - setup, recovery, and runtime-management actions
  - mutation-heavy API routes such as intake, review, export, canon mutation,
    and job-control operations
- This ADR does not choose an auth provider, session mechanism, or final RBAC
  model. Those decisions stay downstream of this product-boundary commitment.

## Consequences
- Track 5 can implement the smallest real auth layer without implying public
  multi-user product support.
- Docs can be explicit that anonymous write access and operator surfaces are
  not part of the intended long-term boundary.
- Public sign-up, tenant provisioning, internet-facing hardening, and richer
  collaboration roles remain deferred roadmap work.
