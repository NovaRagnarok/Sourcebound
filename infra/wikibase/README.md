# Wikibase notes

This seed does **not** bundle a local Wikibase install, and the app no longer depends on one for the default workflow.

Reason:

- the early project needs to move quickly on domain and review logic
- local Wikibase setup is heavier than the rest of the MVP
- the truth store is already abstracted behind `TruthStorePort`

Recommended approach:

1. use the file-backed truth store first
2. keep the domain model stable
3. only bring in a real Wikibase instance if approved-claim needs outgrow the local truth store
4. implement `WikibaseTruthStore` as an adapter

When you do integrate a real Wikibase instance, only the adapter should know about the full Wikibase JSON format.
