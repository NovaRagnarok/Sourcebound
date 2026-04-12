from __future__ import annotations

import json
from pathlib import Path

import httpx

from source_aware_worldbuilding.domain.models import ApprovedClaim
from source_aware_worldbuilding.storage.json_store import JsonListStore


class WikibaseTruthStore:
    """Local cache plus optional remote sync to a Wikibase API."""

    def __init__(self, base_url: str | None, cache_dir: Path):
        self.base_url = base_url
        self.cache = JsonListStore(cache_dir / "wikibase_claims_cache.json")

    def list_claims(self) -> list[ApprovedClaim]:
        return self.cache.read_models(ApprovedClaim)

    def get_claim(self, claim_id: str) -> ApprovedClaim | None:
        return next((item for item in self.list_claims() if item.claim_id == claim_id), None)

    def save_claim(self, claim: ApprovedClaim) -> None:
        claims = {item.claim_id: item for item in self.cache.read_models(ApprovedClaim)}
        claims[claim.claim_id] = claim
        self.cache.write_models(claims.values())

        if not self.base_url:
            return

        try:
            self._sync_claim(claim)
        except Exception:
            # Keep the cache authoritative for development if the remote write fails.
            return

    def _sync_claim(self, claim: ApprovedClaim) -> None:
        entity = {
            "labels": {"en": {"language": "en", "value": claim.subject}},
            "descriptions": {
                "en": {
                    "language": "en",
                    "value": f"{claim.predicate}: {claim.value}",
                }
            },
        }
        payload = {
            "action": "wbeditentity",
            "new": "item",
            "data": json.dumps(entity),
            "format": "json",
        }
        httpx.post(self.base_url, data=payload, timeout=20.0).raise_for_status()
