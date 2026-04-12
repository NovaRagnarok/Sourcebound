from __future__ import annotations

from uuid import uuid4

import pytest

from source_aware_worldbuilding.adapters.qdrant_adapter import QdrantProjectionAdapter
from source_aware_worldbuilding.adapters.wikibase_adapter import WikibaseTruthStore
from source_aware_worldbuilding.adapters.zotero_adapter import ZoteroCorpusAdapter
from source_aware_worldbuilding.domain.enums import ClaimKind, ClaimStatus
from source_aware_worldbuilding.domain.models import ApprovedClaim, EvidenceSnippet
from source_aware_worldbuilding.settings import settings


@pytest.mark.live_zotero
def test_live_zotero_pull_reads_real_library(require_live_zotero) -> None:
    _ = require_live_zotero
    adapter = ZoteroCorpusAdapter()

    sources = adapter.pull_sources()

    assert sources
    assert all(source.source_id.startswith("zotero-") for source in sources[:3])

    text_units = adapter.pull_text_units(sources[:1])

    assert text_units
    assert all(unit.source_id == sources[0].source_id for unit in text_units)
    assert any(unit.text.strip() for unit in text_units)
    child_units = [unit for unit in text_units if unit.notes]
    assert all("zotero_child_" in unit.notes for unit in child_units)


@pytest.mark.live_wikibase
def test_live_wikibase_save_and_round_trip_claim(require_live_wikibase) -> None:
    cache_dir = require_live_wikibase
    adapter = WikibaseTruthStore(
        base_url=settings.wikibase_base_url,
        api_url=settings.wikibase_api_url,
        username=settings.wikibase_username,
        password=settings.wikibase_password,
        property_map_raw=settings.wikibase_property_map,
        cache_dir=cache_dir,
    )
    token = uuid4().hex[:12]
    claim = ApprovedClaim(
        claim_id=f"live-claim-{token}",
        subject=f"Sourcebound live test {token}",
        predicate="validated_in",
        value="wikibase",
        claim_kind=ClaimKind.OBJECT,
        status=ClaimStatus.PROBABLE,
        place="Test Realm",
        notes="Created by Sourcebound live integration test.",
        evidence_ids=[f"live-evidence-{token}"],
    )
    evidence = [
        EvidenceSnippet(
            evidence_id=f"live-evidence-{token}",
            source_id="live-source",
            locator="integration-test",
            text="Sourcebound live integration test evidence.",
            notes="Non-production test reference.",
        )
    ]

    adapter.save_claim(claim, evidence=evidence)

    entity_map = adapter._entity_map()
    assert claim.claim_id in entity_map
    assert entity_map[claim.claim_id]["entity_id"]
    fetched = adapter.get_claim(claim.claim_id)
    assert fetched is not None
    assert fetched.claim_id == claim.claim_id
    assert fetched.predicate == claim.predicate
    assert claim.evidence_ids[0] in fetched.evidence_ids
    assert any(item.claim_id == claim.claim_id for item in adapter.list_claims())


@pytest.mark.live_qdrant
def test_live_qdrant_upsert_and_query_round_trip(live_qdrant_collection: str) -> None:
    _ = live_qdrant_collection
    adapter = QdrantProjectionAdapter()
    claims = [
        ApprovedClaim(
            claim_id=f"claim-{uuid4().hex[:8]}",
            subject="Rouen bread prices",
            predicate="rose_during",
            value="winter shortage",
            claim_kind=ClaimKind.PRACTICE,
            status=ClaimStatus.VERIFIED,
            place="Rouen",
            evidence_ids=["evi-live-1"],
        ),
        ApprovedClaim(
            claim_id=f"claim-{uuid4().hex[:8]}",
            subject="Paris cloth prices",
            predicate="remained_stable",
            value="summer market",
            claim_kind=ClaimKind.PRACTICE,
            status=ClaimStatus.PROBABLE,
            place="Paris",
            evidence_ids=["evi-live-2"],
        ),
    ]
    evidence = [
        EvidenceSnippet(
            evidence_id="evi-live-1",
            source_id="src-live-1",
            locator="folio 1r",
            text="Rouen bread prices rose sharply during the winter shortage.",
        ),
        EvidenceSnippet(
            evidence_id="evi-live-2",
            source_id="src-live-2",
            locator="folio 2r",
            text="Paris cloth prices remained stable across the summer market.",
        ),
    ]

    adapter.upsert_claims(claims, evidence)
    result = adapter.search_claim_ids(
        "Rouen bread prices winter shortage",
        [claim.claim_id for claim in claims],
        limit=2,
    )

    assert result.fallback_used is False
    assert result.claim_ids
    assert result.claim_ids[0] == claims[0].claim_id
