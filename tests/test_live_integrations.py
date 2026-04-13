from __future__ import annotations

import json
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from source_aware_worldbuilding.adapters.file_backed import FileCandidateStore, FileEvidenceStore
from source_aware_worldbuilding.adapters.qdrant_adapter import QdrantProjectionAdapter
from source_aware_worldbuilding.adapters.wikibase_adapter import WikibaseTruthStore
from source_aware_worldbuilding.adapters.zotero_adapter import ZoteroCorpusAdapter
from source_aware_worldbuilding.api.main import app
from source_aware_worldbuilding.domain.enums import ClaimKind, ClaimStatus, ReviewState
from source_aware_worldbuilding.domain.models import ApprovedClaim, CandidateClaim, EvidenceSnippet
from source_aware_worldbuilding.settings import settings


@pytest.mark.live_zotero
def test_live_zotero_pull_reads_real_library(require_live_zotero) -> None:
    _ = require_live_zotero
    adapter = ZoteroCorpusAdapter()

    sources = adapter.pull_sources()
    if not sources:
        pytest.skip("Live Zotero library is configured but currently empty.")

    assert sources
    assert all(source.source_id.startswith("zotero-") for source in sources[:3])

    text_units = adapter.pull_text_units(sources[:1])

    assert text_units
    assert all(unit.source_id == sources[0].source_id for unit in text_units)
    assert any(unit.text.strip() for unit in text_units)
    child_units = [unit for unit in text_units if unit.notes]
    assert all("zotero_child_" in unit.notes for unit in child_units)


@pytest.mark.live_wikibase
def test_live_wikibase_review_flow_round_trips_claim(require_live_wikibase, monkeypatch) -> None:
    cache_dir = require_live_wikibase
    monkeypatch.setattr(settings, "app_state_backend", "file")
    monkeypatch.setattr(settings, "app_truth_backend", "wikibase")
    monkeypatch.setattr(settings, "qdrant_enabled", False)

    token = uuid4().hex[:12]
    FileCandidateStore(cache_dir).save_candidates(
        [
            CandidateClaim(
                candidate_id=f"cand-live-{token}",
                subject=f"Sourcebound live test {token}",
                predicate="validated_in",
                value="wikibase",
                claim_kind=ClaimKind.OBJECT,
                status_suggestion=ClaimStatus.PROBABLE,
                review_state=ReviewState.PENDING,
                place="Test Realm",
                time_start="2026-04-01",
                time_end="2026-04-12",
                viewpoint_scope="integration",
                evidence_ids=[f"live-evidence-{token}"],
                extractor_run_id="live-run",
                notes="Created by Sourcebound live integration test.",
            )
        ]
    )
    FileEvidenceStore(cache_dir).save_evidence(
        [
            EvidenceSnippet(
                evidence_id=f"live-evidence-{token}",
                source_id="live-source",
                locator="integration-test",
                text="Sourcebound live integration test evidence.",
                notes="Non-production test reference.",
            )
        ]
    )

    with TestClient(app) as client:
        approve_response = client.post(
            f"/v1/candidates/cand-live-{token}/review",
            json={"decision": "approve"},
        )

        assert approve_response.status_code == 200
        approved_claim = approve_response.json()["claim"]
        claim_id = approved_claim["claim_id"]

        claim_detail = client.get(f"/v1/claims/{claim_id}")
        assert claim_detail.status_code == 200
        fetched = claim_detail.json()

    assert fetched["claim_id"] == claim_id
    assert fetched["subject"] == f"Sourcebound live test {token}"
    assert fetched["predicate"] == "validated_in"
    assert fetched["value"] == "wikibase"
    assert fetched["claim_kind"] == "object"
    assert fetched["status"] == "probable"
    assert fetched["place"] == "Test Realm"
    assert fetched["time_start"] == "2026-04-01"
    assert fetched["time_end"] == "2026-04-12"
    assert fetched["viewpoint_scope"] == "integration"
    assert fetched["notes"] == "Created by Sourcebound live integration test."
    assert fetched["evidence_ids"] == [f"live-evidence-{token}"]

    adapter = WikibaseTruthStore(
        base_url=settings.wikibase_base_url,
        api_url=settings.wikibase_api_url,
        username=settings.wikibase_username,
        password=settings.wikibase_password,
        property_map_raw=settings.wikibase_property_map,
        cache_dir=cache_dir,
    )
    entity_map = adapter._entity_map()
    assert claim_id in entity_map
    entity_entry = entity_map[claim_id]
    assert entity_entry["entity_id"]

    entity = adapter._fetch_raw_entity(entity_entry["entity_id"])
    statement = adapter._find_claim_statement(
        entity,
        claim_id,
        entity_entry.get("statement_id"),
    )
    assert statement is not None

    property_map = json.loads(settings.wikibase_property_map or "{}")
    assert (statement.get("mainsnak") or {}).get("property") == property_map["main_value"]
    assert adapter._extract_string_value(statement.get("mainsnak")) == "wikibase"
    assert adapter._statement_qualifier_value(statement, "predicate") == "validated_in"
    assert adapter._statement_qualifier_value(statement, "status") == "probable"
    assert adapter._statement_qualifier_value(statement, "claim_kind") == "object"
    assert adapter._statement_qualifier_value(statement, "place") == "Test Realm"
    assert adapter._statement_qualifier_value(statement, "time_start") == "2026-04-01"
    assert adapter._statement_qualifier_value(statement, "time_end") == "2026-04-12"
    assert adapter._statement_qualifier_value(statement, "viewpoint_scope") == "integration"
    assert adapter._statement_qualifier_value(
        statement,
        "notes",
    ) == "Created by Sourcebound live integration test."
    assert adapter._statement_qualifier_value(statement, "app_claim_id") == claim_id
    assert adapter._reference_values(statement, "source_id") == ["live-source"]
    assert adapter._reference_values(statement, "locator") == ["integration-test"]
    assert adapter._reference_values(
        statement,
        "evidence_text",
    ) == ["Sourcebound live integration test evidence."]
    assert adapter._reference_values(statement, "evidence_id") == [f"live-evidence-{token}"]


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
