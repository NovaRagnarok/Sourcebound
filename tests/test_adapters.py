from __future__ import annotations

import json

import httpx

from source_aware_worldbuilding.adapters.graphrag_adapter import GraphRAGExtractionAdapter
from source_aware_worldbuilding.adapters.heuristic_extraction import (
    HeuristicExtractionAdapter,
)
from source_aware_worldbuilding.adapters.qdrant_adapter import QdrantProjectionAdapter
from source_aware_worldbuilding.adapters.wikibase_adapter import WikibaseTruthStore
from source_aware_worldbuilding.adapters.zotero_adapter import ZoteroCorpusAdapter
from source_aware_worldbuilding.api.dependencies import get_extractor
from source_aware_worldbuilding.domain.enums import ClaimKind, ClaimStatus, ExtractionRunStatus
from source_aware_worldbuilding.domain.models import (
    ApprovedClaim,
    EvidenceSnippet,
    ExtractionRun,
    SourceRecord,
    TextUnit,
)
from source_aware_worldbuilding.settings import settings


def test_zotero_adapter_builds_sources_and_text_units_from_items_and_children(
    monkeypatch,
) -> None:
    monkeypatch.setattr(settings, "zotero_library_id", "12345")
    monkeypatch.setattr(settings, "zotero_collection_key", "COLL-1")
    monkeypatch.setattr(settings, "zotero_library_type", "user")
    monkeypatch.setattr(settings, "zotero_base_url", "https://example.test/api")

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/users/12345/collections/COLL-1/items/top"):
            return httpx.Response(
                200,
                json=[
                    {
                        "key": "ITEM-1",
                        "data": {
                            "title": "Market records",
                            "itemType": "document",
                            "creators": [{"firstName": "Ada", "lastName": "Clerk"}],
                            "date": "1421-05-04",
                            "collections": ["COLL-1"],
                            "abstractNote": "<p>Bread prices rose sharply.</p>",
                        },
                    }
                ],
            )
        if request.url.path.endswith("/users/12345/items/ITEM-1/children"):
            return httpx.Response(
                200,
                json=[
                    {
                        "key": "NOTE-1",
                        "data": {
                            "itemType": "note",
                            "title": "Extraction note",
                            "note": (
                                "<p>Merchants whispered that grain was withheld.</p>"
                                "<p>Witnesses in Rouen agreed.</p>"
                            ),
                        },
                    },
                    {
                        "key": "ATT-1",
                        "data": {
                            "itemType": "attachment",
                            "title": "Scan PDF",
                            "contentType": "application/pdf",
                            "filename": "scan.pdf",
                            "linkMode": "imported_file",
                            "url": "https://example.test/scan.pdf",
                        },
                    },
                ],
            )
        return httpx.Response(200, json=[])

    adapter = ZoteroCorpusAdapter()
    monkeypatch.setattr(
        adapter,
        "_client",
        lambda: httpx.Client(
            base_url="https://example.test/api/",
            transport=httpx.MockTransport(handler),
        ),
    )

    sources = adapter.pull_sources()
    text_units = adapter.pull_text_units(sources)

    assert len(sources) == 1
    assert sources[0].source_id == "zotero-ITEM-1"
    assert sources[0].author == "Ada Clerk"
    assert len(text_units) == 4
    assert any("Bread prices rose sharply." in item.text for item in text_units)
    note_unit = next(item for item in text_units if item.notes and "NOTE-1" in item.notes)
    attachment_unit = next(item for item in text_units if item.notes and "ATT-1" in item.notes)
    assert "Merchants whispered that grain was withheld." in note_unit.text
    assert "Witnesses in Rouen agreed." in note_unit.text
    assert "zotero_child_type=note" in note_unit.notes
    assert "Filename: scan.pdf" in attachment_unit.text
    assert "URL: https://example.test/scan.pdf" in attachment_unit.text
    assert "zotero_child_type=attachment" in attachment_unit.notes


def test_zotero_adapter_retries_transient_failures(monkeypatch) -> None:
    monkeypatch.setattr(settings, "zotero_library_id", "12345")
    monkeypatch.setattr(settings, "zotero_collection_key", None)
    monkeypatch.setattr(settings, "zotero_library_type", "user")
    monkeypatch.setattr(settings, "zotero_base_url", "https://example.test/api")
    call_count = {"top": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/users/12345/items/top"):
            call_count["top"] += 1
            if call_count["top"] == 1:
                return httpx.Response(429, json={"error": "slow down"})
            return httpx.Response(
                200,
                json=[
                    {
                        "key": "ITEM-1",
                        "data": {"title": "Market records", "itemType": "document"},
                    }
                ],
            )
        return httpx.Response(200, json=[])

    adapter = ZoteroCorpusAdapter()
    monkeypatch.setattr(
        adapter,
        "_client",
        lambda: httpx.Client(
            base_url="https://example.test/api/",
            transport=httpx.MockTransport(handler),
        ),
    )

    sources = adapter.pull_sources()

    assert len(sources) == 1
    assert call_count["top"] == 2


def test_extraction_adapter_dedupes_candidates_and_infers_place_and_time() -> None:
    adapter = HeuristicExtractionAdapter()
    run = ExtractionRun(run_id="run-test", status=ExtractionRunStatus.RUNNING)
    sources = [
        SourceRecord(
            source_id="src-1",
            title="Municipal price records of Rouen",
            author="Ada Clerk",
            year="1421",
            source_type="record",
            locator_hint="folio 12r",
        )
    ]
    text_units = [
        TextUnit(
            text_unit_id="tu-1",
            source_id="src-1",
            locator="folio 12r",
            text="Bread prices rose sharply during the winter shortage.",
            ordinal=1,
            checksum="seed",
        ),
        TextUnit(
            text_unit_id="tu-2",
            source_id="src-1",
            locator="folio 13r",
            text="  Bread prices   rose sharply during the winter shortage. ",
            ordinal=2,
            checksum="seed-2",
        ),
    ]
    output = adapter.extract_candidates(run=run, sources=sources, text_units=text_units)

    assert output.run.candidate_count == 1
    assert output.run.text_unit_count == 2
    assert len(output.evidence) == 2
    assert len(output.candidates) == 1
    candidate = output.candidates[0]
    assert candidate.predicate == "rose_during"
    assert candidate.place == "Rouen"
    assert candidate.time_start == "1421-12-01"
    assert candidate.time_end == "1422-02-28"
    assert candidate.evidence_ids == [
        output.evidence[0].evidence_id,
        output.evidence[1].evidence_id,
    ]


def test_graphrag_adapter_preserves_backend_seam_with_preview_notes() -> None:
    adapter = GraphRAGExtractionAdapter()
    run = ExtractionRun(run_id="run-preview", status=ExtractionRunStatus.RUNNING)
    sources = [
        SourceRecord(
            source_id="src-1",
            title="Municipal price records of Rouen",
            year="1421",
            source_type="record",
        )
    ]
    text_units = [
        TextUnit(
            text_unit_id="tu-1",
            source_id="src-1",
            locator="folio 12r",
            text="Bread prices rose sharply during the winter shortage.",
            ordinal=1,
        )
    ]

    output = adapter.extract_candidates(run=run, sources=sources, text_units=text_units)

    assert output.run.notes is not None
    assert "preview mode" in output.run.notes
    assert output.candidates
    assert output.candidates[0].notes is not None
    assert "graphrag_preview" in output.candidates[0].notes


def test_get_extractor_selects_backend_from_settings(monkeypatch) -> None:
    monkeypatch.setattr(settings, "graph_rag_enabled", False)
    assert isinstance(get_extractor(), HeuristicExtractionAdapter)

    monkeypatch.setattr(settings, "graph_rag_enabled", True)
    assert isinstance(get_extractor(), GraphRAGExtractionAdapter)


def test_wikibase_adapter_persists_expanded_entity_map(tmp_path) -> None:
    property_map = json.dumps(
        {
            "main_value": "P1",
            "predicate": "P2",
            "status": "P3",
            "claim_kind": "P4",
            "place": "P5",
            "time_start": "P6",
            "time_end": "P7",
            "viewpoint_scope": "P8",
            "notes": "P9",
            "app_claim_id": "P10",
            "source_id": "P11",
            "locator": "P12",
            "evidence_text": "P13",
            "evidence_id": "P14",
        }
    )
    adapter = WikibaseTruthStore(
        base_url=None,
        api_url="https://wikibase.test/api.php",
        username="user",
        password="pass",
        property_map_raw=property_map,
        cache_dir=tmp_path,
    )
    adapter._csrf_token = "token"
    calls = {"post": 0}

    def fake_request(method: str, params: dict, *, auth_required: bool = True):
        _ = params, auth_required
        if method == "POST":
            calls["post"] += 1
            return {
                "entity": {
                    "id": "Q1",
                    "claims": {
                        "P1": [
                            {
                                "id": "Q1$abc",
                                "mainsnak": {
                                    "property": "P1",
                                    "datavalue": {
                                        "value": "winter shortage",
                                        "type": "string",
                                    },
                                },
                                "qualifiers": {
                                    "P10": [{"datavalue": {"value": "claim-1", "type": "string"}}]
                                },
                            }
                        ]
                    },
                }
            }
        raise AssertionError("Unexpected request")

    adapter._request = fake_request

    adapter.save_claim(
        ApprovedClaim(
            claim_id="claim-1",
            subject="Rouen bread prices",
            predicate="rose_during",
            value="winter shortage",
            claim_kind=ClaimKind.PRACTICE,
            status=ClaimStatus.PROBABLE,
            evidence_ids=["evi-1"],
        ),
        evidence=[
            EvidenceSnippet(
                evidence_id="evi-1",
                source_id="src-1",
                locator="folio 12r",
                text="Bread prices rose sharply during the winter shortage.",
            )
        ],
    )

    entity_map = json.loads((tmp_path / "wikibase_entity_map.json").read_text())
    assert calls["post"] == 1
    assert entity_map[0]["entity_id"] == "Q1"
    assert entity_map[0]["statement_id"] == "Q1$abc"
    assert entity_map[0]["statement_property"] == "P1"
    assert entity_map[0]["last_synced_at"]


def test_wikibase_adapter_round_trips_qualifiers_and_references(tmp_path) -> None:
    property_map = json.dumps(
        {
            "main_value": "P1",
            "predicate": "P2",
            "status": "P3",
            "claim_kind": "P4",
            "place": "P5",
            "time_start": "P6",
            "time_end": "P7",
            "viewpoint_scope": "P8",
            "notes": "P9",
            "app_claim_id": "P10",
            "evidence_id": "P14",
        }
    )
    adapter = WikibaseTruthStore(
        base_url=None,
        api_url="https://wikibase.test/api.php",
        username="user",
        password="pass",
        property_map_raw=property_map,
        cache_dir=tmp_path,
    )

    parsed = adapter._parse_entity_to_claim(
        {
            "labels": {"en": {"value": "Rouen bread prices"}},
            "claims": {
                "P1": [
                    {
                        "id": "Q1$abc",
                        "mainsnak": {
                            "property": "P1",
                            "datavalue": {"value": "winter shortage", "type": "string"},
                        },
                        "qualifiers": {
                            "P2": [{"datavalue": {"value": "rose_during", "type": "string"}}],
                            "P3": [{"datavalue": {"value": "verified", "type": "string"}}],
                            "P4": [{"datavalue": {"value": "practice", "type": "string"}}],
                            "P5": [{"datavalue": {"value": "Rouen", "type": "string"}}],
                            "P6": [{"datavalue": {"value": "1421-12-01", "type": "string"}}],
                            "P7": [{"datavalue": {"value": "1422-02-28", "type": "string"}}],
                            "P9": [{"datavalue": {"value": "Structured note", "type": "string"}}],
                            "P10": [{"datavalue": {"value": "claim-1", "type": "string"}}],
                        },
                        "references": [
                            {
                                "snaks": {
                                    "P14": [
                                        {"datavalue": {"value": "evi-1", "type": "string"}}
                                    ]
                                }
                            }
                        ],
                    }
                ]
            },
        },
        ApprovedClaim(
            claim_id="claim-1",
            subject="cached subject",
            predicate="cached_predicate",
            value="cached value",
            claim_kind=ClaimKind.OBJECT,
            status=ClaimStatus.PROBABLE,
            evidence_ids=[],
        ),
        claim_id="claim-1",
        entity_entry={"statement_id": "Q1$abc"},
    )

    assert parsed is not None
    assert parsed.subject == "Rouen bread prices"
    assert parsed.predicate == "rose_during"
    assert parsed.status == ClaimStatus.VERIFIED
    assert parsed.claim_kind == ClaimKind.PRACTICE
    assert parsed.place == "Rouen"
    assert parsed.evidence_ids == ["evi-1"]


def test_qdrant_projection_upserts_idempotently_without_recreating(monkeypatch) -> None:
    monkeypatch.setattr(settings, "qdrant_enabled", True)
    monkeypatch.setattr(settings, "qdrant_collection", "approved_claims")

    class FakeClient:
        def __init__(self) -> None:
            self.exists = False
            self.created = 0
            self.upserts = []

        def collection_exists(self, collection_name: str) -> bool:
            _ = collection_name
            return self.exists

        def create_collection(self, collection_name: str, vectors_config) -> None:
            _ = collection_name, vectors_config
            self.exists = True
            self.created += 1

        def upsert(self, collection_name: str, points) -> None:
            _ = collection_name
            self.upserts.append(points)

    adapter = QdrantProjectionAdapter()
    client = FakeClient()
    monkeypatch.setattr(adapter, "_client", lambda: client)

    claim = ApprovedClaim(
        claim_id="claim-1",
        subject="Rouen bread prices",
        predicate="rose_during",
        value="winter shortage",
        claim_kind=ClaimKind.PRACTICE,
        status=ClaimStatus.VERIFIED,
        evidence_ids=["evi-1"],
    )
    evidence = [
        EvidenceSnippet(
            evidence_id="evi-1",
            source_id="src-1",
            locator="folio 12r",
            text="Bread prices rose sharply during the winter shortage.",
        )
    ]

    adapter.upsert_claims([claim], evidence)
    adapter.upsert_claims([claim], evidence)

    assert client.created == 1
    assert len(client.upserts) == 2
    assert client.upserts[0][0].id == adapter._point_id("claim-1")
    assert client.upserts[0][0].payload["claim_id"] == "claim-1"
