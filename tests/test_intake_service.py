from __future__ import annotations

from source_aware_worldbuilding.domain.models import (
    IntakeTextRequest,
    IntakeUrlRequest,
    SourceDocumentRecord,
    SourceRecord,
    ZoteroCreatedItem,
)
from source_aware_worldbuilding.services.intake import IntakeService


class FakeCorpus:
    def __init__(self):
        self.calls: list[str] = []
        self.last_collection_key = None
        self.created_item = ZoteroCreatedItem(
            zotero_item_key="ITEM-1",
            title="Created source",
            item_type="document",
        )
        self.sources = [
            SourceRecord(
                source_id="zotero-ITEM-1",
                external_id="ITEM-1",
                title="Created source",
                zotero_item_key="ITEM-1",
            )
        ]
        self.source_documents = [
            SourceDocumentRecord(
                document_id="zdoc-ITEM-1-note",
                source_id="zotero-ITEM-1",
                document_kind="note",
                external_id="ITEM-1-NOTE",
                ingest_status="imported",
                raw_text_status="ready",
                claim_extraction_status="queued",
                locator="note",
                raw_text="Bread prices rose sharply during the winter shortage.",
            )
        ]

    def create_text_source(self, request: IntakeTextRequest):
        self.calls.append("create_text_source")
        self.last_collection_key = request.collection_key
        return self.created_item

    def create_url_source(self, request: IntakeUrlRequest):
        self.calls.append("create_url_source")
        self.last_collection_key = request.collection_key
        return self.created_item.model_copy(update={"item_type": "webpage", "url": request.url})

    def create_file_source(self, **kwargs):
        self.calls.append("create_file_source")
        self.last_collection_key = kwargs["collection_key"]
        return self.created_item, ["metadata only"]

    def pull_sources_by_item_keys(self, item_keys: list[str]):
        self.calls.append("pull_sources_by_item_keys")
        assert item_keys == ["ITEM-1"]
        return self.sources

    def discover_source_documents(self, sources):
        self.calls.append("discover_source_documents")
        assert sources == self.sources
        return self.source_documents

    def pull_text_units(self, sources):
        raise AssertionError("Intake should not create text units directly.")

    def pull_sources(self):
        raise AssertionError("Not expected in intake flow")


class InMemorySourceStore:
    def __init__(self):
        self.items = []

    def save_sources(self, sources):
        self.items = list(sources)


class InMemorySourceDocumentStore:
    def __init__(self):
        self.items = []

    def save_source_documents(self, source_documents):
        self.items = list(source_documents)


def build_service():
    return IntakeService(
        corpus=FakeCorpus(),
        source_store=InMemorySourceStore(),
        source_document_store=InMemorySourceDocumentStore(),
    )


def test_intake_service_orchestrates_create_pull_and_queue_documents() -> None:
    service = build_service()

    result = service.intake_text(
        IntakeTextRequest(
            title="Created source",
            text="Bread prices rose sharply during the winter shortage.",
            collection_key="COLL-1",
        )
    )

    assert result.created_item.zotero_item_key == "ITEM-1"
    assert len(result.pulled_sources) == 1
    assert len(result.source_documents) == 1
    assert result.pulled_text_units == []
    assert result.extraction_run is None
    assert result.candidate_count == 0
    assert result.evidence_count == 0
    assert service.corpus.calls == [
        "create_text_source",
        "pull_sources_by_item_keys",
        "discover_source_documents",
    ]
    assert service.corpus.last_collection_key == "COLL-1"


def test_intake_service_returns_warnings_from_file_intake() -> None:
    service = build_service()

    result = service.intake_file(
        filename="archive.pdf",
        content_type="application/pdf",
        content=b"%PDF-1.4",
        collection_key="COLL-2",
    )

    assert result.created_item.zotero_item_key == "ITEM-1"
    assert result.source_documents
    assert result.warnings == ["metadata only"]
    assert service.corpus.last_collection_key == "COLL-2"


def test_intake_service_warns_when_no_documents_are_discovered() -> None:
    service = build_service()
    service.corpus.source_documents = []

    result = service.intake_url(IntakeUrlRequest(url="https://example.test"))

    assert result.source_documents == []
    assert any("No source documents were discovered" in warning for warning in result.warnings)
