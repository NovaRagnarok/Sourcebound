from __future__ import annotations

from source_aware_worldbuilding.domain.models import SourceDocumentRecord
from source_aware_worldbuilding.services.normalization import NormalizationService


class InMemorySourceDocumentStore:
    def __init__(self, documents):
        self.documents = {item.document_id: item for item in documents}

    def list_source_documents(
        self,
        source_id=None,
        *,
        ingest_status=None,
        raw_text_status=None,
        claim_extraction_status=None,
    ):
        documents = list(self.documents.values())
        if source_id is not None:
            documents = [item for item in documents if item.source_id == source_id]
        if ingest_status is not None:
            documents = [item for item in documents if item.ingest_status == ingest_status]
        if raw_text_status is not None:
            documents = [item for item in documents if item.raw_text_status == raw_text_status]
        if claim_extraction_status is not None:
            documents = [
                item
                for item in documents
                if item.claim_extraction_status == claim_extraction_status
            ]
        return documents

    def save_source_documents(self, source_documents):
        for item in source_documents:
            self.documents[item.document_id] = item

    def update_source_document(self, source_document):
        self.documents[source_document.document_id] = source_document


class InMemoryTextUnitStore:
    def __init__(self):
        self.items = []

    def save_text_units(self, text_units):
        self.items.extend(text_units)


def test_normalization_service_creates_text_units_and_marks_documents_ready() -> None:
    document = SourceDocumentRecord(
        document_id="zdoc-1",
        source_id="src-1",
        document_kind="note",
        ingest_status="imported",
        raw_text_status="ready",
        claim_extraction_status="queued",
        locator="note",
        raw_text="Bread prices rose sharply during the winter shortage.",
    )
    source_document_store = InMemorySourceDocumentStore([document])
    text_unit_store = InMemoryTextUnitStore()

    result = NormalizationService(source_document_store, text_unit_store).normalize_documents()

    assert result["document_count"] == 1
    assert result["text_unit_count"] == 1
    assert result["warnings"] == []
    assert len(text_unit_store.items) == 1
    assert "source_document_id=zdoc-1" in text_unit_store.items[0].notes
    updated_document = source_document_store.documents["zdoc-1"]
    assert updated_document.ingest_status == "ready_for_extraction"
    assert updated_document.claim_extraction_status == "ready"


def test_normalization_service_warns_on_empty_ready_documents() -> None:
    document = SourceDocumentRecord(
        document_id="zdoc-2",
        source_id="src-2",
        document_kind="note",
        ingest_status="imported",
        raw_text_status="ready",
        claim_extraction_status="queued",
        raw_text="   ",
    )
    source_document_store = InMemorySourceDocumentStore([document])
    text_unit_store = InMemoryTextUnitStore()

    result = NormalizationService(source_document_store, text_unit_store).normalize_documents()

    assert result["text_unit_count"] == 0
    assert len(result["warnings"]) == 1
    assert source_document_store.documents["zdoc-2"].claim_extraction_status == "failed"
