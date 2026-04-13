from __future__ import annotations

from hashlib import sha1

from source_aware_worldbuilding.domain.models import SourceDocumentRecord, TextUnit
from source_aware_worldbuilding.ports import SourceDocumentStorePort, TextUnitStorePort


class NormalizationService:
    def __init__(
        self,
        source_document_store: SourceDocumentStorePort,
        text_unit_store: TextUnitStorePort,
    ):
        self.source_document_store = source_document_store
        self.text_unit_store = text_unit_store

    def normalize_documents(self) -> dict[str, object]:
        documents = self.source_document_store.list_source_documents(raw_text_status="ready")
        normalized: list[TextUnit] = []
        updated_documents: list[SourceDocumentRecord] = []
        warnings: list[str] = []

        for document in documents:
            if document.claim_extraction_status not in {"queued", "failed"}:
                continue
            if not document.raw_text:
                document.claim_extraction_status = "failed"
                updated_documents.append(document)
                warnings.append(f"Document {document.document_id} had ready raw text status but no raw text.")
                continue
            text = document.raw_text.strip()
            if not text:
                document.claim_extraction_status = "failed"
                updated_documents.append(document)
                warnings.append(f"Document {document.document_id} had empty raw text.")
                continue

            text_unit_id = f"text-{sha1(f'{document.document_id}:{document.source_id}'.encode()).hexdigest()[:12]}"
            normalized.append(
                TextUnit(
                    text_unit_id=text_unit_id,
                    source_id=document.source_id,
                    locator=document.locator or document.filename or document.document_kind,
                    text=text,
                    ordinal=1,
                    checksum=sha1(text.encode()).hexdigest(),
                    notes=f"source_document_id={document.document_id}; document_kind={document.document_kind}",
                )
            )
            document.ingest_status = "ready_for_extraction"
            document.claim_extraction_status = "ready"
            updated_documents.append(document)

        if normalized:
            self.text_unit_store.save_text_units(normalized)
        for document in updated_documents:
            self.source_document_store.update_source_document(document)

        return {
            "document_count": len(updated_documents),
            "text_unit_count": len(normalized),
            "warnings": warnings,
        }
