from __future__ import annotations

from collections.abc import Callable
from hashlib import sha1

from source_aware_worldbuilding.domain.models import (
    SourceDocumentRecord,
    TextUnit,
    sync_source_with_documents,
)
from source_aware_worldbuilding.ports import (
    SourceDocumentStorePort,
    SourceStorePort,
    TextUnitStorePort,
)


class NormalizationService:
    def __init__(
        self,
        source_document_store: SourceDocumentStorePort,
        text_unit_store: TextUnitStorePort,
        source_store: SourceStorePort | None = None,
    ):
        self.source_document_store = source_document_store
        self.text_unit_store = text_unit_store
        self.source_store = source_store

    def normalize_documents(
        self,
        *,
        document_ids: list[str] | None = None,
        source_ids: list[str] | None = None,
        retry_failed: bool = False,
        checkpoint: Callable[[], None] | None = None,
    ) -> dict[str, object]:
        documents = self.source_document_store.list_source_documents()
        document_id_filter = set(document_ids or [])
        source_id_filter = set(source_ids or [])
        if document_id_filter:
            documents = [item for item in documents if item.document_id in document_id_filter]
        if source_id_filter:
            documents = [item for item in documents if item.source_id in source_id_filter]
        normalized: list[TextUnit] = []
        updated_documents: list[SourceDocumentRecord] = []
        warnings: list[str] = []

        for document in documents:
            if checkpoint is not None:
                checkpoint()
            if document.normalization_status not in {"queued", "failed"}:
                continue
            if document.normalization_status == "failed" and not retry_failed:
                continue
            if document.text_extraction_status != "extracted":
                document.normalization_status = "failed"
                document.claim_extraction_status = "failed"
                document.ingest_status = "extraction_failed"
                document.stage_errors.append(
                    "Document "
                    f"{document.document_id} is not ready for normalization because "
                    "text extraction did not complete."
                )
                updated_documents.append(document)
                continue
            if not document.raw_text:
                document.normalization_status = "failed"
                document.claim_extraction_status = "failed"
                document.ingest_status = "extraction_failed"
                document.stage_errors.append(
                    f"Document {document.document_id} had extracted text status but no raw text."
                )
                updated_documents.append(document)
                warnings.append(
                    f"Document {document.document_id} had extracted text status but no raw text."
                )
                continue
            text = document.raw_text.strip()
            if not text:
                document.normalization_status = "failed"
                document.claim_extraction_status = "failed"
                document.ingest_status = "extraction_failed"
                document.stage_errors.append(f"Document {document.document_id} had empty raw text.")
                updated_documents.append(document)
                warnings.append(f"Document {document.document_id} had empty raw text.")
                continue

            text_unit_id = (
                f"text-"
                f"{sha1(f'{document.document_id}:{document.source_id}'.encode()).hexdigest()[:12]}"
            )
            normalized.append(
                TextUnit(
                    text_unit_id=text_unit_id,
                    source_id=document.source_id,
                    locator=document.locator or document.filename or document.document_kind,
                    text=text,
                    ordinal=1,
                    checksum=sha1(text.encode()).hexdigest(),
                    notes=(
                        f"source_document_id={document.document_id}; "
                        f"document_kind={document.document_kind}"
                    ),
                )
            )
            document.normalization_status = "completed"
            document.claim_extraction_status = "ready"
            document.ingest_status = "ready_for_extraction"
            document.raw_text_status = "ready"
            document.stage_errors = [
                error for error in document.stage_errors if document.document_id not in error
            ]
            updated_documents.append(document)

        if normalized:
            self.text_unit_store.save_text_units(normalized)
        for document in updated_documents:
            self.source_document_store.update_source_document(document)
        if self.source_store is not None and updated_documents:
            updated_source_ids = {document.source_id for document in updated_documents}
            for source_id in updated_source_ids:
                source = self.source_store.get_source(source_id)
                if source is None:
                    continue
                sync_source_with_documents(
                    source,
                    self.source_document_store.list_source_documents(source_id=source_id),
                )
                self.source_store.save_sources([source])

        return {
            "document_count": len(updated_documents),
            "text_unit_count": len(normalized),
            "warnings": warnings,
        }
