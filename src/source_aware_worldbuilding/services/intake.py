from __future__ import annotations

from source_aware_worldbuilding.domain.models import (
    IntakeResult,
    IntakeTextRequest,
    IntakeUrlRequest,
)
from source_aware_worldbuilding.ports import (
    CorpusPort,
    SourceDocumentStorePort,
    SourceStorePort,
)


class IntakeService:
    def __init__(
        self,
        corpus: CorpusPort,
        source_store: SourceStorePort,
        source_document_store: SourceDocumentStorePort,
    ):
        self.corpus = corpus
        self.source_store = source_store
        self.source_document_store = source_document_store

    def intake_text(self, request: IntakeTextRequest) -> IntakeResult:
        created = self.corpus.create_text_source(request)
        return self._pull_and_queue_documents(created.zotero_item_key, created)

    def intake_url(self, request: IntakeUrlRequest) -> IntakeResult:
        created = self.corpus.create_url_source(request)
        return self._pull_and_queue_documents(created.zotero_item_key, created)

    def intake_file(
        self,
        *,
        filename: str,
        content_type: str | None,
        content: bytes,
        title: str | None = None,
        source_type: str = "document",
        notes: str | None = None,
        collection_key: str | None = None,
    ) -> IntakeResult:
        created, warnings = self.corpus.create_file_source(
            filename=filename,
            content_type=content_type,
            content=content,
            title=title,
            source_type=source_type,
            notes=notes,
            collection_key=collection_key,
        )
        return self._pull_and_queue_documents(created.zotero_item_key, created, warnings=warnings)

    def _pull_and_queue_documents(
        self,
        zotero_item_key: str,
        created_item,
        *,
        warnings: list[str] | None = None,
    ) -> IntakeResult:
        warnings = list(warnings or [])
        sources = self.corpus.pull_sources_by_item_keys([zotero_item_key])
        source_documents = self.corpus.discover_source_documents(sources)
        self.source_store.save_sources(sources)
        self.source_document_store.save_source_documents(source_documents)
        if not source_documents:
            warnings.append("No source documents were discovered; nothing has been queued for normalization.")
        return IntakeResult(
            created_item=created_item,
            pulled_sources=sources,
            source_documents=source_documents,
            warnings=warnings,
        )
