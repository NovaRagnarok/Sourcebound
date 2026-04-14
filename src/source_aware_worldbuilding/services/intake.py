from __future__ import annotations

from source_aware_worldbuilding.domain.models import (
    IntakeResult,
    IntakeTextRequest,
    IntakeUrlRequest,
    sync_source_with_documents,
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
        if hasattr(self.source_document_store, "list_source_documents"):
            existing_documents = [
                item
                for item in self.source_document_store.list_source_documents()
                if item.source_id in {source.source_id for source in sources}
            ]
        else:
            existing_documents = []
        try:
            source_documents = self.corpus.discover_source_documents(
                sources,
                existing_documents=existing_documents,
                force_refresh=True,
            )
        except TypeError:
            source_documents = self.corpus.discover_source_documents(sources)
        source_documents_by_source: dict[str, list] = {}
        for document in source_documents:
            source_documents_by_source.setdefault(document.source_id, []).append(document)
        for source in sources:
            sync_source_with_documents(source, source_documents_by_source.get(source.source_id, []))
        self.source_store.save_sources(sources)
        self.source_document_store.save_source_documents(source_documents)
        if not source_documents:
            warnings.append(
                "No source documents were discovered; nothing has been queued for normalization."
            )
        warnings.extend(
            error
            for document in source_documents
            for error in document.stage_errors
            if error
        )
        return IntakeResult(
            created_item=created_item,
            pulled_sources=sources,
            source_documents=source_documents,
            warnings=list(dict.fromkeys(warnings)),
        )
