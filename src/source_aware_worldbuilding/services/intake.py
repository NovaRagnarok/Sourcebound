from __future__ import annotations

import mimetypes
from hashlib import sha1
from urllib.parse import urlparse
from uuid import uuid4

from source_aware_worldbuilding.domain.errors import ZoteroConfigError
from source_aware_worldbuilding.domain.models import (
    IntakeResult,
    IntakeTextRequest,
    IntakeUrlRequest,
    SourceDocumentRecord,
    SourceRecord,
    ZoteroCreatedItem,
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
        try:
            created = self.corpus.create_text_source(request)
        except ZoteroConfigError:
            return self._create_local_text_source(request)
        return self._pull_and_queue_documents(created.zotero_item_key, created)

    def intake_url(self, request: IntakeUrlRequest) -> IntakeResult:
        try:
            created = self.corpus.create_url_source(request)
        except ZoteroConfigError:
            return self._create_local_url_source(request)
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
        try:
            created, warnings = self.corpus.create_file_source(
                filename=filename,
                content_type=content_type,
                content=content,
                title=title,
                source_type=source_type,
                notes=notes,
                collection_key=collection_key,
            )
        except ZoteroConfigError:
            return self._create_local_file_source(
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

    def _create_local_text_source(self, request: IntakeTextRequest) -> IntakeResult:
        token = uuid4().hex[:12]
        item_key = f"LOCAL-{token.upper()}"
        source_id = f"local-{token}"
        source = SourceRecord(
            source_id=source_id,
            external_source="local",
            external_id=item_key,
            title=request.title,
            author=request.author,
            year=request.year,
            source_type=request.source_type,
            collection_key=request.collection_key,
            abstract=request.notes,
            raw_metadata_json={"intake_mode": "text", "local_only": True},
        )
        source_document = SourceDocumentRecord(
            document_id=f"ldoc-{token}",
            source_id=source_id,
            document_kind="manual_text",
            external_id=item_key,
            filename=f"{self._slugify(request.title) or 'manual-source'}.txt",
            mime_type="text/plain",
            ingest_status="awaiting_text_extraction",
            raw_text_status="ready",
            claim_extraction_status="queued",
            text_extraction_status="extracted",
            normalization_status="queued",
            locator="manual text",
            raw_text=request.text,
            raw_metadata_json={"intake_mode": "text", "local_only": True},
        )
        return self._save_local_source(
            source=source,
            source_documents=[source_document],
            created_item=ZoteroCreatedItem(
                zotero_item_key=item_key,
                title=request.title,
                item_type=request.source_type,
                collection_key=request.collection_key,
            ),
            warnings=[
                "Saved in the local workspace because Zotero is not configured yet."
            ],
        )

    def _create_local_url_source(self, request: IntakeUrlRequest) -> IntakeResult:
        token = uuid4().hex[:12]
        item_key = f"LOCAL-{token.upper()}"
        source_id = f"local-{token}"
        hostname = urlparse(request.url).netloc or "local-url"
        title = request.title or request.url
        raw_text = "\n".join(
            line
            for line in (
                f"URL: {request.url}",
                f"Host: {hostname}",
                f"Title: {title}" if request.title else None,
                request.notes.strip() if request.notes else None,
            )
            if line
        )
        source = SourceRecord(
            source_id=source_id,
            external_source="local",
            external_id=item_key,
            title=title,
            source_type="webpage",
            collection_key=request.collection_key,
            abstract=request.notes,
            url=request.url,
            raw_metadata_json={"intake_mode": "url", "local_only": True},
        )
        source_document = SourceDocumentRecord(
            document_id=f"ldoc-{token}",
            source_id=source_id,
            document_kind="snapshot",
            external_id=item_key,
            filename=f"{hostname or 'local-url'}.txt",
            mime_type="text/plain",
            ingest_status="awaiting_text_extraction",
            raw_text_status="ready",
            claim_extraction_status="queued",
            text_extraction_status="extracted",
            normalization_status="queued",
            locator=request.url,
            raw_text=raw_text,
            raw_metadata_json={"intake_mode": "url", "local_only": True},
        )
        return self._save_local_source(
            source=source,
            source_documents=[source_document],
            created_item=ZoteroCreatedItem(
                zotero_item_key=item_key,
                title=title,
                item_type="webpage",
                collection_key=request.collection_key,
                url=request.url,
            ),
            warnings=[
                "Saved in the local workspace because Zotero is not configured yet.",
                "URL intake stored the link and your notes locally; it did not fetch the page body.",
            ],
        )

    def _create_local_file_source(
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
        token = uuid4().hex[:12]
        item_key = f"LOCAL-{token.upper()}"
        source_id = f"local-{token}"
        extracted_text = self._extract_local_file_text(
            filename=filename,
            content_type=content_type,
            content=content,
        )
        warnings = ["Saved in the local workspace because Zotero is not configured yet."]
        stage_errors: list[str] = []
        if extracted_text is None:
            warnings.append(
                "Local file intake could not extract text from this file type yet. "
                "Upload a text-like file or configure Zotero for attachment workflows."
            )
            stage_errors.append(
                "Local file intake could not extract text from this file type yet."
            )

        final_title = title or filename
        source = SourceRecord(
            source_id=source_id,
            external_source="local",
            external_id=item_key,
            title=final_title,
            source_type=source_type,
            collection_key=collection_key,
            abstract=notes,
            raw_metadata_json={
                "intake_mode": "file",
                "local_only": True,
                "filename": filename,
                "content_type": content_type,
                "byte_size": len(content),
                "checksum": sha1(content).hexdigest(),
            },
        )
        source_document = SourceDocumentRecord(
            document_id=f"ldoc-{token}",
            source_id=source_id,
            document_kind="attachment",
            external_id=item_key,
            filename=filename,
            mime_type=content_type,
            ingest_status="awaiting_text_extraction" if extracted_text else "extraction_failed",
            raw_text_status="ready" if extracted_text else "failed",
            claim_extraction_status="queued" if extracted_text else "failed",
            attachment_discovery_status="discovered",
            attachment_fetch_status="not_applicable",
            text_extraction_status="extracted" if extracted_text else "failed",
            normalization_status="queued" if extracted_text else "failed",
            content_checksum=sha1(content).hexdigest(),
            locator=filename,
            raw_text=extracted_text,
            stage_errors=stage_errors,
            raw_metadata_json={
                "intake_mode": "file",
                "local_only": True,
                "filename": filename,
                "content_type": content_type,
                "byte_size": len(content),
            },
        )
        return self._save_local_source(
            source=source,
            source_documents=[source_document],
            created_item=ZoteroCreatedItem(
                zotero_item_key=item_key,
                title=final_title,
                item_type=source_type,
                collection_key=collection_key,
            ),
            warnings=warnings,
        )

    def _save_local_source(
        self,
        *,
        source: SourceRecord,
        source_documents: list[SourceDocumentRecord],
        created_item: ZoteroCreatedItem,
        warnings: list[str] | None = None,
    ) -> IntakeResult:
        sync_source_with_documents(source, source_documents)
        self.source_store.save_sources([source])
        self.source_document_store.save_source_documents(source_documents)
        warnings = list(warnings or [])
        warnings.extend(
            error
            for document in source_documents
            for error in document.stage_errors
            if error
        )
        return IntakeResult(
            created_item=created_item,
            pulled_sources=[source],
            source_documents=source_documents,
            warnings=list(dict.fromkeys(warnings)),
        )

    def _extract_local_file_text(
        self,
        *,
        filename: str,
        content_type: str | None,
        content: bytes,
    ) -> str | None:
        guessed_content_type = content_type or mimetypes.guess_type(filename)[0] or ""
        lower_name = filename.lower()
        is_text_like = guessed_content_type.startswith("text/") or lower_name.endswith(
            (".txt", ".md", ".markdown", ".json", ".csv", ".html", ".htm", ".xml")
        )
        if not is_text_like:
            return None
        for encoding in ("utf-8", "latin-1"):
            try:
                text = content.decode(encoding)
                cleaned = text.strip()
                return cleaned or None
            except UnicodeDecodeError:
                continue
        return None

    def _slugify(self, value: str) -> str:
        return "-".join(value.lower().split())
