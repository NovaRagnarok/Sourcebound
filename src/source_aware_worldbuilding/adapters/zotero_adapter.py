from __future__ import annotations

import json
import mimetypes
import re
from hashlib import sha1
from html import unescape
from pathlib import Path

import httpx
from tenacity import Retrying, retry_if_exception, stop_after_attempt, wait_exponential

from source_aware_worldbuilding.domain.errors import (
    ZoteroAuthError,
    ZoteroConfigError,
    ZoteroExtractionError,
    ZoteroFetchError,
    ZoteroNotFoundError,
    ZoteroRateLimitError,
    ZoteroWriteError,
)
from source_aware_worldbuilding.domain.models import (
    IntakeTextRequest,
    IntakeUrlRequest,
    SourceDocumentRecord,
    SourceRecord,
    TextUnit,
    ZoteroCreatedItem,
    utc_now,
)
from source_aware_worldbuilding.settings import settings

HTML_BREAK_RE = re.compile(r"(?i)</?(?:p|div|li|ul|ol|br|hr)[^>]*>")
HTML_TAG_RE = re.compile(r"<[^>]+>")


class ZoteroCorpusAdapter:
    """Pull a narrow Zotero corpus, including note-derived text units."""

    def pull_sources(self) -> list[SourceRecord]:
        self._ensure_configured()

        sources: list[SourceRecord] = []
        with self._client() as client:
            for item in self._request_paginated(client, self._top_items_path()):
                source = self._build_source_record(item)
                if source is not None:
                    sources.append(source)
        return sources

    def pull_sources_by_item_keys(self, item_keys: list[str]) -> list[SourceRecord]:
        self._ensure_configured()
        if not item_keys:
            return []

        sources: list[SourceRecord] = []
        with self._client() as client:
            path = f"{self._library_path()}/items"
            response = self._request_with_retry(
                client,
                path,
                params={"itemKey": ",".join(sorted(set(item_keys)))},
            )
            for item in response.json():
                source = self._build_source_record(item)
                if source is not None:
                    sources.append(source)
        return sources

    def discover_source_documents(
        self,
        sources: list[SourceRecord],
        *,
        existing_documents: list[SourceDocumentRecord] | None = None,
        force_refresh: bool = False,
    ) -> list[SourceDocumentRecord]:
        self._ensure_configured()

        source_documents: list[SourceDocumentRecord] = []
        existing_by_child_key = {
            (item.zotero_child_item_key or item.external_id): item
            for item in existing_documents or []
            if item.zotero_child_item_key or item.external_id
        }
        with self._client() as client:
            for source in sources:
                if not source.zotero_item_key:
                    continue
                child_path = f"{self._library_path()}/items/{source.zotero_item_key}/children"
                children = self._request_paginated(client, child_path)
                seen_child_keys: set[str] = set()
                for child in children:
                    child_key = child.get("key")
                    data = child.get("data", {})
                    if not child_key:
                        continue
                    seen_child_keys.add(str(child_key))

                    item_type = data.get("itemType", "")
                    if item_type == "note":
                        source_documents.append(
                            self._build_note_document_record(
                                source=source,
                                child_key=str(child_key),
                                data=data,
                                existing=existing_by_child_key.get(str(child_key)),
                                force_refresh=force_refresh,
                            )
                        )
                        continue

                    if item_type != "attachment":
                        continue

                    source_documents.append(
                        self._build_attachment_document_record(
                            client=client,
                            source=source,
                            child_key=str(child_key),
                            data=data,
                            existing=existing_by_child_key.get(str(child_key)),
                            force_refresh=force_refresh,
                        )
                    )
                for existing in existing_by_child_key.values():
                    if (
                        existing.source_id != source.source_id
                        or not existing.zotero_child_item_key
                        or existing.zotero_child_item_key in seen_child_keys
                    ):
                        continue
                    missing = existing.model_copy(deep=True)
                    missing.present_in_latest_pull = False
                    missing.attachment_discovery_status = "missing"
                    missing.stage_errors = list(
                        dict.fromkeys(
                            [
                                *missing.stage_errors,
                                "Attachment or note was not present in the latest Zotero pull.",
                            ]
                        )
                    )
                    source_documents.append(missing)
        return source_documents

    def create_text_source(self, request: IntakeTextRequest) -> ZoteroCreatedItem:
        parent_key = self._create_parent_item(
            {
                "itemType": self._zotero_item_type(request.source_type, has_url=False),
                "title": request.title,
                "creators": self._creators_for_author(request.author),
                "date": request.year or "",
                "abstractNote": request.notes or "",
                "collections": self._collections_list(request.collection_key),
                "extra": self._extra_for_source_type(request.source_type),
            }
        )
        self._create_child_note(parent_key, self._text_note_body(request.text, request.notes))
        return ZoteroCreatedItem(
            zotero_item_key=parent_key,
            title=request.title,
            item_type=self._zotero_item_type(request.source_type, has_url=False),
            collection_key=request.collection_key or settings.zotero_collection_key,
        )

    def create_url_source(self, request: IntakeUrlRequest) -> ZoteroCreatedItem:
        title = request.title or request.url
        parent_key = self._create_parent_item(
            {
                "itemType": "webpage",
                "title": title,
                "url": request.url,
                "collections": self._collections_list(request.collection_key),
                "abstractNote": request.notes or "",
            }
        )
        if request.notes:
            self._create_child_note(parent_key, self._notes_only_body(request.notes))
        return ZoteroCreatedItem(
            zotero_item_key=parent_key,
            title=title,
            item_type="webpage",
            collection_key=request.collection_key or settings.zotero_collection_key,
            url=request.url,
        )

    def create_file_source(
        self,
        *,
        filename: str,
        content_type: str | None,
        content: bytes,
        title: str | None = None,
        source_type: str = "document",
        notes: str | None = None,
        collection_key: str | None = None,
    ) -> tuple[ZoteroCreatedItem, list[str]]:
        warnings: list[str] = []
        extracted_text = self._extract_file_text(filename, content_type, content)
        if extracted_text is None:
            warnings.append(
                "File text extraction is not implemented for this file type yet; "
                "Zotero note contains metadata only."
            )
            extracted_text = self._file_placeholder_text(filename, content_type, len(content))

        final_title = title or filename
        parent_key = self._create_parent_item(
            {
                "itemType": self._zotero_item_type(source_type, has_url=False),
                "title": final_title,
                "collections": self._collections_list(collection_key),
                "abstractNote": notes or "",
                "extra": self._extra_for_source_type(source_type),
            }
        )
        self._create_child_note(
            parent_key,
            self._file_note_body(
                filename=filename,
                content_type=content_type,
                extracted_text=extracted_text,
                notes=notes,
            ),
        )
        return (
            ZoteroCreatedItem(
                zotero_item_key=parent_key,
                title=final_title,
                item_type=self._zotero_item_type(source_type, has_url=False),
                collection_key=collection_key or settings.zotero_collection_key,
            ),
            warnings,
        )

    def pull_text_units(self, sources: list[SourceRecord]) -> list[TextUnit]:
        self._ensure_configured()

        text_units: list[TextUnit] = []
        with self._client() as client:
            for source in sources:
                ordinal = 1
                for fragment in self._source_fragments(source):
                    text_units.append(self._build_text_unit(source, ordinal, "metadata", fragment))
                    ordinal += 1

                if not source.zotero_item_key:
                    continue

                child_path = f"{self._library_path()}/items/{source.zotero_item_key}/children"
                for child in self._request_paginated(client, child_path):
                    child_key = child.get("key")
                    data = child.get("data", {})
                    item_type = data.get("itemType", "")
                    if item_type == "note":
                        note_text = self._clean_text(data.get("note"))
                        if note_text:
                            locator = data.get("title") or data.get("parentItem") or "note"
                            text_units.append(
                                self._build_text_unit(
                                    source,
                                    ordinal,
                                    locator,
                                    note_text,
                                    notes=self._child_provenance_notes(child_key, item_type),
                                )
                            )
                            ordinal += 1
                    elif item_type == "attachment":
                        attachment_text = self._attachment_fragment(data)
                        if attachment_text:
                            locator = data.get("title") or data.get("contentType") or "attachment"
                            text_units.append(
                                self._build_text_unit(
                                    source,
                                    ordinal,
                                    locator,
                                    attachment_text,
                                    notes=self._child_provenance_notes(child_key, item_type),
                                )
                            )
                            ordinal += 1
        return text_units

    def _ensure_configured(self) -> None:
        if not settings.zotero_library_id:
            raise ZoteroConfigError("Zotero source pulls require ZOTERO_LIBRARY_ID.")

    def _build_source_record(self, item: dict) -> SourceRecord | None:
        data = item.get("data", {})
        if data.get("parentItem"):
            return None
        item_key = item.get("key")
        if not item_key:
            return None
        creators = data.get("creators") or []
        author = ", ".join(
            filter(None, [self._creator_name(creator) for creator in creators[:2]])
        ) or None
        date_value = data.get("date") or ""
        year = date_value[:4] if isinstance(date_value, str) and date_value else None
        checksum = sha1(json.dumps(data, sort_keys=True).encode("utf-8")).hexdigest()
        version = data.get("version") or data.get("dateModified")
        return SourceRecord(
            source_id=f"zotero-{item_key}",
            external_source="zotero",
            external_id=item_key,
            title=data.get("title") or f"Untitled Zotero item {item_key}",
            author=author,
            year=year,
            source_type=data.get("itemType", "document"),
            locator_hint=data.get("archiveLocation") or data.get("callNumber"),
            zotero_item_key=item_key,
            collection_key=self._collection_key_from_item(data),
            abstract=self._clean_text(data.get("abstractNote")),
            url=data.get("url") or None,
            sync_status="imported",
            workflow_stage="metadata_imported",
            last_synced_at=utc_now(),
            last_zotero_version=str(version) if version is not None else None,
            last_source_checksum=checksum,
            raw_metadata_json=data,
        )

    def _build_note_document_record(
        self,
        *,
        source: SourceRecord,
        child_key: str,
        data: dict,
        existing: SourceDocumentRecord | None,
        force_refresh: bool,
    ) -> SourceDocumentRecord:
        raw_text = self._clean_text(data.get("note"))
        checksum = sha1((raw_text or "").encode("utf-8")).hexdigest()
        document = SourceDocumentRecord(
            document_id=f"zdoc-{child_key}",
            source_id=source.source_id,
            document_kind="note",
            external_id=child_key,
            filename=data.get("title"),
            mime_type="text/html",
            metadata_import_status="imported",
            attachment_discovery_status="not_applicable",
            attachment_fetch_status="not_applicable",
            text_extraction_status="extracted" if raw_text else "failed",
            normalization_status="queued",
            content_checksum=checksum,
            last_synced_at=utc_now(),
            present_in_latest_pull=True,
            locator=data.get("title") or data.get("parentItem") or "note",
            raw_text=raw_text,
            raw_metadata_json=data,
            zotero_parent_item_key=source.zotero_item_key,
            zotero_child_item_key=child_key,
        )
        if not raw_text:
            document.stage_errors.append("Zotero note did not contain extractable text.")
        return self._merge_existing_document(
            document,
            existing=existing,
            force_refresh=force_refresh,
        )

    def _build_attachment_document_record(
        self,
        *,
        client: httpx.Client,
        source: SourceRecord,
        child_key: str,
        data: dict,
        existing: SourceDocumentRecord | None,
        force_refresh: bool,
    ) -> SourceDocumentRecord:
        content_type = data.get("contentType")
        filename = data.get("filename") or data.get("title") or f"{child_key}.bin"
        document_kind = (
            "snapshot"
            if isinstance(content_type, str) and content_type.startswith("text/html")
            else "attachment"
        )
        downloaded_bytes: bytes | None = None
        storage_path: str | None = None
        stage_errors: list[str] = []
        fetch_status = "pending"
        try:
            downloaded_bytes, storage_path = self._fetch_attachment_bytes(
                client=client,
                source=source,
                child_key=child_key,
                filename=filename,
            )
            fetch_status = "fetched"
        except ZoteroFetchError as exc:
            stage_errors.append(str(exc))
            fetch_status = "failed"

        raw_text = None
        extraction_status = "pending"
        if downloaded_bytes is not None:
            try:
                raw_text = self._extract_attachment_text(
                    filename=filename,
                    content_type=content_type,
                    content=downloaded_bytes,
                )
                extraction_status = "extracted"
            except ZoteroExtractionError as exc:
                stage_errors.append(str(exc))
                extraction_status = "failed"
        else:
            extraction_status = "failed" if fetch_status == "failed" else "pending"

        checksum_seed = downloaded_bytes if downloaded_bytes is not None else json.dumps(
            data, sort_keys=True
        ).encode("utf-8")
        document = SourceDocumentRecord(
            document_id=f"zdoc-{child_key}",
            source_id=source.source_id,
            document_kind=document_kind,
            external_id=child_key,
            filename=filename,
            mime_type=content_type,
            storage_path=storage_path,
            metadata_import_status="imported",
            attachment_discovery_status="discovered",
            attachment_fetch_status=fetch_status,
            text_extraction_status=extraction_status,
            normalization_status="queued" if extraction_status == "extracted" else "failed",
            content_checksum=sha1(checksum_seed).hexdigest(),
            last_synced_at=utc_now(),
            present_in_latest_pull=True,
            stage_errors=stage_errors,
            locator=data.get("title") or content_type or "attachment",
            raw_text=raw_text,
            raw_metadata_json=data,
            zotero_parent_item_key=source.zotero_item_key,
            zotero_child_item_key=child_key,
        )
        return self._merge_existing_document(
            document,
            existing=existing,
            force_refresh=force_refresh,
        )

    def _merge_existing_document(
        self,
        document: SourceDocumentRecord,
        *,
        existing: SourceDocumentRecord | None,
        force_refresh: bool,
    ) -> SourceDocumentRecord:
        if existing is None:
            return document
        if self._should_preserve_existing_content(existing, document):
            preserved = existing.model_copy(deep=True)
            preserved.document_kind = document.document_kind
            preserved.filename = document.filename or preserved.filename
            preserved.mime_type = document.mime_type or preserved.mime_type
            preserved.locator = document.locator or preserved.locator
            preserved.raw_metadata_json = document.raw_metadata_json
            preserved.zotero_parent_item_key = (
                document.zotero_parent_item_key or preserved.zotero_parent_item_key
            )
            preserved.zotero_child_item_key = (
                document.zotero_child_item_key or preserved.zotero_child_item_key
            )
            preserved.last_synced_at = document.last_synced_at
            preserved.present_in_latest_pull = document.present_in_latest_pull
            preserved.attachment_discovery_status = document.attachment_discovery_status
            preserved.stage_errors = self._merge_stage_errors(existing, document)
            return preserved
        changed = existing.content_checksum != document.content_checksum
        if not changed and not force_refresh:
            document.normalization_status = existing.normalization_status
            document.claim_extraction_status = existing.claim_extraction_status
            document.raw_text = existing.raw_text
            document.storage_path = existing.storage_path or document.storage_path
            document.stage_errors = self._merge_stage_errors(existing, document)
            document.content_checksum = existing.content_checksum or document.content_checksum
        elif document.text_extraction_status == "extracted":
            document.normalization_status = "queued"
            document.claim_extraction_status = "queued"
        return document

    def _should_preserve_existing_content(
        self,
        existing: SourceDocumentRecord,
        document: SourceDocumentRecord,
    ) -> bool:
        if document.document_kind not in {"attachment", "snapshot"}:
            return False
        if (
            document.attachment_fetch_status != "failed"
            and document.text_extraction_status != "failed"
        ):
            return False
        return bool(existing.content_checksum and (existing.storage_path or existing.raw_text))

    def _merge_stage_errors(
        self,
        existing: SourceDocumentRecord,
        document: SourceDocumentRecord,
    ) -> list[str]:
        existing_errors = list(existing.stage_errors)
        if document.present_in_latest_pull:
            existing_errors = [
                error
                for error in existing_errors
                if "not present in the latest zotero pull" not in error.lower()
            ]
        return list(dict.fromkeys([*existing_errors, *document.stage_errors]))

    def _fetch_attachment_bytes(
        self,
        *,
        client: httpx.Client,
        source: SourceRecord,
        child_key: str,
        filename: str,
    ) -> tuple[bytes, str]:
        response = self._request_with_retry(
            client,
            f"{self._library_path()}/items/{child_key}/file",
            params={},
        )
        payload = response.content
        if not payload:
            raise ZoteroFetchError(f"Attachment {child_key} did not return any file content.")
        safe_name = re.sub(r"[^A-Za-z0-9._-]+", "-", filename).strip("-") or f"{child_key}.bin"
        target_dir = Path(settings.app_data_dir) / "source_attachments" / "zotero" / (
            source.zotero_item_key or source.source_id
        )
        target_dir.mkdir(parents=True, exist_ok=True)
        target_path = target_dir / f"{child_key}-{safe_name}"
        target_path.write_bytes(payload)
        return payload, str(target_path)

    def _extract_attachment_text(
        self,
        *,
        filename: str,
        content_type: str | None,
        content: bytes,
    ) -> str:
        extracted = self._extract_file_text(filename, content_type, content)
        if extracted is None:
            raise ZoteroExtractionError(
                "Fetched attachment "
                f"{filename} but could not extract text from content type "
                f"{content_type or 'unknown'}."
            )
        return extracted

    def _client(self) -> httpx.Client:
        headers = {"Zotero-API-Version": "3"}
        if settings.zotero_api_key:
            headers["Zotero-API-Key"] = settings.zotero_api_key
        return httpx.Client(
            base_url=settings.zotero_base_url.rstrip("/") + "/",
            headers=headers,
            timeout=20.0,
            follow_redirects=True,
        )

    def _request_paginated(self, client: httpx.Client, path: str) -> list[dict]:
        results: list[dict] = []
        start = 0
        limit = 100
        while True:
            response = self._request_with_retry(
                client,
                path,
                params={"limit": limit, "start": start},
            )
            payload = response.json()
            if not payload:
                break
            results.extend(payload)
            if len(payload) < limit:
                break
            start += limit
        return results

    def _request_with_retry(
        self,
        client: httpx.Client,
        path: str,
        *,
        params: dict[str, int | str] | None,
    ) -> httpx.Response:
        for attempt in Retrying(
            stop=stop_after_attempt(3),
            wait=wait_exponential(multiplier=0.2, min=0.2, max=1.0),
            retry=retry_if_exception(self._is_retryable_error),
            reraise=True,
        ):
            with attempt:
                try:
                    response = client.get(path, params=params)
                    response.raise_for_status()
                    return response
                except httpx.HTTPStatusError as exc:
                    raise self._map_http_error(exc) from exc
                except httpx.TimeoutException as exc:
                    raise ZoteroFetchError(f"Zotero request timed out for {path}.") from exc
                except httpx.RequestError as exc:
                    raise ZoteroFetchError(
                        f"Zotero request failed for {path}: {exc.__class__.__name__}: {exc}"
                    ) from exc
        raise RuntimeError("Zotero retry loop exhausted unexpectedly.")

    def _library_path(self) -> str:
        return f"{settings.zotero_library_type}s/{settings.zotero_library_id}"

    def _top_items_path(self) -> str:
        library_path = self._library_path()
        if settings.zotero_collection_key:
            return f"{library_path}/collections/{settings.zotero_collection_key}/items/top"
        return f"{library_path}/items/top"

    def _collection_key_from_item(self, data: dict) -> str | None:
        collections = data.get("collections") or []
        if settings.zotero_collection_key and settings.zotero_collection_key in collections:
            return settings.zotero_collection_key
        return collections[0] if collections else None

    def _creator_name(self, creator: dict) -> str | None:
        parts = [creator.get("firstName"), creator.get("lastName")]
        full_name = " ".join(filter(None, parts)).strip()
        return full_name or creator.get("name")

    def _source_fragments(self, source: SourceRecord) -> list[str]:
        fragments: list[str] = []
        if source.abstract:
            fragments.append(source.abstract)
        metadata_bits = [
            source.title,
            f"Author: {source.author}" if source.author else None,
            f"Year: {source.year}" if source.year else None,
            f"Locator: {source.locator_hint}" if source.locator_hint else None,
            f"URL: {source.url}" if source.url else None,
        ]
        metadata = ". ".join(bit for bit in metadata_bits if bit)
        if metadata:
            fragments.append(metadata)
        return fragments

    def _attachment_fragment(self, data: dict) -> str | None:
        note_text = self._clean_text(data.get("note"))
        bits = [
            f"Attachment: {data.get('title')}" if data.get("title") else "Attachment item",
            f"Content type: {data.get('contentType')}" if data.get("contentType") else None,
            f"Filename: {data.get('filename')}" if data.get("filename") else None,
            f"Path: {data.get('path')}" if data.get("path") else None,
            f"Link mode: {data.get('linkMode')}" if data.get("linkMode") else None,
            f"URL: {data.get('url')}" if data.get("url") else None,
            f"Note: {note_text}" if note_text else None,
        ]
        fragment = ". ".join(bit for bit in bits if bit)
        return fragment or None

    def _build_text_unit(
        self,
        source: SourceRecord,
        ordinal: int,
        locator: str,
        text: str,
        *,
        notes: str | None = None,
    ) -> TextUnit:
        checksum = sha1(text.encode()).hexdigest()
        return TextUnit(
            text_unit_id=f"text-{source.source_id}-{ordinal}",
            source_id=source.source_id,
            locator=locator,
            text=text,
            ordinal=ordinal,
            checksum=checksum,
            notes=notes,
        )

    def _clean_text(self, value: str | None) -> str | None:
        if not value:
            return None
        with_breaks = HTML_BREAK_RE.sub("\n", value)
        stripped = HTML_TAG_RE.sub(" ", with_breaks)
        lines = [" ".join(unescape(line).split()) for line in stripped.splitlines()]
        collapsed = "\n".join(line for line in lines if line)
        return collapsed or None

    def _child_provenance_notes(self, child_key: str | None, child_type: str) -> str:
        parts = [f"zotero_child_type={child_type}"]
        if child_key:
            parts.insert(0, f"zotero_child_key={child_key}")
        return "; ".join(parts)

    def _is_retryable_error(self, exc: BaseException) -> bool:
        if isinstance(exc, httpx.TimeoutException):
            return True
        if isinstance(exc, ZoteroRateLimitError):
            return True
        if isinstance(exc, httpx.HTTPStatusError):
            status_code = exc.response.status_code
            return status_code in {408, 429} or status_code >= 500
        if isinstance(exc, ZoteroFetchError):
            return True
        return False

    def _map_http_error(self, exc: httpx.HTTPStatusError):
        status_code = exc.response.status_code
        if status_code in {401, 403}:
            return ZoteroAuthError("Zotero credentials were rejected by the API.")
        if status_code == 404:
            return ZoteroNotFoundError("Configured Zotero library or item was not found.")
        if status_code == 429:
            return ZoteroRateLimitError("Zotero API rate limit reached. Try again shortly.")
        if status_code < 500:
            return ZoteroConfigError(f"Zotero API rejected the request with status {status_code}.")
        return ZoteroFetchError(f"Zotero API returned {status_code}.")

    def _create_parent_item(self, payload: dict) -> str:
        self._ensure_configured()
        if not settings.zotero_api_key:
            raise ZoteroConfigError("Zotero writes require ZOTERO_API_KEY.")
        with self._client() as client:
            response = client.post(
                f"{self._library_path()}/items",
                json=[payload],
            )
            try:
                response.raise_for_status()
            except httpx.HTTPStatusError as exc:
                raise ZoteroWriteError(
                    f"Zotero parent item creation failed: {self._map_http_error(exc)}"
                ) from exc
            except httpx.HTTPError as exc:
                raise ZoteroWriteError(f"Zotero parent item creation failed: {exc}") from exc
        data = response.json()
        key = ((data.get("successful") or {}).get("0") or {}).get("key")
        if not key:
            raise ZoteroWriteError("Zotero parent item creation did not return a key.")
        return str(key)

    def _create_child_note(self, parent_item_key: str, note_body: str) -> str:
        if not settings.zotero_api_key:
            raise ZoteroConfigError("Zotero writes require ZOTERO_API_KEY.")
        with self._client() as client:
            response = client.post(
                f"{self._library_path()}/items",
                json=[
                    {
                        "itemType": "note",
                        "parentItem": parent_item_key,
                        "note": note_body,
                    }
                ],
            )
            try:
                response.raise_for_status()
            except httpx.HTTPStatusError as exc:
                raise ZoteroWriteError(
                    f"Zotero child note creation failed: {self._map_http_error(exc)}"
                ) from exc
            except httpx.HTTPError as exc:
                raise ZoteroWriteError(f"Zotero child note creation failed: {exc}") from exc
        data = response.json()
        key = ((data.get("successful") or {}).get("0") or {}).get("key")
        if not key:
            raise ZoteroWriteError("Zotero child note creation did not return a key.")
        return str(key)

    def _collections_list(self, request_collection_key: str | None) -> list[str]:
        collection_key = request_collection_key or settings.zotero_collection_key
        return [collection_key] if collection_key else []

    def _creators_for_author(self, author: str | None) -> list[dict]:
        if not author:
            return []
        parts = author.strip().split()
        if len(parts) == 1:
            return [{"creatorType": "author", "name": author.strip()}]
        return [
            {
                "creatorType": "author",
                "firstName": " ".join(parts[:-1]),
                "lastName": parts[-1],
            }
        ]

    def _zotero_item_type(self, source_type: str, *, has_url: bool) -> str:
        if has_url:
            return "webpage"
        mapping = {
            "document": "document",
            "record": "document",
            "chronicle": "manuscript",
            "note": "document",
        }
        return mapping.get(source_type.lower(), "document")

    def _extra_for_source_type(self, source_type: str) -> str:
        return f"Sourcebound source_type: {source_type}"

    def _text_note_body(self, text: str, notes: str | None) -> str:
        fragments = []
        if notes:
            fragments.append(
                f"<p><strong>Sourcebound notes:</strong> {self._html_escape(notes)}</p>"
            )
        fragments.extend(
            f"<p>{self._html_escape(line)}</p>" for line in text.splitlines() if line.strip()
        )
        return "".join(fragments) or "<p></p>"

    def _notes_only_body(self, notes: str) -> str:
        return f"<p>{self._html_escape(notes)}</p>"

    def _file_note_body(
        self,
        *,
        filename: str,
        content_type: str | None,
        extracted_text: str,
        notes: str | None,
    ) -> str:
        parts = [
            f"<p><strong>Filename:</strong> {self._html_escape(filename)}</p>",
            f"<p><strong>Content type:</strong> {self._html_escape(content_type or 'unknown')}</p>",
        ]
        if notes:
            parts.append(f"<p><strong>Sourcebound notes:</strong> {self._html_escape(notes)}</p>")
        parts.extend(
            f"<p>{self._html_escape(line)}</p>"
            for line in extracted_text.splitlines()
            if line.strip()
        )
        return "".join(parts)

    def _file_placeholder_text(self, filename: str, content_type: str | None, size: int) -> str:
        return (
            f"File ingested by Sourcebound.\n"
            f"Filename: {filename}\n"
            f"Content type: {content_type or 'unknown'}\n"
            f"Byte size: {size}\n"
            "Full binary attachment upload is not enabled yet."
        )

    def _extract_file_text(
        self,
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
        try:
            text = content.decode("utf-8")
        except UnicodeDecodeError:
            try:
                text = content.decode("latin-1")
            except UnicodeDecodeError:
                return None
        cleaned = self._clean_text(text) or text.strip()
        return cleaned or None

    def _html_escape(self, value: str) -> str:
        return value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    def _stub_sources(self) -> list[SourceRecord]:
        return [
            SourceRecord(
                source_id="src-1",
                external_source="zotero",
                external_id="src-1",
                title="Municipal price records of Rouen",
                author="City clerk",
                year="1421",
                source_type="record",
                locator_hint="folios 10-14",
                abstract="Bread prices rose sharply during the winter shortage.",
                sync_status="imported",
                raw_metadata_json={
                    "itemType": "record",
                    "title": "Municipal price records of Rouen",
                },
            ),
            SourceRecord(
                source_id="src-2",
                external_source="zotero",
                external_id="src-2",
                title="Later chronicle of unrest",
                author="Anonymous chronicler",
                year="1450",
                source_type="chronicle",
                locator_hint="chapter 7",
                abstract="Townspeople whispered that merchants were withholding grain.",
                sync_status="imported",
                raw_metadata_json={"itemType": "chronicle", "title": "Later chronicle of unrest"},
            ),
        ]

    def _stub_text_units(self, sources: list[SourceRecord]) -> list[TextUnit]:
        text_units: list[TextUnit] = []
        for source in sources:
            for ordinal, fragment in enumerate(self._source_fragments(source), start=1):
                text_units.append(self._build_text_unit(source, ordinal, "metadata", fragment))
        return text_units

    def _stub_source_documents(self, sources: list[SourceRecord]) -> list[SourceDocumentRecord]:
        source_documents: list[SourceDocumentRecord] = []
        for source in sources:
            source.sync_status = "awaiting_text_extraction"
            source_documents.append(
                SourceDocumentRecord(
                    document_id=f"zdoc-{source.source_id}-note",
                    source_id=source.source_id,
                    document_kind="note",
                    external_id=f"{source.source_id}-note",
                    filename="stub-note",
                    mime_type="text/plain",
                    ingest_status="imported",
                    raw_text_status="ready",
                    claim_extraction_status="queued",
                    locator=source.locator_hint or "note",
                    raw_text=source.abstract or source.title,
                    raw_metadata_json={"stub": True},
                )
            )
        return source_documents

    def bootstrap_stub_corpus(
        self,
    ) -> tuple[list[SourceRecord], list[SourceDocumentRecord], list[TextUnit]]:
        sources = self._stub_sources()
        source_documents = self._stub_source_documents(sources)
        text_units = self._stub_text_units(sources)
        return sources, source_documents, text_units
