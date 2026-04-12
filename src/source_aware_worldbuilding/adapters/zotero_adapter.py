from __future__ import annotations

import re
from hashlib import sha1
from html import unescape

import httpx
from tenacity import Retrying, retry_if_exception, stop_after_attempt, wait_exponential

from source_aware_worldbuilding.domain.models import SourceRecord, TextUnit
from source_aware_worldbuilding.settings import settings

HTML_BREAK_RE = re.compile(r"(?i)</?(?:p|div|li|ul|ol|br|hr)[^>]*>")
HTML_TAG_RE = re.compile(r"<[^>]+>")


class ZoteroCorpusAdapter:
    """Pull a narrow Zotero corpus, including note-derived text units."""

    def pull_sources(self) -> list[SourceRecord]:
        if not settings.zotero_library_id:
            return self._stub_sources()

        sources: list[SourceRecord] = []
        with self._client() as client:
            for item in self._request_paginated(client, self._top_items_path()):
                data = item.get("data", {})
                item_key = item.get("key")
                if not item_key:
                    continue

                creators = data.get("creators") or []
                author = (
                    ", ".join(
                        filter(None, [self._creator_name(creator) for creator in creators[:2]])
                    )
                    or None
                )
                date_value = data.get("date") or ""
                year = date_value[:4] if isinstance(date_value, str) and date_value else None
                sources.append(
                    SourceRecord(
                        source_id=f"zotero-{item_key}",
                        title=data.get("title") or f"Untitled Zotero item {item_key}",
                        author=author,
                        year=year,
                        source_type=data.get("itemType", "document"),
                        locator_hint=data.get("archiveLocation") or data.get("callNumber"),
                        zotero_item_key=item_key,
                        collection_key=self._collection_key_from_item(data),
                        abstract=self._clean_text(data.get("abstractNote")),
                        url=data.get("url") or None,
                    )
                )
        return sources

    def pull_text_units(self, sources: list[SourceRecord]) -> list[TextUnit]:
        if not settings.zotero_library_id:
            return self._stub_text_units(sources)

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
        params: dict[str, int],
    ) -> httpx.Response:
        for attempt in Retrying(
            stop=stop_after_attempt(3),
            wait=wait_exponential(multiplier=0.2, min=0.2, max=1.0),
            retry=retry_if_exception(self._is_retryable_error),
            reraise=True,
        ):
            with attempt:
                response = client.get(path, params=params)
                response.raise_for_status()
                return response
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
        if isinstance(exc, httpx.HTTPStatusError):
            status_code = exc.response.status_code
            return status_code in {408, 429} or status_code >= 500
        return False

    def _stub_sources(self) -> list[SourceRecord]:
        return [
            SourceRecord(
                source_id="src-1",
                title="Municipal price records of Rouen",
                author="City clerk",
                year="1421",
                source_type="record",
                locator_hint="folios 10-14",
                abstract="Bread prices rose sharply during the winter shortage.",
            ),
            SourceRecord(
                source_id="src-2",
                title="Later chronicle of unrest",
                author="Anonymous chronicler",
                year="1450",
                source_type="chronicle",
                locator_hint="chapter 7",
                abstract="Townspeople whispered that merchants were withholding grain.",
            ),
        ]

    def _stub_text_units(self, sources: list[SourceRecord]) -> list[TextUnit]:
        text_units: list[TextUnit] = []
        for source in sources:
            for ordinal, fragment in enumerate(self._source_fragments(source), start=1):
                text_units.append(self._build_text_unit(source, ordinal, "metadata", fragment))
        return text_units
