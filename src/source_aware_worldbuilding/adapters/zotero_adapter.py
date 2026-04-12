from __future__ import annotations

from hashlib import sha1

import httpx

from source_aware_worldbuilding.domain.models import SourceRecord, TextUnit
from source_aware_worldbuilding.settings import settings


class ZoteroCorpusAdapter:
    """Pull a narrow Zotero corpus, with a fixture-friendly local fallback."""

    def pull_sources(self) -> list[SourceRecord]:
        if not settings.zotero_library_id:
            return self._stub_sources()

        endpoint = self._items_endpoint()
        headers = {"Zotero-API-Version": "3"}
        if settings.zotero_api_key:
            headers["Zotero-API-Key"] = settings.zotero_api_key

        response = httpx.get(endpoint, headers=headers, params={"limit": 50}, timeout=20.0)
        response.raise_for_status()
        payload = response.json()

        sources: list[SourceRecord] = []
        for item in payload:
            data = item.get("data", {})
            item_key = item.get("key")
            creators = data.get("creators") or []
            author = (
                ", ".join(
                    filter(
                        None,
                        [
                            " ".join(
                                filter(None, [creator.get("firstName"), creator.get("lastName")])
                            ).strip()
                            for creator in creators[:2]
                        ],
                    )
                )
                or None
            )
            year = None
            date_value = data.get("date")
            if isinstance(date_value, str) and date_value:
                year = date_value[:4]
            sources.append(
                SourceRecord(
                    source_id=f"zotero-{item_key}",
                    title=data.get("title") or f"Untitled Zotero item {item_key}",
                    author=author,
                    year=year,
                    source_type=data.get("itemType", "document"),
                    locator_hint=data.get("archiveLocation") or data.get("callNumber"),
                    zotero_item_key=item_key,
                    collection_key=settings.zotero_collection_key,
                    abstract=data.get("abstractNote") or None,
                    url=data.get("url") or None,
                )
            )
        return sources

    def pull_text_units(self, sources: list[SourceRecord]) -> list[TextUnit]:
        text_units: list[TextUnit] = []
        for index, source in enumerate(sources, start=1):
            body = (source.abstract or "").strip()
            if not body:
                fragments = [source.title]
                if source.author:
                    fragments.append(f"Author: {source.author}")
                if source.year:
                    fragments.append(f"Year: {source.year}")
                if source.locator_hint:
                    fragments.append(f"Locator: {source.locator_hint}")
                if source.url:
                    fragments.append(f"URL: {source.url}")
                body = ". ".join(fragment for fragment in fragments if fragment)
            checksum = sha1(body.encode("utf-8")).hexdigest()
            text_units.append(
                TextUnit(
                    text_unit_id=f"text-{source.source_id}-{index}",
                    source_id=source.source_id,
                    locator=source.locator_hint or "metadata",
                    text=body,
                    ordinal=index,
                    checksum=checksum,
                )
            )
        return text_units

    def _items_endpoint(self) -> str:
        library_root = (
            f"{settings.zotero_base_url}/"
            f"{settings.zotero_library_type}s/{settings.zotero_library_id}"
        )
        if settings.zotero_collection_key:
            return f"{library_root}/collections/{settings.zotero_collection_key}/items/top"
        return f"{library_root}/items/top"

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
