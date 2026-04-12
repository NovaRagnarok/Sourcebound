from __future__ import annotations

from source_aware_worldbuilding.domain.models import SourceRecord


class ZoteroCorpusAdapter:
    """Placeholder adapter.

    Replace this with real Zotero API logic that:
    - lists a library or collection
    - resolves item metadata
    - fetches attachments or linked files
    - emits normalized SourceRecord entries
    """

    def pull_sources(self) -> list[SourceRecord]:
        return [
            SourceRecord(
                source_id="stub-source-1",
                title="Stub source pulled from Zotero adapter",
                author="System",
                year="2026",
                source_type="document",
            )
        ]
