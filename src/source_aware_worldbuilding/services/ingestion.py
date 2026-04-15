from __future__ import annotations

from collections.abc import Callable
from uuid import uuid4

from source_aware_worldbuilding.domain.enums import ExtractionRunStatus
from source_aware_worldbuilding.domain.errors import ZoteroConfigError
from source_aware_worldbuilding.domain.models import (
    ExtractionOutput,
    ExtractionRun,
    SourceDetailResponse,
    SourceDocumentRecord,
    SourceRecord,
    TextUnit,
    ZoteroPullRequest,
    ZoteroPullResult,
    summarize_source_documents,
    sync_source_with_documents,
)
from source_aware_worldbuilding.ports import (
    CandidateStorePort,
    CorpusPort,
    EvidenceStorePort,
    ExtractionPort,
    ExtractionRunStorePort,
    SourceDocumentStorePort,
    SourceStorePort,
    TextUnitStorePort,
)


class IngestionService:
    def __init__(
        self,
        corpus: CorpusPort,
        extractor: ExtractionPort,
        source_store: SourceStorePort,
        text_unit_store: TextUnitStorePort,
        source_document_store: SourceDocumentStorePort | None,
        run_store: ExtractionRunStorePort,
        candidate_store: CandidateStorePort,
        evidence_store: EvidenceStorePort,
    ):
        self.corpus = corpus
        self.extractor = extractor
        self.source_store = source_store
        self.text_unit_store = text_unit_store
        self.source_document_store = source_document_store
        self.run_store = run_store
        self.candidate_store = candidate_store
        self.evidence_store = evidence_store

    def pull_sources(self, request: ZoteroPullRequest | None = None) -> ZoteroPullResult:
        request = request or ZoteroPullRequest()
        existing_sources = {item.source_id: item for item in self.source_store.list_sources()}
        requested_item_keys = set(request.item_keys)
        for source_id in request.source_ids:
            source = existing_sources.get(source_id) or self.source_store.get_source(source_id)
            if source and source.zotero_item_key:
                requested_item_keys.add(source.zotero_item_key)

        if request.item_keys or request.source_ids:
            if not requested_item_keys:
                return ZoteroPullResult(
                    warnings=[
                        "No live Zotero item keys were available for the requested pull scope."
                    ]
                )
            sources = self.corpus.pull_sources_by_item_keys(sorted(requested_item_keys))
        else:
            sources = self.corpus.pull_sources()

        if not sources:
            return ZoteroPullResult(
                warnings=["No Zotero sources matched the requested pull scope."]
            )

        existing_documents: list[SourceDocumentRecord] = []
        if self.source_document_store is not None:
            source_ids = {source.source_id for source in sources}
            existing_documents = [
                item
                for item in self.source_document_store.list_source_documents()
                if item.source_id in source_ids
            ]

        try:
            source_documents = self.corpus.discover_source_documents(
                sources,
                existing_documents=existing_documents,
                force_refresh=request.force_refresh,
            )
        except TypeError:
            source_documents = self.corpus.discover_source_documents(sources)
        source_documents_by_source: dict[str, list[SourceDocumentRecord]] = {}
        for document in source_documents:
            source_documents_by_source.setdefault(document.source_id, []).append(document)

        synced_sources = []
        for source in sources:
            source_documents_for_source = source_documents_by_source.get(source.source_id, [])
            synced = sync_source_with_documents(source, source_documents_for_source)
            if not source_documents_for_source:
                synced.workflow_stage = "attention_required"
                synced.stage_errors = [
                    "No child notes or attachments were discovered for this item."
                ]
                synced.stage_summary = {"total": 0, "missing": 1}
            synced_sources.append(synced)

        self.source_store.save_sources(synced_sources)
        if self.source_document_store is not None:
            self.source_document_store.save_source_documents(source_documents)

        existing_source_payloads = {
            key: self._stable_source_payload(value) for key, value in existing_sources.items()
        }
        existing_document_payloads = {
            item.document_id: self._stable_document_payload(item) for item in existing_documents
        }
        inserted_source_count = 0
        updated_source_count = 0
        unchanged_source_count = 0
        for source in synced_sources:
            if source.source_id not in existing_source_payloads:
                inserted_source_count += 1
            elif existing_source_payloads[source.source_id] == self._stable_source_payload(source):
                unchanged_source_count += 1
            else:
                updated_source_count += 1

        inserted_document_count = 0
        updated_document_count = 0
        unchanged_document_count = 0
        failed_document_count = 0
        for document in source_documents:
            if document.document_id not in existing_document_payloads:
                inserted_document_count += 1
            elif (
                existing_document_payloads[document.document_id]
                == self._stable_document_payload(document)
            ):
                unchanged_document_count += 1
            else:
                updated_document_count += 1
            if document.metadata_import_status == "failed" or any(
                status == "failed"
                for status in (
                    document.attachment_fetch_status,
                    document.text_extraction_status,
                    document.normalization_status,
                )
            ):
                failed_document_count += 1

        warnings = list(
            dict.fromkeys(
                error
                for document in source_documents
                for error in document.stage_errors
                if error
            )
        )
        return ZoteroPullResult(
            count=len(synced_sources),
            sources=synced_sources,
            source_documents=source_documents,
            inserted_source_count=inserted_source_count,
            updated_source_count=updated_source_count,
            unchanged_source_count=unchanged_source_count,
            inserted_document_count=inserted_document_count,
            updated_document_count=updated_document_count,
            unchanged_document_count=unchanged_document_count,
            failed_document_count=failed_document_count,
            warnings=warnings,
        )

    def list_sources(self) -> list[SourceRecord]:
        return self.source_store.list_sources()

    def get_source_detail(self, source_id: str) -> SourceDetailResponse | None:
        source = self.source_store.get_source(source_id)
        if source is None:
            return None
        source_documents = (
            self.source_document_store.list_source_documents(source_id=source_id)
            if self.source_document_store is not None
            else []
        )
        return SourceDetailResponse(
            source=source,
            source_documents=source_documents,
            text_units=self.text_unit_store.list_text_units(source_id=source_id),
            stage_summary=source.stage_summary or summarize_source_documents(source_documents),
            stage_errors=source.stage_errors,
        )

    def get_source_text_units(self, source_id: str | None = None) -> list[TextUnit]:
        return self.text_unit_store.list_text_units(source_id=source_id)

    def list_runs(self) -> list[ExtractionRun]:
        return self.run_store.list_runs()

    def extract_candidates(
        self,
        *,
        source_ids: list[str] | None = None,
        checkpoint: Callable[[], None] | None = None,
    ) -> ExtractionOutput:
        source_id_filter = set(source_ids or [])
        if checkpoint is not None:
            checkpoint()
        sources = self.source_store.list_sources()
        if source_id_filter:
            sources = [item for item in sources if item.source_id in source_id_filter]
        elif not sources:
            sources = self._hydrate_sources_for_extraction()

        if source_id_filter:
            text_units: list[TextUnit] = []
            for source_id in source_id_filter:
                if checkpoint is not None:
                    checkpoint()
                text_units.extend(self.text_unit_store.list_text_units(source_id=source_id))
        else:
            text_units = self.text_unit_store.list_text_units()

        run = ExtractionRun(run_id=f"run-{uuid4().hex[:12]}", status=ExtractionRunStatus.RUNNING)
        self.run_store.save_run(run)
        self._mark_document_extraction_status(text_units, "running")
        try:
            if checkpoint is not None:
                checkpoint()
            output = self.extractor.extract_candidates(
                run=run,
                sources=sources,
                text_units=text_units,
            )
            self.evidence_store.save_evidence(output.evidence)
            self.candidate_store.save_candidates(output.candidates)
            output.run.status = ExtractionRunStatus.COMPLETED
            self.run_store.update_run(output.run)
            self._mark_document_extraction_status(text_units, "completed")
            return output
        except Exception as exc:
            run.status = ExtractionRunStatus.FAILED
            run.error = str(exc)
            self.run_store.update_run(run)
            self._mark_document_extraction_status(text_units, "failed")
            raise

    def _mark_document_extraction_status(self, text_units: list[TextUnit], status: str) -> None:
        if self.source_document_store is None:
            return
        document_ids: set[str] = set()
        for text_unit in text_units:
            notes = text_unit.notes or ""
            marker = "source_document_id="
            if marker not in notes:
                continue
            document_id = notes.split(marker, 1)[1].split(";", 1)[0].strip()
            if document_id:
                document_ids.add(document_id)
        if not document_ids:
            return
        for document in self.source_document_store.list_source_documents():
            if document.document_id in document_ids:
                document.claim_extraction_status = status
                self.source_document_store.update_source_document(document)

    def _hydrate_sources_for_extraction(self) -> list[SourceRecord]:
        try:
            return self.pull_sources().sources
        except ZoteroConfigError:
            bootstrap_stub_corpus = getattr(self.corpus, "bootstrap_stub_corpus", None)
            if not callable(bootstrap_stub_corpus):
                raise

        sources, source_documents, text_units = bootstrap_stub_corpus()
        source_documents_by_source: dict[str, list[SourceDocumentRecord]] = {}
        for document in source_documents:
            source_documents_by_source.setdefault(document.source_id, []).append(document)

        synced_sources = []
        for source in sources:
            synced_sources.append(
                sync_source_with_documents(
                    source,
                    source_documents_by_source.get(source.source_id, []),
                )
            )

        self.source_store.save_sources(synced_sources)
        if self.source_document_store is not None:
            self.source_document_store.save_source_documents(source_documents)
        self.text_unit_store.save_text_units(text_units)
        return synced_sources

    def _stable_source_payload(self, source: SourceRecord) -> dict[str, object]:
        payload = source.model_dump(mode="json")
        payload.pop("last_synced_at", None)
        return payload

    def _stable_document_payload(self, document: SourceDocumentRecord) -> dict[str, object]:
        payload = document.model_dump(mode="json")
        payload.pop("last_synced_at", None)
        return payload
