from __future__ import annotations

from uuid import uuid4

from source_aware_worldbuilding.domain.enums import ExtractionRunStatus
from source_aware_worldbuilding.domain.models import (
    ExtractionOutput,
    ExtractionRun,
    SourceRecord,
    TextUnit,
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

    def pull_sources(self) -> list[SourceRecord]:
        sources = self.corpus.pull_sources()
        source_documents = self.corpus.discover_source_documents(sources)
        self.source_store.save_sources(sources)
        if self.source_document_store is not None:
            self.source_document_store.save_source_documents(source_documents)
        return sources

    def list_sources(self) -> list[SourceRecord]:
        return self.source_store.list_sources()

    def get_source_text_units(self, source_id: str | None = None) -> list[TextUnit]:
        return self.text_unit_store.list_text_units(source_id=source_id)

    def list_runs(self) -> list[ExtractionRun]:
        return self.run_store.list_runs()

    def extract_candidates(self, *, source_ids: list[str] | None = None) -> ExtractionOutput:
        source_id_filter = set(source_ids or [])
        sources = self.source_store.list_sources()
        if source_id_filter:
            sources = [item for item in sources if item.source_id in source_id_filter]
        elif not sources:
            sources = self.pull_sources()

        if source_id_filter:
            text_units: list[TextUnit] = []
            for source_id in source_id_filter:
                text_units.extend(self.text_unit_store.list_text_units(source_id=source_id))
        else:
            text_units = self.text_unit_store.list_text_units()

        run = ExtractionRun(run_id=f"run-{uuid4().hex[:12]}", status=ExtractionRunStatus.RUNNING)
        self.run_store.save_run(run)
        self._mark_document_extraction_status(text_units, "running")
        try:
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
