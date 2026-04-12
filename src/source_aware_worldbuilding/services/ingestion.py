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
        run_store: ExtractionRunStorePort,
        candidate_store: CandidateStorePort,
        evidence_store: EvidenceStorePort,
    ):
        self.corpus = corpus
        self.extractor = extractor
        self.source_store = source_store
        self.text_unit_store = text_unit_store
        self.run_store = run_store
        self.candidate_store = candidate_store
        self.evidence_store = evidence_store

    def pull_sources(self) -> list[SourceRecord]:
        sources = self.corpus.pull_sources()
        text_units = self.corpus.pull_text_units(sources)
        self.source_store.save_sources(sources)
        self.text_unit_store.save_text_units(text_units)
        return sources

    def list_sources(self) -> list[SourceRecord]:
        return self.source_store.list_sources()

    def get_source_text_units(self, source_id: str | None = None) -> list[TextUnit]:
        return self.text_unit_store.list_text_units(source_id=source_id)

    def list_runs(self) -> list[ExtractionRun]:
        return self.run_store.list_runs()

    def extract_candidates(self) -> ExtractionOutput:
        sources = self.source_store.list_sources()
        if not sources:
            sources = self.pull_sources()
        text_units = self.text_unit_store.list_text_units()

        run = ExtractionRun(run_id=f"run-{uuid4().hex[:12]}", status=ExtractionRunStatus.RUNNING)
        self.run_store.save_run(run)
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
            return output
        except Exception as exc:
            run.status = ExtractionRunStatus.FAILED
            run.error = str(exc)
            self.run_store.update_run(run)
            raise
