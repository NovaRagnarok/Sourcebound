from __future__ import annotations

from source_aware_worldbuilding.domain.models import CandidateClaim, SourceRecord
from source_aware_worldbuilding.ports import CandidateStorePort, CorpusPort, ExtractionPort


class IngestionService:
    def __init__(
        self,
        corpus: CorpusPort,
        extractor: ExtractionPort,
        candidate_store: CandidateStorePort,
    ):
        self.corpus = corpus
        self.extractor = extractor
        self.candidate_store = candidate_store

    def pull_sources(self) -> list[SourceRecord]:
        return self.corpus.pull_sources()

    def extract_candidates(self) -> list[CandidateClaim]:
        sources = self.pull_sources()
        candidates = self.extractor.extract_candidates(sources)
        self.candidate_store.save_candidates(candidates)
        return candidates
