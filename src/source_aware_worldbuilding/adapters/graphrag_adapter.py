from __future__ import annotations

from source_aware_worldbuilding.adapters.heuristic_extraction import HeuristicExtractionAdapter
from source_aware_worldbuilding.domain.models import (
    ExtractionOutput,
    ExtractionRun,
    SourceRecord,
    TextUnit,
)


class GraphRAGExtractionAdapter:
    """Preview seam for a future GraphRAG-backed extractor.

    The real GraphRAG pipeline is not wired in yet, so this adapter currently
    delegates to the heuristic backend while preserving the backend selection seam.
    """

    backend_name = "graphrag_preview"

    def __init__(self, fallback: HeuristicExtractionAdapter | None = None):
        self.fallback = fallback or HeuristicExtractionAdapter()

    def extract_candidates(
        self,
        run: ExtractionRun,
        sources: list[SourceRecord],
        text_units: list[TextUnit],
    ) -> ExtractionOutput:
        output = self.fallback.extract_candidates(run=run, sources=sources, text_units=text_units)
        output.run.notes = self._merge_notes(
            output.run.notes,
            "GraphRAG backend is selected in preview mode; "
            "heuristic extraction generated the candidates.",
        )

        for candidate in output.candidates:
            candidate.notes = self._merge_notes(
                candidate.notes,
                "Extractor backend: graphrag_preview.",
            )
        return output

    def _merge_notes(self, existing: str | None, addition: str) -> str:
        if not existing:
            return addition
        if addition in existing:
            return existing
        return f"{existing} {addition}"
