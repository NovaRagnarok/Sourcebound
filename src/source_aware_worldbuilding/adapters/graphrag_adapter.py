from __future__ import annotations

import asyncio
import importlib.util
import json
import re
from dataclasses import dataclass
from hashlib import sha1
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from source_aware_worldbuilding.adapters.heuristic_extraction import HeuristicExtractionAdapter
from source_aware_worldbuilding.domain.enums import ClaimKind, ClaimStatus, ReviewState
from source_aware_worldbuilding.domain.models import (
    CandidateClaim,
    EvidenceSnippet,
    ExtractionOutput,
    ExtractionRun,
    SourceRecord,
    TextUnit,
)
from source_aware_worldbuilding.domain.normalization import (
    infer_place,
    infer_time_range,
    normalized_candidate_key,
)
from source_aware_worldbuilding.settings import settings

GRAPH_RAG_REQUIRED_TABLES = ("covariates", "text_units", "documents")
GRAPH_RAG_STATUS_MAP = {
    "TRUE": ClaimStatus.VERIFIED,
    "FALSE": ClaimStatus.CONTESTED,
    "SUSPECTED": ClaimStatus.PROBABLE,
}
CONFIG_FILE_NAMES = ("settings.yaml", "settings.yml", "settings.json")
WORD_BOUNDARY_RE = re.compile(r"[^0-9a-zA-Z]+")


class GraphRAGExtractionError(RuntimeError):
    """Raised when the GraphRAG extraction backend cannot produce usable candidates."""


@dataclass(slots=True)
class GraphRAGArtifactBundle:
    covariates: list[dict[str, Any]]
    text_units: list[dict[str, Any]]
    documents: list[dict[str, Any]]
    output_dir: Path


@dataclass(slots=True)
class GraphRAGRuntimeProbe:
    configured: bool
    ready: bool
    detail: str
    mode: str


class GraphRAGExtractionAdapter:
    backend_name = "graphrag"

    def __init__(
        self,
        *,
        graph_rag_root: Path | None = None,
        mode: str | None = None,
        artifacts_dir: Path | None = None,
        claim_kind_helper: HeuristicExtractionAdapter | None = None,
    ):
        self.graph_rag_root = Path(graph_rag_root or settings.graph_rag_root)
        self.mode = mode or settings.graph_rag_mode
        configured_artifacts_dir = artifacts_dir or settings.graph_rag_artifacts_dir
        self.artifacts_dir = Path(configured_artifacts_dir) if configured_artifacts_dir else None
        self.claim_kind_helper = claim_kind_helper or HeuristicExtractionAdapter()

    @classmethod
    def runtime_probe(cls) -> GraphRAGRuntimeProbe:
        mode = settings.graph_rag_mode
        root = Path(settings.graph_rag_root)
        config_file = _find_graph_rag_config_file(root)

        if mode == "in_process":
            if importlib.util.find_spec("graphrag") is None:
                return GraphRAGRuntimeProbe(
                    configured=False,
                    ready=False,
                    mode=f"graphrag:{mode}",
                    detail=(
                        "GraphRAG mode is enabled, but the optional `graphrag` "
                        "package is not installed."
                    ),
                )
            if config_file is None:
                return GraphRAGRuntimeProbe(
                    configured=False,
                    ready=False,
                    mode=f"graphrag:{mode}",
                    detail=(
                        f"GraphRAG root {root} is missing settings.yaml/settings.yml/settings.json."
                    ),
                )
            return GraphRAGRuntimeProbe(
                configured=True,
                ready=True,
                mode=f"graphrag:{mode}",
                detail=f"GraphRAG in-process extraction is configured from {root}.",
            )

        if importlib.util.find_spec("pandas") is None:
            return GraphRAGRuntimeProbe(
                configured=False,
                ready=False,
                mode=f"graphrag:{mode}",
                detail="Artifact import requires `pandas` with parquet support.",
            )

        output_dir = resolve_graph_rag_artifacts_dir(
            root=root,
            explicit_artifacts_dir=settings.graph_rag_artifacts_dir,
        )
        missing = [
            table
            for table in GRAPH_RAG_REQUIRED_TABLES
            if not output_dir.joinpath(f"{table}.parquet").exists()
        ]
        if missing:
            return GraphRAGRuntimeProbe(
                configured=False,
                ready=False,
                mode=f"graphrag:{mode}",
                detail=(
                    "GraphRAG artifact import expects "
                    f"{', '.join(missing)} parquet files in {output_dir}."
                ),
            )
        return GraphRAGRuntimeProbe(
            configured=True,
            ready=True,
            mode=f"graphrag:{mode}",
            detail=f"GraphRAG artifact import is configured from {output_dir}.",
        )

    def extract_candidates(
        self,
        run: ExtractionRun,
        sources: list[SourceRecord],
        text_units: list[TextUnit],
    ) -> ExtractionOutput:
        if not text_units:
            raise GraphRAGExtractionError("GraphRAG extraction requires at least one text unit.")

        artifacts = (
            self._run_in_process_graph_rag(text_units)
            if self.mode == "in_process"
            else self._load_graph_rag_artifacts(
                output_dir=resolve_graph_rag_artifacts_dir(
                    root=self.graph_rag_root,
                    explicit_artifacts_dir=self.artifacts_dir,
                )
            )
        )
        output = self._map_artifacts_to_output(
            artifacts=artifacts,
            run=run,
            sources=sources,
            source_text_units=text_units,
        )
        output.run.notes = self._merge_notes(
            output.run.notes,
            f"Extractor backend: graphrag (mode={self.mode}, output_dir={artifacts.output_dir}).",
        )
        return output

    def _run_in_process_graph_rag(self, text_units: list[TextUnit]) -> GraphRAGArtifactBundle:
        runtime = self.runtime_probe()
        if not runtime.ready or runtime.mode != f"graphrag:{self.mode}":
            raise GraphRAGExtractionError(runtime.detail)

        document_rows = [self._document_row(text_unit) for text_unit in text_units]
        with TemporaryDirectory(prefix="sourcebound-graphrag-") as temp_dir:
            output_dir = Path(temp_dir) / "output"
            config = self._load_graph_rag_config(
                self.graph_rag_root,
                output_dir=output_dir,
            )
            self._invoke_graph_rag_pipeline(config=config, document_rows=document_rows)
            return self._load_graph_rag_artifacts(output_dir=output_dir)

    def _load_graph_rag_artifacts(self, output_dir: Path | None = None) -> GraphRAGArtifactBundle:
        output_dir = output_dir or resolve_graph_rag_artifacts_dir(
            root=self.graph_rag_root,
            explicit_artifacts_dir=self.artifacts_dir,
        )
        tables = {
            table_name: self._read_parquet_records(output_dir / f"{table_name}.parquet")
            for table_name in GRAPH_RAG_REQUIRED_TABLES
        }
        return GraphRAGArtifactBundle(
            covariates=tables["covariates"],
            text_units=tables["text_units"],
            documents=tables["documents"],
            output_dir=output_dir,
        )

    def _load_graph_rag_config(self, root: Path, *, output_dir: Path):
        if importlib.util.find_spec("graphrag") is None:
            raise GraphRAGExtractionError("GraphRAG package is not installed.")

        from graphrag.config.load_config import load_config

        return load_config(
            root,
            cli_overrides={"output": {"base_dir": str(output_dir)}},
        )

    def _invoke_graph_rag_pipeline(
        self,
        *,
        config,
        document_rows: list[dict[str, Any]],
    ) -> None:
        if importlib.util.find_spec("graphrag") is None:
            raise GraphRAGExtractionError("GraphRAG package is not installed.")
        if importlib.util.find_spec("pandas") is None:
            raise GraphRAGExtractionError("GraphRAG in-process mode requires `pandas`.")

        import pandas as pd
        from graphrag.api.index import build_index

        try:
            asyncio.run(
                build_index(
                    config=config,
                    input_documents=pd.DataFrame(document_rows),
                )
            )
        except Exception as exc:
            raise GraphRAGExtractionError(f"GraphRAG indexing failed: {exc}") from exc

    def _read_parquet_records(self, path: Path) -> list[dict[str, Any]]:
        if not path.exists():
            raise GraphRAGExtractionError(f"GraphRAG artifact {path} does not exist.")
        if importlib.util.find_spec("pandas") is None:
            raise GraphRAGExtractionError("Artifact import requires `pandas` with parquet support.")

        import pandas as pd

        try:
            frame = pd.read_parquet(path)
        except Exception as exc:
            raise GraphRAGExtractionError(
                f"Could not read GraphRAG artifact {path}: {exc}"
            ) from exc
        return frame.to_dict(orient="records")

    def _map_artifacts_to_output(
        self,
        *,
        artifacts: GraphRAGArtifactBundle,
        run: ExtractionRun,
        sources: list[SourceRecord],
        source_text_units: list[TextUnit],
    ) -> ExtractionOutput:
        source_by_id = {source.source_id: source for source in sources}
        app_text_units_by_id = {
            text_unit.text_unit_id: text_unit for text_unit in source_text_units
        }
        graphrag_text_unit_by_id = {
            str(item.get("id")): item for item in artifacts.text_units if item.get("id") is not None
        }
        document_by_id = {
            str(item.get("id")): item for item in artifacts.documents if item.get("id") is not None
        }

        evidence_by_key: dict[tuple[Any, ...], EvidenceSnippet] = {}
        candidate_by_key: dict[tuple[str, str, str], CandidateClaim] = {}
        skipped_missing_join = 0
        skipped_unmappable_span = 0
        total_claim_rows = 0

        for covariate in artifacts.covariates:
            if str(covariate.get("covariate_type") or "claim").lower() != "claim":
                continue
            total_claim_rows += 1

            app_text_unit = self._resolve_source_text_unit(
                covariate=covariate,
                graphrag_text_unit_by_id=graphrag_text_unit_by_id,
                document_by_id=document_by_id,
                app_text_units_by_id=app_text_units_by_id,
            )
            if app_text_unit is None:
                skipped_missing_join += 1
                continue

            span = self._resolve_span(app_text_unit.text, str(covariate.get("source_text") or ""))
            if span is None:
                skipped_unmappable_span += 1
                continue
            span_start, span_end = span
            evidence_text = app_text_unit.text[span_start:span_end]

            source = source_by_id.get(app_text_unit.source_id) or SourceRecord(
                source_id=app_text_unit.source_id,
                title=app_text_unit.source_id,
            )
            evidence = self._upsert_evidence(
                evidence_by_key=evidence_by_key,
                run=run,
                text_unit=app_text_unit,
                text=evidence_text,
                span_start=span_start,
                span_end=span_end,
            )

            candidate = self._candidate_from_covariate(
                covariate=covariate,
                run=run,
                source=source,
                evidence=evidence,
            )
            candidate_key = normalized_candidate_key(
                candidate.subject,
                candidate.predicate,
                candidate.value,
            )
            existing = candidate_by_key.get(candidate_key)
            if existing is not None:
                if evidence.evidence_id not in existing.evidence_ids:
                    existing.evidence_ids.append(evidence.evidence_id)
                continue
            candidate_by_key[candidate_key] = candidate

        candidates = list(candidate_by_key.values())
        if not candidates:
            raise GraphRAGExtractionError(
                "GraphRAG did not produce any usable span-backed candidates "
                f"(claim_rows={total_claim_rows}, missing_join={skipped_missing_join}, "
                f"unmappable_span={skipped_unmappable_span})."
            )

        run.candidate_count = len(candidates)
        run.text_unit_count = len(source_text_units)
        run.source_count = len(sources)
        run.notes = self._merge_notes(
            run.notes,
            " ".join(
                [
                    f"GraphRAG mapped {len(candidates)} candidates "
                    f"from {total_claim_rows} claim rows.",
                    f"Skipped {skipped_missing_join} rows without a source text-unit join.",
                    f"Skipped {skipped_unmappable_span} rows without a resolvable evidence span.",
                ]
            ),
        )
        return ExtractionOutput(
            run=run,
            candidates=candidates,
            evidence=list(evidence_by_key.values()),
        )

    def _resolve_source_text_unit(
        self,
        *,
        covariate: dict[str, Any],
        graphrag_text_unit_by_id: dict[str, dict[str, Any]],
        document_by_id: dict[str, dict[str, Any]],
        app_text_units_by_id: dict[str, TextUnit],
    ) -> TextUnit | None:
        graphrag_text_unit = graphrag_text_unit_by_id.get(str(covariate.get("text_unit_id") or ""))
        if graphrag_text_unit is None:
            return None

        document = document_by_id.get(str(graphrag_text_unit.get("document_id") or ""))
        if document is None:
            return None

        metadata = _coerce_metadata(document.get("metadata"))
        app_text_unit_id = metadata.get("text_unit_id")
        if not app_text_unit_id:
            return None
        return app_text_units_by_id.get(str(app_text_unit_id))

    def _upsert_evidence(
        self,
        *,
        evidence_by_key: dict[tuple[Any, ...], EvidenceSnippet],
        run: ExtractionRun,
        text_unit: TextUnit,
        text: str,
        span_start: int,
        span_end: int,
    ) -> EvidenceSnippet:
        evidence_key = (
            text_unit.source_id,
            text_unit.text_unit_id,
            text_unit.locator,
            span_start,
            span_end,
            text,
        )
        existing = evidence_by_key.get(evidence_key)
        if existing is not None:
            return existing

        evidence_id = f"evi-{sha1(f'{run.run_id}:{evidence_key}'.encode()).hexdigest()[:12]}"
        evidence = EvidenceSnippet(
            evidence_id=evidence_id,
            source_id=text_unit.source_id,
            locator=text_unit.locator,
            text=text,
            text_unit_id=text_unit.text_unit_id,
            span_start=span_start,
            span_end=span_end,
            notes=f"GraphRAG evidence span generated during extraction run {run.run_id}.",
            checksum=sha1(text.encode()).hexdigest(),
        )
        evidence_by_key[evidence_key] = evidence
        return evidence

    def _candidate_from_covariate(
        self,
        *,
        covariate: dict[str, Any],
        run: ExtractionRun,
        source: SourceRecord,
        evidence: EvidenceSnippet,
    ) -> CandidateClaim:
        claim_text = str(
            covariate.get("description") or covariate.get("source_text") or evidence.text
        ).strip()
        subject = str(covariate.get("subject_id") or source.title).strip() or source.title
        predicate = _snake_case(str(covariate.get("type") or "described_as")) or "described_as"
        value = str(covariate.get("object_id") or covariate.get("description") or "").strip()
        if not value:
            value = evidence.text[:160]

        time_start = _clean_optional_string(covariate.get("start_date"))
        time_end = _clean_optional_string(covariate.get("end_date"))
        if not time_start and not time_end:
            inferred_start, inferred_end = infer_time_range(claim_text, source.year)
            time_start = inferred_start
            time_end = inferred_end

        candidate_key = normalized_candidate_key(subject, predicate, value)
        candidate_seed = sha1(f"{run.run_id}:{'|'.join(candidate_key)}".encode()).hexdigest()[:12]
        raw_status = str(covariate.get("status") or "").upper()
        notes = self._merge_notes(
            _clean_optional_string(covariate.get("description")),
            "Extractor backend: graphrag "
            f"(mode={self.mode}, raw_status={raw_status or 'unknown'}).",
        )

        return CandidateClaim(
            candidate_id=f"cand-{candidate_seed}",
            subject=subject,
            predicate=predicate,
            value=value[:160],
            claim_kind=self._claim_kind(claim_text or evidence.text, source),
            status_suggestion=GRAPH_RAG_STATUS_MAP.get(raw_status, ClaimStatus.PROBABLE),
            review_state=ReviewState.PENDING,
            place=infer_place(claim_text or evidence.text, source),
            time_start=time_start,
            time_end=time_end,
            evidence_ids=[evidence.evidence_id],
            extractor_run_id=run.run_id,
            notes=notes,
        )

    def _claim_kind(self, text: str, source: SourceRecord) -> ClaimKind:
        return self.claim_kind_helper._claim_kind(text, source)

    def _document_row(self, text_unit: TextUnit) -> dict[str, Any]:
        document_seed = text_unit.checksum or text_unit.text_unit_id or text_unit.text
        document_id = sha1(f"sourcebound:{document_seed}".encode()).hexdigest()
        return {
            "id": document_id,
            "text": text_unit.text,
            "title": text_unit.locator,
            "creation_date": None,
            "metadata": {
                "text_unit_id": text_unit.text_unit_id,
                "source_id": text_unit.source_id,
                "locator": text_unit.locator,
                "ordinal": text_unit.ordinal,
            },
        }

    def _resolve_span(self, haystack: str, needle: str) -> tuple[int, int] | None:
        excerpt = needle.strip()
        if not haystack.strip() or not excerpt:
            return None

        exact = haystack.find(needle)
        if exact >= 0:
            return exact, exact + len(needle)

        direct = haystack.find(excerpt)
        if direct >= 0:
            return direct, direct + len(excerpt)

        pattern = r"\s+".join(re.escape(part) for part in excerpt.split())
        whitespace_flexible = re.search(pattern, haystack, flags=re.IGNORECASE)
        if whitespace_flexible is not None:
            return whitespace_flexible.span()
        return None

    def _merge_notes(self, existing: str | None, addition: str | None) -> str | None:
        if not addition:
            return existing
        if not existing:
            return addition
        if addition in existing:
            return existing
        return f"{existing} {addition}"


def resolve_graph_rag_artifacts_dir(
    *,
    root: Path,
    explicit_artifacts_dir: Path | str | None,
) -> Path:
    if explicit_artifacts_dir:
        explicit_path = Path(explicit_artifacts_dir)
        return explicit_path if explicit_path.is_absolute() else root / explicit_path

    config_output_dir = _graph_rag_output_base_dir_from_config(root)
    if config_output_dir is not None:
        return config_output_dir
    return root / "output"


def _graph_rag_output_base_dir_from_config(root: Path) -> Path | None:
    if importlib.util.find_spec("graphrag") is None or _find_graph_rag_config_file(root) is None:
        return None

    try:
        from graphrag.config.load_config import load_config

        config = load_config(root)
    except Exception:
        return None

    output_config = getattr(config, "output", None)
    base_dir = getattr(output_config, "base_dir", None)
    if not base_dir:
        return None

    path = Path(str(base_dir))
    return path if path.is_absolute() else root / path


def _find_graph_rag_config_file(root: Path) -> Path | None:
    for name in CONFIG_FILE_NAMES:
        candidate = root / name
        if candidate.exists():
            return candidate
    return None


def _coerce_metadata(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        if isinstance(parsed, dict):
            return parsed
    return {}


def _clean_optional_string(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _snake_case(value: str) -> str:
    normalized = WORD_BOUNDARY_RE.sub("_", value).strip("_")
    return normalized.lower()
