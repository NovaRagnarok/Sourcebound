from __future__ import annotations

from pathlib import Path

import httpx
from psycopg import connect

from source_aware_worldbuilding.adapters.graphrag_adapter import GraphRAGExtractionAdapter
from source_aware_worldbuilding.adapters.qdrant_adapter import (
    QdrantProjectionAdapter,
    QdrantResearchSemanticAdapter,
)
from source_aware_worldbuilding.domain.models import RuntimeDependencyStatus, RuntimeStatus
from source_aware_worldbuilding.settings import settings

_DEFAULT_STACK_BLOCKER_PREFIXES = (
    "Required because",
    "Required for local startup",
    "Required for the recommended default stack",
    "Non-default local mode detected",
)


def build_runtime_status() -> RuntimeStatus:
    services = [
        _state_store_status(),
        _truth_store_status(),
        _bible_workspace_status(),
        _bible_export_status(),
        _corpus_status(),
        _extraction_status(),
        _projection_status(),
        _research_semantics_status(),
        _job_worker_status(),
    ]
    next_steps = _next_steps(services)
    overall_status = _overall_status(services)

    return RuntimeStatus(
        app_name=settings.app_name,
        app_env=settings.app_env,
        operator_ui_enabled=settings.app_ui_enabled,
        state_backend=settings.app_state_backend,
        truth_backend=settings.app_truth_backend,
        extraction_backend="graphrag" if settings.graph_rag_enabled else "heuristic",
        overall_status=overall_status,
        services=services,
        next_steps=next_steps,
    )


def enforce_runtime_startup_checks(*, strict_runtime_checks: bool | None = None) -> None:
    issues = runtime_startup_issues(strict_runtime_checks=strict_runtime_checks)
    if issues:
        raise RuntimeError(_format_startup_validation_errors(issues))


def runtime_startup_issues(*, strict_runtime_checks: bool | None = None) -> list[str]:
    issues = _startup_validation_errors()

    strict = (
        settings.app_strict_startup_checks
        if strict_runtime_checks is None
        else strict_runtime_checks
    )
    if not strict or not settings.qdrant_enabled:
        return issues

    mode, _reachable, ready, detail = QdrantProjectionAdapter().runtime_probe()
    if ready:
        return issues

    next_step = (
        "Run `saw seed-dev-data` for the default newcomer path, or "
        "`saw qdrant-rebuild` to initialize and backfill the projection manually."
        if mode == "qdrant:uninitialized"
        else "Bring Qdrant online or disable strict startup checks for non-retrieval environments."
    )
    issues.append(
        f"{detail} Strict startup checks are enabled, so Sourcebound refused to start. {next_step}"
    )
    return issues


def default_stack_verification_issues(
    runtime_status: RuntimeStatus | None = None,
) -> list[str]:
    runtime = build_runtime_status() if runtime_status is None else runtime_status
    issues = [
        step
        for step in runtime.next_steps
        if step.startswith(_DEFAULT_STACK_BLOCKER_PREFIXES)
    ]
    if runtime.overall_status != "ready" and not issues:
        issues.append(
            "Current runtime status is not `ready`. Review `/health/runtime` and fix the "
            "remaining degraded service before treating the default stack as usable."
        )
    return issues


def _startup_validation_errors() -> list[str]:
    issues = [f"{issue.summary} {issue.fix}" for issue in settings.startup_validation_issues()]

    if settings.graph_rag_enabled:
        probe = GraphRAGExtractionAdapter.runtime_probe()
        if not probe.ready:
            issues.append(
                "GRAPH_RAG_ENABLED=true, but GraphRAG is not ready. "
                f"{probe.detail} Set `GRAPH_RAG_ENABLED=false` for the default local path, "
                "or finish the GraphRAG setup and install the optional dependency with "
                "`make bootstrap-graphrag` or `.venv/bin/python -m pip install -e .[graphrag]`."
            )

    return issues


def _format_startup_validation_errors(issues: list[str]) -> str:
    lines = ["Sourcebound cannot start with the current configuration:"]
    lines.extend(f"- {issue}" for issue in issues)
    return "\n".join(lines)


def _state_store_status() -> RuntimeDependencyStatus:
    backend = settings.app_state_backend
    if backend == "postgres":
        configured = bool(settings.app_postgres_dsn)
        reachable, detail = (
            _probe_postgres(settings.app_postgres_dsn)
            if configured
            else (
                None,
                "APP_POSTGRES_DSN is required when Postgres is selected for app state. "
                "Run `cp .env.example .env` or set "
                "`APP_POSTGRES_DSN=postgresql://saw:saw@localhost:5432/saw`.",
            )
        )
        return RuntimeDependencyStatus(
            name="app_state",
            role="Workflow and review state",
            mode="postgres",
            configured=configured,
            reachable=reachable,
            ready=configured and reachable is True,
            detail=detail,
        )
    if backend == "sqlite":
        sqlite_path = Path(settings.app_sqlite_path)
        directory = sqlite_path.parent
        ready = directory.exists() or _can_create_directory(directory)
        detail = f"SQLite app state at {sqlite_path}."
        if not ready:
            detail = f"SQLite directory is not writable: {directory}"
        return RuntimeDependencyStatus(
            name="app_state",
            role="Workflow and review state",
            mode="sqlite",
            reachable=None,
            ready=ready,
            detail=detail,
        )
    return RuntimeDependencyStatus(
        name="app_state",
        role="Workflow and review state",
        mode="file",
        reachable=None,
        ready=True,
        detail=f"File-backed app state in {settings.app_data_dir}.",
    )


def _truth_store_status() -> RuntimeDependencyStatus:
    if settings.app_truth_backend == "file":
        return RuntimeDependencyStatus(
            name="truth_store",
            role="Approved claims",
            mode="file",
            reachable=None,
            ready=True,
            detail=f"File-backed approved claims in {settings.app_data_dir / 'claims.json'}.",
        )
    if settings.app_truth_backend == "postgres":
        configured = bool(settings.app_postgres_dsn)
        reachable, detail = (
            _probe_postgres(settings.app_postgres_dsn)
            if configured
            else (
                None,
                "APP_POSTGRES_DSN is required when Postgres is selected for the truth store. "
                "Run `cp .env.example .env` or set "
                "`APP_POSTGRES_DSN=postgresql://saw:saw@localhost:5432/saw`.",
            )
        )
        return RuntimeDependencyStatus(
            name="truth_store",
            role="Approved claims",
            mode="postgres",
            configured=configured,
            reachable=reachable,
            ready=configured and reachable is True,
            detail=detail if reachable is not True else "Postgres canonical claim store is ready.",
        )

    configured = all(
        [
            settings.wikibase_api_url,
            settings.wikibase_username,
            settings.wikibase_password,
            settings.wikibase_property_map,
        ]
    )
    if not configured:
        detail = (
            "Canonical approved claims require WIKIBASE_API_URL, WIKIBASE_USERNAME, "
            "WIKIBASE_PASSWORD, and WIKIBASE_PROPERTY_MAP."
        )
        reachable = None
    else:
        reachable, detail = _probe_wikibase(settings.wikibase_api_url or "")
    return RuntimeDependencyStatus(
        name="truth_store",
        role="Approved claims",
        mode="wikibase",
        configured=configured,
        reachable=reachable,
        ready=configured and reachable is True,
        detail=detail,
    )


def _corpus_status() -> RuntimeDependencyStatus:
    configured = bool(settings.zotero_library_id)
    if not configured:
        return RuntimeDependencyStatus(
            name="corpus",
            role="Source intake",
            mode="stub",
            configured=False,
            reachable=None,
            ready=True,
            detail=(
                "Zotero is supported but optional for the current release. Intake routes still "
                "work, but they stay on manual and stub flows until you configure a live "
                "library."
            ),
        )

    reachable, detail = _probe_http(settings.zotero_base_url, path="/")
    return RuntimeDependencyStatus(
        name="corpus",
        role="Source intake",
        mode="zotero",
        configured=True,
        reachable=reachable,
        ready=reachable is True,
        detail=detail if reachable is not True else "Live Zotero corpus appears reachable.",
    )


def _bible_workspace_status() -> RuntimeDependencyStatus:
    if settings.app_state_backend == "postgres":
        return RuntimeDependencyStatus(
            name="bible_workspace",
            role="Bible profiles, sections, and provenance state",
            mode="postgres",
            reachable=None,
            ready=True,
            detail="Bible workspace state shares the configured Postgres app-state store.",
        )
    if settings.app_state_backend == "sqlite":
        sqlite_path = Path(settings.app_sqlite_path)
        writable = sqlite_path.parent.exists() or _can_create_directory(sqlite_path.parent)
        return RuntimeDependencyStatus(
            name="bible_workspace",
            role="Bible profiles, sections, and provenance state",
            mode="sqlite",
            reachable=None,
            ready=writable,
            detail=(
                f"Bible workspace state persists in SQLite at {settings.app_sqlite_path}."
                if writable
                else f"SQLite bible workspace path is not writable: {sqlite_path.parent}"
            ),
        )
    data_dir = settings.app_data_dir
    ready = data_dir.exists() or _can_create_directory(data_dir)
    return RuntimeDependencyStatus(
        name="bible_workspace",
        role="Bible profiles, sections, and provenance state",
        mode="file",
        reachable=None,
        ready=ready,
        detail=(
            f"Bible workspace state persists in {settings.app_data_dir}."
            if ready
            else f"Bible workspace directory is not writable: {settings.app_data_dir}"
        ),
    )


def _bible_export_status() -> RuntimeDependencyStatus:
    worker_ready = settings.app_job_worker_enabled
    return RuntimeDependencyStatus(
        name="bible_export",
        role="Background bible export bundles",
        mode="job_backed",
        configured=True,
        reachable=None,
        ready=worker_ready,
        detail=(
            "Bible export runs as a persisted background job and returns bundles through "
            "job results."
            if worker_ready
            else "Bible export requests will queue, but they will not complete until "
            "the background worker is enabled."
        ),
    )


def _extraction_status() -> RuntimeDependencyStatus:
    if not settings.graph_rag_enabled:
        return RuntimeDependencyStatus(
            name="extraction",
            role="Candidate generation",
            mode="heuristic",
            reachable=None,
            ready=True,
            detail=(
                "Heuristic extraction is active for local startup. This is the shipped default "
                "path and keeps first run dependency-light while GraphRAG remains optional."
            ),
        )

    probe = GraphRAGExtractionAdapter.runtime_probe()
    return RuntimeDependencyStatus(
        name="extraction",
        role="Candidate generation",
        mode=probe.mode,
        configured=probe.configured,
        reachable=None,
        ready=probe.ready,
        detail=probe.detail,
    )


def _projection_status() -> RuntimeDependencyStatus:
    mode, reachable, ready, detail = QdrantProjectionAdapter().runtime_probe()
    return RuntimeDependencyStatus(
        name="projection",
        role="Semantic retrieval projection",
        mode=mode,
        configured=settings.qdrant_enabled,
        reachable=reachable,
        ready=ready,
        detail=detail,
    )


def _research_semantics_status() -> RuntimeDependencyStatus:
    mode, reachable, ready, detail = QdrantResearchSemanticAdapter().runtime_probe()
    return RuntimeDependencyStatus(
        name="research_semantics",
        role="Research dedupe and reranking",
        mode=mode,
        configured=settings.research_semantic_enabled,
        reachable=reachable,
        ready=ready,
        detail=detail,
    )


def _job_worker_status() -> RuntimeDependencyStatus:
    return RuntimeDependencyStatus(
        name="job_worker",
        role="Persisted background research and bible jobs",
        mode="in_process",
        configured=settings.app_job_worker_enabled,
        reachable=None,
        ready=settings.app_job_worker_enabled,
        detail=(
            f"In-process worker enabled with {settings.app_job_poll_interval_seconds}s polling."
            if settings.app_job_worker_enabled
            else "Background job worker is disabled; long-running routes will queue "
            "without auto-processing."
        ),
    )


def _overall_status(services: list[RuntimeDependencyStatus]) -> str:
    by_name = {service.name: service for service in services}
    if any(
        by_name[name].ready is False
        for name in ("app_state", "truth_store", "bible_workspace", "bible_export", "job_worker")
    ):
        return "needs_setup"
    if _has_quality_degradation(by_name):
        return "degraded"
    return "ready"


def _has_quality_degradation(by_name: dict[str, RuntimeDependencyStatus]) -> bool:
    projection = by_name["projection"]
    if settings.qdrant_enabled and projection.ready is False:
        return True
    research_semantics = by_name["research_semantics"]
    if settings.research_semantic_enabled and research_semantics.ready is False:
        return True
    extraction = by_name["extraction"]
    if settings.graph_rag_enabled and extraction.ready is False:
        return True
    return False


def _next_steps(services: list[RuntimeDependencyStatus]) -> list[str]:
    steps: list[str] = []
    by_name = {service.name: service for service in services}

    if settings.app_state_backend != "postgres":
        steps.append(
            "Non-default local mode detected: set `APP_STATE_BACKEND=postgres` and ensure "
            "`APP_POSTGRES_DSN` points at the default Postgres service."
        )
    if settings.app_truth_backend != "postgres":
        steps.append(
            "Non-default local mode detected: set `APP_TRUTH_BACKEND=postgres` so canonical "
            "truth follows the documented default stack."
        )
    if by_name["corpus"].ready is False:
        steps.append(
            "Optional after first run: verify ZOTERO_BASE_URL, ZOTERO_LIBRARY_ID, and "
            "network access so live Zotero pulls can succeed."
        )
    elif not settings.zotero_library_id:
        steps.append(
            "Optional after first run: configure Zotero if you want live library pulls "
            "instead of manual or seeded local sources."
        )
    if by_name["bible_workspace"].ready is False:
        steps.append(
            "Required for the current app-state backend: choose a writable location so "
            "Bible profiles, sections, and provenance can persist safely."
        )
    if by_name["job_worker"].ready is False:
        steps.append(
            "Required for local startup: set APP_JOB_WORKER_ENABLED=true "
            "so research, Bible regeneration, and export jobs run without manual intervention."
        )
    postgres_state_needed = (
        settings.app_state_backend == "postgres" and by_name["app_state"].ready is False
    )
    postgres_truth_needed = (
        settings.app_truth_backend == "postgres" and by_name["truth_store"].ready is False
    )
    if postgres_state_needed and postgres_truth_needed:
        steps.append(
            "Required because Postgres backs both app state and canonical truth: run "
            "`docker compose up -d postgres` and ensure APP_POSTGRES_DSN points at that database."
        )
    elif postgres_state_needed:
        steps.append(
            "Required because the Postgres app-state store is enabled: run "
            "`docker compose up -d postgres` "
            "and ensure APP_POSTGRES_DSN points at that database."
        )
    elif postgres_truth_needed:
        steps.append(
            "Required because the Postgres truth store is enabled: run "
            "`docker compose up -d postgres` "
            "and ensure APP_POSTGRES_DSN points at that database."
        )
    if settings.app_truth_backend == "wikibase" and by_name["truth_store"].ready is False:
        steps.append(
            "Required because APP_TRUTH_BACKEND=wikibase: configure WIKIBASE_API_URL, "
            "WIKIBASE_USERNAME, WIKIBASE_PASSWORD, and "
            "WIKIBASE_PROPERTY_MAP, or switch back to Postgres or file-backed truth for "
            "local startup."
        )
    if settings.qdrant_enabled and by_name["projection"].ready is False:
        if by_name["projection"].mode == "qdrant:uninitialized":
            steps.append(
                "Required because QDRANT_ENABLED=true: run `saw seed-dev-data` to load the "
                "sample project and initialize Qdrant, or run `saw qdrant-rebuild` if you "
                "only need to repair the projection manually."
            )
        else:
            steps.append(
                "Required because QDRANT_ENABLED=true: run "
                "`docker compose up -d qdrant` and verify QDRANT_URL so projection-backed "
                "retrieval is available."
            )
    elif not settings.qdrant_enabled:
        steps.append(
            "Non-default local mode detected: re-enable Qdrant projection with "
            "`QDRANT_ENABLED=true` and `docker compose up -d qdrant` so retrieval uses "
            "the default local projection path instead of in-memory ranking."
        )
    if settings.research_semantic_enabled and by_name["research_semantics"].ready is False:
        if by_name["research_semantics"].mode == "qdrant:uninitialized":
            steps.append(
                "Required because RESEARCH_SEMANTIC_ENABLED=true: run `saw seed-dev-data` "
                "or `saw qdrant-init` to create the research semantic collection."
            )
        else:
            steps.append(
                "Required because RESEARCH_SEMANTIC_ENABLED=true: run "
                "`docker compose up -d qdrant` and verify QDRANT_URL."
            )
    elif not settings.research_semantic_enabled:
        steps.append(
            "Optional after first run: enable RESEARCH_SEMANTIC_ENABLED if you want "
            "Qdrant-backed duplicate detection and reranking for research findings."
        )
    if by_name["extraction"].mode == "heuristic":
        steps.append(
            "Optional after first run: enable GraphRAG or another richer extraction "
            "path if you want higher-quality candidate generation beyond the shipped "
            "heuristic default."
        )
    elif by_name["extraction"].ready is False:
        steps.append(
            "Optional after first run: install/configure GraphRAG and ensure the "
            "selected GRAPH_RAG_MODE has a runnable workspace or artifact directory."
        )

    return steps


def _probe_postgres(dsn: str) -> tuple[bool, str]:
    try:
        with connect(dsn, connect_timeout=2, autocommit=True) as connection:
            connection.execute("SELECT 1")
        return True, "Postgres connection succeeded."
    except Exception as exc:
        return (
            False,
            "Postgres connection failed: "
            f"{exc}. Start it with `docker compose up -d postgres` and verify "
            f"`APP_POSTGRES_DSN={dsn}`.",
        )


def _probe_wikibase(api_url: str) -> tuple[bool, str]:
    return _probe_http(
        api_url,
        path="",
        params={"action": "query", "meta": "siteinfo", "format": "json"},
    )


def _probe_http(
    base_url: str,
    *,
    path: str,
    params: dict[str, str] | None = None,
) -> tuple[bool, str]:
    try:
        with httpx.Client(timeout=2.0, follow_redirects=True) as client:
            response = client.get(f"{base_url.rstrip('/')}{path}", params=params)
            response.raise_for_status()
        return True, "HTTP probe succeeded."
    except Exception as exc:
        return False, f"HTTP probe failed: {exc}"


def _can_create_directory(path: Path) -> bool:
    try:
        path.mkdir(parents=True, exist_ok=True)
        return True
    except OSError:
        return False
