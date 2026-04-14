from __future__ import annotations

from pathlib import Path

import httpx
from psycopg import connect

from source_aware_worldbuilding.adapters.graphrag_adapter import GraphRAGExtractionAdapter
from source_aware_worldbuilding.domain.models import RuntimeDependencyStatus, RuntimeStatus
from source_aware_worldbuilding.settings import settings


def build_runtime_status() -> RuntimeStatus:
    services = [
        _state_store_status(),
        _truth_store_status(),
        _bible_workspace_status(),
        _bible_export_status(),
        _corpus_status(),
        _extraction_status(),
        _projection_status(),
        _job_worker_status(),
    ]
    next_steps = _next_steps(services)
    overall_status = "ready" if all(service.ready for service in services) else "needs_setup"

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


def _state_store_status() -> RuntimeDependencyStatus:
    backend = settings.app_state_backend
    if backend == "postgres":
        configured = bool(settings.app_postgres_dsn)
        reachable, detail = (
            _probe_postgres(settings.app_postgres_dsn)
            if configured
            else (
                None,
                "APP_POSTGRES_DSN is required for the Postgres state backend.",
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
                "APP_POSTGRES_DSN is required for the Postgres truth backend.",
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
            ready=False,
            detail="Zotero is not configured; ingest routes will fall back to stub sources.",
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
            "Bible export runs as a persisted background job and returns bundles through job results."
            if worker_ready
            else "Bible export can still be requested, but the background worker is disabled so queued export jobs will not auto-run."
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
            detail="Heuristic sentence-based extractor is active for local MVP work.",
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
    if not settings.qdrant_enabled:
        return RuntimeDependencyStatus(
            name="projection",
            role="Semantic retrieval projection",
            mode="disabled",
            configured=False,
            reachable=None,
            ready=True,
            detail="Qdrant projection is optional and disabled; query falls back to in-memory ranking.",
        )

    reachable, detail = _probe_http(settings.qdrant_url, path="/collections")
    return RuntimeDependencyStatus(
        name="projection",
        role="Semantic retrieval projection",
        mode="qdrant",
        configured=True,
        reachable=reachable,
        ready=reachable is True,
        detail=detail if reachable is not True else "Qdrant appears reachable.",
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
            else "Background job worker is disabled; long-running routes will queue without auto-processing."
        ),
    )


def _next_steps(services: list[RuntimeDependencyStatus]) -> list[str]:
    steps: list[str] = []
    by_name = {service.name: service for service in services}

    if by_name["corpus"].ready is False:
        steps.append(
            "Set ZOTERO_LIBRARY_ID to pull a real pilot corpus instead of stub sources."
        )
    if by_name["bible_workspace"].ready is False:
        steps.append(
            "Choose a writable app-state location so Bible profiles, sections, and provenance can persist safely."
        )
    if by_name["job_worker"].ready is False:
        steps.append(
            "Enable APP_JOB_WORKER_ENABLED=true so research, Bible regeneration, and export jobs run without manual intervention."
        )
    if settings.qdrant_enabled and by_name["projection"].ready is False:
        steps.append(
            "Run `docker compose up -d qdrant` and set QDRANT_ENABLED=true "
            "to exercise projection-backed retrieval."
        )
    elif not settings.qdrant_enabled:
        steps.append(
            "Set QDRANT_ENABLED=true and run `docker compose up -d qdrant` for recommended local retrieval quality."
        )
    if settings.app_truth_backend == "postgres" and by_name["truth_store"].ready is False:
        steps.append(
            "Configure APP_POSTGRES_DSN so Postgres can serve as the canonical approved-claim store."
        )
    if settings.app_truth_backend == "wikibase" and by_name["truth_store"].ready is False:
        steps.append(
            "Configure WIKIBASE_API_URL, WIKIBASE_USERNAME, WIKIBASE_PASSWORD, and "
            "WIKIBASE_PROPERTY_MAP to enable canonical approved-claim reads and writes."
        )
    if by_name["extraction"].mode == "heuristic":
        steps.append(
            "Replace or augment the heuristic extractor with the real GraphRAG "
            "or LLM-backed extraction path."
        )
    elif by_name["extraction"].ready is False:
        steps.append(
            "Install/configure GraphRAG and ensure the selected GRAPH_RAG_MODE "
            "has a runnable workspace or artifact directory."
        )

    return steps


def _probe_postgres(dsn: str) -> tuple[bool, str]:
    try:
        with connect(dsn, connect_timeout=2, autocommit=True) as connection:
            connection.execute("SELECT 1")
        return True, "Postgres connection succeeded."
    except Exception as exc:
        return False, f"Postgres connection failed: {exc}"


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
