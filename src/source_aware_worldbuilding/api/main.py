from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles

from source_aware_worldbuilding.api.dependencies import get_job_service
from source_aware_worldbuilding.api.routes.bible import router as bible_router
from source_aware_worldbuilding.api.routes.candidates import router as candidates_router
from source_aware_worldbuilding.api.routes.claims import router as claims_router
from source_aware_worldbuilding.api.routes.exports import router as exports_router
from source_aware_worldbuilding.api.routes.health import router as health_router
from source_aware_worldbuilding.api.routes.ingest import router as ingest_router
from source_aware_worldbuilding.api.routes.intake import router as intake_router
from source_aware_worldbuilding.api.routes.jobs import router as jobs_router
from source_aware_worldbuilding.api.routes.query import router as query_router
from source_aware_worldbuilding.api.routes.research import router as research_router
from source_aware_worldbuilding.api.routes.runs import router as runs_router
from source_aware_worldbuilding.api.routes.sources import router as sources_router
from source_aware_worldbuilding.services.status import enforce_runtime_startup_checks
from source_aware_worldbuilding.settings import settings


@asynccontextmanager
async def lifespan(_app: FastAPI):
    enforce_runtime_startup_checks()
    if settings.app_job_worker_enabled:
        get_job_service().start_worker()
    try:
        yield
    finally:
        get_job_service().stop_worker()


app = FastAPI(title=settings.app_name, lifespan=lifespan)
app.include_router(health_router)
app.include_router(bible_router)
app.include_router(exports_router)
app.include_router(ingest_router)
app.include_router(intake_router)
app.include_router(jobs_router)
app.include_router(sources_router)
app.include_router(runs_router)
app.include_router(research_router)
app.include_router(candidates_router)
app.include_router(claims_router)
app.include_router(query_router)

frontend_dir = Path(__file__).resolve().parents[3] / "frontend" / "operator-ui"
if settings.app_ui_enabled and frontend_dir.exists():
    app.mount("/operator", StaticFiles(directory=frontend_dir, html=True), name="operator")

    @app.get("/", include_in_schema=False)
    def root() -> RedirectResponse:
        return RedirectResponse(url="/operator/")
