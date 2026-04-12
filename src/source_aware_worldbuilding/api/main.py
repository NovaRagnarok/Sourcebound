from fastapi import FastAPI

from source_aware_worldbuilding.api.routes.candidates import router as candidates_router
from source_aware_worldbuilding.api.routes.claims import router as claims_router
from source_aware_worldbuilding.api.routes.health import router as health_router
from source_aware_worldbuilding.api.routes.ingest import router as ingest_router
from source_aware_worldbuilding.api.routes.query import router as query_router
from source_aware_worldbuilding.settings import settings

app = FastAPI(title=settings.app_name)
app.include_router(health_router)
app.include_router(ingest_router)
app.include_router(candidates_router)
app.include_router(claims_router)
app.include_router(query_router)
