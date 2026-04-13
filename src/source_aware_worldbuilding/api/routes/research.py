from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from source_aware_worldbuilding.api.dependencies import get_research_service
from source_aware_worldbuilding.domain.models import (
    ResearchProgramCreateRequest,
    ResearchRunRequest,
)
from source_aware_worldbuilding.services.research import ResearchService

router = APIRouter(prefix="/v1/research", tags=["research"])


@router.post("/runs")
def create_research_run(
    payload: ResearchRunRequest,
    service: ResearchService = Depends(get_research_service),
) -> dict:
    try:
        return service.run_research(payload).model_dump(mode="json")
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/runs")
def list_research_runs(service: ResearchService = Depends(get_research_service)) -> list[dict]:
    return [item.model_dump(mode="json") for item in service.list_runs()]


@router.get("/runs/{run_id}")
def get_research_run(
    run_id: str,
    service: ResearchService = Depends(get_research_service),
) -> dict:
    detail = service.get_run_detail(run_id)
    if detail is None:
        raise HTTPException(status_code=404, detail="Research run not found")
    return detail.model_dump(mode="json")


@router.post("/runs/{run_id}/stage")
def stage_research_run(
    run_id: str,
    service: ResearchService = Depends(get_research_service),
) -> dict:
    try:
        return service.stage_run(run_id).model_dump(mode="json")
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/runs/{run_id}/extract")
def extract_research_run(
    run_id: str,
    service: ResearchService = Depends(get_research_service),
) -> dict:
    try:
        return service.extract_run(run_id).model_dump(mode="json")
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/programs")
def create_research_program(
    payload: ResearchProgramCreateRequest,
    service: ResearchService = Depends(get_research_service),
) -> dict:
    return service.create_program(payload).model_dump(mode="json")


@router.get("/programs")
def list_research_programs(
    service: ResearchService = Depends(get_research_service),
) -> list[dict]:
    return [item.model_dump(mode="json") for item in service.list_programs()]
