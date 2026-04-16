from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status

from source_aware_worldbuilding.api.dependencies import get_research_service, require_operator_actor
from source_aware_worldbuilding.api.routes._job_runtime import (
    require_job_service,
    try_get_job_service,
)
from source_aware_worldbuilding.domain.errors import WorkerUnavailableError
from source_aware_worldbuilding.domain.models import (
    ResearchProgramCreateRequest,
    ResearchRunRequest,
)
from source_aware_worldbuilding.services.research import ResearchService

router = APIRouter(prefix="/v1/research", tags=["research"])


@router.post("/runs", status_code=status.HTTP_202_ACCEPTED)
def create_research_run(
    payload: ResearchRunRequest,
    _actor=Depends(require_operator_actor),
) -> dict:
    service = require_job_service(action="queueing research runs")
    try:
        return service.enqueue_research_run(payload).model_dump(mode="json")
    except WorkerUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/runs")
def list_research_runs(
    service: ResearchService = Depends(get_research_service),
) -> list[dict]:
    runs = service.list_runs()
    job_service = try_get_job_service()
    if job_service is not None:
        for run in runs:
            run.latest_job = job_service.summarize_latest_for_run(run.run_id)
    return [item.model_dump(mode="json") for item in runs]


@router.get("/runs/{run_id}")
def get_research_run(
    run_id: str,
    service: ResearchService = Depends(get_research_service),
) -> dict:
    detail = service.get_run_detail(run_id)
    if detail is None:
        raise HTTPException(status_code=404, detail="Research run not found")
    job_service = try_get_job_service()
    if job_service is not None:
        detail.run.latest_job = job_service.summarize_latest_for_run(run_id)
    return detail.model_dump(mode="json")


@router.post("/runs/{run_id}/stage", status_code=status.HTTP_202_ACCEPTED)
def stage_research_run(
    run_id: str,
    _actor=Depends(require_operator_actor),
) -> dict:
    service = require_job_service(action="staging research findings")
    try:
        return service.enqueue_research_stage(run_id).model_dump(mode="json")
    except WorkerUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/runs/{run_id}/extract", status_code=status.HTTP_202_ACCEPTED)
def extract_research_run(
    run_id: str,
    _actor=Depends(require_operator_actor),
) -> dict:
    service = require_job_service(action="extracting staged research findings")
    try:
        return service.enqueue_research_extract(run_id).model_dump(mode="json")
    except WorkerUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/programs")
def create_research_program(
    payload: ResearchProgramCreateRequest,
    service: ResearchService = Depends(get_research_service),
    _actor=Depends(require_operator_actor),
) -> dict:
    return service.create_program(payload).model_dump(mode="json")


@router.get("/programs")
def list_research_programs(
    service: ResearchService = Depends(get_research_service),
) -> list[dict]:
    return [item.model_dump(mode="json") for item in service.list_programs()]
