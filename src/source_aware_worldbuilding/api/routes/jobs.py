from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from source_aware_worldbuilding.api.dependencies import get_job_service
from source_aware_worldbuilding.domain.errors import WorkerUnavailableError
from source_aware_worldbuilding.services.jobs import JobService

router = APIRouter(prefix="/v1/jobs", tags=["jobs"])


@router.get("")
def list_jobs(
    status: str | None = None,
    service: JobService = Depends(get_job_service),
) -> list[dict]:
    return [item.model_dump(mode="json") for item in service.list_jobs(status=status)]


@router.get("/{job_id}")
def get_job(
    job_id: str,
    service: JobService = Depends(get_job_service),
) -> dict:
    job = service.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    return job.model_dump(mode="json")


@router.post("/{job_id}/cancel")
def cancel_job(
    job_id: str,
    service: JobService = Depends(get_job_service),
) -> dict:
    try:
        return service.cancel_job(job_id).model_dump(mode="json")
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/{job_id}/retry")
def retry_job(
    job_id: str,
    service: JobService = Depends(get_job_service),
) -> dict:
    try:
        return service.retry_job(job_id).model_dump(mode="json")
    except WorkerUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
