from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status

from source_aware_worldbuilding.api.dependencies import get_bible_workspace_service, get_job_service
from source_aware_worldbuilding.domain.models import (
    BibleProjectProfileUpdateRequest,
    BibleSectionCreateRequest,
    BibleSectionRegenerateRequest,
    BibleSectionUpdateRequest,
)
from source_aware_worldbuilding.services.jobs import JobService
from source_aware_worldbuilding.services.bible import BibleWorkspaceService

router = APIRouter(prefix="/v1/bible", tags=["bible"])


@router.get("/profiles")
def list_profiles(service: BibleWorkspaceService = Depends(get_bible_workspace_service)) -> list[dict]:
    return [item.model_dump(mode="json") for item in service.list_profiles()]


@router.get("/profiles/{project_id}")
def get_profile(
    project_id: str,
    service: BibleWorkspaceService = Depends(get_bible_workspace_service),
) -> dict:
    profile = service.get_profile(project_id)
    if profile is None:
        raise HTTPException(status_code=404, detail="Bible project profile not found")
    return profile.model_dump(mode="json")


@router.put("/profiles/{project_id}")
def save_profile(
    project_id: str,
    payload: BibleProjectProfileUpdateRequest,
    service: BibleWorkspaceService = Depends(get_bible_workspace_service),
) -> dict:
    return service.save_profile(project_id, payload).model_dump(mode="json")


@router.get("/sections")
def list_sections(
    project_id: str,
    service: BibleWorkspaceService = Depends(get_bible_workspace_service),
    job_service: JobService = Depends(get_job_service),
) -> list[dict]:
    sections = service.list_sections(project_id)
    for section in sections:
        section.latest_job = job_service.summarize_latest_for_section(section.section_id)
    return [item.model_dump(mode="json") for item in sections]


@router.post("/sections", status_code=status.HTTP_202_ACCEPTED)
def create_section(
    payload: BibleSectionCreateRequest,
    service: JobService = Depends(get_job_service),
) -> dict:
    return service.enqueue_bible_compose(payload).model_dump(mode="json")


@router.get("/sections/{section_id}")
def get_section(
    section_id: str,
    service: BibleWorkspaceService = Depends(get_bible_workspace_service),
    job_service: JobService = Depends(get_job_service),
) -> dict:
    section = service.get_section(section_id)
    if section is None:
        raise HTTPException(status_code=404, detail="Bible section not found")
    section.latest_job = job_service.summarize_latest_for_section(section_id)
    return section.model_dump(mode="json")


@router.put("/sections/{section_id}")
def update_section(
    section_id: str,
    payload: BibleSectionUpdateRequest,
    service: BibleWorkspaceService = Depends(get_bible_workspace_service),
) -> dict:
    try:
        return service.update_section(section_id, payload).model_dump(mode="json")
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/sections/{section_id}/regenerate", status_code=status.HTTP_202_ACCEPTED)
def regenerate_section(
    section_id: str,
    payload: BibleSectionRegenerateRequest,
    service: JobService = Depends(get_job_service),
) -> dict:
    try:
        return service.enqueue_bible_regenerate(section_id, payload).model_dump(mode="json")
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/sections/{section_id}/provenance")
def get_section_provenance(
    section_id: str,
    service: BibleWorkspaceService = Depends(get_bible_workspace_service),
) -> dict:
    provenance = service.get_section_provenance(section_id)
    if provenance is None:
        raise HTTPException(status_code=404, detail="Bible section not found")
    return provenance.model_dump(mode="json")


@router.get("/exports/{project_id}")
def export_project(
    project_id: str,
    service: BibleWorkspaceService = Depends(get_bible_workspace_service),
    job_service: JobService = Depends(get_job_service),
) -> dict:
    try:
        cached = job_service.latest_completed_export_for_project(project_id)
        if cached is not None:
            return cached
        return service.export_project(project_id).model_dump(mode="json")
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/exports/{project_id}", status_code=status.HTTP_202_ACCEPTED)
def queue_export_project(
    project_id: str,
    service: JobService = Depends(get_job_service),
) -> dict:
    try:
        return service.enqueue_bible_export(project_id).model_dump(mode="json")
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
