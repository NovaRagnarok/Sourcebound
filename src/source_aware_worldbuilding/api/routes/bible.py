from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status

from source_aware_worldbuilding.api.dependencies import (
    get_bible_workspace_service,
    require_writer_actor,
    require_operator_actor,
)
from source_aware_worldbuilding.api.routes._job_runtime import (
    require_job_service,
    try_get_job_service,
)
from source_aware_worldbuilding.domain.errors import WorkerUnavailableError
from source_aware_worldbuilding.domain.models import (
    BibleProjectProfileUpdateRequest,
    BibleSectionCreateRequest,
    BibleSectionRegenerateRequest,
    BibleSectionUpdateRequest,
)
from source_aware_worldbuilding.services.bible import BibleWorkspaceService

router = APIRouter(prefix="/v1/bible", tags=["bible"])


@router.get("/profiles")
def list_profiles(
    service: BibleWorkspaceService = Depends(get_bible_workspace_service),
) -> list[dict]:
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
    actor=Depends(require_writer_actor),
) -> dict:
    return service.save_profile(project_id, payload, actor=actor).model_dump(mode="json")


@router.get("/sections")
def list_sections(
    project_id: str,
    service: BibleWorkspaceService = Depends(get_bible_workspace_service),
) -> list[dict]:
    sections = service.list_sections(project_id)
    job_service = try_get_job_service()
    if job_service is not None:
        for section in sections:
            section.latest_job = job_service.summarize_latest_for_section(section.section_id)
    return [item.model_dump(mode="json") for item in sections]


@router.post("/sections", status_code=status.HTTP_202_ACCEPTED)
def create_section(
    payload: BibleSectionCreateRequest,
    _actor=Depends(require_writer_actor),
) -> dict:
    service = require_job_service(action="queueing Bible composition")
    try:
        return service.enqueue_bible_compose(payload).model_dump(mode="json")
    except WorkerUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.get("/sections/{section_id}")
def get_section(
    section_id: str,
    service: BibleWorkspaceService = Depends(get_bible_workspace_service),
) -> dict:
    section = service.get_section(section_id)
    if section is None:
        raise HTTPException(status_code=404, detail="Bible section not found")
    job_service = try_get_job_service()
    if job_service is not None:
        section.latest_job = job_service.summarize_latest_for_section(section_id)
    return section.model_dump(mode="json")


@router.put("/sections/{section_id}")
def update_section(
    section_id: str,
    payload: BibleSectionUpdateRequest,
    service: BibleWorkspaceService = Depends(get_bible_workspace_service),
    actor=Depends(require_writer_actor),
) -> dict:
    try:
        return service.update_section(section_id, payload, actor=actor).model_dump(mode="json")
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/sections/{section_id}/regenerate", status_code=status.HTTP_202_ACCEPTED)
def regenerate_section(
    section_id: str,
    payload: BibleSectionRegenerateRequest,
    workspace_service: BibleWorkspaceService = Depends(get_bible_workspace_service),
    actor=Depends(require_operator_actor),
) -> dict:
    job_service = require_job_service(action="queueing Bible regeneration")
    try:
        event = workspace_service.record_regeneration_request(section_id, actor, payload)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    try:
        job = job_service.enqueue_bible_regenerate(section_id, payload)
    except WorkerUnavailableError as exc:
        workspace_service.discard_regeneration_request(section_id, event.event_id)
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except ValueError as exc:
        workspace_service.discard_regeneration_request(section_id, event.event_id)
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return job.model_dump(mode="json")


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
) -> dict:
    job_service = try_get_job_service()
    try:
        cached = (
            job_service.latest_completed_export_for_project(project_id)
            if job_service is not None
            else None
        )
        if cached is not None:
            latest = service.export_project(project_id).model_dump(mode="json")
            cached["profile"] = latest["profile"]
            cached["sections"] = latest["sections"]
            return cached
        return service.export_project(project_id).model_dump(mode="json")
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/exports/{project_id}", status_code=status.HTTP_202_ACCEPTED)
def queue_export_project(
    project_id: str,
    workspace_service: BibleWorkspaceService = Depends(get_bible_workspace_service),
    actor=Depends(require_operator_actor),
) -> dict:
    job_service = require_job_service(action="queueing Bible export")
    try:
        event = workspace_service.record_export_request(project_id, actor)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    try:
        job = job_service.enqueue_bible_export(project_id)
    except WorkerUnavailableError as exc:
        workspace_service.discard_export_request(project_id, event.event_id)
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except ValueError as exc:
        workspace_service.discard_export_request(project_id, event.event_id)
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return job.model_dump(mode="json")
