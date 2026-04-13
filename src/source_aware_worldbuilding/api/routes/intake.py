from __future__ import annotations

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile

from source_aware_worldbuilding.api.dependencies import get_intake_service
from source_aware_worldbuilding.domain.errors import ZoteroWriteError
from source_aware_worldbuilding.domain.models import IntakeTextRequest, IntakeUrlRequest
from source_aware_worldbuilding.services.intake import IntakeService

router = APIRouter(prefix="/v1/intake", tags=["intake"])


@router.post("/text")
def intake_text(
    payload: IntakeTextRequest,
    service: IntakeService = Depends(get_intake_service),
) -> dict:
    try:
        result = service.intake_text(payload)
    except ZoteroWriteError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    return result.model_dump(mode="json")


@router.post("/url")
def intake_url(
    payload: IntakeUrlRequest,
    service: IntakeService = Depends(get_intake_service),
) -> dict:
    try:
        result = service.intake_url(payload)
    except ZoteroWriteError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    return result.model_dump(mode="json")


@router.post("/file")
async def intake_file(
    file: UploadFile = File(...),
    title: str | None = Form(default=None),
    notes: str | None = Form(default=None),
    source_type: str = Form(default="document"),
    collection_key: str | None = Form(default=None),
    service: IntakeService = Depends(get_intake_service),
) -> dict:
    try:
        result = service.intake_file(
            filename=file.filename or "uploaded-file",
            content_type=file.content_type,
            content=await file.read(),
            title=title,
            source_type=source_type,
            notes=notes,
            collection_key=collection_key,
        )
    except ZoteroWriteError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    return result.model_dump(mode="json")
