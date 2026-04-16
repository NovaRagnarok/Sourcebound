from __future__ import annotations

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile

from source_aware_worldbuilding.api.dependencies import get_intake_service, require_operator_actor
from source_aware_worldbuilding.domain.errors import (
    ZoteroAuthError,
    ZoteroConfigError,
    ZoteroError,
    ZoteroNotFoundError,
    ZoteroRateLimitError,
    ZoteroWriteError,
)
from source_aware_worldbuilding.domain.models import IntakeTextRequest, IntakeUrlRequest
from source_aware_worldbuilding.services.intake import IntakeService

router = APIRouter(
    prefix="/v1/intake",
    tags=["intake"],
    dependencies=[Depends(require_operator_actor)],
)


@router.post("/text")
def intake_text(
    payload: IntakeTextRequest,
    service: IntakeService = Depends(get_intake_service),
) -> dict:
    try:
        result = service.intake_text(payload)
    except ZoteroError as exc:
        raise _zotero_http_error(exc) from exc
    return result.model_dump(mode="json")


@router.post("/url")
def intake_url(
    payload: IntakeUrlRequest,
    service: IntakeService = Depends(get_intake_service),
) -> dict:
    try:
        result = service.intake_url(payload)
    except ZoteroError as exc:
        raise _zotero_http_error(exc) from exc
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
    except ZoteroError as exc:
        raise _zotero_http_error(exc) from exc
    return result.model_dump(mode="json")


def _zotero_http_error(exc: ZoteroError) -> HTTPException:
    if isinstance(exc, ZoteroConfigError):
        return HTTPException(status_code=400, detail=str(exc))
    if isinstance(exc, ZoteroAuthError):
        return HTTPException(status_code=401, detail=str(exc))
    if isinstance(exc, ZoteroNotFoundError):
        return HTTPException(status_code=404, detail=str(exc))
    if isinstance(exc, ZoteroRateLimitError):
        return HTTPException(status_code=429, detail=str(exc))
    if isinstance(exc, ZoteroWriteError):
        return HTTPException(status_code=502, detail=str(exc))
    return HTTPException(status_code=502, detail=str(exc))
