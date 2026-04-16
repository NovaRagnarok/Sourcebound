from __future__ import annotations

from fastapi import HTTPException, status

from source_aware_worldbuilding.api.dependencies import get_job_service
from source_aware_worldbuilding.services.jobs import JobService
from source_aware_worldbuilding.services.status import build_runtime_status


def try_get_job_service() -> JobService | None:
    try:
        return get_job_service()
    except Exception:
        return None


def require_job_service(*, action: str) -> JobService:
    try:
        return get_job_service()
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=_job_service_unavailable_message(action),
        ) from exc


def _job_service_unavailable_message(action: str) -> str:
    try:
        runtime_status = build_runtime_status()
        guidance = " ".join(runtime_status.next_steps[:2]).strip()
    except Exception:
        guidance = ""

    suffix = (
        f" {guidance} See `/health/runtime` for the full setup checklist."
        if guidance
        else " Open `/health/runtime` for the current setup checklist."
    )
    return f"Background job support is not ready, so {action} is unavailable.{suffix}"
