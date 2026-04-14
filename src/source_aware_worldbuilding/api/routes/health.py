from fastapi import APIRouter

from source_aware_worldbuilding.services.status import build_runtime_status

router = APIRouter(tags=["health"])


@router.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/health/runtime")
def runtime_health() -> dict:
    return build_runtime_status().model_dump(mode="json")
