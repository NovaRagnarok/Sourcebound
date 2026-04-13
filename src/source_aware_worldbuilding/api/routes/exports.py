from fastapi import APIRouter, Depends, HTTPException

from source_aware_worldbuilding.api.dependencies import get_lore_packet_service
from source_aware_worldbuilding.domain.errors import CanonUnavailableError, WikibaseSyncError
from source_aware_worldbuilding.domain.models import LorePacketRequest
from source_aware_worldbuilding.services.lore_packet import LorePacketService

router = APIRouter(prefix="/v1/exports", tags=["exports"])


@router.post("/lore-packet")
def export_lore_packet(
    payload: LorePacketRequest,
    service: LorePacketService = Depends(get_lore_packet_service),
) -> dict:
    try:
        return service.export(payload).model_dump(mode="json")
    except CanonUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except WikibaseSyncError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
