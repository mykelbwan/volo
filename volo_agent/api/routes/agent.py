from __future__ import annotations

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from api.error_mappings import build_error_response_body, map_turn_error
from api.schemas.agent import AgentTurnRequest, AgentTurnResponse

router = APIRouter(prefix="/v1/agent", tags=["agent"])


@router.post("/turn", response_model=AgentTurnResponse)
async def agent_turn(payload: AgentTurnRequest):
    try:
        from api.runtime.turn_runtime import run_turn

        return await run_turn(payload)
    except Exception as exc:
        mapping = map_turn_error(exc)
        return JSONResponse(
            status_code=mapping.status_code,
            content=build_error_response_body(
                mapping=mapping,
                message=str(exc) or None,
                details={"exception_type": exc.__class__.__name__},
            ),
        )
