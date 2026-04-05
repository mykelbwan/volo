from fastapi import APIRouter, status
from fastapi.responses import JSONResponse

from core.health import run_startup_checks_async

router = APIRouter(prefix="/health", tags=["health"])


@router.get("/live")
async def health_live() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/ready")
async def health_ready() -> JSONResponse:
    result = await run_startup_checks_async(raise_on_failure=False)
    is_ready = bool(result.ok)
    return JSONResponse(
        status_code=(
            status.HTTP_200_OK
            if is_ready
            else status.HTTP_503_SERVICE_UNAVAILABLE
        ),
        content={
            "status": "ok" if is_ready else "not_ready",
            "checks": result.checks,
        },
    )
