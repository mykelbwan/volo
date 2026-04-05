from fastapi import APIRouter

from api.routes.agent import router as agent_router
from api.routes.health import router as health_router

router = APIRouter()
router.include_router(health_router)
router.include_router(agent_router)
