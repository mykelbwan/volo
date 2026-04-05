from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI

from api.lifespan.workers import start_workers, stop_workers


@asynccontextmanager
async def lifespan(_app: FastAPI):
    handles = await start_workers()
    try:
        yield
    finally:
        await stop_workers(handles)

