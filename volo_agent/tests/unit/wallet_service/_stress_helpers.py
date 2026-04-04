from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any


async def sleep_ticks(ticks: int) -> None:
    for _ in range(max(0, int(ticks))):
        await asyncio.sleep(0)


@dataclass
class Heartbeat:
    ticks: int = 0
    _running: bool = field(default=True, init=False)

    async def run(self) -> None:
        while self._running:
            self.ticks += 1
            await asyncio.sleep(0)

    def stop(self) -> None:
        self._running = False


@dataclass
class FakeHTTPResponse:
    status_code: int = 200
    payload: Any = field(default_factory=dict)
    text: str = ""

    def json(self) -> Any:
        return self.payload
