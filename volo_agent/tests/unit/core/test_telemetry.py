import asyncio

import pytest

from core.utils.telemetry import wrap_node


def test_wrap_node_returns_result():
    async def _node():
        return {"ok": True}

    wrapped = wrap_node("test_node", _node, timeout_seconds=1.0)
    result = asyncio.run(wrapped())

    assert result == {"ok": True}


def test_wrap_node_times_out():
    async def _slow():
        await asyncio.sleep(0.05)
        return {"ok": True}

    wrapped = wrap_node("slow_node", _slow, timeout_seconds=0.01)
    with pytest.raises(asyncio.TimeoutError):
        asyncio.run(wrapped())
