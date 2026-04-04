from __future__ import annotations

import asyncio

import pytest
from pydantic import BaseModel, ValidationError

from core.tools.base import Registry, Tool


class _ArgsSchema(BaseModel):
    amount: float
    sender: str


async def _echo_tool(args):
    return {"ok": True, "args": args}


def test_registry_instances_do_not_share_tool_dict():
    left = Registry()
    right = Registry()

    left.register(
        Tool(
            name="swap",
            description="swap tool",
            func=_echo_tool,
        )
    )

    assert "swap" in left.tools
    assert right.tools == {}


def test_tool_run_validates_args_schema_before_execution():
    tool = Tool(
        name="schema_tool",
        description="schema tool",
        func=_echo_tool,
        args_schema=_ArgsSchema,
    )

    result = asyncio.run(tool.run({"amount": 1.5, "sender": "0xabc"}))

    assert result["ok"] is True
    assert result["args"]["amount"] == 1.5


def test_tool_run_replaces_dynamic_markers_for_schema_validation():
    tool = Tool(
        name="marker_tool",
        description="marker tool",
        func=_echo_tool,
        args_schema=_ArgsSchema,
    )

    result = asyncio.run(tool.run({"amount": "{{AMOUNT}}", "sender": "{{SENDER_ADDRESS}}"}))

    assert result["ok"] is True
    assert result["args"]["amount"] == "{{AMOUNT}}"


def test_tool_run_raises_on_invalid_args():
    tool = Tool(
        name="invalid_tool",
        description="invalid tool",
        func=_echo_tool,
        args_schema=_ArgsSchema,
    )

    with pytest.raises(ValidationError):
        asyncio.run(tool.run({"sender": "0xabc"}))
