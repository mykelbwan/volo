import asyncio
import time
from typing import Any, Dict, Tuple, Union, cast

from eth_typing import HexStr
from web3 import Web3

from config.chains import get_chain_by_name
from core.history.task_history import TaskHistoryRegistry
from core.idempotency.store import (
    IdempotencyStore,
)
from core.reservations.service import get_reservation_service
from core.tasks.registry import ConversationTaskRegistry
from core.utils.async_resources import async_resource_scope
from core.utils.async_tools import run_blocking
from core.utils.event_stream import publish_event, publish_event_async
from core.utils.timeouts import resolve_tool_timeout
from graph.agent_state import AgentState
from graph.nodes.runtime_factories import build_execution_runtime

_NATIVE = "0x0000000000000000000000000000000000000000"
_TASK_HISTORY_WRITE_TIMEOUT_SECONDS = 0.2
tools_registry: Any | None = None
_TOOLS_REGISTRY_CACHE: Any | None = None
_W3_CACHE: Dict[str, Web3] = {}


def _get_w3(rpc_url: str) -> Web3:
    if rpc_url not in _W3_CACHE:
        _W3_CACHE[rpc_url] = Web3(Web3.HTTPProvider(rpc_url))
    return _W3_CACHE[rpc_url]


def _get_tools_registry():
    global tools_registry, _TOOLS_REGISTRY_CACHE
    if tools_registry is not None:
        return tools_registry
    if _TOOLS_REGISTRY_CACHE is None:
        from tools_registry.register import tools_registry as _tools_registry

        _TOOLS_REGISTRY_CACHE = _tools_registry
    return _TOOLS_REGISTRY_CACHE


def _normalize_output(node, result: Dict[str, Any]) -> Dict[str, Any] | None:
    if not isinstance(result, dict):
        return None

    output: Dict[str, Any] = {"tool": node.tool}
    network = (
        result.get("network")
        or result.get("chain")
        or result.get("source_chain")
        or node.args.get("network")
        or node.args.get("chain")
    )
    if network:
        output["network"] = network
        output["chain"] = network

    if result.get("tx_hash"):
        output["tx_hash"] = result.get("tx_hash")
    if result.get("approve_hash"):
        output["approve_hash"] = result.get("approve_hash")

    if node.tool == "swap":
        output_amount = result.get("amount_out") or result.get("amount_out_minimum")
        output["amount_in"] = result.get("amount_in") or node.args.get("amount_in")
        if output_amount is not None:
            output["output_amount"] = output_amount
        output["token_in_symbol"] = node.args.get("token_in_symbol")
        output["token_out_symbol"] = node.args.get("token_out_symbol")
        output["protocol"] = result.get("protocol")
        return output

    if node.tool == "bridge":
        output["amount_in"] = result.get("input_amount") or node.args.get("amount")
        output["output_amount"] = result.get("output_amount")
        output["token_symbol"] = result.get("token_symbol") or node.args.get(
            "token_symbol"
        )
        output["source_chain"] = result.get("source_chain") or node.args.get(
            "source_chain"
        )
        output["dest_chain"] = result.get("dest_chain") or node.args.get("target_chain")
        output["protocol"] = result.get("protocol")
        return output

    if node.tool == "transfer":
        output["amount"] = node.args.get("amount")
        asset_symbol = (
            result.get("asset_symbol")
            or result.get("token_symbol")
            or node.args.get("asset_symbol")
            or node.args.get("token_symbol")
        )
        if asset_symbol:
            output["asset_symbol"] = asset_symbol
            output["token_symbol"] = asset_symbol
        output["recipient"] = node.args.get("recipient")
        return output

    if node.tool == "unwrap":
        output["amount"] = result.get("amount") or node.args.get("amount")
        token_symbol = result.get("token_symbol") or node.args.get("token_symbol")
        if token_symbol:
            output["token_symbol"] = token_symbol
        wrapped_symbol = result.get("wrapped_token_symbol") or node.args.get(
            "wrapped_token_symbol"
        )
        if wrapped_symbol:
            output["wrapped_token_symbol"] = wrapped_symbol
        return output

    if node.tool == "check_balance":
        balances = result.get("balances")
        if balances is not None:
            output["balances"] = balances
        return output

    return output if len(output) > 1 else None


def _swap_failure_message(chain: str | None, has_suggestion: bool) -> str:
    if chain and str(chain).strip().lower() not in {"", "unknown"}:
        prefix = f"The swap didn't go through on {chain}."
    else:
        prefix = "The swap didn't go through."
    if has_suggestion:
        return f"{prefix} I can try a safer setting. Reply 'go ahead' to try that."
    return f"{prefix} Reply 'retry' to try again."


def _bridge_failure_message(chain: str | None, has_suggestion: bool) -> str:
    if chain and str(chain).strip().lower() not in {"", "unknown"}:
        prefix = f"The bridge didn't go through on {chain}."
    else:
        prefix = "The bridge didn't go through."
    if has_suggestion:
        return f"{prefix} I can try a smaller amount. Reply 'go ahead' to try that."
    return f"{prefix} Reply 'retry' to try again."


def _tx_receipt_status(chain_name: str, tx_hash: str) -> str:
    try:
        chain = get_chain_by_name(chain_name)
        w3 = _get_w3(chain.rpc_url)
        receipt = w3.eth.get_transaction_receipt(cast(HexStr, tx_hash))
    except Exception:
        return "pending"
    if receipt is None:
        return "pending"
    status = getattr(receipt, "status", None)
    if status == 1:
        return "success"
    if status == 0:
        return "failed"
    return "pending"


def _resolve_tool_timeout(tool_obj: Any) -> float | None:
    explicit = getattr(tool_obj, "timeout_seconds", None)
    return resolve_tool_timeout(getattr(tool_obj, "name", None), explicit)


async def _run_with_timing(
    tool_obj: Any, args: Dict[str, Any]
) -> Tuple[Union[Dict[str, Any], Exception], float]:
    """Wraps tool execution to measure wall-clock time."""
    start = time.time()
    try:
        timeout_seconds = _resolve_tool_timeout(tool_obj)
        async with async_resource_scope():
            if timeout_seconds is None:
                res = await tool_obj.run(args)
            else:
                res = await asyncio.wait_for(
                    tool_obj.run(args), timeout=timeout_seconds
                )
        return res, time.time() - start
    except asyncio.TimeoutError:
        timeout_seconds = _resolve_tool_timeout(tool_obj)
        msg = "tool timeout"
        if timeout_seconds is not None:
            msg = f"tool timeout after {timeout_seconds:.2f}s"
        return TimeoutError(f"{tool_obj.name} {msg}"), time.time() - start
    except Exception as e:
        return e, time.time() - start


async def execution_engine_node(state: AgentState) -> Dict[str, Any]:
    history = state.get("plan_history", [])
    if not history:
        return {}

    plan = history[-1]
    execution_state = state.get("execution_state")

    if not plan or not execution_state:
        return {}
    runtime = build_execution_runtime(
        task_history_registry_cls=TaskHistoryRegistry,
        task_registry_cls=ConversationTaskRegistry,
        idempotency_store_cls=IdempotencyStore,
        reservation_service_getter=get_reservation_service,
        run_with_timing=_run_with_timing,
        run_blocking=run_blocking,
        tools_registry=_get_tools_registry(),
        normalize_output=_normalize_output,
        swap_failure_message=_swap_failure_message,
        bridge_failure_message=_bridge_failure_message,
        tx_receipt_status=_tx_receipt_status,
        publish_event=publish_event,
        publish_event_async=publish_event_async,
        task_history_write_timeout_seconds=_TASK_HISTORY_WRITE_TIMEOUT_SECONDS,
        native_marker=_NATIVE,
    )
    return await runtime.run(plan=plan, execution_state=execution_state, state=state)
