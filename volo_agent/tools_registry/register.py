import logging
from importlib import import_module
from typing import Any

from core.memory.ledger import ErrorCategory
from core.tools.base import Registry, Tool
from core.tools.schemas import (
    BalanceArgs,
    BridgeArgs,
    SolanaSwapArgs,
    SwapArgs,
    TransferArgs,
    UnwrapArgs,
)
from core.utils.timeouts import TOOL_DEFAULT_TIMEOUTS, resolve_tool_timeout

_LOGGER = logging.getLogger("volo.tools")
_LOADED_SYMBOLS: dict[str, Any] = {}


def _load_symbol(module_path: str, symbol_name: str):
    cache_key = f"{module_path}:{symbol_name}"
    cached = _LOADED_SYMBOLS.get(cache_key)
    if cached is not None:
        return cached
    try:
        module = import_module(module_path)
        resolved = getattr(module, symbol_name)
        _LOADED_SYMBOLS[cache_key] = resolved
        return resolved
    except (ImportError, AttributeError) as exc:
        _LOGGER.error("failed_to_load_symbol %s %s: %s", module_path, symbol_name, exc)
        raise


async def _run_lazy_tool(module_path: str, symbol_name: str, args: dict[str, Any]):
    tool_fn = _load_symbol(module_path, symbol_name)
    return await tool_fn(args)


def _run_lazy_fix(
    module_path: str,
    symbol_name: str,
    error_category: Any,
    current_args: dict[str, Any],
    error_msg: str,
):
    fix_fn = _load_symbol(module_path, symbol_name)
    return fix_fn(error_category, current_args, error_msg)


def _suggest_slippage(
    args: dict[str, Any], *, step: float = 0.5, max_slippage: float = 5.0
) -> dict[str, Any] | None:
    try:
        current_slippage = float(args.get("slippage", 0.5))
    except (ValueError, TypeError):
        current_slippage = 0.5

    if current_slippage >= max_slippage:
        return None
    new_args = args.copy()
    new_args["slippage"] = round(min(current_slippage + step, max_slippage), 2)
    return new_args


def _suggest_amount_reduction(
    args: dict[str, Any], amount_key: str
) -> dict[str, Any] | None:
    try:
        current_amount = float(args.get(amount_key, 0))
    except (ValueError, TypeError):
        current_amount = 0

    if current_amount <= 0:
        return None
    new_args = args.copy()
    new_args[amount_key] = round(current_amount * 0.9, 6)
    return new_args


async def _swap_tool(args: dict[str, Any]):
    return await _run_lazy_tool("tool_nodes.dex.swap", "swap_token", args)


def _swap_fix(error_category: Any, current_args: dict[str, Any], error_msg: str):
    if error_category == ErrorCategory.SLIPPAGE:
        return _suggest_slippage(current_args)
    if error_category == ErrorCategory.LIQUIDITY:
        return _suggest_amount_reduction(current_args, "amount_in")
    return None


async def _bridge_tool(args: dict[str, Any]):
    return await _run_lazy_tool("tool_nodes.bridge.bridge_tool", "bridge_token", args)


def _bridge_fix(error_category: Any, current_args: dict[str, Any], error_msg: str):
    if error_category == ErrorCategory.LIQUIDITY:
        return _suggest_amount_reduction(current_args, "amount")
    return None


async def _transfer_tool(args: dict[str, Any]):
    return await _run_lazy_tool("tool_nodes.wallet.transfer", "transfer_token", args)


def _transfer_fix(error_category: Any, current_args: dict[str, Any], error_msg: str):
    if error_category == ErrorCategory.GAS:
        return current_args.copy()
    return None


async def _check_balance_tool(args: dict[str, Any]):
    return await _run_lazy_tool("tool_nodes.wallet.balance", "check_balance", args)


async def _unwrap_tool(args: dict[str, Any]):
    return await _run_lazy_tool("tool_nodes.wallet.unwrap", "unwrap_token", args)


async def _solana_swap_tool(args: dict[str, Any]):
    return await _run_lazy_tool("tool_nodes.solana.swap", "solana_swap_token", args)


def _solana_swap_fix(error_category: Any, current_args: dict[str, Any], error_msg: str):
    return _run_lazy_fix(
        "tool_nodes.solana.swap",
        "suggest_solana_swap_fix",
        error_category,
        current_args,
        error_msg,
    )


def _log_tool_timeouts(registry: Registry) -> None:
    entries = []
    for name, tool in registry.tools.items():
        timeout = resolve_tool_timeout(name, tool.timeout_seconds)
        if timeout is None:
            entries.append(f"{name}=none")
        else:
            entries.append(f"{name}={timeout:.2f}s")
    _LOGGER.info("tool_timeouts %s", ", ".join(entries))


# Define a central tool registry
tools_registry = Registry()

tools_registry.register(
    Tool(
        name="swap",
        description="Swap one crypto token for another on a specific blockchain using the best available DEX protocol (V3 preferred, V2 fallback).",
        func=_swap_tool,
        on_suggest_fix=_swap_fix,
        args_schema=SwapArgs,
        category="dex",
        timeout_seconds=TOOL_DEFAULT_TIMEOUTS.get("swap"),
    )
)

tools_registry.register(
    Tool(
        name="bridge",
        description="Transfer a crypto token from one blockchain to another.",
        func=_bridge_tool,
        on_suggest_fix=_bridge_fix,
        args_schema=BridgeArgs,
        category="bridge",
        timeout_seconds=TOOL_DEFAULT_TIMEOUTS.get("bridge"),
    )
)

tools_registry.register(
    Tool(
        name="transfer",
        description="Transfer a crypto token (Native or ERC20) to another wallet address on the same chain.",
        func=_transfer_tool,
        on_suggest_fix=_transfer_fix,
        args_schema=TransferArgs,
        category="wallet",
        timeout_seconds=TOOL_DEFAULT_TIMEOUTS.get("transfer"),
    )
)

tools_registry.register(
    Tool(
        name="check_balance",
        description="Check all token balances (Native and ERC20) for a wallet on a specific chain using the indexer.",
        func=_check_balance_tool,
        args_schema=BalanceArgs,
        category="wallet",
        timeout_seconds=TOOL_DEFAULT_TIMEOUTS.get("check_balance"),
    )
)

tools_registry.register(
    Tool(
        name="unwrap",
        description="Unwrap wrapped native tokens (e.g. WETH to ETH) on the same chain.",
        func=_unwrap_tool,
        args_schema=UnwrapArgs,
        category="wallet",
        timeout_seconds=TOOL_DEFAULT_TIMEOUTS.get("unwrap", 45.0),
    )
)

tools_registry.register(
    Tool(
        name="solana_swap",
        description=("Swap one token for another on the Solana blockchain. "),
        func=_solana_swap_tool,
        on_suggest_fix=_solana_swap_fix,
        args_schema=SolanaSwapArgs,
        category="dex",
        timeout_seconds=TOOL_DEFAULT_TIMEOUTS.get("solana_swap", 60.0),
    )
)

_log_tool_timeouts(tools_registry)
