import inspect
from importlib import import_module
from typing import Any, Awaitable, Callable, cast

from intent_hub.ontology.intent import Intent
from intent_hub.utils.messages import format_with_recovery

_RESOLVER_IMPORTS = {
    "swap": ("intent_hub.resolver.swap_resolver", "resolve_swap"),
    "bridge": ("intent_hub.resolver.bridge_resolver", "resolve_bridge"),
    "transfer": ("intent_hub.resolver.transfer_resolver", "resolve_transfer"),
    "unwrap": ("intent_hub.resolver.unwrap_resolver", "resolve_unwrap"),
    "balance": ("intent_hub.resolver.balance_resolver", "resolve_balance"),
}
_RESOLVER_CACHE: dict[str, Callable[[Intent], Awaitable[Any]]] = {}


def _get_resolver(intent_type: str) -> Callable[[Intent], Awaitable[Any]] | None:
    cached = _RESOLVER_CACHE.get(intent_type)
    if cached is not None:
        return cached
    mapping = _RESOLVER_IMPORTS.get(intent_type)
    if mapping is None:
        return None
    module_name, fn_name = mapping
    module = import_module(module_name)
    resolver = getattr(module, fn_name, None)
    if callable(resolver):
        if not inspect.iscoroutinefunction(resolver):
            raise TypeError(f"Resolver for {intent_type} must be a coroutine function")
        _RESOLVER_CACHE[intent_type] = cast(
            Callable[[Intent], Awaitable[Any]], resolver
        )
        return resolver
    return None


async def resolve_intent(intent: Intent):
    resolver = _get_resolver(intent.intent_type)
    if resolver is None:
        raise NotImplementedError(
            format_with_recovery(
                f"Intent type '{intent.intent_type}' is not supported",
                "retry with one of: swap, bridge, transfer, unwrap, or balance",
            )
        )
    return await resolver(intent)
