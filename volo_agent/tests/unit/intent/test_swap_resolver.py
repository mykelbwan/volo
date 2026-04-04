import asyncio
from unittest.mock import AsyncMock

from intent_hub.ontology.intent import Intent, IntentStatus
import intent_hub.resolver.swap_resolver as swap_resolver


def test_resolve_swap_splits_amount_prefixed_symbol_when_full_symbol_fails(
    monkeypatch,
):
    async def fake_get_token_data(symbol: str, chain: str):
        symbol = symbol.upper()
        if symbol == "1STT":
            return {"symbol": symbol, "decimals": 18, "chains": {}}
        if symbol == "STT":
            return {"symbol": symbol, "decimals": 18, "chains": {chain: {"address": "0xstt"}}}
        if symbol == "NIA":
            return {"symbol": symbol, "decimals": 18, "chains": {chain: {"address": "0xnia"}}}
        return {"symbol": symbol, "decimals": 18, "chains": {}}

    monkeypatch.setattr(swap_resolver, "get_token_data_async", fake_get_token_data)

    intent = Intent(
        intent_type="swap",
        slots={
            "token_in": {"symbol": "1STT"},
            "token_out": {"symbol": "NIA"},
            "amount": None,
            "chain": "somnia testnet",
        },
        missing_slots=[],
        constraints=None,
        confidence=0.9,
        status=IntentStatus.COMPLETE,
        raw_input="buy 1stt worth of nia on somnia testnet",
    )

    plan = asyncio.run(swap_resolver.resolve_swap(intent))

    assert plan.parameters["token_in_symbol"] == "STT"
    assert plan.parameters["amount_in"] == "1"
    assert plan.parameters["token_out_symbol"] == "NIA"
    assert plan.parameters["amount_in_wei"] == "1000000000000000000"


def test_resolve_swap_keeps_numeric_leading_symbol_when_it_resolves(monkeypatch):
    async def fake_get_token_data(symbol: str, chain: str):
        symbol = symbol.upper()
        if symbol == "1INCH":
            return {"symbol": symbol, "decimals": 18, "chains": {chain: {"address": "0x1inch"}}}
        if symbol == "INCH":
            return {"symbol": symbol, "decimals": 18, "chains": {chain: {"address": "0xinch"}}}
        if symbol == "USDC":
            return {"symbol": symbol, "decimals": 6, "chains": {chain: {"address": "0xusdc"}}}
        return {"symbol": symbol, "decimals": 18, "chains": {}}

    monkeypatch.setattr(swap_resolver, "get_token_data_async", fake_get_token_data)

    intent = Intent(
        intent_type="swap",
        slots={
            "token_in": {"symbol": "1INCH"},
            "token_out": {"symbol": "USDC"},
            "amount": 1,
            "chain": "base",
        },
        missing_slots=[],
        constraints=None,
        confidence=0.9,
        status=IntentStatus.COMPLETE,
        raw_input="swap 1 1inch to usdc on base",
    )

    plan = asyncio.run(swap_resolver.resolve_swap(intent))

    assert plan.parameters["token_in_symbol"] == "1INCH"
    assert plan.parameters["amount_in"] == 1


def test_resolve_swap_native_symbol_maps_to_zero_address(monkeypatch):
    async def fake_get_token_data(symbol: str, chain: str):
        symbol = symbol.upper()
        if symbol == "ETH":
            return {"symbol": symbol, "decimals": 18, "chains": {}}
        if symbol == "USDC":
            return {"symbol": symbol, "decimals": 6, "chains": {chain: {"address": "0xusdc"}}}
        return {"symbol": symbol, "decimals": 18, "chains": {}}

    monkeypatch.setattr(swap_resolver, "get_token_data_async", fake_get_token_data)

    intent = Intent(
        intent_type="swap",
        slots={
            "token_in": {"symbol": "ETH"},
            "token_out": {"symbol": "USDC"},
            "amount": 1,
            "chain": "base",
        },
        missing_slots=[],
        constraints=None,
        confidence=0.9,
        status=IntentStatus.COMPLETE,
        raw_input="swap 1 eth to usdc on base",
    )

    plan = asyncio.run(swap_resolver.resolve_swap(intent))

    assert plan.parameters["token_in_address"] == "0x0000000000000000000000000000000000000000"


def test_resolve_swap_native_input_uses_default_decimals_when_registry_missing(monkeypatch):
    async def fake_get_token_data(symbol: str, chain: str):
        symbol = symbol.upper()
        if symbol == "ETH":
            return {"symbol": symbol, "decimals": None, "chains": {}}
        if symbol == "USDC":
            return {"symbol": symbol, "decimals": 6, "chains": {chain: {"address": "0xusdc"}}}
        return {"symbol": symbol, "decimals": 18, "chains": {}}

    registry_lookup = AsyncMock(return_value=None)
    monkeypatch.setattr(swap_resolver, "get_token_data_async", fake_get_token_data)
    monkeypatch.setattr(
        swap_resolver,
        "get_registry_decimals_by_address_async",
        registry_lookup,
    )

    intent = Intent(
        intent_type="swap",
        slots={
            "token_in": {"symbol": "ETH"},
            "token_out": {"symbol": "USDC"},
            "amount": 2,
            "chain": "base",
        },
        missing_slots=[],
        constraints=None,
        confidence=0.9,
        status=IntentStatus.COMPLETE,
        raw_input="swap 2 eth to usdc on base",
    )

    plan = asyncio.run(swap_resolver.resolve_swap(intent))

    assert plan.parameters["amount_in_wei"] == "2000000000000000000"
    assert registry_lookup.await_count == 0
