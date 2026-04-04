import asyncio

from intent_hub.ontology.intent import Intent, IntentStatus
import intent_hub.resolver.bridge_resolver as bridge_resolver
import intent_hub.resolver.transfer_resolver as transfer_resolver


def test_resolve_bridge_splits_amount_prefixed_symbol(monkeypatch):
    async def fake_get_token_data(symbol: str, chain: str):
        symbol = symbol.upper()
        if symbol == "1STT":
            return {"symbol": symbol, "decimals": 18, "chains": {}}
        if symbol == "STT":
            return {
                "symbol": symbol,
                "decimals": 18,
                "chains": {
                    "sepolia": {"address": "0xstt"},
                    "base sepolia": {"address": "0xsttbase"},
                },
            }
        return {"symbol": symbol, "decimals": 18, "chains": {}}

    monkeypatch.setattr(bridge_resolver, "get_token_data_async", fake_get_token_data)

    intent = Intent(
        intent_type="bridge",
        slots={
            "token_in": {"symbol": "1STT"},
            "amount": None,
            "chain": "sepolia",
            "target_chain": "base sepolia",
        },
        missing_slots=[],
        constraints=None,
        confidence=0.9,
        status=IntentStatus.COMPLETE,
        raw_input="bridge 1stt from sepolia to base sepolia",
    )

    plan = asyncio.run(bridge_resolver.resolve_bridge(intent))

    assert plan.parameters["token_symbol"] == "STT"
    assert plan.parameters["amount"] == "1"
    assert plan.parameters["amount_in_wei"] == "1000000000000000000"


def test_resolve_transfer_splits_amount_prefixed_symbol(monkeypatch):
    async def fake_get_token_data(symbol: str, chain: str):
        symbol = symbol.upper()
        if symbol == "1STT":
            return {"symbol": symbol, "decimals": 18, "chains": {}}
        if symbol == "STT":
            return {
                "symbol": symbol,
                "decimals": 18,
                "chains": {chain: {"address": "0xstt"}},
            }
        return {"symbol": symbol, "decimals": 18, "chains": {}}

    monkeypatch.setattr(transfer_resolver, "get_token_data_async", fake_get_token_data)

    intent = Intent(
        intent_type="transfer",
        slots={
            "token": {"symbol": "1STT"},
            "amount": None,
            "recipient": "0x000000000000000000000000000000000000dead",
            "chain": "somnia testnet",
        },
        missing_slots=[],
        constraints=None,
        confidence=0.9,
        status=IntentStatus.COMPLETE,
        raw_input="send 1stt to 0xdead on somnia testnet",
    )

    plan = asyncio.run(transfer_resolver.resolve_transfer(intent))

    assert plan.parameters["asset_symbol"] == "STT"
    assert plan.parameters["asset_ref"] == "0xstt"
    assert plan.parameters["amount"] == "1"
    assert plan.parameters["network"] == "somnia testnet"
