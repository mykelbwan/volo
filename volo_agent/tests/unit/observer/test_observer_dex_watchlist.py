import asyncio

from unittest.mock import AsyncMock

from core.observer.watcher import ObserverWatcher


class _DummyPriceObserver:
    def __init__(self):
        self.tokens = []
        self.symbols = []

    async def set_dex_tokens(self, tokens):
        self.tokens = tokens

    async def set_symbols(self, symbols):
        self.symbols = symbols


def _make_watcher(registry):
    watcher = ObserverWatcher.__new__(ObserverWatcher)
    watcher._registry = registry
    watcher._price_observer = _DummyPriceObserver()
    watcher._dex_address_cache = {}
    watcher._static_symbols = []
    watcher._volume_symbols = set()
    return watcher


def test_refresh_dex_watchlist_uses_address_when_present():
    registry = AsyncMock()
    registry.get_pending_price_triggers.return_value = [
        {
            "trigger_id": "t-1",
            "trigger_condition": {
                "type": "price_above",
                "asset": "PEPE",
                "chain": "ethereum",
                "token_address": "0xabcdefabcdef0000000000000000000000000000",
                "target": 1.0,
            },
        }
    ]

    watcher = _make_watcher(registry)
    asyncio.run(watcher._refresh_price_watchlist())

    assert len(watcher._price_observer.tokens) == 1
    token = watcher._price_observer.tokens[0]
    assert token.price_key == "ethereum:0xabcdefabcdef0000000000000000000000000000"


def test_refresh_dex_watchlist_resolves_address_from_symbol(monkeypatch):
    registry = AsyncMock()
    registry.get_pending_price_triggers.return_value = [
        {
            "trigger_id": "t-2",
            "trigger_condition": {
                "type": "price_below",
                "asset": "PEPE",
                "chain": "ethereum",
                "target": 1.0,
            },
        }
    ]

    def _fake_get_token_data(symbol, chain):
        return {
            "symbol": symbol.upper(),
            "decimals": 18,
            "chains": {chain: {"address": "0xabcdefabcdef0000000000000000000000000000"}},
        }

    def _fake_get_address_for_chain(token_data, chain):
        return token_data["chains"][chain]["address"]

    import intent_hub.registry.token_service as token_service

    monkeypatch.setattr(token_service, "get_token_data", _fake_get_token_data)
    monkeypatch.setattr(token_service, "get_address_for_chain", _fake_get_address_for_chain)

    watcher = _make_watcher(registry)
    asyncio.run(watcher._refresh_price_watchlist())

    assert len(watcher._price_observer.tokens) == 1
    token = watcher._price_observer.tokens[0]
    assert token.price_key == "ethereum:0xabcdefabcdef0000000000000000000000000000"
