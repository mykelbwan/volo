"""
core.observer – event-driven execution components for Volo.

Modules
-------
trigger_registry  – CRUD interface for the intent_triggers MongoDB collection.
price_observer    – async CoinGecko REST + Dexscreener price feed.
trigger_matcher   – evaluates pending triggers against live price data.
watcher           – top-level ObserverWatcher orchestrator service.
"""
