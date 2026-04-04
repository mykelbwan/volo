from pathlib import Path


def _reset_capabilities_cache(caps) -> None:
    with caps._LOCK:
        caps._CACHE.clear()
        caps._LOADED = False


def test_router_capabilities_persist(tmp_path, monkeypatch):
    path = tmp_path / "caps.json"
    monkeypatch.setenv("CHAIN_CAPABILITIES_PATH", str(path))

    from core import chain_capabilities as caps

    _reset_capabilities_cache(caps)

    initial = caps.get_router_capabilities(1, "v2", "0xRouter", True)
    assert initial.supports_native_swaps is True

    caps.set_router_capabilities(
        1, "v2", "0xRouter", supports_native_swaps=False
    )
    updated = caps.get_router_capabilities(1, "v2", "0xRouter", True)
    assert updated.supports_native_swaps is False
    assert Path(path).exists()


def test_router_capabilities_hard_disable(monkeypatch, tmp_path):
    path = tmp_path / "caps.json"
    monkeypatch.setenv("CHAIN_CAPABILITIES_PATH", str(path))

    from core import chain_capabilities as caps

    _reset_capabilities_cache(caps)
    caps.set_router_capabilities(1, "v2", "0xRouter", supports_native_swaps=True)

    # Even if cache says True, default False should hard-disable.
    result = caps.get_router_capabilities(1, "v2", "0xRouter", False)
    assert result.supports_native_swaps is False
