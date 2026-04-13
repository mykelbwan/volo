from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Iterable, Optional

from config.chains import CHAINS, find_chain_by_id, find_chain_by_name
from config.solana_chains import SOLANA_CHAINS, get_solana_chain, get_solana_chain_by_id

EVM_NATIVE_ASSET_REF = "0x0000000000000000000000000000000000000000"


@dataclass(frozen=True)
class ChainCatalogEntry:
    family: str
    key: str
    display_name: str
    chain_id: int
    native_symbol: str
    native_asset_ref: str
    explorer_url: Optional[str]
    is_testnet: bool
    rpc_configured: bool


@dataclass(frozen=True)
class _FamilyResolver:
    family: str
    resolve_by_name: Callable[[str], Optional[ChainCatalogEntry]]
    resolve_by_id: Callable[[int], Optional[ChainCatalogEntry]]
    list_entries: Callable[[], Iterable[ChainCatalogEntry]]


def _normalize(value: str | None) -> str:
    return str(value or "").strip().lower()


def _evm_entry_from_config(chain_cfg) -> ChainCatalogEntry:
    return ChainCatalogEntry(
        family="evm",
        key=chain_cfg.name.strip().lower(),
        display_name=chain_cfg.name,
        chain_id=chain_cfg.chain_id,
        native_symbol=chain_cfg.native_symbol,
        native_asset_ref=EVM_NATIVE_ASSET_REF,
        explorer_url=chain_cfg.explorer_url,
        is_testnet=bool(chain_cfg.is_testnet),
        rpc_configured=bool(str(chain_cfg.rpc_url or "").strip()),
    )


def _solana_entry_from_config(chain_cfg) -> ChainCatalogEntry:
    return ChainCatalogEntry(
        family="solana",
        key=chain_cfg.network,
        display_name=chain_cfg.name,
        chain_id=chain_cfg.chain_id,
        native_symbol=chain_cfg.native_symbol,
        native_asset_ref=chain_cfg.native_mint,
        explorer_url=chain_cfg.explorer_url,
        is_testnet=bool(chain_cfg.is_testnet),
        rpc_configured=bool(str(chain_cfg.rpc_url or "").strip()),
    )


def _resolve_evm_by_name(value: str) -> Optional[ChainCatalogEntry]:
    try:
        return _evm_entry_from_config(find_chain_by_name(value))
    except KeyError:
        return None


def _resolve_evm_by_id(chain_id: int) -> Optional[ChainCatalogEntry]:
    try:
        return _evm_entry_from_config(find_chain_by_id(chain_id))
    except KeyError:
        return None


def _list_evm_entries() -> Iterable[ChainCatalogEntry]:
    for chain in CHAINS.values():
        yield _evm_entry_from_config(chain)


def _resolve_solana_by_name(value: str) -> Optional[ChainCatalogEntry]:
    try:
        return _solana_entry_from_config(get_solana_chain(value))
    except KeyError:
        return None


def _resolve_solana_by_id(chain_id: int) -> Optional[ChainCatalogEntry]:
    try:
        return _solana_entry_from_config(get_solana_chain_by_id(chain_id))
    except KeyError:
        return None


def _list_solana_entries() -> Iterable[ChainCatalogEntry]:
    for chain in SOLANA_CHAINS.values():
        yield _solana_entry_from_config(chain)


_FAMILY_RESOLVERS: tuple[_FamilyResolver, ...] = (
    _FamilyResolver(
        family="evm",
        resolve_by_name=_resolve_evm_by_name,
        resolve_by_id=_resolve_evm_by_id,
        list_entries=_list_evm_entries,
    ),
    _FamilyResolver(
        family="solana",
        resolve_by_name=_resolve_solana_by_name,
        resolve_by_id=_resolve_solana_by_id,
        list_entries=_list_solana_entries,
    ),
)


def resolve_chain(value: str | None) -> Optional[ChainCatalogEntry]:
    normalized = _normalize(value)
    if not normalized:
        return None

    matches: list[ChainCatalogEntry] = []
    for resolver in _FAMILY_RESOLVERS:
        entry = resolver.resolve_by_name(normalized)
        if entry is not None:
            matches.append(entry)

    if not matches:
        return None
    if len(matches) == 1:
        return matches[0]

    first = matches[0]
    if all(
        m.chain_id == first.chain_id and m.key == first.key and m.family == first.family
        for m in matches[1:]
    ):
        return first

    families = ", ".join(sorted({m.family for m in matches}))
    raise KeyError(f"Chain {value!r} is ambiguous across families: {families}")


def resolve_chain_or_raise(value: str | None) -> ChainCatalogEntry:
    entry = resolve_chain(value)
    if entry is not None:
        return entry
    raise KeyError(f"Chain {value!r} is not registered.")


def canonicalize_chain_key(value: str | None) -> Optional[str]:
    entry = resolve_chain(value)
    return entry.key if entry is not None else None


def resolve_chain_by_id(chain_id: int) -> Optional[ChainCatalogEntry]:
    matches: list[ChainCatalogEntry] = []
    for resolver in _FAMILY_RESOLVERS:
        entry = resolver.resolve_by_id(chain_id)
        if entry is not None:
            matches.append(entry)

    if not matches:
        return None
    if len(matches) == 1:
        return matches[0]

    first = matches[0]
    if all(
        m.chain_id == first.chain_id and m.key == first.key and m.family == first.family
        for m in matches[1:]
    ):
        return first

    families = ", ".join(sorted({m.family for m in matches}))
    raise KeyError(f"Chain id {chain_id!r} is ambiguous across families: {families}")


def list_chain_catalog(
    *,
    include_testnets: bool = True,
    require_rpc: bool = False,
) -> list[ChainCatalogEntry]:
    entries: list[ChainCatalogEntry] = []
    for resolver in _FAMILY_RESOLVERS:
        for entry in resolver.list_entries():
            if not include_testnets and entry.is_testnet:
                continue
            if require_rpc and not entry.rpc_configured:
                continue
            entries.append(entry)

    entries.sort(key=lambda item: (item.is_testnet, item.family, item.display_name.lower()))
    return entries

