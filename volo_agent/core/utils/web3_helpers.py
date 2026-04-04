from __future__ import annotations

from typing import Any, Iterable


def encode_contract_call(contract: Any, fn_name: str, args: Iterable[Any]) -> str:
    if hasattr(contract, "encodeABI"):
        return contract.encodeABI(fn_name=fn_name, args=list(args))
    if hasattr(contract, "encode_abi"):
        try:
            return contract.encode_abi(fn_name=fn_name, args=list(args))
        except TypeError:
            return contract.encode_abi(fn_name, args=list(args))
    try:
        fn = getattr(contract.functions, fn_name)(*list(args))
        return fn._encode_transaction_data()
    except Exception as exc:  # pragma: no cover - defensive fallback
        raise RuntimeError(f"Unable to encode calldata for {fn_name}") from exc
