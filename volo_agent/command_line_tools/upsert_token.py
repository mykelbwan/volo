"""
Manual token registry upsert utility.

Writes a single (symbol, chain_id) entry into the `token_registry`
collection without importing `config.env` (so it won't fail on missing
API keys). Use for testnets or manual overrides.

Quick start
-----------
1) Ensure you have a MongoDB URI:
   export MONGODB_URI

2) Run the script:
   .venv/bin/python tools_registry/upsert_token.py \
     --symbol NIA \
     --address 0xF2F773753cEbEFaF9b68b841d80C083b18C69311 \
     --decimals 18 \
     --chain-id 50312 \
     --chain-name chain_name

Notes
-----
- `--chain-id` is required. If the chain ID exists in `config/chains.py`,
  the chain name is inferred automatically. Otherwise pass `--chain-name`.
- The default database is `auraagent`. Override with `--db`.
- This tool does not validate token metadata on-chain; it just upserts
  the registry record.
"""

from __future__ import annotations

import argparse
import os
import re
from datetime import datetime, timezone
from typing import Optional

from pymongo import MongoClient
from dotenv import load_dotenv


def _load_local_env() -> None:
    try:
        load_dotenv()
    except Exception:
        pass

try:
    from config.chains import get_chain_by_id
except Exception:  # pragma: no cover - optional convenience
    get_chain_by_id = None  # type: ignore[assignment]


_ADDRESS_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")


def _now() -> datetime:
    return datetime.now(tz=timezone.utc)


def _require_mongo_uri() -> str:
    uri = os.getenv("MONGODB_URI")
    if not uri:
        raise SystemExit(
            "MONGODB_URI is not set. Export it and re-run. "
            "Example: export MONGODB_URI='mongodb://localhost:27017'"
        )
    return uri


def _resolve_chain_name(chain_id: int, chain_name: Optional[str]) -> str:
    if chain_name:
        return chain_name.strip().lower()
    if get_chain_by_id is None:
        raise SystemExit(
            "Chain name is required because config.chains could not be loaded. "
            "Pass --chain-name explicitly."
        )
    try:
        return get_chain_by_id(chain_id).name.lower()
    except Exception:
        raise SystemExit(
            f"Unknown chain_id {chain_id}. Pass --chain-name explicitly."
        )


def _validate_address(address: str) -> str:
    addr = address.strip()
    if not _ADDRESS_RE.match(addr):
        raise SystemExit(f"Invalid ERC-20 address: {address}")
    return addr


def main() -> None:
    _load_local_env()
    parser = argparse.ArgumentParser(description="Upsert token into MongoDB registry.")
    parser.add_argument("--symbol", required=True, help="Token symbol, e.g. NIA")
    parser.add_argument("--address", required=True, help="ERC-20 contract address")
    parser.add_argument("--decimals", required=True, type=int, help="Token decimals")
    parser.add_argument("--chain-id", required=True, type=int, help="EIP-155 chain id")
    parser.add_argument(
        "--chain-name",
        help="Chain name (optional if chain-id is known in config/chains.py)",
    )
    parser.add_argument("--name", help="Token name (optional)")
    parser.add_argument(
        "--aliases",
        help="Comma-separated aliases (optional), e.g. 'nia,token nia'",
    )
    parser.add_argument(
        "--db",
        default="auraagent",
        help="MongoDB database name (default: auraagent)",
    )
    parser.add_argument(
        "--source",
        default="manual",
        help="Provenance label (default: manual)",
    )

    args = parser.parse_args()

    symbol = args.symbol.strip().upper()
    address = _validate_address(args.address)
    decimals = int(args.decimals)
    chain_id = int(args.chain_id)
    chain_name = _resolve_chain_name(chain_id, args.chain_name)
    aliases = [a.strip().lower() for a in (args.aliases or "").split(",") if a.strip()]

    reg_key = f"{symbol}:{chain_id}"
    now = _now()

    doc = {
        "_reg_key": reg_key,
        "symbol": symbol,
        "name": args.name,
        "chain_name": chain_name,
        "chain_id": chain_id,
        "address": address,
        "decimals": decimals,
        "aliases": aliases,
        "is_active": True,
        "source": args.source,
        "added_at": now,
        "updated_at": now,
    }

    uri = _require_mongo_uri()
    client = MongoClient(uri)
    col = client[args.db]["token_registry"]
    col.replace_one({"_reg_key": reg_key}, doc, upsert=True)

    print("Upserted token:")
    print(f"  symbol:     {symbol}")
    print(f"  chain_id:   {chain_id}")
    print(f"  chain_name: {chain_name}")
    print(f"  address:    {address}")
    print(f"  decimals:   {decimals}")


if __name__ == "__main__":
    main()
