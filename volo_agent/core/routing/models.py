from __future__ import annotations

import time
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Dict, List, Optional, Union  # noqa: F401


def _is_stale(fetched_at: float, ttl_seconds: float) -> bool:
    return (time.time() - fetched_at) > ttl_seconds


class StaleMixin:
    fetched_at: float

    def is_stale(self, ttl_seconds: float = 45.0) -> bool:
        """True if the quote is older than *ttl_seconds*."""
        return _is_stale(self.fetched_at, ttl_seconds)


@dataclass
class SwapRouteQuote(StaleMixin):
    aggregator: str
    chain_id: int
    token_in: str
    token_out: str
    amount_in: Decimal
    amount_out: Decimal
    amount_out_min: Decimal
    gas_estimate: int
    gas_cost_usd: Optional[Decimal]
    price_impact_pct: Decimal
    calldata: Optional[str] = None
    to: Optional[str] = None
    approval_address: Optional[str] = None
    fetched_at: float = field(default_factory=time.time)
    raw: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "aggregator": self.aggregator,
            "chain_id": self.chain_id,
            "token_in": self.token_in,
            "token_out": self.token_out,
            "amount_in": str(self.amount_in),
            "amount_out": str(self.amount_out),
            "amount_out_min": str(self.amount_out_min),
            "gas_estimate": self.gas_estimate,
            "gas_cost_usd": str(self.gas_cost_usd)
            if self.gas_cost_usd is not None
            else None,
            "price_impact_pct": str(self.price_impact_pct),
            "calldata": self.calldata,
            "to": self.to,
            "approval_address": self.approval_address,
            "fetched_at": self.fetched_at,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "SwapRouteQuote":
        return cls(
            aggregator=d["aggregator"],
            chain_id=int(d["chain_id"]),
            token_in=d["token_in"],
            token_out=d["token_out"],
            amount_in=Decimal(str(d["amount_in"])),
            amount_out=Decimal(str(d["amount_out"])),
            amount_out_min=Decimal(str(d["amount_out_min"])),
            gas_estimate=int(d["gas_estimate"]),
            gas_cost_usd=Decimal(str(d["gas_cost_usd"]))
            if d.get("gas_cost_usd") is not None
            else None,
            price_impact_pct=Decimal(str(d["price_impact_pct"])),
            calldata=d.get("calldata"),
            to=d.get("to"),
            approval_address=d.get("approval_address"),
            fetched_at=float(d.get("fetched_at", time.time())),
            raw={},
        )


@dataclass
class SolanaSwapRouteQuote:
    aggregator: str
    network: str
    input_mint: str
    output_mint: str
    amount_in: Decimal
    amount_out: Decimal
    amount_out_min: Decimal
    amount_in_lamports: int
    amount_out_lamports: int
    price_impact_pct: Decimal
    swap_transaction: Optional[str] = None
    fetched_at: float = field(default_factory=time.time)
    raw: Dict[str, Any] = field(default_factory=dict)

    def is_stale(self, ttl_seconds: float = 30.0) -> bool:
        return (time.time() - self.fetched_at) > ttl_seconds

    def to_dict(self) -> Dict[str, Any]:
        return {
            "aggregator": self.aggregator,
            "network": self.network,
            "input_mint": self.input_mint,
            "output_mint": self.output_mint,
            "amount_in": str(self.amount_in),
            "amount_out": str(self.amount_out),
            "amount_out_min": str(self.amount_out_min),
            "amount_in_lamports": self.amount_in_lamports,
            "amount_out_lamports": self.amount_out_lamports,
            "price_impact_pct": str(self.price_impact_pct),
            "swap_transaction": self.swap_transaction,
            "fetched_at": self.fetched_at,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "SolanaSwapRouteQuote":
        return cls(
            aggregator=d["aggregator"],
            network=d["network"],
            input_mint=d["input_mint"],
            output_mint=d["output_mint"],
            amount_in=Decimal(str(d["amount_in"])),
            amount_out=Decimal(str(d["amount_out"])),
            amount_out_min=Decimal(str(d["amount_out_min"])),
            amount_in_lamports=int(d["amount_in_lamports"]),
            amount_out_lamports=int(d["amount_out_lamports"]),
            price_impact_pct=Decimal(str(d["price_impact_pct"])),
            swap_transaction=d.get("swap_transaction"),
            fetched_at=float(d.get("fetched_at", time.time())),
            raw={},
        )


@dataclass
class BridgeRouteQuote(StaleMixin):
    aggregator: str
    token_symbol: str
    source_chain_id: int
    dest_chain_id: int
    source_chain_name: str
    dest_chain_name: str
    input_amount: Decimal
    output_amount: Decimal
    total_fee: Decimal
    total_fee_pct: Decimal
    estimated_fill_time_seconds: int
    gas_cost_source: Optional[Decimal] = None
    gas_cost_usd: Optional[Decimal] = None
    calldata: Optional[str] = None
    to: Optional[str] = None
    tool_data: Optional[Dict[str, Any]] = None
    fetched_at: float = field(default_factory=time.time)
    raw: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "aggregator": self.aggregator,
            "token_symbol": self.token_symbol,
            "source_chain_id": self.source_chain_id,
            "dest_chain_id": self.dest_chain_id,
            "source_chain_name": self.source_chain_name,
            "dest_chain_name": self.dest_chain_name,
            "input_amount": str(self.input_amount),
            "output_amount": str(self.output_amount),
            "total_fee": str(self.total_fee),
            "total_fee_pct": str(self.total_fee_pct),
            "estimated_fill_time_seconds": self.estimated_fill_time_seconds,
            "gas_cost_source": str(self.gas_cost_source)
            if self.gas_cost_source is not None
            else None,
            "gas_cost_usd": str(self.gas_cost_usd)
            if self.gas_cost_usd is not None
            else None,
            "calldata": self.calldata,
            "to": self.to,
            "tool_data": self.tool_data,
            "fetched_at": self.fetched_at,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "BridgeRouteQuote":
        return cls(
            aggregator=d["aggregator"],
            token_symbol=d["token_symbol"],
            source_chain_id=int(d["source_chain_id"]),
            dest_chain_id=int(d["dest_chain_id"]),
            source_chain_name=d["source_chain_name"],
            dest_chain_name=d["dest_chain_name"],
            input_amount=Decimal(str(d["input_amount"])),
            output_amount=Decimal(str(d["output_amount"])),
            total_fee=Decimal(str(d["total_fee"])),
            total_fee_pct=Decimal(str(d["total_fee_pct"])),
            estimated_fill_time_seconds=int(d["estimated_fill_time_seconds"]),
            gas_cost_source=Decimal(str(d["gas_cost_source"]))
            if d.get("gas_cost_source") is not None
            else None,
            gas_cost_usd=Decimal(str(d["gas_cost_usd"]))
            if d.get("gas_cost_usd") is not None
            else None,
            calldata=d.get("calldata"),
            to=d.get("to"),
            tool_data=d.get("tool_data"),
            fetched_at=float(d.get("fetched_at", time.time())),
            raw={},
        )


#: Union type alias used in type hints throughout the routing layer.
AnyRouteQuote = Union[SwapRouteQuote, SolanaSwapRouteQuote, BridgeRouteQuote]


@dataclass
class RouteDecision:
    node_id: str
    intent_type: str
    selected: AnyRouteQuote
    all_quotes: List[AnyRouteQuote]
    score: float
    decided_at: float = field(default_factory=time.time)

    # Aggregator quotes expire fast.  1inch locks prices for ~30 s,
    # Li.Fi for ~1–2 min.  We use a conservative 45 s to be safe.
    _DEFAULT_TTL: float = 45.0

    def is_stale(self, ttl_seconds: float = _DEFAULT_TTL) -> bool:
        return (time.time() - self.decided_at) > ttl_seconds

    def to_dict(self) -> Dict[str, Any]:
        return {
            "node_id": self.node_id,
            "intent_type": self.intent_type,
            "selected": self.selected.to_dict(),
            # Store only the top-3 runner-up quotes to keep the state lean.
            "all_quotes": [q.to_dict() for q in self.all_quotes[:3]],
            "score": self.score,
            "decided_at": self.decided_at,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "RouteDecision":
        intent_type = d["intent_type"]
        if intent_type == "swap":
            _deserialise = SwapRouteQuote.from_dict
        elif intent_type == "solana_swap":
            _deserialise = SolanaSwapRouteQuote.from_dict
        else:
            _deserialise = BridgeRouteQuote.from_dict
        return cls(
            node_id=d["node_id"],
            intent_type=intent_type,
            selected=_deserialise(d["selected"]),
            all_quotes=[_deserialise(q) for q in d.get("all_quotes", [])],
            score=float(d["score"]),
            decided_at=float(d.get("decided_at", time.time())),
        )
