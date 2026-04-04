"""
core/memory/ledger.py
---------------------
Performance ledger — records tool execution outcomes, error patterns, and
fee revenue keyed by "tool:chain".

Primary backend  : MongoDB (atomic pipeline upserts; thread- and
                   multi-instance-safe).
Fallback backend : local JSON file (single-process only; used when MongoDB
                   is unavailable, e.g. local development without a DB).

Obtain an instance via the module-level singleton helper::

    from core.memory.ledger import get_ledger
    ledger = get_ledger()

Never call ``PerformanceLedger()`` directly at a call-site — doing so
performs a fresh availability check on every invocation and bypasses the
connection-reuse benefit of the singleton.
"""

from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, List, Optional

try:
    from bson.decimal128 import Decimal128
except Exception:  # pragma: no cover - optional dependency
    Decimal128 = None  # type: ignore[assignment]

from core.database.mongodb_async import AsyncMongoDB
from core.memory.ledger_reports import (
    aggregate_revenue,
    build_fee_revenue_summary,
    build_summary,
)

# ---------------------------------------------------------------------------
# Error taxonomy
# ---------------------------------------------------------------------------


class ErrorCategory(str, Enum):
    """
    Categorises failures to help the system decide on recovery strategies.
    """

    NONE = "none"
    NETWORK = "network"  # RPC timeouts, provider 503s, transport errors
    LIQUIDITY = "liquidity"  # Insufficient liquidity for swap/bridge routes
    SLIPPAGE = "slippage"  # Price impact exceeded user/system bounds
    GAS = "gas"  # Insufficient gas or gas price spike
    SECURITY = "security"  # Guardrail violations (risk, blacklist, limits)
    LOGIC = "logic"  # Invalid parameters, schema mismatch, tool logic
    NON_RETRYABLE = "non_retryable"  # Explicit stop condition (do not retry)
    UNKNOWN = "unknown"  # Unclassified exceptions


# ---------------------------------------------------------------------------
# Internal constants
# ---------------------------------------------------------------------------

_COLLECTION = "performance_ledger"
_RECENT_ERRORS_LIMIT = 5

# Default field set used when seeding a brand-new document.
_DOC_DEFAULTS: Dict[str, Any] = {
    "successes": 0,
    "failures": 0,
    "consecutive_failures": 0,
    "total_runs": 0,
    "avg_time": 0.0,
    "last_run": "",
    "error_distribution": {},
    "recent_errors": [],
    "fee_revenue_native": "0",
    "fee_collections": 0,
}


def _decimal128_literal(value: Decimal | str) -> Any:
    text = str(value)
    if Decimal128 is not None:
        return Decimal128(text)
    return text


def _mongo_decimal_expr(field_ref: str) -> Dict[str, Any]:
    zero = _decimal128_literal("0")
    return {
        "$convert": {
            "input": {"$ifNull": [field_ref, "0"]},
            "to": "decimal",
            "onError": zero,
            "onNull": zero,
        }
    }


def _fee_update_pipeline(fee_amount_native: Decimal) -> List[Dict[str, Any]]:
    fee_literal = _decimal128_literal(fee_amount_native)
    return [
        {
            "$set": {
                "successes": {"$ifNull": ["$successes", 0]},
                "failures": {"$ifNull": ["$failures", 0]},
                "consecutive_failures": {"$ifNull": ["$consecutive_failures", 0]},
                "total_runs": {"$ifNull": ["$total_runs", 0]},
                "avg_time": {"$ifNull": ["$avg_time", 0.0]},
                "last_run": {"$ifNull": ["$last_run", ""]},
                "error_distribution": {"$ifNull": ["$error_distribution", {}]},
                "recent_errors": {"$ifNull": ["$recent_errors", []]},
                "fee_collections": {"$add": [{"$ifNull": ["$fee_collections", 0]}, 1]},
                "fee_revenue_native": {
                    "$toString": {
                        "$add": [
                            _mongo_decimal_expr("$fee_revenue_native"),
                            fee_literal,
                        ]
                    }
                },
            }
        }
    ]


# ---------------------------------------------------------------------------
# File-based fallback (single-process / no-MongoDB environments)
# ---------------------------------------------------------------------------


class _FileLedger:
    """
    JSON-file-backed ledger used as a fallback when MongoDB is unavailable.
    Not safe for concurrent writes across threads or processes.
    """

    _DEFAULT_PATH = "performance_ledger.json"

    def __init__(self, storage_path: str = _DEFAULT_PATH) -> None:
        self.storage_path = storage_path
        self.data: Dict[str, Any] = self._load()

    # ── Persistence ───────────────────────────────────────────────────────────

    def _load(self) -> Dict[str, Any]:
        if os.path.exists(self.storage_path):
            try:
                with open(self.storage_path, "r") as fh:
                    return json.load(fh)
            except Exception:
                return {}
        return {}

    def _save(self) -> None:
        try:
            with open(self.storage_path, "w") as fh:
                json.dump(self.data, fh, indent=2)
        except Exception:
            pass

    def _ensure_key(self, key: str) -> None:
        if key not in self.data:
            self.data[key] = dict(_DOC_DEFAULTS)
        else:
            for k, v in _DOC_DEFAULTS.items():
                self.data[key].setdefault(k, v)

    # ── Public API ────────────────────────────────────────────────────────────

    def get_stats(self, key: str) -> Optional[Dict[str, Any]]:
        return self.data.get(key)

    def record_execution(
        self,
        tool: str,
        chain: str,
        success: bool,
        execution_time: float = 0.0,
        error_msg: Optional[str] = None,
        category: ErrorCategory = ErrorCategory.NONE,
    ) -> None:
        key = f"{tool}:{chain.lower()}"
        self._ensure_key(key)
        stats = self.data[key]
        stats["total_runs"] += 1

        if success:
            stats["successes"] += 1
            stats["consecutive_failures"] = 0
        else:
            stats["failures"] += 1
            stats["consecutive_failures"] = stats.get("consecutive_failures", 0) + 1
            cat_name = category.value
            stats["error_distribution"][cat_name] = (
                stats["error_distribution"].get(cat_name, 0) + 1
            )
            error_entry = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "msg": error_msg or "Unknown error",
                "category": cat_name,
            }
            stats["recent_errors"] = [error_entry] + stats["recent_errors"][
                : _RECENT_ERRORS_LIMIT - 1
            ]

        if execution_time > 0:
            n = stats["total_runs"]
            stats["avg_time"] = ((stats["avg_time"] * (n - 1)) + execution_time) / n

        stats["last_run"] = datetime.now(timezone.utc).isoformat()
        self._save()

    def record_fee(
        self,
        tool: str,
        chain: str,
        fee_amount_native: Decimal,
    ) -> None:
        key = f"{tool}:{chain.lower()}"
        self._ensure_key(key)
        stats = self.data[key]
        previous = Decimal(stats["fee_revenue_native"])
        stats["fee_revenue_native"] = str(previous + fee_amount_native)
        stats["fee_collections"] += 1
        self._save()

    def get_summary(self) -> str:
        return build_summary(self.data)

    def get_fee_revenue_summary(self) -> str:
        return build_fee_revenue_summary(self.data)

    def get_fee_revenue_by_tool(self) -> Dict[str, Decimal]:
        return aggregate_revenue(self.data, 0)

    def get_fee_revenue_by_chain(self) -> Dict[str, Decimal]:
        return aggregate_revenue(self.data, 1)

    def get_total_lifetime_txs(self, sender: str = "") -> int:
        return sum(v.get("successes", 0) for v in self.data.values())


# ---------------------------------------------------------------------------
# MongoDB-backed primary implementation
# ---------------------------------------------------------------------------


class PerformanceLedger:
    """
    Persistent record of tool performance, error patterns, and fee revenue.

    Backed by MongoDB with atomic aggregation-pipeline upserts (requires
    MongoDB 4.2+).  All counter increments and the rolling avg_time are
    computed server-side in a single round-trip — no read-before-write race
    regardless of how many agent instances write concurrently.

    Automatically falls back to ``_FileLedger`` when MongoDB is unavailable
    so the rest of the system never needs to handle the absence of a ledger.

    Storage layout (one MongoDB document per "tool:chain" key):

        _id                   "swap:base"
        successes             int
        failures              int
        consecutive_failures  int
        total_runs            int
        avg_time              float   (rolling average, seconds)
        last_run              ISO-8601 string
        error_distribution    { category: count, ... }
        recent_errors         [ {timestamp, msg, category}, ... ]  — max 5
        fee_revenue_native    str(Decimal)
        fee_collections       int
    """

    def __init__(self) -> None:
        self._col = None
        self._async_col = None
        self._fallback: Optional[_FileLedger] = None
        self._async_indexes_ready = False
        self._async_index_lock: asyncio.Lock | None = None
        try:
            from core.database.mongodb import MongoDB

            self._col = MongoDB.get_collection(_COLLECTION)
        except Exception:
            self._col = None
        try:
            self._async_col = AsyncMongoDB.get_collection(_COLLECTION)
        except Exception:
            self._async_col = None
        if self._col is None and self._async_col is None:
            self._fallback = _FileLedger()

    @property
    def _available(self) -> bool:
        return self._col is not None

    @property
    def _async_available(self) -> bool:
        return self._async_col is not None

    def _fallback_ledger(self) -> _FileLedger:
        if self._fallback is None:
            self._fallback = _FileLedger()
        return self._fallback

    async def _ensure_indexes_async(self) -> None:
        if self._async_indexes_ready or not self._async_available:
            return
        if self._async_index_lock is None:
            self._async_index_lock = asyncio.Lock()
        async with self._async_index_lock:
            if self._async_indexes_ready or not self._async_available:
                return
            # _id has a built-in unique index; no extra indexes required here.
            self._async_indexes_ready = True

    # ── Backward-compatible full-data property ────────────────────────────────

    @property
    def data(self) -> Dict[str, Any]:
        """
        Return all records as ``{tool:chain -> stats_dict}``.

        Retained for backward compatibility with ``CircuitBreaker`` and any
        other code that iterates the full dataset.  For single-key lookups
        call ``get_stats(key)`` instead — it issues a targeted find_one
        rather than a full collection scan.
        """
        if not self._available:
            return self._fallback.data  # type: ignore[union-attr]
        try:
            return {
                doc["_id"]: {k: v for k, v in doc.items() if k != "_id"}
                for doc in self._col.find({})  # type: ignore[union-attr]
            }
        except Exception:
            return {}

    # ── Targeted single-key lookup ────────────────────────────────────────────

    def get_stats(self, key: str) -> Optional[Dict[str, Any]]:
        """
        Return the stats dict for one ``tool:chain`` key, or ``None``.

        More efficient than ``ledger.data.get(key)`` because it issues a
        single targeted ``find_one`` rather than scanning the whole collection.
        Used by the route scorer to retrieve per-aggregator success rates.
        """
        if not self._available:
            return self._fallback.get_stats(key)  # type: ignore[union-attr]
        try:
            doc = self._col.find_one({"_id": key})  # type: ignore[union-attr]
            if doc is None:
                return None
            return {k: v for k, v in doc.items() if k != "_id"}
        except Exception:
            return None

    # ── Writes ────────────────────────────────────────────────────────────────

    def record_execution(
        self,
        tool: str,
        chain: str,
        success: bool,
        execution_time: float = 0.0,
        error_msg: Optional[str] = None,
        category: ErrorCategory = ErrorCategory.NONE,
    ) -> None:
        """
        Atomically record a single tool execution outcome.

        Uses a two-stage aggregation pipeline upsert so the rolling
        ``avg_time`` is computed server-side — no extra round-trip needed.

        Stage 1  — increment counters, prepend the error entry (on failure),
                   and seed missing fields on new documents.
        Stage 2  — recompute ``avg_time`` from the already-incremented
                   ``total_runs`` produced by stage 1.

        On failure an additional plain ``$inc`` updates the nested
        ``error_distribution.{category}`` counter using dot notation, which
        is simpler and more readable than the equivalent pipeline expression.
        """
        if not self._available:
            self._fallback.record_execution(  # type: ignore[union-attr]
                tool, chain, success, execution_time, error_msg, category
            )
            return

        key = f"{tool}:{chain.lower()}"
        now = datetime.now(timezone.utc).isoformat()

        # Shared stage 2: rolling average expression.
        # At this point $total_runs already holds the post-increment value
        # from stage 1, so the formula is:
        #   new_avg = (old_avg × (new_total − 1) + execution_time) / new_total
        _avg_expr: Dict[str, Any] = {
            "$cond": {
                "if": {"$gt": [execution_time, 0.0]},
                "then": {
                    "$divide": [
                        {
                            "$add": [
                                {
                                    "$multiply": [
                                        {"$ifNull": ["$avg_time", 0.0]},
                                        {"$subtract": ["$total_runs", 1]},
                                    ]
                                },
                                execution_time,
                            ]
                        },
                        "$total_runs",
                    ]
                },
                "else": {"$ifNull": ["$avg_time", 0.0]},
            }
        }

        try:
            if success:
                pipeline: List[Dict[str, Any]] = [
                    {
                        "$set": {
                            "total_runs": {
                                "$add": [{"$ifNull": ["$total_runs", 0]}, 1]
                            },
                            "successes": {"$add": [{"$ifNull": ["$successes", 0]}, 1]},
                            "consecutive_failures": 0,
                            "last_run": now,
                            # Preserve existing values; seed defaults on insert.
                            "failures": {"$ifNull": ["$failures", 0]},
                            "error_distribution": {
                                "$ifNull": ["$error_distribution", {}]
                            },
                            "recent_errors": {"$ifNull": ["$recent_errors", []]},
                            "fee_revenue_native": {
                                "$ifNull": ["$fee_revenue_native", "0"]
                            },
                            "fee_collections": {"$ifNull": ["$fee_collections", 0]},
                        }
                    },
                    {"$set": {"avg_time": _avg_expr}},
                ]
                self._col.update_one(  # type: ignore[union-attr]
                    {"_id": key}, pipeline, upsert=True
                )

            else:
                cat_name = category.value
                error_entry: Dict[str, Any] = {
                    "timestamp": now,
                    "msg": error_msg or "Unknown error",
                    "category": cat_name,
                }
                pipeline = [
                    {
                        "$set": {
                            "total_runs": {
                                "$add": [{"$ifNull": ["$total_runs", 0]}, 1]
                            },
                            "failures": {"$add": [{"$ifNull": ["$failures", 0]}, 1]},
                            "consecutive_failures": {
                                "$add": [{"$ifNull": ["$consecutive_failures", 0]}, 1]
                            },
                            "last_run": now,
                            # Prepend new error entry; keep last N via $slice.
                            "recent_errors": {
                                "$slice": [
                                    {
                                        "$concatArrays": [
                                            [error_entry],
                                            {"$ifNull": ["$recent_errors", []]},
                                        ]
                                    },
                                    _RECENT_ERRORS_LIMIT,
                                ]
                            },
                            # Preserve / seed defaults.
                            "successes": {"$ifNull": ["$successes", 0]},
                            "error_distribution": {
                                "$ifNull": ["$error_distribution", {}]
                            },
                            "fee_revenue_native": {
                                "$ifNull": ["$fee_revenue_native", "0"]
                            },
                            "fee_collections": {"$ifNull": ["$fee_collections", 0]},
                        }
                    },
                    {"$set": {"avg_time": _avg_expr}},
                ]
                self._col.update_one(  # type: ignore[union-attr]
                    {"_id": key}, pipeline, upsert=True
                )
                # Increment the specific error category with dot-notation $inc.
                # Simpler and more readable than the equivalent $mergeObjects
                # expression inside the pipeline.
                self._col.update_one(  # type: ignore[union-attr]
                    {"_id": key},
                    {"$inc": {f"error_distribution.{cat_name}": 1}},
                )

        except Exception:
            # Never let a ledger write disrupt the main execution flow.
            if self._fallback:
                self._fallback.record_execution(
                    tool, chain, success, execution_time, error_msg, category
                )

    def record_fee(
        self,
        tool: str,
        chain: str,
        fee_amount_native: Decimal,
    ) -> None:
        """
        Accumulate a successfully collected platform fee.

        ``fee_revenue_native`` is stored as a Decimal-serialised string to
        preserve precision.  The addition is performed client-side after a
        targeted projection read; the race window is negligible because fee
        collections are sequential per execution and rare across instances.
        """
        if not self._available:
            self._fallback.record_fee(tool, chain, fee_amount_native)  # type: ignore[union-attr]
            return

        key = f"{tool}:{chain.lower()}"
        try:
            self._col.update_one(  # type: ignore[union-attr]
                {"_id": key},
                _fee_update_pipeline(fee_amount_native),
                upsert=True,
            )
        except Exception:
            if self._fallback:
                self._fallback.record_fee(tool, chain, fee_amount_native)

    async def arecord_execution(
        self,
        tool: str,
        chain: str,
        success: bool,
        execution_time: float = 0.0,
        error_msg: Optional[str] = None,
        category: ErrorCategory = ErrorCategory.NONE,
    ) -> None:
        """
        Async counterpart to ``record_execution`` using Motor.

        Falls back to the file ledger if async Mongo is unavailable.
        """
        if not self._async_available:
            self._fallback_ledger().record_execution(
                tool, chain, success, execution_time, error_msg, category
            )
            return
        await self._ensure_indexes_async()

        key = f"{tool}:{chain.lower()}"
        now = datetime.now(timezone.utc).isoformat()
        _avg_expr: Dict[str, Any] = {
            "$cond": {
                "if": {"$gt": [execution_time, 0.0]},
                "then": {
                    "$divide": [
                        {
                            "$add": [
                                {
                                    "$multiply": [
                                        {"$ifNull": ["$avg_time", 0.0]},
                                        {"$subtract": ["$total_runs", 1]},
                                    ]
                                },
                                execution_time,
                            ]
                        },
                        "$total_runs",
                    ]
                },
                "else": {"$ifNull": ["$avg_time", 0.0]},
            }
        }

        try:
            if success:
                pipeline: List[Dict[str, Any]] = [
                    {
                        "$set": {
                            "total_runs": {
                                "$add": [{"$ifNull": ["$total_runs", 0]}, 1]
                            },
                            "successes": {"$add": [{"$ifNull": ["$successes", 0]}, 1]},
                            "consecutive_failures": 0,
                            "last_run": now,
                            "failures": {"$ifNull": ["$failures", 0]},
                            "error_distribution": {
                                "$ifNull": ["$error_distribution", {}]
                            },
                            "recent_errors": {"$ifNull": ["$recent_errors", []]},
                            "fee_revenue_native": {
                                "$ifNull": ["$fee_revenue_native", "0"]
                            },
                            "fee_collections": {"$ifNull": ["$fee_collections", 0]},
                        }
                    },
                    {"$set": {"avg_time": _avg_expr}},
                ]
                await self._async_col.update_one(  # type: ignore[union-attr]
                    {"_id": key}, pipeline, upsert=True
                )
            else:
                cat_name = category.value
                error_entry: Dict[str, Any] = {
                    "timestamp": now,
                    "msg": error_msg or "Unknown error",
                    "category": cat_name,
                }
                pipeline = [
                    {
                        "$set": {
                            "total_runs": {
                                "$add": [{"$ifNull": ["$total_runs", 0]}, 1]
                            },
                            "failures": {"$add": [{"$ifNull": ["$failures", 0]}, 1]},
                            "consecutive_failures": {
                                "$add": [{"$ifNull": ["$consecutive_failures", 0]}, 1]
                            },
                            "last_run": now,
                            "recent_errors": {
                                "$slice": [
                                    {
                                        "$concatArrays": [
                                            [error_entry],
                                            {"$ifNull": ["$recent_errors", []]},
                                        ]
                                    },
                                    _RECENT_ERRORS_LIMIT,
                                ]
                            },
                            "successes": {"$ifNull": ["$successes", 0]},
                            "error_distribution": {
                                "$ifNull": ["$error_distribution", {}]
                            },
                            "fee_revenue_native": {
                                "$ifNull": ["$fee_revenue_native", "0"]
                            },
                            "fee_collections": {"$ifNull": ["$fee_collections", 0]},
                        }
                    },
                    {"$set": {"avg_time": _avg_expr}},
                ]
                await self._async_col.update_one(  # type: ignore[union-attr]
                    {"_id": key}, pipeline, upsert=True
                )
                await self._async_col.update_one(  # type: ignore[union-attr]
                    {"_id": key},
                    {"$inc": {f"error_distribution.{cat_name}": 1}},
                )
        except Exception:
            self._fallback_ledger().record_execution(
                tool, chain, success, execution_time, error_msg, category
            )

    async def arecord_fee(
        self,
        tool: str,
        chain: str,
        fee_amount_native: Decimal,
    ) -> None:
        """
        Async counterpart to ``record_fee`` using Motor.

        Falls back to the file ledger if async Mongo is unavailable.
        """
        if not self._async_available:
            self._fallback_ledger().record_fee(tool, chain, fee_amount_native)
            return
        await self._ensure_indexes_async()

        key = f"{tool}:{chain.lower()}"
        try:
            await self._async_col.update_one(  # type: ignore[union-attr]
                {"_id": key},
                _fee_update_pipeline(fee_amount_native),
                upsert=True,
            )
        except Exception:
            self._fallback_ledger().record_fee(tool, chain, fee_amount_native)

    # ── Reads / summaries ─────────────────────────────────────────────────────

    def get_summary(self) -> str:
        """
        Concise performance summary for the Planner's system prompt.
        Includes semantic error distribution and most recent failure context.
        """
        if not self._available:
            return self._fallback.get_summary()  # type: ignore[union-attr]
        try:
            docs = list(self._col.find({}))  # type: ignore[union-attr]
            data = {
                doc["_id"]: {k: v for k, v in doc.items() if k != "_id"} for doc in docs
            }
            return build_summary(data)
        except Exception:
            return "No previous execution history available."

    def get_fee_revenue_summary(self) -> str:
        """Human-readable table of accumulated fee revenue per tool and chain."""
        if not self._available:
            return self._fallback.get_fee_revenue_summary()  # type: ignore[union-attr]
        try:
            docs = list(self._col.find({}))  # type: ignore[union-attr]
            data = {
                doc["_id"]: {k: v for k, v in doc.items() if k != "_id"} for doc in docs
            }
            return build_fee_revenue_summary(data)
        except Exception:
            return "No fee revenue recorded yet."

    def get_fee_revenue_by_tool(self) -> Dict[str, Decimal]:
        """Aggregate fee revenue across all chains, grouped by tool."""
        if not self._available:
            return self._fallback.get_fee_revenue_by_tool()  # type: ignore[union-attr]
        try:
            pipeline = [
                {
                    "$group": {
                        "_id": {"$arrayElemAt": [{"$split": ["$_id", ":"]}, 0]},
                        "total": {
                            "$sum": {
                                "$toDouble": {"$ifNull": ["$fee_revenue_native", "0"]}
                            }
                        },
                    }
                }
            ]
            return {
                doc["_id"]: Decimal(str(doc["total"]))
                for doc in self._col.aggregate(pipeline)  # type: ignore[union-attr]
            }
        except Exception:
            return {}

    def get_fee_revenue_by_chain(self) -> Dict[str, Decimal]:
        """Aggregate fee revenue across all tools, grouped by chain."""
        if not self._available:
            return self._fallback.get_fee_revenue_by_chain()  # type: ignore[union-attr]
        try:
            pipeline = [
                {
                    "$group": {
                        "_id": {"$arrayElemAt": [{"$split": ["$_id", ":"]}, 1]},
                        "total": {
                            "$sum": {
                                "$toDouble": {"$ifNull": ["$fee_revenue_native", "0"]}
                            }
                        },
                    }
                }
            ]
            return {
                doc["_id"]: Decimal(str(doc["total"]))
                for doc in self._col.aggregate(pipeline)  # type: ignore[union-attr]
            }
        except Exception:
            return {}

    def get_total_lifetime_txs(self, sender: str = "") -> int:
        """Total number of successful executions across all tools and chains."""
        if not self._available:
            return self._fallback.get_total_lifetime_txs(sender)  # type: ignore[union-attr]
        try:
            pipeline = [{"$group": {"_id": None, "total": {"$sum": "$successes"}}}]
            result = list(self._col.aggregate(pipeline))  # type: ignore[union-attr]
            return int(result[0]["total"]) if result else 0
        except Exception:
            return 0


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

_ledger_instance: Optional[PerformanceLedger] = None


def get_ledger() -> PerformanceLedger:
    """
    Return the process-level ``PerformanceLedger`` singleton.

    The first call creates the instance (performing the MongoDB availability
    check once) and caches it.  All subsequent calls return the same object,
    reusing the underlying MongoDB collection handle without re-checking
    connectivity on every invocation.

    This is the preferred way to obtain a ledger at any call site::

        from core.memory.ledger import get_ledger
        ledger = get_ledger()
        ledger.record_execution("swap", "base", success=True, execution_time=1.2)
    """
    global _ledger_instance
    if _ledger_instance is None:
        _ledger_instance = PerformanceLedger()
    return _ledger_instance
