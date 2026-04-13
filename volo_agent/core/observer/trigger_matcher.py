from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

from core.observer.price_keys import key_for_condition
from core.observer.price_observer import PriceCache
from core.observer.trigger_registry import TriggerRegistry
from intent_hub.ontology.trigger import TriggerCondition, TriggerType

logger = logging.getLogger(__name__)


@dataclass
class MatchResult:
    trigger_id: str
    thread_id: str
    user_id: str
    resume_payload: dict[str, Any]
    trigger_doc: dict[str, Any] = field(default_factory=dict)
    next_execute_at: Optional[str] = None


@dataclass
class SkippedTrigger:
    trigger_id: str
    asset: Optional[str]
    reason: str
    current_price: Optional[float] = None
    target_price: Optional[float] = None


@dataclass
class EvaluationReport:
    matches: list[MatchResult] = field(default_factory=list)
    skipped: list[SkippedTrigger] = field(default_factory=list)
    expired_count: int = 0

    @property
    def total_evaluated(self) -> int:
        return len(self.matches) + len(self.skipped)

    def summary(self) -> str:
        return (
            f"evaluated={self.total_evaluated}, "
            f"matched={len(self.matches)}, "
            f"skipped={len(self.skipped)}, "
            f"expired={self.expired_count}"
        )


class TriggerMatcher:
    def __init__(
        self,
        registry: TriggerRegistry,
        cache: PriceCache,
        max_price_age_seconds: float = 300.0,
    ) -> None:
        self.registry = registry
        self.cache = cache
        self.max_price_age_seconds = max_price_age_seconds

    async def evaluate(
        self, current_prices: Optional[dict[str, float]] = None
    ) -> EvaluationReport:
        report = EvaluationReport()

        # Expire stale triggers
        try:
            report.expired_count = await self.registry.expire_old_triggers()
            if report.expired_count:
                logger.info(
                    "TriggerMatcher: expired %d stale trigger(s).",
                    report.expired_count,
                )
        except Exception as exc:
            logger.error("TriggerMatcher: expire sweep failed: %s", exc, exc_info=True)

        # Fetch all pending price triggers
        try:
            pending = await self.registry.get_pending_price_triggers()
        except Exception as exc:
            logger.error(
                "TriggerMatcher: could not fetch pending triggers: %s",
                exc,
                exc_info=True,
            )
            return report

        if not pending:
            logger.debug("TriggerMatcher: no pending price triggers found.")
            return report

        logger.debug(
            "TriggerMatcher: evaluating %d pending price trigger(s).", len(pending)
        )

        # Evaluate each trigger
        for doc in pending:
            result = self._evaluate_price_trigger(doc, current_prices)
            if isinstance(result, MatchResult):
                report.matches.append(result)
            elif isinstance(result, SkippedTrigger):
                report.skipped.append(result)

        # Log summary
        if report.matches:
            matched_ids = [m.trigger_id[:8] for m in report.matches]
            logger.info(
                "TriggerMatcher: %d trigger(s) MATCHED this cycle: %s",
                len(report.matches),
                matched_ids,
            )

        logger.debug("TriggerMatcher cycle: %s", report.summary())
        return report

    async def evaluate_time_triggers(self) -> EvaluationReport:
        report = EvaluationReport()

        try:
            pending = await self.registry.get_pending_time_triggers()
        except Exception as exc:
            logger.error(
                "TriggerMatcher: could not fetch pending time triggers: %s",
                exc,
                exc_info=True,
            )
            return report

        for doc in pending:
            trigger_id: str = doc["trigger_id"]
            thread_id: str = doc["thread_id"]
            user_id: str = doc["user_id"]
            condition_dict: dict = doc.get("trigger_condition", {})

            resume_payload = _build_resume_payload(
                trigger_id=trigger_id,
                condition_dict=condition_dict,
                matched_price=None,
                trigger_doc=doc,
                extra={"trigger_type": "time_at"},
            )

            next_execute_at = _next_execute_at_from_condition(condition_dict)

            match = MatchResult(
                trigger_id=trigger_id,
                thread_id=thread_id,
                user_id=user_id,
                resume_payload=resume_payload,
                trigger_doc=doc,
                next_execute_at=next_execute_at,
            )
            report.matches.append(match)
            logger.info(
                "TriggerMatcher: TIME_AT trigger %s matched for user %s (thread=%s).",
                trigger_id[:8],
                user_id,
                thread_id[:8],
            )

        return report

    def _evaluate_price_trigger(
        self,
        doc: dict[str, Any],
        price_snapshot: Optional[dict[str, float]],
    ) -> MatchResult | SkippedTrigger:
        trigger_id: str = doc.get("trigger_id", "unknown")
        thread_id: str = doc.get("thread_id", "")
        user_id: str = doc.get("user_id", "")
        condition_dict: dict[str, Any] = doc.get("trigger_condition", {})

        # Parse the condition 
        try:
            condition = TriggerCondition.from_dict(condition_dict)
        except Exception as exc:
            logger.warning(
                "TriggerMatcher: could not parse condition for trigger %s: %s",
                trigger_id[:8],
                exc,
            )
            return SkippedTrigger(
                trigger_id=trigger_id,
                asset=condition_dict.get("asset"),
                reason="invalid_condition",
            )

        asset: Optional[str] = condition.asset
        target: Optional[float] = condition.target

        price_key = key_for_condition(condition)

        if not asset and not price_key:
            return SkippedTrigger(
                trigger_id=trigger_id,
                asset=None,
                reason="missing_asset",
                target_price=target,
            )

        # Resolve current price 
        if price_snapshot is not None:
            current_price = price_snapshot.get(price_key or "")
        else:
            current_price = self.cache.get_sync(price_key or "")

        if current_price is None:
            logger.debug(
                "TriggerMatcher: no price available for %s (trigger %s) — skipping.",
                asset or price_key,
                trigger_id[:8],
            )
            return SkippedTrigger(
                trigger_id=trigger_id,
                asset=asset or price_key,
                reason="no_price",
                target_price=target,
            )

        # Stale price guard 
        if price_snapshot is None and self.cache.is_stale(
            price_key or "", self.max_price_age_seconds
        ):
            age = self.cache.age_seconds(price_key or "")
            logger.warning(
                "TriggerMatcher: price for %s is stale (%.0fs old) — "
                "skipping trigger %s for safety.",
                asset or price_key,
                age or 0,
                trigger_id[:8],
            )
            return SkippedTrigger(
                trigger_id=trigger_id,
                asset=asset or price_key,
                reason="stale_price",
                current_price=current_price,
                target_price=target,
            )

        # Condition evaluation 
        satisfied = condition.is_satisfied_by(current_price)

        direction_label = _direction_label(condition.type)
        logger.debug(
            "TriggerMatcher: %s %s $%.4f (target %s $%.4f) → %s  [trigger=%s]",
            asset or price_key,
            direction_label,
            current_price,
            direction_label,
            target or 0,
            "MATCH" if satisfied else "no match",
            trigger_id[:8],
        )

        if not satisfied:
            return SkippedTrigger(
                trigger_id=trigger_id,
                asset=asset or price_key,
                reason="price_not_met",
                current_price=current_price,
                target_price=target,
            )

        # Build resume payload 
        resume_payload = _build_resume_payload(
            trigger_id=trigger_id,
            condition_dict=condition_dict,
            matched_price=current_price,
            trigger_doc=doc,
            extra={
                "asset": asset or price_key,
                "target_price": target,
                "trigger_type": condition.type.value,
            },
        )

        logger.info(
            "TriggerMatcher: PRICE trigger MATCHED — %s %s $%.2f "
            "(current=$%.4f, trigger=%s, user=%s, thread=%s).",
            asset,
            direction_label,
            target or 0,
            current_price,
            trigger_id[:8],
            user_id,
            thread_id[:8],
        )

        return MatchResult(
            trigger_id=trigger_id,
            thread_id=thread_id,
            user_id=user_id,
            resume_payload=resume_payload,
            trigger_doc=doc,
        )


def _build_resume_payload(
    trigger_id: str,
    condition_dict: dict[str, Any],
    matched_price: Optional[float],
    trigger_doc: Optional[dict[str, Any]] = None,
    extra: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "condition_met": True,
        "trigger_id": trigger_id,
        "trigger_condition": condition_dict,
        "matched_price": matched_price,
    }
    payload_body = (trigger_doc or {}).get("payload") or {}
    resume_auth = payload_body.get("resume_auth") or {}
    resume_token = str(resume_auth.get("resume_token") or "").strip()
    if resume_token:
        payload["resume_token"] = resume_token
    if extra:
        payload.update(extra)
    return payload


def _parse_iso(dt_str: Optional[str]) -> Optional[datetime]:
    if not dt_str:
        return None
    try:
        # Handle "Z" suffix
        if dt_str.endswith("Z"):
            dt_str = dt_str.replace("Z", "+00:00")
        return datetime.fromisoformat(dt_str).astimezone(timezone.utc)
    except Exception:
        return None


def _next_execute_at_from_condition(condition_dict: dict[str, Any]) -> Optional[str]:
    try:
        condition = TriggerCondition.from_dict(condition_dict)
    except Exception:
        return None

    if not condition.schedule or not condition.execute_at:
        return None

    base = _parse_iso(condition.execute_at)
    if not base:
        return None

    delta = condition.schedule.to_timedelta()
    next_dt = base + delta
    return next_dt.isoformat()


def _direction_label(trigger_type: TriggerType) -> str:
    if trigger_type == TriggerType.PRICE_BELOW:
        return "below"
    if trigger_type == TriggerType.PRICE_ABOVE:
        return "above"
    return "at"
