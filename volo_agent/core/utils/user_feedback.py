from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Iterable, Tuple


class FeedbackTone(str, Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


class FeedbackAction(str, Enum):
    RETRY = "retry"
    EDIT = "edit"
    CANCEL = "cancel"


@dataclass(frozen=True)
class UserFeedback:
    message: str
    actions: Tuple[FeedbackAction, ...] = ()
    tone: FeedbackTone = FeedbackTone.INFO

    def render(self) -> str:
        if not self.actions:
            return self.message
        action_list = ", ".join(a.value for a in self.actions)
        return f"{self.message}\n\nReply with: {action_list}"


def _dedupe_actions(actions: Iterable[FeedbackAction]) -> Tuple[FeedbackAction, ...]:
    seen = set()
    ordered = []
    for action in actions:
        if action.value in seen:
            continue
        seen.add(action.value)
        ordered.append(action)
    return tuple(ordered)


def intent_parsing_failed() -> UserFeedback:
    message = (
        "I couldn't understand that. Please tell me the action, amount, token, "
        "and chain.\n"
        "Examples:\n"
        "- swap 1 ETH to USDC on Base\n"
        "- bridge 0.1 ETH from Ethereum to Base"
    )
    return UserFeedback(message=message, tone=FeedbackTone.WARNING)


def _missing_slot_question(slot: str) -> str | None:
    prompts = {
        "token": "Which token?",
        "token_in": "Which token?",
        "amount": "How much?",
        "token_out": "Which token do you want to receive?",
        "recipient": "Which wallet address?",
        "chain": "Which chain?",
        "target_chain": "Which destination chain?",
    }
    return prompts.get(slot)


def _missing_slot_label(slot: str) -> str:
    labels = {
        "token": "token",
        "token_in": "token",
        "amount": "amount",
        "token_out": "output token",
        "recipient": "wallet address",
        "chain": "chain",
        "target_chain": "destination chain",
    }
    return labels.get(slot, slot.replace("_", " "))


def _join_with_and(items: list[str]) -> str:
    if not items:
        return ""
    if len(items) == 1:
        return items[0]
    if len(items) == 2:
        return f"{items[0]} and {items[1]}"
    return ", ".join(items[:-1]) + f", and {items[-1]}"


def intent_missing_info(missing_slots: Iterable[str]) -> UserFeedback:
    missing = [str(s).strip() for s in missing_slots if str(s).strip()]
    if not missing:
        return intent_parsing_failed()

    if len(missing) == 1:
        slot = missing[0]
        message = _missing_slot_question(slot)
        if not message:
            message = f"Please share the {_missing_slot_label(slot)}."
    else:
        labels = [_missing_slot_label(slot) for slot in missing]
        needs = _join_with_and(labels)
        message = f"Please share the {needs}."
    return UserFeedback(message=message, tone=FeedbackTone.INFO)


def intent_resolution_failed(detail: str | None = None) -> UserFeedback:
    message = (
        "I couldn't resolve that token or chain. Please confirm the amount, token "
        "symbol, and chain (e.g., swap 1 STT to NIA on Somnia testnet)."
    )
    if detail:
        message = f"{message}\n\nDetails: {detail}"
    actions = _dedupe_actions((FeedbackAction.EDIT, FeedbackAction.CANCEL))
    return UserFeedback(message=message, actions=actions, tone=FeedbackTone.WARNING)


def token_resolution_failed(
    token_symbols: Iterable[str],
    chain: str | None = None,
) -> UserFeedback:
    tokens = [t for t in token_symbols if t]
    if not tokens:
        return intent_resolution_failed()

    if len(tokens) == 1:
        token_text = tokens[0]
    else:
        token_text = ", ".join(tokens[:-1]) + f", and {tokens[-1]}"

    if chain:
        message = f"I couldn't find {token_text} on {chain}."
    else:
        message = f"I couldn't find {token_text}."
    message += " Please check the symbol or share the contract address."

    actions = _dedupe_actions((FeedbackAction.EDIT, FeedbackAction.CANCEL))
    return UserFeedback(message=message, actions=actions, tone=FeedbackTone.WARNING)


def _format_amount(value) -> str:
    try:
        return f"{float(value):.6f}"
    except Exception:
        return str(value)


def insufficient_balance(
    shortfalls: Iterable[dict], sender_address: str | None = None
) -> UserFeedback:
    shortfall_list = [s for s in shortfalls if isinstance(s, dict)]
    if not shortfall_list:
        message = "Insufficient balance to continue. Please top up and try again."
        return UserFeedback(message=message, tone=FeedbackTone.WARNING)

    lines = ["Insufficient balance to continue:"]
    for entry in shortfall_list:
        kind = str(entry.get("kind") or "token").lower()
        symbol = str(entry.get("symbol") or "token").upper()
        chain = entry.get("chain")
        failure_label = str(entry.get("label") or "").strip()
        required = _format_amount(entry.get("required"))
        available = _format_amount(entry.get("available"))
        needed = _format_amount(entry.get("shortfall"))
        address = entry.get("sender") or sender_address

        if chain:
            resource_label = f"{symbol} on {chain}"
        else:
            resource_label = symbol

        if kind == "gas":
            balance_label = failure_label or "gas"
            detail = (
                f"{balance_label.capitalize()} on {chain}: need {required} {symbol}, "
                f"have {available} {symbol}."
                if chain
                else f"{balance_label.capitalize()}: need {required} {symbol}, have {available} {symbol}."
            )
        else:
            detail = f"{resource_label}: need {required} {symbol}, have {available} {symbol}."

        if address:
            detail += f" Add at least {needed} {symbol} to {address}."
        else:
            detail += f" Add at least {needed} {symbol}."

        lines.append(f"• {detail}")

    lines.append("After funding, try again.")
    return UserFeedback(message="\n".join(lines), tone=FeedbackTone.WARNING)


def _tool_label(tool: str | None) -> str:
    if not tool:
        return "transaction"
    tool_lower = str(tool).strip().lower()
    if tool_lower in {"swap", "bridge", "transfer", "unwrap"}:
        return tool_lower
    return "transaction"


def _reason_for_category(category: str) -> str:
    if category == "slippage":
        return "The price moved too much for the current slippage setting."
    if category == "liquidity":
        return "There isn’t enough liquidity to complete this right now."
    if category == "gas":
        return "Network fees were too high or gas was insufficient."
    if category == "network":
        return "The network is congested or temporarily unavailable."
    if category == "logic":
        return "The transaction was rejected by the contract."
    if category == "security":
        return "This action is blocked by safety checks."
    if category == "non_retryable":
        return "This action can’t be retried as submitted."
    return "Something went wrong while submitting the transaction."


def execution_failed(
    category,
    tool: str | None = None,
    chain: str | None = None,
    retrying_now: bool = False,
) -> UserFeedback:
    if hasattr(category, "value"):
        category_value = str(category.value)
    else:
        category_value = str(category)
    category_norm = category_value.strip().lower()

    tool_label = _tool_label(tool)
    if tool_label == "transaction":
        prefix = "Transaction failed."
    else:
        if chain and str(chain).strip().lower() not in {"", "unknown"}:
            prefix = f"{tool_label.capitalize()} failed on {chain}."
        else:
            prefix = f"{tool_label.capitalize()} failed."

    reason = _reason_for_category(category_norm)
    message = f"{prefix} {reason}".strip()
    if retrying_now:
        message += " Retrying now."

    if category_norm in {"security", "non_retryable"}:
        actions = _dedupe_actions((FeedbackAction.EDIT, FeedbackAction.CANCEL))
    else:
        actions = _dedupe_actions(
            (FeedbackAction.RETRY, FeedbackAction.EDIT, FeedbackAction.CANCEL)
        )

    return UserFeedback(message=message, actions=actions, tone=FeedbackTone.ERROR)


def bridge_not_supported(
    token_symbol: str | None,
    source_chain: str | None,
    target_chain: str | None,
    *,
    chain_pairs: Iterable[str] | None = None,
    tokens: Iterable[str] | None = None,
) -> UserFeedback:
    token = token_symbol or "token"
    source = source_chain or "the source chain"
    target = target_chain or "the destination chain"
    message = (
        f"Bridge not supported for {token} from {source} to {target}. "
        "Try a different chain or token."
    )
    suggestions = []
    pair_list = [p for p in (chain_pairs or []) if p]
    token_list = [t for t in (tokens or []) if t]
    if pair_list:
        suggestions.append("Supported chains for this token:")
        suggestions.extend(f"- {p}" for p in pair_list)
    if token_list:
        suggestions.append("Supported tokens for this chain pair:")
        suggestions.extend(f"- {t}" for t in token_list)
    if suggestions:
        message = message + "\n\n" + "\n".join(suggestions)
    actions = _dedupe_actions((FeedbackAction.EDIT, FeedbackAction.CANCEL))
    return UserFeedback(message=message, actions=actions, tone=FeedbackTone.WARNING)
