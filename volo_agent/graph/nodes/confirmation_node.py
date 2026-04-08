from __future__ import annotations

from collections import defaultdict
from decimal import Decimal
from typing import Any, Dict, List, Optional, cast

from langchain_core.messages import AIMessage

from core.fees.models import FeeQuote
from core.tasks.updater import upsert_task_from_state
from graph.agent_state import AgentState


def _format_fill_time(seconds: int) -> str:
    if seconds <= 0:
        return "unknown"
    if seconds < 60:
        return f"~{seconds} seconds"
    minutes = seconds // 60
    return f"~{minutes} minute{'s' if minutes != 1 else ''}"


def _swap_receive_line(quote: Dict[str, Any], token_out_symbol: Any) -> str | None:
    amount_out = quote.get("amount_out")
    if amount_out is not None and str(amount_out).strip():
        return f"   You should receive about {amount_out} {token_out_symbol}."

    amount_out_min = quote.get("amount_out_minimum")
    if amount_out_min is None or not str(amount_out_min).strip():
        amount_out_min = quote.get("amount_out_min")
    if amount_out_min is not None and str(amount_out_min).strip():
        return f"   You should receive at least {amount_out_min} {token_out_symbol}."
    return None


async def confirmation_node(state: AgentState) -> Dict[str, Any]:
    history = state.get("plan_history", [])
    if not history:
        return {}

    plan = history[-1]
    execution_state = state.get("execution_state")
    raw_quotes: List[Dict[str, Any]] = state.get("fee_quotes") or []
    fee_map: Dict[str, FeeQuote] = {}
    for d in raw_quotes:
        try:
            q = FeeQuote.from_dict(d)
            fee_map[q.node_id] = q
        except Exception:
            pass  # malformed quote — skip silently

    receipt_lines: list[str] = ["Review this request.", ""]

    # Accumulate totals per (chain, native_symbol) for the footer.
    # Using a tuple key so multi-chain plans show separate lines.
    fee_totals: Dict[tuple, Decimal] = defaultdict(Decimal)
    gas_totals: Dict[tuple, Decimal] = defaultdict(Decimal)
    preflight_estimates = state.get("preflight_estimates") or {}

    count = 1
    for node_id, node in plan.nodes.items():
        # Skip nodes that are already successfully executed (re-confirmation
        # after planner adds new steps should only show pending work).
        if execution_state:
            node_state = execution_state.node_states.get(node_id)
            if node_state and node_state.status == "success":
                continue

        p = node.args
        preflight = preflight_estimates.get(node_id, {})

        if node.tool == "swap":
            receipt_lines.append(
                f"{count}. Swap {p.get('amount_in')} {p.get('token_in_symbol')} "
                f"to {p.get('token_out_symbol')} on {p.get('chain')}."
            )
        elif node.tool == "bridge":
            amt = p.get("amount")
            if isinstance(amt, str) and "{{" in amt:
                amt = "all"
            receipt_lines.append(
                f"{count}. Bridge {amt} {p.get('token_symbol')} "
                f"from {p.get('source_chain')} to {p.get('target_chain')}."
            )
        elif node.tool == "transfer":
            receipt_lines.append(
                f"{count}. Send "
                f"{p.get('amount')} "
                f"{p.get('asset_symbol') or p.get('token_symbol', 'tokens')} "
                f"to {p.get('recipient', p.get('to', '?'))} "
                f"on {p.get('network') or p.get('chain')}."
            )
        elif node.tool == "unwrap":
            amount = p.get("amount")
            amount_text = amount if amount is not None else "all available"
            receipt_lines.append(
                f"{count}. Unwrap {amount_text} "
                f"{p.get('wrapped_token_symbol') or p.get('token_symbol', 'wrapped native')} "
                f"on {p.get('network') or p.get('chain')}."
            )
        else:
            receipt_lines.append(f"{count}. {node.tool.capitalize()}.")

        if node.tool == "swap":
            quote = preflight.get("swap_quote")
            if isinstance(quote, dict):
                receive_line = _swap_receive_line(quote, p.get("token_out_symbol"))
                if receive_line:
                    receipt_lines.append(receive_line)
            else:
                err = preflight.get("swap_quote_error")
                if err:
                    receipt_lines.append("   Quote unavailable right now.")
        elif node.tool == "bridge":
            quote = preflight.get("bridge_quote")
            if isinstance(quote, dict):
                out_amt = quote.get("output_amount")
                fill_time = _format_fill_time(
                    int(quote.get("estimated_fill_time_seconds", 0))
                )
                target_chain = quote.get("target_chain", p.get("target_chain"))
                if out_amt is not None:
                    line = (
                        f"   You should receive about {out_amt} {p.get('token_symbol')} "
                        f"on {target_chain}."
                    )
                    if fill_time != "unknown":
                        line += f" ETA {fill_time}."
                    receipt_lines.append(line)
            else:
                err = preflight.get("bridge_quote_error")
                if err:
                    receipt_lines.append("   Quote unavailable right now.")

        gas_est = preflight.get("gas_estimate_native")
        if gas_est is not None:
            symbol = preflight.get("native_symbol") or "NATIVE"
            chain_label = preflight.get("chain") or p.get("network") or p.get("chain")
            gas_totals[(chain_label, symbol)] += Decimal(str(gas_est))
            receipt_lines.append(f"   Network fee: about {gas_est} {symbol}.")

        quote: Optional[FeeQuote] = fee_map.get(node_id)
        if quote:
            receipt_lines.append(f"   Platform fee: {quote.formatted_amount()}.")

            # Accumulate for the footer total
            fee_totals[(quote.chain, quote.native_symbol)] += quote.fee_amount_native
        else:
            if node.tool not in {"transfer", "unwrap"}:
                receipt_lines.append("  Platform fee: unavailable.")

        receipt_lines.append("")
        count += 1

    if len(plan.nodes) > 1 and fee_totals:
        receipt_lines.append("Total platform fees:")
        for (chain, symbol), total in sorted(fee_totals.items()):
            receipt_lines.append(f"- {total:.6f} {symbol} on {chain}.")
        receipt_lines.append("")
    if len(plan.nodes) > 1 and gas_totals:
        receipt_lines.append("Estimated network fees:")
        for (chain, symbol), total in sorted(gas_totals.items()):
            receipt_lines.append(f"- {total:.6f} {symbol} on {chain}.")
        receipt_lines.append("")

    receipt_lines.append("Reply 'confirm' to continue or 'cancel' to stop.")
    receipt = "\n".join(receipt_lines).strip()

    await upsert_task_from_state(
        cast(Dict[str, Any], state),
        title=str(getattr(plan, "goal", "") or "Task"),
        status="WAITING_CONFIRMATION",
        latest_summary="Waiting for your confirmation.",
        tool=(
            next(iter(plan.nodes.values())).tool
            if getattr(plan, "nodes", None)
            else None
        ),
    )

    return {
        "confirmation_status": "WAITING",
        "messages": [AIMessage(content=receipt)],
    }
