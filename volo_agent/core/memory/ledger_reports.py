from __future__ import annotations

from decimal import Decimal
from typing import Any, Dict, List


def build_summary(data: Dict[str, Any]) -> str:
    """Build the planner-prompt performance string from a flat stats dict."""
    if not data:
        return "No previous execution history available."

    lines = ["Recent Performance Insights:"]
    for key, stats in data.items():
        if ":" not in key:
            continue
        tool, chain = key.split(":", 1)
        total = stats.get("total_runs", 0)
        if total == 0:
            continue

        success_rate = (stats.get("successes", 0) / total) * 100
        avg_time = stats.get("avg_time", 0.0)
        line = (
            f"  - {tool.upper()} on {chain}: "
            f"{success_rate:.1f}% success rate, avg {avg_time:.2f}s"
        )

        if stats.get("failures", 0) > 0:
            errors = stats.get("error_distribution", {})
            top_errors = sorted(errors.items(), key=lambda x: x[1], reverse=True)[:2]
            error_str = ", ".join(f"{cat}: {count}" for cat, count in top_errors)
            line += f" (Main issues: {error_str})"

        lines.append(line)

        recent = stats.get("recent_errors", [])
        if recent and success_rate < 100:
            last_err = recent[0]
            lines.append(
                f"    * Last failure ({last_err['category']}): {last_err['msg']}"
            )

    return "\n".join(lines) if len(lines) > 1 else "No execution history yet."


def build_fee_revenue_summary(data: Dict[str, Any]) -> str:
    """Build the human-readable fee revenue table from a flat stats dict."""
    lines: List[str] = []
    total_entries = 0

    for key, stats in data.items():
        collections = stats.get("fee_collections", 0)
        if collections == 0 or ":" not in key:
            continue
        tool, chain = key.split(":", 1)
        revenue = Decimal(str(stats.get("fee_revenue_native", "0")))
        native_symbol = resolve_native_symbol(chain)
        lines.append(
            f"  - {tool.upper():<8} on {chain:<16}: "
            f"{revenue:.6f} {native_symbol}  "
            f"({collections} collection{'s' if collections != 1 else ''})"
        )
        total_entries += 1

    if not lines:
        return "No fee revenue recorded yet."

    lines.sort()
    return "\n".join(
        ["Fee Revenue Summary:"] + lines + [f"  Total entries: {total_entries}"]
    )


def aggregate_revenue(data: Dict[str, Any], field_index: int) -> Dict[str, Decimal]:
    """Aggregate fee_revenue_native by tool (index 0) or chain (index 1)."""
    totals: Dict[str, Decimal] = {}
    for key, stats in data.items():
        if ":" not in key:
            continue
        parts = key.split(":", 1)
        group_key = parts[field_index]
        revenue = Decimal(str(stats.get("fee_revenue_native", "0")))
        totals[group_key] = totals.get(group_key, Decimal("0")) + revenue
    return totals


def resolve_native_symbol(chain_lower: str) -> str:
    """Best-effort lookup of the native token symbol for a chain name."""
    try:
        from config.chains import get_chain_by_name

        return get_chain_by_name(chain_lower).native_symbol
    except Exception:
        return "native"
