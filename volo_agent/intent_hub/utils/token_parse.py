import re

_AMOUNT_PREFIX_RE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*([A-Za-z][A-Za-z0-9]*)\s*$")


def split_amount_prefixed_symbol(raw_symbol: str) -> tuple[str, str] | None:
    if raw_symbol is None:
        return None
    text = str(raw_symbol).strip()
    if not text:
        return None
    if len(text) > 64:
        return None
    match = _AMOUNT_PREFIX_RE.match(text)
    if not match:
        return None
    amount_str, symbol = match.groups()
    if not amount_str or not symbol:
        return None
    return amount_str, symbol.upper()
