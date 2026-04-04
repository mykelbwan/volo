from __future__ import annotations

import json
import re
from typing import Any

from intent_hub.utils.messages import format_with_recovery

_CODE_FENCE_JSON_RE = re.compile(r"```json\s*(.*?)\s*```", re.IGNORECASE | re.DOTALL)
_CODE_FENCE_RE = re.compile(r"```(.*?)```", re.DOTALL)


def content_to_text(content: object) -> str:
    if isinstance(content, list):
        return "".join(part if isinstance(part, str) else str(part) for part in content)
    if isinstance(content, str):
        return content
    return str(content)


def extract_json_text(content: object) -> str:
    text = content_to_text(content).strip()
    if not text:
        raise ValueError(
            format_with_recovery(
                "LLM returned empty content",
                "retry the request with a clearer instruction",
            )
        )

    match = _CODE_FENCE_JSON_RE.search(text)
    if match:
        candidate = match.group(1).strip()
        if candidate:
            return candidate

    fence_match = _CODE_FENCE_RE.search(text)
    if fence_match:
        candidate = fence_match.group(1).strip()
        if candidate:
            return candidate

    return text


def parse_json_payload(content: object) -> Any:
    primary = extract_json_text(content)
    try:
        return json.loads(primary)
    except json.JSONDecodeError:
        # Best-effort extraction if wrapper text still exists around JSON.
        candidates: list[str] = []
        object_start = primary.find("{")
        object_end = primary.rfind("}")
        if object_start >= 0 and object_end > object_start:
            candidates.append(primary[object_start : object_end + 1].strip())
        array_start = primary.find("[")
        array_end = primary.rfind("]")
        if array_start >= 0 and array_end > array_start:
            candidates.append(primary[array_start : array_end + 1].strip())

        for candidate in candidates:
            try:
                return json.loads(candidate)
            except json.JSONDecodeError:
                continue

    raise ValueError(
        format_with_recovery(
            "LLM response was not valid JSON",
            "retry with a concise transaction request including token, amount, and chain",
        )
    )
