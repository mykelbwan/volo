import asyncio
import logging
import os
from typing import Any, Awaitable, Sequence, cast

from langchain_core.messages import BaseMessage

from core.utils.async_tools import run_blocking
from intent_hub.parser.json_utils import parse_json_payload
from intent_hub.utils.messages import format_with_recovery

# Lazy-loaded to avoid expensive LLM stack imports during module import.
json_parser: Any | None = None
_JSON_PARSER_CACHE: Any | None = None
_DEFAULT_LLM_TIMEOUT_SECONDS = 20.0
logger = logging.getLogger(__name__)


def _get_json_parser() -> Any:
    global json_parser, _JSON_PARSER_CACHE
    if json_parser is not None:
        return json_parser
    if _JSON_PARSER_CACHE is None:
        from llms.llms_init import json_parser as _json_parser

        _JSON_PARSER_CACHE = _json_parser
    return _JSON_PARSER_CACHE


def _llm_timeout_seconds() -> float:
    raw = os.getenv("INTENT_PARSER_TIMEOUT_SECONDS", "").strip()
    if not raw:
        return _DEFAULT_LLM_TIMEOUT_SECONDS
    try:
        value = float(raw)
    except ValueError:
        return _DEFAULT_LLM_TIMEOUT_SECONDS
    if value <= 0:
        return _DEFAULT_LLM_TIMEOUT_SECONDS
    return value


async def call_llm_async(prompt: Sequence[BaseMessage]) -> dict | list:
    parser = json_parser
    if parser is None and _JSON_PARSER_CACHE is None:
        parser = _get_json_parser()
    elif parser is None:
        parser = _JSON_PARSER_CACHE
    if parser is None:
        raise RuntimeError(
            format_with_recovery(
                "Intent parser LLM is not configured",
                "configure LLM initialization and retry",
            )
        )

    timeout_seconds = _llm_timeout_seconds()
    try:
        ainvoke = getattr(parser, "ainvoke", None)
        if callable(ainvoke):
            coro = ainvoke(prompt)
            response = await asyncio.wait_for(
                cast(Awaitable[Any], coro), timeout_seconds
            )
        else:
            invoke = getattr(parser, "invoke", None)
            if not callable(invoke):
                raise RuntimeError(
                    format_with_recovery(
                        "Intent parser LLM does not expose invoke/ainvoke",
                        "check parser model initialization and retry",
                    )
                )
            response = await run_blocking(invoke, prompt, timeout=timeout_seconds)
    except asyncio.TimeoutError as exc:
        raise TimeoutError(
            format_with_recovery(
                f"Intent parser timed out after {timeout_seconds:.1f}s",
                "retry in a moment; if this repeats, reduce prompt size or check LLM health",
            )
        ) from exc
    except Exception as exc:
        logger.warning("call_llm_async: parser invocation failed: %s", exc)
        raise RuntimeError(
            format_with_recovery(
                "Intent parser call failed",
                "retry; if it persists, verify model/provider configuration",
            )
        ) from exc

    return parse_json_payload(getattr(response, "content", response))
