from __future__ import annotations

import threading
from typing import Any, Callable

from langchain_cohere import ChatCohere
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_huggingface import ChatHuggingFace, HuggingFaceEndpoint

from config.env import (
    COHERE_API_KEY,
    GEMINI_API_KEY1,
    GEMINI_API_KEY2,
    HUGGINGFACE_API_KEY,
)

_MODEL_CACHE: dict[str, Any] = {}
_CACHE_LOCK = threading.RLock()


def _build_conversation_llm() -> ChatGoogleGenerativeAI:
    return ChatGoogleGenerativeAI(
        model="gemma-3-27b-it",
        google_api_key=GEMINI_API_KEY1.get_secret_value(),
        temperature=0.7,
    )


def _build_interaction_layer() -> ChatGoogleGenerativeAI:
    return ChatGoogleGenerativeAI(
        model="gemma-3-27b-it",
        google_api_key=GEMINI_API_KEY2.get_secret_value(),
        temperature=0,
    )


def _build_json_parser_llm() -> HuggingFaceEndpoint:
    return HuggingFaceEndpoint(
        task="text-generation",
        model="Qwen/Qwen3-32B",
        max_new_tokens=512,
        do_sample=False,
        provider="auto",
        huggingfacehub_api_token=HUGGINGFACE_API_KEY.get_secret_value(),
    )


def _build_json_parser() -> ChatHuggingFace:
    return ChatHuggingFace(llm=_get_or_build("json_parser_llm"))


def _build_planning_llm() -> ChatCohere:
    return ChatCohere(
        model="command-r-08-2024",
        cohere_api_key=COHERE_API_KEY,
        temperature=0,
    )


_BUILDERS: dict[str, Callable[[], Any]] = {
    "conversation_llm": _build_conversation_llm,
    "interaction_layer": _build_interaction_layer,
    "json_parser_llm": _build_json_parser_llm,
    "json_parser": _build_json_parser,
    "planning_llm": _build_planning_llm,
}


def _get_or_build(name: str) -> Any:
    # Double-checked locking pattern
    cached = _MODEL_CACHE.get(name)
    if cached is not None:
        return cached

    with _CACHE_LOCK:
        cached = _MODEL_CACHE.get(name)
        if cached is not None:
            return cached

        builder = _BUILDERS.get(name)
        if builder is None:
            raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
        value = builder()
        _MODEL_CACHE[name] = value
        return value


def __getattr__(name: str) -> Any:
    return _get_or_build(name)