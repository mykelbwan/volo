import asyncio
import os
import re
import logging
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict

from langchain_core.messages import AIMessage, HumanMessage

from config.chains import supported_chains
from core.identity.errors import LinkAccountError
from core.security.policy_store import PolicyStore
from core.utils.async_tools import run_blocking
from core.utils.linking import (
    extract_link_token,
    is_create_wallet_request,
    is_link_account_request,
)
from graph.agent_state import AgentState


def _load_positive_float_env(name: str, default: float) -> float:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError:
        return default
    return value if value > 0 else default


def _load_positive_int_env(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return value if value > 0 else default


_POLICY_LOOKUP_TIMEOUT_SECONDS = _load_positive_float_env(
    "VOLO_POLICY_LOOKUP_TIMEOUT_SECONDS",
    2.0,
)
_POLICY_LOOKUP_RETRY_SECONDS = _load_positive_float_env(
    "VOLO_POLICY_LOOKUP_RETRY_SECONDS",
    60.0,
)
_POLICY_CACHE_TTL_SECONDS = _load_positive_float_env(
    "VOLO_POLICY_CACHE_TTL_SECONDS",
    30.0,
)
_POLICY_STATE_MAX_ENTRIES = _load_positive_int_env(
    "VOLO_POLICY_STATE_MAX_ENTRIES",
    5000,
)
_LOGGER = logging.getLogger("volo.onboarding")
AsyncIdentityService = None
_ASYNC_IDENTITY_SERVICE_CACHE = None


@dataclass(frozen=True)
class _PolicyCacheEntry:
    expires_monotonic: float
    policy: Dict[str, Any] | None


_POLICY_CACHE: Dict[str, _PolicyCacheEntry] = {}
_POLICY_LOOKUP_RETRY_AFTER: Dict[str, float] = {}
_POLICY_CACHE_LOCK = threading.Lock()
_POLICY_LOOKUP_INFLIGHT: Dict[str, asyncio.Task[Dict[str, Any] | None]] = {}
_POLICY_LOOKUP_INFLIGHT_LOCK = threading.Lock()


def _prune_expired_policy_state_locked(now: float) -> None:
    expired_cache = [
        key
        for key, entry in _POLICY_CACHE.items()
        if entry.expires_monotonic <= now
    ]
    for key in expired_cache:
        _POLICY_CACHE.pop(key, None)
    expired_retries = [
        key
        for key, retry_after in _POLICY_LOOKUP_RETRY_AFTER.items()
        if retry_after <= now
    ]
    for key in expired_retries:
        _POLICY_LOOKUP_RETRY_AFTER.pop(key, None)


def _enforce_policy_state_limits_locked() -> None:
    cache_overflow = len(_POLICY_CACHE) - _POLICY_STATE_MAX_ENTRIES
    if cache_overflow > 0:
        evict_cache_keys = sorted(
            _POLICY_CACHE.items(),
            key=lambda kv: kv[1].expires_monotonic,
        )[:cache_overflow]
        for key, _ in evict_cache_keys:
            _POLICY_CACHE.pop(key, None)

    retry_overflow = len(_POLICY_LOOKUP_RETRY_AFTER) - _POLICY_STATE_MAX_ENTRIES
    if retry_overflow > 0:
        evict_retry_keys = sorted(
            _POLICY_LOOKUP_RETRY_AFTER.items(),
            key=lambda kv: kv[1],
        )[:retry_overflow]
        for key, _ in evict_retry_keys:
            _POLICY_LOOKUP_RETRY_AFTER.pop(key, None)


def _get_identity_service_cls():
    global AsyncIdentityService, _ASYNC_IDENTITY_SERVICE_CACHE
    if AsyncIdentityService is not None:
        return AsyncIdentityService
    if _ASYNC_IDENTITY_SERVICE_CACHE is None:
        # Lazy import avoids loading heavy wallet/CDP deps during module import.
        from core.identity.service import AsyncIdentityService as _AsyncIdentityService

        _ASYNC_IDENTITY_SERVICE_CACHE = _AsyncIdentityService
    return _ASYNC_IDENTITY_SERVICE_CACHE


def _build_identity_service():
    return _get_identity_service_cls()()


async def _build_identity_service_async():
    if AsyncIdentityService is None and _ASYNC_IDENTITY_SERVICE_CACHE is None:
        await run_blocking(_get_identity_service_cls)
    return _build_identity_service()


def _get_last_user_message(state: AgentState) -> str:
    messages = state.get("messages") or []
    for m in reversed(messages):
        if isinstance(m, HumanMessage):
            return str(m.content)
    return ""


def _is_retry_request(text: str) -> bool:
    normalized = re.sub(r"[^a-z0-9]+", " ", (text or "").lower()).strip()
    if not normalized:
        return False
    hay = f" {normalized} "
    phrases = {
        "retry",
        "try again",
        "retry setup",
        "retry wallet",
    }
    return any(f" {phrase} " in hay for phrase in phrases)


def _skip_user_db() -> bool:
    value = os.getenv("SKIP_MONGODB_USERS", "").strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    value = os.getenv("SKIP_MONGODB_HEALTHCHECK", "").strip().lower()
    return value in {"1", "true", "yes", "on"}


async def _load_effective_policy(volo_user_id: str) -> Dict[str, Any] | None:
    return await PolicyStore().aget_effective_policy(str(volo_user_id))


async def _load_effective_policy_deduped(volo_user_id: str) -> Dict[str, Any] | None:
    with _POLICY_LOOKUP_INFLIGHT_LOCK:
        task = _POLICY_LOOKUP_INFLIGHT.get(volo_user_id)
        if task is None or task.done():
            task = asyncio.create_task(
                asyncio.wait_for(
                    _load_effective_policy(volo_user_id),
                    timeout=_POLICY_LOOKUP_TIMEOUT_SECONDS,
                )
            )
            _POLICY_LOOKUP_INFLIGHT[volo_user_id] = task
    try:
        return await task
    finally:
        if task.done():
            with _POLICY_LOOKUP_INFLIGHT_LOCK:
                current = _POLICY_LOOKUP_INFLIGHT.get(volo_user_id)
                if current is task:
                    _POLICY_LOOKUP_INFLIGHT.pop(volo_user_id, None)


def _policy_lookup_message_retry_seconds(retry_after_seconds: float | None) -> str:
    if retry_after_seconds is None or retry_after_seconds >= 45:
        return "in about a minute"
    rounded = max(1, int(round(retry_after_seconds)))
    unit = "second" if rounded == 1 else "seconds"
    return f"in about {rounded} {unit}"


def _wallet_setup_failure_message(*, detail: str | None = None) -> str:
    msg = "We couldn't finish setting up your wallets."
    if detail:
        msg = f"{msg} Reason: {detail}."
    return f"{msg} Please reply 'retry' to try again."


def _wallet_refresh_warning_message() -> str:
    return (
        "We couldn't refresh all wallet networks right now. "
        "You can continue with EVM actions and reply 'retry' later."
    )


def _policy_lookup_failure_message(*, retry_after_seconds: float | None = None) -> str:
    return (
        "We couldn't verify your security policy right now, so I paused before any "
        "actions could run. Please reply 'retry' "
        f"{_policy_lookup_message_retry_seconds(retry_after_seconds)}."
    )


def _get_cached_policy(volo_user_id: str) -> tuple[bool, Dict[str, Any] | None]:
    now = time.monotonic()
    with _POLICY_CACHE_LOCK:
        _prune_expired_policy_state_locked(now)
        entry = _POLICY_CACHE.get(volo_user_id)
        if entry is None:
            return False, None
        if entry.expires_monotonic <= now:
            _POLICY_CACHE.pop(volo_user_id, None)
            return False, None
        if entry.policy is None:
            return True, None
        return True, dict(entry.policy)


def _cache_policy(volo_user_id: str, policy: Dict[str, Any] | None) -> None:
    with _POLICY_CACHE_LOCK:
        now = time.monotonic()
        _prune_expired_policy_state_locked(now)
        _POLICY_CACHE[volo_user_id] = _PolicyCacheEntry(
            expires_monotonic=now + _POLICY_CACHE_TTL_SECONDS,
            policy=dict(policy) if isinstance(policy, dict) else None,
        )
        _enforce_policy_state_limits_locked()


def _get_policy_retry_after(volo_user_id: str) -> float | None:
    now = time.monotonic()
    with _POLICY_CACHE_LOCK:
        _prune_expired_policy_state_locked(now)
        retry_after = _POLICY_LOOKUP_RETRY_AFTER.get(volo_user_id)
        if retry_after is None:
            return None
        if retry_after <= now:
            _POLICY_LOOKUP_RETRY_AFTER.pop(volo_user_id, None)
            return None
        return retry_after


def _mark_policy_lookup_failed(volo_user_id: str) -> float:
    retry_after = time.monotonic() + _POLICY_LOOKUP_RETRY_SECONDS
    with _POLICY_CACHE_LOCK:
        _prune_expired_policy_state_locked(time.monotonic())
        _POLICY_LOOKUP_RETRY_AFTER[volo_user_id] = retry_after
        _enforce_policy_state_limits_locked()
    return retry_after


def _clear_policy_lookup_failed(volo_user_id: str) -> None:
    with _POLICY_CACHE_LOCK:
        _POLICY_LOOKUP_RETRY_AFTER.pop(volo_user_id, None)


def _seconds_until(deadline_monotonic: float) -> float:
    return max(0.0, deadline_monotonic - time.monotonic())


def _policy_lookup_error_detail(exc: Exception) -> str:
    if isinstance(exc, asyncio.TimeoutError):
        return f"timeout_after={_POLICY_LOOKUP_TIMEOUT_SECONDS:.2f}s"
    detail = str(exc).strip()
    if detail:
        return detail
    return type(exc).__name__


def _solana_pending_retry_message(user_data: Dict[str, Any]) -> str:
    detail = ""
    metadata = user_data.get("metadata")
    if isinstance(metadata, dict):
        last_error = str(metadata.get("solana_provision_last_error") or "").strip()
        if last_error:
            detail = f" Latest issue: {last_error}."
    return (
        "Solana wallet setup is still pending."
        f"{detail} You can continue with EVM actions. "
        "Please reply 'retry' again in about a minute."
    )


def _needs_existing_user_wallet_refresh(
    user_data: Dict[str, Any], *, retry_requested: bool
) -> bool:
    if retry_requested:
        return True
    has_evm_sub_org = bool(
        user_data.get("evm_sub_org_id") or user_data.get("sub_org_id")
    )
    has_evm_address = bool(
        user_data.get("evm_address") or user_data.get("sender_address")
    )
    return not (has_evm_sub_org and has_evm_address)


async def onboarding_node(state: AgentState) -> Dict[str, Any]:
    user_id_raw = state.get("user_id")
    user_id = str(user_id_raw).strip() if user_id_raw is not None else ""
    provider_raw = state.get("provider")
    provider = str(provider_raw).strip().lower() if provider_raw is not None else ""
    if not provider:
        provider = "unknown"
    username = state.get("username")
    if not user_id:
        # If no user_id provided by the interface, we cannot proceed securely
        return {
            "messages": [
                AIMessage(
                    content="System error: Identification failed. Please try again."
                )
            ],
            "route_decision": "end",
        }

    if provider == "cli" and _skip_user_db():
        sender_address = os.getenv("CLI_SENDER_ADDRESS", "").strip()
        sub_org_id = os.getenv("CLI_SUB_ORG_ID", "").strip()
        solana_address = os.getenv("CLI_SOLANA_ADDRESS", "").strip()
        solana_sub_org_id = os.getenv("CLI_SOLANA_SUB_ORG_ID", "").strip()
        if not sender_address or not sub_org_id or not solana_address or not solana_sub_org_id:
            return {
                "messages": [
                    AIMessage(
                        content=(
                            "CLI mode: set CLI_SENDER_ADDRESS, CLI_SUB_ORG_ID, "
                            "CLI_SOLANA_ADDRESS, and CLI_SOLANA_SUB_ORG_ID "
                            "to bypass MongoDB user lookup."
                        )
                    )
                ],
                "route_decision": "end",
            }
        user_data = {
            "volo_user_id": str(user_id),
            "sub_org_id": sub_org_id,
            "sender_address": sender_address,
            "evm_sub_org_id": sub_org_id,
            "evm_address": sender_address,
            "solana_sub_org_id": solana_sub_org_id or None,
            "solana_address": solana_address or None,
            "is_new_user": False,
        }
        return {
            "user_info": user_data,
            "artifacts": {
                "sub_org_id": sub_org_id,
                "sender_address": sender_address,
                "evm_sub_org_id": sub_org_id,
                "evm_address": sender_address,
                "solana_sub_org_id": solana_sub_org_id or None,
                "solana_address": solana_address or None,
            },
            "guardrail_policy": None,
            "route_decision": None,
        }

    try:
        user_service = await _build_identity_service_async()
    except Exception as exc:
        _LOGGER.warning(
            "identity_service_init_failed provider=%s provider_user_id=%s detail=%s",
            provider,
            user_id,
            exc,
        )
        return {
            "messages": [
                AIMessage(
                    content=_wallet_setup_failure_message()
                )
            ],
            "route_decision": "end",
        }
    last_user_msg = _get_last_user_message(state)
    retry_requested = _is_retry_request(last_user_msg)
    link_token = extract_link_token(last_user_msg)

    # If user already exists, continue without prompting.
    try:
        existing_user = await user_service.get_user_by_identity(provider, user_id)
    except Exception as exc:
        _LOGGER.warning(
            "user_lookup_failed provider=%s provider_user_id=%s detail=%s",
            provider,
            user_id,
            exc,
        )
        return {
            "messages": [
                AIMessage(
                    content=_wallet_setup_failure_message()
                )
            ],
            "route_decision": "end",
        }
    if existing_user:
        await user_service.sync_username(existing_user, provider, user_id, username)
        user_data = dict(existing_user)
        if _needs_existing_user_wallet_refresh(
            existing_user, retry_requested=retry_requested
        ):
            try:
                user_data = await user_service.ensure_multi_chain_wallets(
                    existing_user,
                    force_solana_retry=retry_requested,
                )
            except Exception as exc:
                _LOGGER.warning(
                    "wallet_refresh_failed provider=%s provider_user_id=%s detail=%s",
                    provider,
                    user_id,
                    exc,
                )
                user_data = dict(existing_user)
                user_data["wallet_setup_warning"] = _wallet_refresh_warning_message()
        if retry_requested and not user_data.get("solana_address"):
            user_data["wallet_setup_warning"] = _solana_pending_retry_message(user_data)
        user_data["is_new_user"] = False
    else:
        # New user flow: require explicit choice.
        user_data: Dict[str, Any] = {}
        if retry_requested:
            try:
                user_data = await user_service.reprovision_wallets(
                    str(user_id), provider, user_id, username=username
                )
                user_data["is_new_user"] = True
                # Skip the normal create/link gating if the user explicitly retried.
                link_token = None
            except Exception:
                return {
                    "messages": [
                        AIMessage(
                            content=_wallet_setup_failure_message()
                        )
                    ],
                    "route_decision": "end",
                }
        if link_token and not user_data.get("is_new_user"):
            try:
                user_data = await user_service.link_identity_by_token(
                    link_token, provider, user_id, username=username
                )
            except LinkAccountError as exc:
                return {
                    "messages": [
                        AIMessage(
                            content=exc.user_message
                        )
                    ],
                    "route_decision": "end",
                }
            except Exception:
                return {
                    "messages": [
                        AIMessage(
                            content=(
                                "We couldn't link this account right now. "
                                "Request a new code on the platform where your wallet already exists, "
                                "then send 'link <CODE>' here."
                            )
                        )
                    ],
                    "route_decision": "end",
                }
            try:
                user_data = await user_service.ensure_multi_chain_wallets(user_data)
            except Exception as exc:
                _LOGGER.warning(
                    "linked_wallet_refresh_failed provider=%s provider_user_id=%s detail=%s",
                    provider,
                    user_id,
                    exc,
                )
                user_data = dict(user_data)
                user_data["wallet_setup_warning"] = _wallet_refresh_warning_message()
            user_data["is_new_user"] = False
        elif not user_data and is_create_wallet_request(last_user_msg):
            try:
                user_data = await user_service.register_user(
                    provider, user_id, username=username
                )
                user_data["is_new_user"] = True
            except Exception:
                return {
                    "messages": [
                        AIMessage(
                            content=_wallet_setup_failure_message()
                        )
                    ],
                    "route_decision": "end",
                }
        elif not user_data and is_link_account_request(last_user_msg):
            return {
                "messages": [
                    AIMessage(
                        content=(
                            "To link an existing wallet, open the platform where it already exists "
                            "and type 'link account' to get a code. Then come back here and send "
                            "'link <CODE>'. After linking, say 'linked accounts' to review every "
                            "connected provider."
                        )
                    )
                ],
                "route_decision": "end",
            }
        elif not user_data:
            return {
                "messages": [
                    AIMessage(
                        content=(
                            "Do you want to link an existing wallet or create a new one? "
                            "Reply with 'link' or 'create'."
                        )
                    )
                ],
                "route_decision": "end",
            }

    required_fields = [
        "sub_org_id",
        "sender_address",
    ]
    missing = [field for field in required_fields if not user_data.get(field)]
    if missing:
        missing_fields = ", ".join(sorted(missing))
        return {
            "messages": [
                AIMessage(
                    content=_wallet_setup_failure_message(
                        detail=f"missing required fields: {missing_fields}"
                    )
                )
            ],
            "route_decision": "end",
        }

    # Store user data in state for downstream resolution
    is_new = user_data.get("is_new_user", False)

    updates: Dict[str, Any] = {
        "user_info": user_data,
        # Inject global markers into artifacts for resolver/executor
        "artifacts": {
            "sub_org_id": user_data["sub_org_id"],
            "sender_address": user_data["sender_address"],
            "evm_sub_org_id": user_data.get("evm_sub_org_id", user_data["sub_org_id"]),
            "evm_address": user_data.get("evm_address", user_data["sender_address"]),
            "solana_sub_org_id": user_data.get("solana_sub_org_id"),
            "solana_address": user_data.get("solana_address"),
        },
        "guardrail_policy": None,
        # Clear any previous terminal decision so the router can run this turn.
        "route_decision": None,
    }

    volo_user_id = user_data.get("volo_user_id")
    if volo_user_id:
        volo_user_id = str(volo_user_id)
        cached_hit, cached_policy = _get_cached_policy(volo_user_id)
        if cached_hit:
            updates["guardrail_policy"] = cached_policy
        else:
            retry_after = _get_policy_retry_after(volo_user_id)
            if retry_after is not None:
                return {
                    "messages": [
                        AIMessage(
                            content=_policy_lookup_failure_message(
                                retry_after_seconds=_seconds_until(retry_after)
                            )
                        )
                    ],
                    "route_decision": "end",
                }
            try:
                policy = await _load_effective_policy_deduped(volo_user_id)
            except Exception as exc:
                retry_after = _mark_policy_lookup_failed(volo_user_id)
                detail = _policy_lookup_error_detail(exc)
                retry_after_seconds = _seconds_until(retry_after)
                _LOGGER.warning(
                    "policy_lookup_failed volo_user_id=%s detail=%s retry_after_seconds=%.2f",
                    volo_user_id,
                    detail,
                    retry_after_seconds,
                )
                return {
                    "messages": [
                        AIMessage(
                            content=_policy_lookup_failure_message(
                                retry_after_seconds=retry_after_seconds
                            )
                        )
                    ],
                    "route_decision": "end",
                }
            _clear_policy_lookup_failed(volo_user_id)
            _cache_policy(volo_user_id, policy)
            updates["guardrail_policy"] = (
                dict(policy) if isinstance(policy, dict) else None
            )

    if is_new:
        chains_list = supported_chains()
        if chains_list:
            chains = ", ".join([c.title() for c in chains_list])
        else:
            chains = "Currently unavailable. Please contact support."
        evm_address = user_data.get("evm_address") or user_data.get("sender_address")
        solana_address = user_data.get("solana_address")
        solana_line = (
            f"Solana wallet address: {solana_address}.\n\n"
            if solana_address
            else ""
        )
        solana_note = (
            "Solana wallet setup is still pending right now. "
            "You can continue with EVM actions, then reply 'retry' later to "
            "try Solana setup again.\n\n"
            if not solana_address
            else ""
        )
        msg = (
            "Welcome! I've automatically registered you and created your wallets.\n\n"
            f"EVM wallet address: {evm_address}.\n"
            f"{solana_line}"
            f"{solana_note}"
            "To get started, please fund these addresses on their respective networks.\n\n"
            f"Supported EVM networks: {chains}.\n\n"
            "When you link another platform later, say 'linked accounts' to review them. "
            "To remove one, say 'unlink <provider>' or 'unlink @username'."
        )
        updates["messages"] = [AIMessage(content=msg)]
    elif link_token:
        evm_address = user_data.get("evm_address") or user_data.get("sender_address")
        solana_address = user_data.get("solana_address")
        details = f"EVM address: {evm_address}."
        if solana_address:
            details = f"{details} Solana address: {solana_address}."
        warning = str(user_data.get("wallet_setup_warning") or "").strip()
        warning_suffix = f" {warning}" if warning else ""
        updates["messages"] = [
            AIMessage(
                content=(
                    f"Accounts linked. {details} "
                    "Say 'linked accounts' to review connected providers. "
                    "To remove one later, say 'unlink <provider>' or 'unlink @username'."
                    f"{warning_suffix}"
                )
            )
        ]
    elif user_data.get("wallet_setup_warning"):
        updates["messages"] = [AIMessage(content=str(user_data["wallet_setup_warning"]))]

    return updates
