from __future__ import annotations

import asyncio
import graph.nodes.onboarding_node as onboarding_node_module
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from langchain_core.messages import AIMessage, HumanMessage

from core.identity import ExpiredLinkTokenError
from graph.agent_state import AgentState
from graph.nodes.onboarding_node import onboarding_node


@pytest.fixture(autouse=True)
def _reset_policy_lookup_state():
    with (
        patch.dict(onboarding_node_module._POLICY_CACHE, {}, clear=True),
        patch.dict(onboarding_node_module._POLICY_LOOKUP_RETRY_AFTER, {}, clear=True),
        patch.dict(onboarding_node_module._POLICY_LOOKUP_INFLIGHT, {}, clear=True),
    ):
        yield


def _make_state(
    *,
    user_id: str = "",
    provider: str = "unknown",
    username: str | None = None,
    message: str | None = None,
) -> AgentState:
    return {
        "user_id": user_id,
        "provider": provider,
        "username": username,
        "user_info": None,
        "intents": [],
        "plans": [],
        "goal_parameters": {},
        "plan_history": [],
        "execution_state": None,
        "artifacts": {},
        "context": {},
        "route_decision": None,
        "confirmation_status": None,
        "pending_transactions": [],
        "reasoning_logs": [],
        "messages": [HumanMessage(content=message)] if message else [],
        "fee_quotes": [],
        "trigger_id": None,
        "is_triggered_execution": None,
    }


def _make_service() -> MagicMock:
    service = MagicMock()
    service.get_user_by_identity = AsyncMock()
    service.sync_username = AsyncMock()
    service.ensure_multi_chain_wallets = AsyncMock()
    service.reprovision_wallets = AsyncMock()
    service.link_identity_by_token = AsyncMock()
    service.register_user = AsyncMock()
    return service


def test_onboarding_missing_user_id_returns_end():
    state = _make_state(user_id="")

    with patch("graph.nodes.onboarding_node.AsyncIdentityService") as mock_service:
        result = asyncio.run(onboarding_node(state))

    assert result["route_decision"] == "end"
    assert isinstance(result["messages"][0], AIMessage)
    assert "Identification failed" in result["messages"][0].content
    mock_service.assert_not_called()


def test_onboarding_new_user_prompts_choice():
    mock_service = _make_service()
    mock_service.get_user_by_identity.return_value = None

    with patch("graph.nodes.onboarding_node.AsyncIdentityService", return_value=mock_service):
        state = _make_state(user_id="user-1", provider="discord", username="alice")
        result = asyncio.run(onboarding_node(state))

    assert result["route_decision"] == "end"
    assert "link" in result["messages"][0].content.lower()
    assert "create" in result["messages"][0].content.lower()
    mock_service.register_user.assert_not_called()
    mock_service.link_identity_by_token.assert_not_called()


def test_onboarding_new_user_create_adds_message_and_artifacts():
    user_data = {
        "sub_org_id": "sub-123",
        "sender_address": "0xabc",
        "solana_sub_org_id": "sol-sub-123",
        "solana_address": "So1anaAddress",
        "is_new_user": True,
    }
    mock_service = _make_service()
    mock_service.get_user_by_identity.return_value = None
    mock_service.register_user.return_value = user_data

    with patch("graph.nodes.onboarding_node.AsyncIdentityService", return_value=mock_service):
        with patch(
            "graph.nodes.onboarding_node.supported_chains",
            return_value=["ethereum", "base"],
        ):
            state = _make_state(
                user_id="user-1", provider="discord", username="alice", message="create"
            )
            result = asyncio.run(onboarding_node(state))

    mock_service.register_user.assert_awaited_once_with(
        "discord", "user-1", username="alice"
    )
    assert result["user_info"] == user_data
    assert result["artifacts"]["sub_org_id"] == "sub-123"
    assert result["artifacts"]["sender_address"] == "0xabc"
    assert result["artifacts"]["solana_sub_org_id"] == "sol-sub-123"
    assert result["artifacts"]["solana_address"] == "So1anaAddress"
    content = result["messages"][0].content
    assert "Welcome!" in content
    assert "0xabc" in content
    assert "So1anaAddress" in content
    assert "Supported EVM networks: Ethereum, Base" in content


def test_onboarding_existing_user_skips_message():
    user_data = {
        "sub_org_id": "sub-999",
        "sender_address": "0xdef",
        "evm_sub_org_id": "sub-999",
        "evm_address": "0xdef",
        "solana_sub_org_id": "sol-sub-999",
        "solana_address": "So1anaExisting",
    }
    mock_service = _make_service()
    mock_service.get_user_by_identity.return_value = user_data
    mock_service.ensure_multi_chain_wallets.return_value = user_data

    with patch("graph.nodes.onboarding_node.AsyncIdentityService", return_value=mock_service):
        state = _make_state(user_id="user-2")
        result = asyncio.run(onboarding_node(state))

    mock_service.get_user_by_identity.assert_awaited_once_with("unknown", "user-2")
    mock_service.sync_username.assert_awaited_once_with(
        user_data, "unknown", "user-2", None
    )
    mock_service.ensure_multi_chain_wallets.assert_not_awaited()
    assert result["user_info"] == {**user_data, "is_new_user": False}
    assert result["artifacts"]["sub_org_id"] == "sub-999"
    assert result["artifacts"]["sender_address"] == "0xdef"
    assert result["artifacts"]["solana_sub_org_id"] == "sol-sub-999"
    assert result["artifacts"]["solana_address"] == "So1anaExisting"
    assert "messages" not in result


def test_onboarding_existing_user_policy_lookup_failure_stops_flow():
    user_data = {
        "volo_user_id": "volo-2",
        "sub_org_id": "sub-999",
        "sender_address": "0xdef",
        "evm_sub_org_id": "sub-999",
        "evm_address": "0xdef",
        "solana_sub_org_id": "sol-sub-999",
        "solana_address": "So1anaExisting",
    }
    mock_service = _make_service()
    mock_service.get_user_by_identity.return_value = user_data

    with (
        patch("graph.nodes.onboarding_node.AsyncIdentityService", return_value=mock_service),
        patch(
            "graph.nodes.onboarding_node._load_effective_policy",
            new=AsyncMock(side_effect=asyncio.TimeoutError()),
        ),
    ):
        state = _make_state(user_id="user-2")
        result = asyncio.run(onboarding_node(state))

    assert result["route_decision"] == "end"
    assert "security policy" in result["messages"][0].content.lower()
    assert "reply 'retry'" in result["messages"][0].content.lower()


def test_onboarding_existing_user_unexpected_policy_error_stops_flow():
    user_data = {
        "volo_user_id": "volo-2",
        "sub_org_id": "sub-999",
        "sender_address": "0xdef",
        "evm_sub_org_id": "sub-999",
        "evm_address": "0xdef",
        "solana_sub_org_id": "sol-sub-999",
        "solana_address": "So1anaExisting",
    }
    mock_service = _make_service()
    mock_service.get_user_by_identity.return_value = user_data

    with (
        patch("graph.nodes.onboarding_node.AsyncIdentityService", return_value=mock_service),
        patch(
            "graph.nodes.onboarding_node._load_effective_policy",
            new=AsyncMock(side_effect=RuntimeError("mongo not configured")),
        ),
    ):
        state = _make_state(user_id="user-2")
        result = asyncio.run(onboarding_node(state))

    assert result["route_decision"] == "end"
    assert "security policy" in result["messages"][0].content.lower()
    assert "reply 'retry'" in result["messages"][0].content.lower()


def test_onboarding_existing_user_policy_lookup_timeout_uses_backoff():
    user_data = {
        "volo_user_id": "volo-2",
        "sub_org_id": "sub-999",
        "sender_address": "0xdef",
        "evm_sub_org_id": "sub-999",
        "evm_address": "0xdef",
        "solana_sub_org_id": "sol-sub-999",
        "solana_address": "So1anaExisting",
    }
    mock_service = _make_service()
    mock_service.get_user_by_identity.return_value = user_data
    load_policy = AsyncMock(side_effect=asyncio.TimeoutError())

    with (
        patch("graph.nodes.onboarding_node.AsyncIdentityService", return_value=mock_service),
        patch("graph.nodes.onboarding_node._load_effective_policy", new=load_policy),
    ):
        first = asyncio.run(onboarding_node(_make_state(user_id="user-2")))
        second = asyncio.run(onboarding_node(_make_state(user_id="user-2")))

    assert first["route_decision"] == "end"
    assert second["route_decision"] == "end"
    assert load_policy.await_count == 1
    assert "security policy" in second["messages"][0].content.lower()
    assert "retry" in second["messages"][0].content.lower()


def test_onboarding_existing_user_policy_lookup_success_uses_cache():
    user_data = {
        "volo_user_id": "volo-2",
        "sub_org_id": "sub-999",
        "sender_address": "0xdef",
        "evm_sub_org_id": "sub-999",
        "evm_address": "0xdef",
        "solana_sub_org_id": "sol-sub-999",
        "solana_address": "So1anaExisting",
    }
    mock_service = _make_service()
    mock_service.get_user_by_identity.return_value = user_data
    load_policy = AsyncMock(return_value={"max_parallel_nodes": 3})

    with (
        patch("graph.nodes.onboarding_node.AsyncIdentityService", return_value=mock_service),
        patch("graph.nodes.onboarding_node._load_effective_policy", new=load_policy),
    ):
        first = asyncio.run(onboarding_node(_make_state(user_id="user-2")))
        second = asyncio.run(onboarding_node(_make_state(user_id="user-2")))

    assert first["guardrail_policy"] == {"max_parallel_nodes": 3}
    assert second["guardrail_policy"] == {"max_parallel_nodes": 3}
    assert load_policy.await_count == 1


def test_onboarding_existing_user_policy_none_clears_stale_state():
    user_data = {
        "volo_user_id": "volo-2",
        "sub_org_id": "sub-999",
        "sender_address": "0xdef",
        "evm_sub_org_id": "sub-999",
        "evm_address": "0xdef",
    }
    mock_service = _make_service()
    mock_service.get_user_by_identity.return_value = user_data

    with (
        patch("graph.nodes.onboarding_node.AsyncIdentityService", return_value=mock_service),
        patch("graph.nodes.onboarding_node._load_effective_policy", new=AsyncMock(return_value=None)),
    ):
        state = _make_state(user_id="user-2")
        state["guardrail_policy"] = {"max_parallel_nodes": 7}
        result = asyncio.run(onboarding_node(state))

    assert "guardrail_policy" in result
    assert result["guardrail_policy"] is None


def test_onboarding_concurrent_policy_lookup_is_deduplicated():
    user_data = {
        "volo_user_id": "volo-2",
        "sub_org_id": "sub-999",
        "sender_address": "0xdef",
        "evm_sub_org_id": "sub-999",
        "evm_address": "0xdef",
    }
    mock_service = _make_service()
    mock_service.get_user_by_identity.return_value = user_data

    async def load_policy(_: str):
        await asyncio.sleep(0.02)
        return {"max_parallel_nodes": 3}

    load_policy_mock = AsyncMock(side_effect=load_policy)

    async def run_parallel():
        first, second = await asyncio.gather(
            onboarding_node(_make_state(user_id="user-2")),
            onboarding_node(_make_state(user_id="user-2")),
        )
        return first, second

    with (
        patch("graph.nodes.onboarding_node.AsyncIdentityService", return_value=mock_service),
        patch("graph.nodes.onboarding_node._load_effective_policy", new=load_policy_mock),
    ):
        first, second = asyncio.run(run_parallel())

    assert first["guardrail_policy"] == {"max_parallel_nodes": 3}
    assert second["guardrail_policy"] == {"max_parallel_nodes": 3}
    assert load_policy_mock.await_count == 1


def test_onboarding_existing_user_without_solana_still_proceeds():
    user_data = {
        "sub_org_id": "sub-999",
        "sender_address": "0xdef",
        "evm_sub_org_id": "sub-999",
        "evm_address": "0xdef",
        "solana_sub_org_id": None,
        "solana_address": None,
    }
    mock_service = _make_service()
    mock_service.get_user_by_identity.return_value = user_data
    mock_service.ensure_multi_chain_wallets.return_value = user_data

    with patch("graph.nodes.onboarding_node.AsyncIdentityService", return_value=mock_service):
        state = _make_state(user_id="user-2")
        result = asyncio.run(onboarding_node(state))

    mock_service.ensure_multi_chain_wallets.assert_not_awaited()
    assert result["user_info"]["sub_org_id"] == "sub-999"
    assert result["artifacts"]["sub_org_id"] == "sub-999"
    assert result["artifacts"]["sender_address"] == "0xdef"
    assert result["artifacts"]["solana_sub_org_id"] is None
    assert result["artifacts"]["solana_address"] is None
    assert result.get("route_decision") is None


def test_onboarding_existing_user_missing_evm_fields_refreshes_wallets():
    user_data = {
        "sub_org_id": "sub-999",
        "sender_address": None,
        "solana_sub_org_id": None,
        "solana_address": None,
    }
    refreshed = {
        **user_data,
        "sender_address": "0xdef",
        "evm_sub_org_id": "sub-999",
        "evm_address": "0xdef",
    }
    mock_service = _make_service()
    mock_service.get_user_by_identity.return_value = user_data
    mock_service.ensure_multi_chain_wallets.return_value = refreshed

    with patch("graph.nodes.onboarding_node.AsyncIdentityService", return_value=mock_service):
        state = _make_state(user_id="user-2")
        result = asyncio.run(onboarding_node(state))

    mock_service.ensure_multi_chain_wallets.assert_awaited_once_with(
        user_data,
        force_solana_retry=False,
    )
    assert result["user_info"]["sender_address"] == "0xdef"
    assert result["artifacts"]["sender_address"] == "0xdef"
    assert result.get("route_decision") is None
    assert "messages" not in result


def test_onboarding_existing_user_retry_wallet_refresh_failure_still_proceeds():
    user_data = {
        "sub_org_id": "sub-999",
        "sender_address": "0xdef",
        "evm_sub_org_id": "sub-999",
        "evm_address": "0xdef",
        "solana_sub_org_id": None,
        "solana_address": None,
    }
    mock_service = _make_service()
    mock_service.get_user_by_identity.return_value = user_data
    mock_service.ensure_multi_chain_wallets.side_effect = RuntimeError("provisioner timeout")

    with patch("graph.nodes.onboarding_node.AsyncIdentityService", return_value=mock_service):
        state = _make_state(user_id="user-2", message="retry")
        result = asyncio.run(onboarding_node(state))

    mock_service.ensure_multi_chain_wallets.assert_awaited_once_with(
        user_data,
        force_solana_retry=True,
    )
    assert result["user_info"]["sub_org_id"] == "sub-999"
    assert result["artifacts"]["sender_address"] == "0xdef"
    assert result.get("route_decision") is None
    assert "messages" in result
    assert "solana wallet setup is still pending" in result["messages"][0].content.lower()
    assert "reply 'retry'" in result["messages"][0].content.lower()


def test_onboarding_existing_user_retry_forces_solana_refresh():
    user_data = {
        "sub_org_id": "sub-999",
        "sender_address": "0xdef",
        "evm_sub_org_id": "sub-999",
        "evm_address": "0xdef",
        "solana_sub_org_id": None,
        "solana_address": None,
        "metadata": {"solana_provision_last_error": "TimeoutError"},
    }
    mock_service = _make_service()
    mock_service.get_user_by_identity.return_value = user_data
    mock_service.ensure_multi_chain_wallets.return_value = user_data

    with patch("graph.nodes.onboarding_node.AsyncIdentityService", return_value=mock_service):
        state = _make_state(user_id="user-2", message="retry")
        result = asyncio.run(onboarding_node(state))

    mock_service.ensure_multi_chain_wallets.assert_awaited_once_with(
        user_data,
        force_solana_retry=True,
    )
    assert "messages" in result
    content = result["messages"][0].content.lower()
    assert "solana wallet setup is still pending" in content
    assert "reply 'retry'" in content


def test_onboarding_new_user_empty_supported_chains():
    user_data = {
        "sub_org_id": "sub-empty",
        "sender_address": "0xempty",
        "solana_sub_org_id": "sol-sub-empty",
        "solana_address": "So1anaEmpty",
        "is_new_user": True,
    }
    mock_service = _make_service()
    mock_service.get_user_by_identity.return_value = None
    mock_service.register_user.return_value = user_data

    with patch("graph.nodes.onboarding_node.AsyncIdentityService", return_value=mock_service):
        with patch("graph.nodes.onboarding_node.supported_chains", return_value=[]):
            state = _make_state(user_id="user-3", message="create")
            result = asyncio.run(onboarding_node(state))

    content = result["messages"][0].content
    assert "Supported EVM networks: Currently unavailable." in content
    assert "Please contact support." in content


def test_onboarding_new_user_without_solana_adds_recovery_note():
    user_data = {
        "sub_org_id": "sub-empty",
        "sender_address": "0xempty",
        "solana_sub_org_id": None,
        "solana_address": None,
        "is_new_user": True,
    }
    mock_service = _make_service()
    mock_service.get_user_by_identity.return_value = None
    mock_service.register_user.return_value = user_data

    with patch("graph.nodes.onboarding_node.AsyncIdentityService", return_value=mock_service):
        with patch(
            "graph.nodes.onboarding_node.supported_chains",
            return_value=["ethereum"],
        ):
            state = _make_state(user_id="user-3", message="create")
            result = asyncio.run(onboarding_node(state))

    content = result["messages"][0].content.lower()
    assert "solana wallet setup is still pending" in content
    assert "reply 'retry'" in content


def test_onboarding_missing_user_data_fields_returns_end():
    user_data = {"is_new_user": True, "sub_org_id": ""}
    mock_service = _make_service()
    mock_service.get_user_by_identity.return_value = None
    mock_service.register_user.return_value = user_data

    with patch("graph.nodes.onboarding_node.AsyncIdentityService", return_value=mock_service):
        result = asyncio.run(onboarding_node(_make_state(user_id="user-4", message="create")))

    assert result["route_decision"] == "end"
    assert "We couldn't finish setting up your wallets." in result["messages"][0].content


def test_onboarding_user_service_error_returns_end():
    mock_service = _make_service()
    mock_service.get_user_by_identity.return_value = None
    mock_service.register_user.side_effect = RuntimeError("db unavailable")

    with patch("graph.nodes.onboarding_node.AsyncIdentityService", return_value=mock_service):
        result = asyncio.run(onboarding_node(_make_state(user_id="user-5", message="create")))

    assert result["route_decision"] == "end"
    assert "We couldn't finish setting up your wallets." in result["messages"][0].content


def test_onboarding_cli_skip_mongodb_uses_env(monkeypatch):
    monkeypatch.setenv("SKIP_MONGODB_HEALTHCHECK", "1")
    monkeypatch.setenv("CLI_SENDER_ADDRESS", "0xabc")
    monkeypatch.setenv("CLI_SUB_ORG_ID", "sub-cli")
    monkeypatch.setenv("CLI_SOLANA_ADDRESS", "So1anaCLIAddress")
    monkeypatch.setenv("CLI_SOLANA_SUB_ORG_ID", "sol-cli")

    state = _make_state(user_id="cli-user", provider="cli")
    with patch("graph.nodes.onboarding_node.AsyncIdentityService") as mock_service:
        result = asyncio.run(onboarding_node(state))

    mock_service.assert_not_called()
    assert result["artifacts"]["sender_address"] == "0xabc"
    assert result["artifacts"]["sub_org_id"] == "sub-cli"
    assert result["artifacts"]["solana_address"] == "So1anaCLIAddress"
    assert result["artifacts"]["solana_sub_org_id"] == "sol-cli"


def test_onboarding_cli_skip_mongodb_missing_env(monkeypatch):
    monkeypatch.setenv("SKIP_MONGODB_HEALTHCHECK", "1")
    monkeypatch.delenv("CLI_SENDER_ADDRESS", raising=False)
    monkeypatch.delenv("CLI_SUB_ORG_ID", raising=False)
    monkeypatch.delenv("CLI_SOLANA_ADDRESS", raising=False)
    monkeypatch.delenv("CLI_SOLANA_SUB_ORG_ID", raising=False)

    state = _make_state(user_id="cli-user", provider="cli")
    with patch("graph.nodes.onboarding_node.AsyncIdentityService") as mock_service:
        result = asyncio.run(onboarding_node(state))

    mock_service.assert_not_called()
    assert result["route_decision"] == "end"
    assert "CLI mode" in result["messages"][0].content


def test_onboarding_retry_reprovisions_wallets():
    user_data = {
        "sub_org_id": "sub-retry",
        "sender_address": "0xretry",
        "solana_sub_org_id": "sol-retry",
        "solana_address": "So1anaRetry",
        "is_new_user": True,
    }
    mock_service = _make_service()
    mock_service.get_user_by_identity.return_value = None
    mock_service.reprovision_wallets.return_value = user_data

    with patch("graph.nodes.onboarding_node.AsyncIdentityService", return_value=mock_service):
        with patch(
            "graph.nodes.onboarding_node.supported_chains",
            return_value=["ethereum"],
        ):
            state = _make_state(user_id="user-retry", message="retry")
            result = asyncio.run(onboarding_node(state))

    mock_service.reprovision_wallets.assert_awaited_once_with(
        "user-retry", "unknown", "user-retry", username=None
    )
    assert result["artifacts"]["sub_org_id"] == "sub-retry"
    assert result["artifacts"]["solana_sub_org_id"] == "sol-retry"
    assert "Please reply 'retry'" not in result["messages"][0].content


def test_onboarding_link_error_surfaces_clear_recovery_message():
    mock_service = _make_service()
    mock_service.get_user_by_identity.return_value = None
    mock_service.link_identity_by_token.side_effect = ExpiredLinkTokenError()

    with patch("graph.nodes.onboarding_node.AsyncIdentityService", return_value=mock_service):
        state = _make_state(user_id="user-link", message="link ABCD1234")
        result = asyncio.run(onboarding_node(state))

    assert result["route_decision"] == "end"
    assert "expired" in result["messages"][0].content.lower()
    assert "request a new code" in result["messages"][0].content.lower()


def test_onboarding_link_success_wallet_refresh_failure_still_links():
    linked_user = {
        "volo_user_id": "volo-link",
        "sub_org_id": "sub-link",
        "sender_address": "0xlink",
        "evm_sub_org_id": "sub-link",
        "evm_address": "0xlink",
        "solana_sub_org_id": None,
        "solana_address": None,
    }
    mock_service = _make_service()
    mock_service.get_user_by_identity.return_value = None
    mock_service.link_identity_by_token.return_value = linked_user
    mock_service.ensure_multi_chain_wallets.side_effect = RuntimeError("solana timeout")

    with patch("graph.nodes.onboarding_node.AsyncIdentityService", return_value=mock_service):
        state = _make_state(user_id="user-link", message="link ABCD1234")
        result = asyncio.run(onboarding_node(state))

    assert result.get("route_decision") is None
    assert "accounts linked" in result["messages"][0].content.lower()
    assert "couldn't refresh all wallet networks" in result["messages"][0].content.lower()
