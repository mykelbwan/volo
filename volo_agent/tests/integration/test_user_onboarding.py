from __future__ import annotations

import asyncio
import uuid
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

from langchain_core.messages import HumanMessage

from graph.agent_state import AgentState
from graph.nodes.onboarding_node import onboarding_node


def _make_state(*, user_id: str, message: str | None = None) -> AgentState:
    return {
        "user_id": user_id,
        "provider": "discord",
        "username": None,
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
    service.register_user = AsyncMock()
    service.link_identity_by_token = AsyncMock()
    service.reprovision_wallets = AsyncMock()
    return service


def test_onboarding_flow_prompts_new_user():
    service = _make_service()
    service.get_user_by_identity.return_value = None

    with patch("graph.nodes.onboarding_node.AsyncIdentityService", return_value=service):
        result = asyncio.run(onboarding_node(_make_state(user_id="test_discord_user_99")))

    assert result["route_decision"] == "end"
    assert "link" in result["messages"][0].content.lower()
    assert "create" in result["messages"][0].content.lower()
    service.register_user.assert_not_called()


def test_onboarding_flow_creates_new_user():
    created_user = {
        "volo_user_id": str(uuid.uuid4()),
        "sub_org_id": "mock-sub-org-123",
        "sender_address": "0x1234567890abcdef1234567890abcdef12345678",
        "solana_sub_org_id": "mock-solana-sub-org-123",
        "solana_address": "So1anaMockAddress",
        "is_new_user": True,
    }
    service = _make_service()
    service.get_user_by_identity.return_value = None
    service.register_user.return_value = created_user

    with patch("graph.nodes.onboarding_node.AsyncIdentityService", return_value=service):
        result = asyncio.run(
            onboarding_node(_make_state(user_id="test_discord_user_99", message="create"))
        )

    service.register_user.assert_awaited_once_with(
        "discord", "test_discord_user_99", username=None
    )
    assert result["user_info"]["is_new_user"] is True
    assert result["artifacts"]["sender_address"] == created_user["sender_address"]
    assert "Welcome!" in result["messages"][0].content
    assert "Ethereum" in result["messages"][0].content


def test_onboarding_flow_reuses_existing_user():
    user_id = "test_discord_user_99"
    existing_record = {
        "volo_user_id": str(uuid.uuid4()),
        "identities": [
            {
                "provider": "discord",
                "provider_user_id": user_id,
                "username": None,
                "username_history": [],
                "linked_at": datetime.utcnow(),
            }
        ],
        "sub_org_id": "mock-sub-org-123",
        "sender_address": "0x1234567890abcdef1234567890abcdef12345678",
        "solana_sub_org_id": "mock-solana-sub-org-123",
        "solana_address": "So1anaMockAddress",
        "is_active": True,
    }
    service = _make_service()
    service.get_user_by_identity.return_value = existing_record
    service.ensure_multi_chain_wallets.return_value = existing_record

    with patch("graph.nodes.onboarding_node.AsyncIdentityService", return_value=service):
        result = asyncio.run(onboarding_node(_make_state(user_id=user_id)))

    service.sync_username.assert_awaited_once_with(
        existing_record, "discord", user_id, None
    )
    assert result["user_info"]["volo_user_id"] == existing_record["volo_user_id"]
    assert result.get("messages") is None
    assert result["artifacts"]["sub_org_id"] == existing_record["sub_org_id"]


def test_onboarding_link_token_flow():
    linked_user = {
        "volo_user_id": "volo-1",
        "sub_org_id": "sub-1",
        "sender_address": "0xabc",
        "solana_sub_org_id": "sol-sub-1",
        "solana_address": "So1anaLinkedAddress",
        "identities": [],
    }
    service = _make_service()
    service.get_user_by_identity.return_value = None
    service.link_identity_by_token.return_value = linked_user
    service.ensure_multi_chain_wallets.return_value = linked_user

    with patch("graph.nodes.onboarding_node.AsyncIdentityService", return_value=service):
        result = asyncio.run(onboarding_node(_make_state(user_id="new-user", message="link ABCD1234")))

    service.link_identity_by_token.assert_awaited_once_with(
        "ABCD1234", "discord", "new-user", username=None
    )
    assert result["artifacts"]["sender_address"] == "0xabc"
    assert "Accounts linked" in result["messages"][0].content
