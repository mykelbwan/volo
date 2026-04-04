from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock


def modified_result(count: int = 1) -> SimpleNamespace:
    return SimpleNamespace(modified_count=count)


def make_identity_db_mocks():
    mock_db = MagicMock()

    users = MagicMock()
    users.create_indexes = AsyncMock()
    users.find_one = AsyncMock()
    users.insert_one = AsyncMock()
    users.update_one = AsyncMock()

    link_tokens = MagicMock()
    link_tokens.create_indexes = AsyncMock()
    link_tokens.find_one = AsyncMock()
    link_tokens.find_one_and_update = AsyncMock()
    link_tokens.insert_one = AsyncMock()
    link_tokens.update_many = AsyncMock()

    collections = {
        "users": users,
        "link_tokens": link_tokens,
    }
    mock_db.__getitem__.side_effect = collections.__getitem__
    return mock_db, users, link_tokens
