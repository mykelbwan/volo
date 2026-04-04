from core.utils.linking import extract_unlink_target, is_unlink_account_request


def test_is_unlink_account_request_accepts_targeted_provider():
    assert is_unlink_account_request("unlink telegram") is True
    assert is_unlink_account_request("disconnect @alice_tg") is True


def test_extract_unlink_target_returns_provider_or_username_hint():
    assert extract_unlink_target("unlink telegram") == "telegram"
    assert extract_unlink_target("unlink account") is None
    assert extract_unlink_target("disconnect @alice_tg") == "@alice_tg"
