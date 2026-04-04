import pytest

from core.tools.schemas import TransferArgs


def test_transfer_args_schema_exposes_neutral_transfer_fields():
    schema = TransferArgs.model_json_schema()
    properties = schema["properties"]

    assert "asset_symbol" in properties
    assert "asset_ref" in properties
    assert "network" in properties
    assert "token_symbol" not in properties
    assert "token_address" not in properties
    assert "chain" not in properties


def test_transfer_args_schema_accepts_canonical_transfer_fields():
    args = TransferArgs(
        asset_symbol="USDC",
        asset_ref="0xabc",
        amount=1,
        recipient="0xdef",
        network="base",
        sub_org_id="sub",
        sender="0xsender",
    )

    assert args.asset_symbol == "USDC"
    assert args.asset_ref == "0xabc"
    assert args.network == "base"


def test_transfer_args_schema_accepts_legacy_transfer_aliases():
    args = TransferArgs(
        token_symbol="USDC",
        token_address="0xabc",
        amount=1,
        recipient="0xdef",
        chain="base",
        sub_org_id="sub",
        sender="0xsender",
    )

    assert args.asset_symbol == "USDC"
    assert args.asset_ref == "0xabc"
    assert args.network == "base"


def test_transfer_args_schema_accepts_matching_old_and_new_aliases():
    args = TransferArgs(
        asset_symbol="USDC",
        token_symbol="usdc",
        asset_ref="0xabc",
        token_address="0xAbC",
        amount=1,
        recipient="0xdef",
        network="base",
        chain="BASE",
        sub_org_id="sub",
        sender="0xsender",
    )

    assert args.asset_symbol == "USDC"
    assert args.asset_ref == "0xabc"
    assert args.network == "base"


@pytest.mark.parametrize(
    "payload, expected_message",
    [
        (
            {
                "asset_symbol": "ETH",
                "token_symbol": "USDC",
                "amount": 1,
                "recipient": "0xdef",
                "network": "base",
                "sub_org_id": "sub",
                "sender": "0xsender",
            },
            "Conflicting transfer asset symbol inputs",
        ),
        (
            {
                "asset_symbol": "USDC",
                "asset_ref": "0x111",
                "token_address": "0x222",
                "amount": 1,
                "recipient": "0xdef",
                "network": "base",
                "sub_org_id": "sub",
                "sender": "0xsender",
            },
            "Conflicting transfer asset inputs",
        ),
        (
            {
                "asset_symbol": "USDC",
                "amount": 1,
                "recipient": "0xdef",
                "network": "base",
                "chain": "solana",
                "sub_org_id": "sub",
                "sender": "0xsender",
            },
            "Conflicting transfer network inputs",
        ),
    ],
)
def test_transfer_args_schema_fails_closed_on_conflicting_alias_inputs(
    payload, expected_message
):
    with pytest.raises(ValueError, match=expected_message):
        TransferArgs(**payload)
