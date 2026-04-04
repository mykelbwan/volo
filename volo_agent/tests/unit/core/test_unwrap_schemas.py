from core.tools.schemas import UnwrapArgs


def test_unwrap_args_schema_requires_chain_and_addresses():
    args = UnwrapArgs(
        token_symbol="ETH",
        token_address="0x4200000000000000000000000000000000000006",
        chain="base sepolia",
        sub_org_id="sub",
        sender="0xabc",
    )

    assert args.token_symbol == "ETH"
    assert args.chain == "base sepolia"
    assert args.amount is None
