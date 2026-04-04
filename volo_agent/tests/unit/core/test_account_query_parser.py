from core.conversation.account_query_parser import parse_account_query


def test_parse_account_query_recognizes_evm_address_short_form():
    query = parse_account_query("evm address")

    assert query is not None
    assert query.kind == "wallet_address"
    assert query.chain_family == "evm"
    assert query.chain is None


def test_parse_account_query_tolerates_solana_address_typo():
    query = parse_account_query("show my sol addess")

    assert query is not None
    assert query.kind == "wallet_address"
    assert query.chain_family == "solana"
    assert query.chain == "solana"


def test_parse_account_query_tolerates_balance_typo_and_chain_alias():
    query = parse_account_query("etherum balnce")

    assert query is not None
    assert query.kind == "balance"
    assert query.chain_family == "evm"
    assert query.chain == "ethereum"


def test_parse_account_query_supports_generic_balance_phrase():
    query = parse_account_query("how much do i have")

    assert query is not None
    assert query.kind == "balance"
    assert query.chain is None


def test_parse_account_query_does_not_convert_bridge_swap_request_to_balance():
    query = parse_account_query(
        "bridge usdc from base sepolia to sepolia and swap it for eth"
    )

    assert query is None


def test_parse_account_query_does_not_convert_unwrap_request_to_balance():
    query = parse_account_query("unwrap eth on base")

    assert query is None
