from core.utils.tx_links import explorer_tx_url


def test_explorer_tx_url_appends_transaction_path_for_plain_base_url():
    assert (
        explorer_tx_url("https://etherscan.io", "0xabc")
        == "https://etherscan.io/tx/0xabc"
    )


def test_explorer_tx_url_preserves_existing_query_parameters():
    assert (
        explorer_tx_url("https://solscan.io/?cluster=devnet", "solsig")
        == "https://solscan.io/tx/solsig?cluster=devnet"
    )
