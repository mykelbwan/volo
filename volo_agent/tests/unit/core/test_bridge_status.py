from core.utils.bridge_status import fetch_across_status
from core.utils.http import ExternalServiceError


class _DummyResponse:
    def json(self):
        return {}


def test_fetch_across_status_returns_not_found_on_error():
    def _raise(_response, _service):
        raise ExternalServiceError(
            "across",
            404,
            '{"error":"DepositNotFoundException","message":"Deposit not found given the provided constraints"}',
        )

    def _request(_method, _url, **_kwargs):
        return _DummyResponse()

    # Patch within module scope
    import core.utils.bridge_status as mod

    orig_request = mod.request_json
    orig_raise = mod.raise_for_status
    try:
        mod.request_json = _request
        mod.raise_for_status = _raise
        status = fetch_across_status("0xdead", is_testnet=True)
    finally:
        mod.request_json = orig_request
        mod.raise_for_status = orig_raise

    assert status == "not_found"
