from core.event_notifier_runtime import run_notifier


class _Client:
    def __init__(self) -> None:
        self.acked: list[tuple[str, str, str]] = []
        self.created = False

    def xgroup_create(self, stream, group, id="0-0", mkstream=True):
        self.created = True

    def xrange(self, *_args, **_kwargs):
        return []

    def xreadgroup(self, group, consumer, streams, count):
        return [
            (
                "volo-events",
                [
                    (
                        "1-0",
                        {
                            "event": "node_progress",
                            "stage": "sending",
                            "thread_id": "thread-1",
                            "node_id": "step_0",
                            "tool": "swap",
                        },
                    )
                ],
            )
        ]

    def xack(self, stream, group, msg_id):
        self.acked.append((stream, group, msg_id))


def test_run_notifier_consumes_and_acks_one_batch():
    client = _Client()
    lines: list[str] = []
    errors: list[str] = []

    result = run_notifier(
        client=client,
        stream="volo-events",
        group="notify",
        consumer="consumer-1",
        block_ms=1,
        count=10,
        once=True,
        tail=0,
        raw=False,
        stdout_write=lines.append,
        stderr_write=errors.append,
    )

    assert result == 0
    assert client.created is True
    assert lines == ["Sending transaction..."]
    assert errors == []
    assert client.acked == [("volo-events", "notify", "1-0")]
