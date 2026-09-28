"""Tests for query-scoped OVOS bus listeners."""

from hivemind_bus_client.message import Message


class QueryBus:
    """Minimal synchronous bus that injects malformed and valid replies."""

    def __init__(self):
        self.listeners = {}

    def on(self, event, callback):
        """Register a listener."""
        self.listeners[event] = callback

    def remove(self, event, callback):
        """Remove the exact listener registered for an event."""
        assert self.listeners.pop(event) is callback

    def emit(self, request):
        """Deliver malformed payloads before a valid correlated response."""
        for payload in ({}, b"invalid", None):
            self.listeners["speak"](payload)
            self.listeners["ovos.utterance.handled"](payload)

        context = {"query_id": request.context["query_id"]}
        self.listeners["speak"](
            Message("speak", {"utterance": "answer"}, context)
        )
        self.listeners["ovos.utterance.handled"](
            Message("ovos.utterance.handled", {}, context)
        )


def test_query_ignores_non_message_listener_payloads(agent):
    """Malformed third-party payloads do not interrupt a valid query."""
    bus = QueryBus()
    agent.bus = bus
    agent._owned_bus = None

    assert list(agent.natural_language_query("question", "en-US")) == [
        "answer",
        None,
    ]
    assert bus.listeners == {}
