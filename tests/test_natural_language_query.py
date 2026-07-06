"""Tests for OVOSAgentProtocol natural language query backpressure."""

import threading
from unittest.mock import MagicMock

from hivemind_ovos_agent_plugin import OVOSAgentProtocol
from ovos_bus_client.message import Message


class _QueryBus:
    def __init__(self, fail_send=False):
        self.fail_send = fail_send
        self.handlers = {}
        self.closed = False

    def on(self, event, handler):
        self.handlers.setdefault(event, []).append(handler)

    def remove(self, event, handler):
        if handler in self.handlers.get(event, []):
            self.handlers[event].remove(handler)

    def emit_checked(self, message):
        if self.fail_send:
            raise RuntimeError("bus closed")
        if "query_id" not in message.context:
            return
        done = Message(
            "ovos.utterance.handled",
            {},
            {"query_id": message.context["query_id"]},
        )
        for handler in list(self.handlers.get("ovos.utterance.handled", [])):
            handler(done)

    def close(self):
        self.closed = True


def test_query_returns_none_when_inflight_gate_is_full():
    agent = OVOSAgentProtocol.__new__(OVOSAgentProtocol)
    agent.bus = MagicMock()
    agent.config = {"inflight_timeout": 0}
    agent._inflight_semaphore = threading.BoundedSemaphore(1)
    agent._inflight_semaphore.acquire()

    assert list(agent.natural_language_query("what time is it", "en-us")) == [None]
    agent.bus.emit.assert_not_called()

    agent._inflight_semaphore.release()


def test_query_response_timeout_can_be_configured():
    agent = OVOSAgentProtocol.__new__(OVOSAgentProtocol)
    agent.config = {"response_timeout": 42}

    assert agent._configured_response_timeout() == 42


def test_query_response_timeout_has_compatible_aliases():
    agent = OVOSAgentProtocol.__new__(OVOSAgentProtocol)
    agent.config = {"query_response_timeout": 12}

    assert agent._configured_response_timeout() == 12

    agent.config = {"utterance_timeout": 7}
    assert agent._configured_response_timeout() == 7


def test_query_retries_when_selected_bus_closes_on_send():
    first = _QueryBus(fail_send=True)
    second = _QueryBus()
    buses = iter([first, second])
    agent = OVOSAgentProtocol.__new__(OVOSAgentProtocol)
    agent.config = {"response_timeout": 0.1}
    agent._inflight_semaphore = threading.BoundedSemaphore(1)
    agent._inflight_timeout = 0
    agent._client_state_lock = threading.RLock()
    agent._bus_emit_locks = {}
    agent.get_bus = lambda *_: next(buses)

    assert list(agent.natural_language_query("what time is it", "en-us")) == [None]
    assert first.closed is True
    assert second.closed is False


def test_emit_client_message_retries_when_selected_bus_closes_on_send():
    first = _QueryBus(fail_send=True)
    second = _QueryBus()
    buses = iter([first, second])
    agent = OVOSAgentProtocol.__new__(OVOSAgentProtocol)
    agent._client_state_lock = threading.RLock()
    agent._bus_emit_locks = {}
    agent.get_bus = lambda *_: next(buses)

    message = Message("recognizer_loop:utterance")
    assert agent.emit_client_message(message) is True
    assert first.closed is True
    assert second.closed is False
