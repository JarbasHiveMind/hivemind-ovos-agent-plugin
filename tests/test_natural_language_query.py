"""Tests for OVOSAgentProtocol natural language query backpressure."""

import threading
from unittest.mock import MagicMock

from hivemind_ovos_agent_plugin import OVOSAgentProtocol


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
