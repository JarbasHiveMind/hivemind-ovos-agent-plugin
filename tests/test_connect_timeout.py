"""The auto-connect branch must fail fast, not hang, when the OVOS bus is down."""
import socket
import time
from unittest.mock import MagicMock

import pytest

import hivemind_ovos_agent_plugin
from hivemind_ovos_agent_plugin import OVOSAgentProtocol


def _closed_port() -> int:
    """Return a port with nothing listening on it."""
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def test_raises_when_messagebus_unreachable():
    port = _closed_port()
    start = time.monotonic()
    with pytest.raises(ConnectionError) as exc:
        # no bus passed -> __post_init__ takes the auto-connect branch and tries
        # to reach a messagebus that isn't there
        OVOSAgentProtocol(config={"host": "127.0.0.1", "port": port,
                                  "connection_timeout": 1})
    elapsed = time.monotonic() - start
    # fails fast (does not hang) and the message is actionable
    assert elapsed < 10, f"took {elapsed:.1f}s — should fail near the 1s timeout"
    assert "messagebus" in str(exc.value).lower()
    assert str(port) in str(exc.value)


def test_invalid_connection_timeout_uses_default(monkeypatch):
    bus = MagicMock()
    bus.connected_event.wait.return_value = False
    bus_factory = MagicMock(return_value=bus)
    monkeypatch.setattr(
        hivemind_ovos_agent_plugin,
        "MessageBusClient",
        bus_factory,
    )
    agent = OVOSAgentProtocol.__new__(OVOSAgentProtocol)
    agent.config = {"connection_timeout": "not-a-number"}

    with pytest.raises(ConnectionError, match="within 10.0s"):
        agent._connect_messagebus("127.0.0.1", 8181)

    bus.connected_event.wait.assert_called_once_with(10.0)
    bus.close.assert_called_once_with()
