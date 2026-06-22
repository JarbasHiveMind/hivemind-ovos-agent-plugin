"""The auto-connect branch must fail fast, not hang, when the OVOS bus is down."""
import socket
import time

import pytest

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
