"""The auto-connect branch must degrade, not hang and not abort, when the OVOS
bus is down at startup."""
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


@pytest.fixture
def unreachable_agent():
    port = _closed_port()
    start = time.monotonic()
    # no bus passed -> __post_init__ takes the auto-connect branch and tries
    # to reach a messagebus that isn't there
    agent = OVOSAgentProtocol(config={"host": "127.0.0.1", "port": port,
                                      "connection_timeout": 1})
    agent._startup_seconds = time.monotonic() - start
    yield agent
    agent.bus.close()


def test_startup_degrades_instead_of_aborting(unreachable_agent):
    """The node must come up and keep serving clients even with no OVOS bus."""
    assert unreachable_agent.bus is unreachable_agent._owned_bus
    assert unreachable_agent._startup_seconds < 10


def test_get_bus_reports_the_bus_as_unavailable(unreachable_agent):
    start = time.monotonic()
    with pytest.raises(ConnectionError):
        unreachable_agent.get_bus()
    elapsed = time.monotonic() - start
    assert elapsed < 0.5, f"get_bus blocked for {elapsed:.2f}s"


def test_get_bus_serves_the_bus_once_it_connects(unreachable_agent):
    unreachable_agent.bus.connected_event.set()
    assert unreachable_agent.get_bus() is unreachable_agent.bus
