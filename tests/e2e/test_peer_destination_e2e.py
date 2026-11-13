"""Two satellites, one hive: neither can reach the other's connection.

Proves that one satellite cannot make the hive deliver a message to another
satellite's connection. Two separate rules cover two different writers:

1. THE CLIENT PATH IS CLOSED UPSTREAM. A satellite cannot put another peer's id
   in `destination`, because hivemind-core STAMPS `destination` with its own
   agent label on injection. A probe on the agent bus shows the arriving value
   is the label, never the peer id the client wrote. So a satellite-driven cell
   cannot discriminate here at all: it passes whatever this repository's relay
   does. The first cell records that fact, and is deliberately not dressed up as
   a test of this repository.

2. THE IN-BUS PATH IS THIS REPOSITORY'S. The threat the relay rule answers is a transformer, a skill or any other bus writer putting a peer
   id in `destination` on a message whose `source` names a CONNECTED peer. That
   is a request travelling from a peer, and a request is never delivered to a
   peer. Core cannot police this, because it happens after injection.

A request is never delivered to any peer, including its own sender: the message
carries a connected peer in `source`, so the relay declines it rather than
denying the send, and the victim hearing NOTHING is the assertion.
`test_a_peer_can_not_address_even_itself` pins the self-addressing case.

Every negative cell here was checked against a mutation that disables the relay
rule: the three that test it red, and the other two stay green.
"""
from __future__ import annotations

import time

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from hivescope.topology import TopologyBuilder

from hivemind_bus_client.message import HiveMessage, HiveMessageType
from hivemind_plugin_manager.protocols import ClientCallbacks
from ovos_bus_client.message import Message
from ovos_utils.fakebus import FakeBus

from hivemind_ovos_agent_plugin import OVOSAgentProtocol

NAME = "KitchenSatellite"
TYPES = ["recognizer_loop:utterance", "ovos.utterance.speak",
         "mycroft.volume.get.response"]
INJECTED = "the front door is unlocked"


def _make_agent() -> OVOSAgentProtocol:
    """OVOSAgentProtocol on an in-process FakeBus, built the way the other
    e2e in this directory builds it: ``__post_init__`` would demand a real
    messagebus, so the instance is wired by hand and the production handlers
    registered exactly as it would."""
    agent = OVOSAgentProtocol.__new__(OVOSAgentProtocol)
    agent.config = {}
    agent.hm_protocol = None
    agent.callbacks = ClientCallbacks()
    agent.bus = FakeBus()
    agent._owned_bus = None
    agent.register_bus_handlers()
    return agent


def _twins(agent: OVOSAgentProtocol) -> "TopologyBuilder":
    """Two satellites that announce the SAME name, so their peer ids collide
    and the master has to disambiguate them. That collision is the point: it is
    what makes one peer able to name the other's connection at all."""
    from hivescope.topology import TopologyBuilder

    b = TopologyBuilder()
    m = b.add_master("M0", agent_protocol=agent)
    m.register_satellite("ovos-key", password="ovos-pw", allowed_types=TYPES)
    for node in ("A", "B"):
        b.add_satellite(node, upstream=m, allowed_types=TYPES)
        b.get_satellite(node).identity.name = NAME
    return b


def _bus_send(satellite, msg_type, data, destination):
    satellite.send(HiveMessage(
        HiveMessageType.BUS,
        payload=Message(msg_type, data, {"destination": destination})))


def _heard_nothing(satellite, timeout: float = 2.0) -> bool:
    """No BUS frame arrived inbound within the window.

    A negative result needs a real wait: asserting an empty list straight after
    a send would pass even if delivery were merely slow, which is the failure
    mode that makes a cell like this worthless.
    """
    return satellite.recorder.wait_for(HiveMessageType.BUS.value,
                                       direction="in",
                                       timeout=timeout) is None


def _wait_until(condition, timeout: float = 5.0, interval: float = 0.02) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(interval)
    return False


def test_a_satellite_can_not_write_its_own_destination():
    """THE ORIGINAL SCENARIO, measured where it is actually settled.

    #380 worried that a satellite could put another satellite's peer id in
    `destination`. It cannot, and the reason is upstream of this repository:
    hivemind-core STAMPS `destination` with its own agent label on injection
    (`_agent_label`), so whatever the client wrote never reaches the agent bus.
    This cell records that, because a reader of #380 needs to know the client
    path is closed before the cells below make sense.
    """
    agent = _make_agent()
    seen = []
    agent.bus.on("ovos.utterance.speak",
                 lambda m: seen.append(m.context.get("destination")))
    b = _twins(agent)
    b.start_all()
    try:
        a, s = b.get_satellite("A"), b.get_satellite("B")
        assert a.peer != s.peer, "the two satellites share one peer id"

        _bus_send(a, "ovos.utterance.speak", {"utterance": INJECTED}, s.peer)

        assert _wait_until(lambda: seen), "nothing reached the agent bus"
        assert s.peer not in (seen[0] or []), (
            f"the client-written peer id survived injection: {seen[0]!r}")
    finally:
        b.stop_all()


def test_a_bus_writer_can_not_address_a_peers_connection():
    """THE CELL THIS REPOSITORY OWES, and the one #73 exists for.

    The threat the merged rule answers is NOT the client - core closed that.
    It is anything INSIDE the bus (a transformer, a skill, any bus writer)
    putting a peer id in `destination` on a message that carries a connected
    peer in `source`. That is a request travelling from a peer, and a request
    reaches no peer.
    """
    agent = _make_agent()
    b = _twins(agent)
    b.start_all()
    try:
        a, s = b.get_satellite("A"), b.get_satellite("B")

        agent.bus.emit(Message("ovos.utterance.speak", {"utterance": INJECTED},
                               {"destination": s.peer, "source": a.peer}))

        assert _heard_nothing(s), (
            "a request carrying one peer in source was delivered to another "
            "peer named in destination")
    finally:
        b.stop_all()


def test_a_list_destination_can_not_hide_a_peer():
    """Carried from the original: the victim buried in a list must not slip
    past the same rule."""
    agent = _make_agent()
    b = _twins(agent)
    b.start_all()
    try:
        a, s = b.get_satellite("A"), b.get_satellite("B")

        agent.bus.emit(Message("mycroft.volume.get.response", {"percent": 1.0},
                               {"destination": ["skills", a.peer, s.peer],
                                "source": a.peer}))

        assert _heard_nothing(s), (
            "a peer id hidden inside a destination list reached the peer")
    finally:
        b.stop_all()


def test_a_peer_can_not_address_even_itself():
    """REPLACES the original's self-addressing assertion, which expected the
    message to come back. Under the merged rule the sender is a connected peer,
    so the message is a request, and a request reaches no peer at all -
    including the one named in source."""
    agent = _make_agent()
    b = _twins(agent)
    b.start_all()
    try:
        a = b.get_satellite("A")

        agent.bus.emit(Message("mycroft.volume.get.response", {"percent": 0.4},
                               {"destination": a.peer, "source": a.peer}))

        assert _heard_nothing(a), (
            "a request addressed to its own sender was delivered; a request "
            "reaches no peer, including the one in source")
    finally:
        b.stop_all()


def test_an_agent_reply_still_reaches_the_satellite_that_asked():
    """THE CONTROL, and the reason the cells above mean anything. If the relay
    delivered nothing to anybody, every negative assertion would pass while the
    hive was broken. A real agent answer, whose source is NOT a peer, must still
    reach the satellite it names and only that one."""
    agent = _make_agent()
    b = _twins(agent)
    b.start_all()
    try:
        a, s = b.get_satellite("A"), b.get_satellite("B")

        agent.bus.emit(Message("ovos.utterance.speak",
                               {"utterance": "it is noon"},
                               {"destination": a.peer, "source": "skills"}))

        assert a.recorder.wait_for(HiveMessageType.BUS.value, direction="in",
                                   timeout=8.0) is not None, \
            "the agent answer never reached the satellite it was addressed to"
        assert _heard_nothing(s), "the agent answer also reached the other peer"
    finally:
        b.stop_all()
