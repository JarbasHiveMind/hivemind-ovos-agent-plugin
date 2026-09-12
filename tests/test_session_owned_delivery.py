"""Session-ownership delivery in handle_internal_mycroft.

A hub bus message whose session belongs to a connected client must reach that
client even when the destination does not name the client's CURRENT peer id --
peer ids are per-message NAT-assigned and do not survive a satellite reconnect,
so a satellite-scheduled event replays the client's session but a stale/absent
peer id. The session (namespaced as ``f"{session_namespace}:{declared}"``,
where session_namespace is durable and survives a reconnect) is the durable
route. Delivery on this inferred path trusts the session owner: the master
sends whatever the OVOS bus addressed to the satellites it serves, and
``allowed_types`` gates only what a satellite sends upward, so nothing here
reads it.
"""

from types import SimpleNamespace

from ovos_bus_client.message import Message

import hivemind_ovos_agent_plugin as hmoap  # noqa: F401


def _client(make_client, peer, namespace, allowed_types):
    client = make_client(peer)
    client.session_namespace = namespace
    client.allowed_types = allowed_types
    client.sess = SimpleNamespace(session_id=None)
    return client


def _msg(msg_type, destination, session_id):
    return Message(
        msg_type,
        {},
        {"destination": destination, "session": {"session_id": session_id}},
    ).serialize()


def test_session_owned_delivery_non_peer_destination(agent, make_client):
    """(a) fail-before: destination ["skills"] but A owns the session."""
    agent.hm_protocol.db = None
    alice = _client(make_client, "ws://alice", "NONCE", ["speak"])
    agent.hm_protocol.clients = {"ws://alice": alice}

    agent.handle_internal_mycroft(_msg("speak", ["skills"], "NONCE:s1"))

    alice.send.assert_called_once()
    sent = alice.send.call_args[0][0]
    assert sent.payload.context["session"]["session_id"] == "s1"


def test_no_utterance_type_not_allowed_still_delivered(agent, make_client):
    """(b') a client allowed only recognizer_loop:utterance still receives a
    speak replaying its session -- implicit trust downward; allowed_types
    never gates master-to-satellite delivery. (fail-before for removing the
    downward gate: the removed gate dropped this speak in silence.)"""
    agent.hm_protocol.db = None
    alice = _client(make_client, "ws://alice", "NONCE",
                    ["recognizer_loop:utterance"])
    agent.hm_protocol.clients = {"ws://alice": alice}

    agent.handle_internal_mycroft(_msg("speak", ["skills"], "NONCE:s1"))

    alice.send.assert_called_once()
    sent = alice.send.call_args[0][0]
    assert sent.payload.context["session"]["session_id"] == "s1"


def test_hive_utterance_handled_reaches_minimal_client(agent, make_client):
    """(b'') the post-intent replies a present satellite needs
    (ovos.utterance.handled, ovos.intent.unmatched under canonical spellings)
    reach a client allowing only the legacy utterance topic; there is no
    receive-side deny path left for master-originated traffic."""
    agent.hm_protocol.db = None
    alice = _client(make_client, "ws://alice", "NONCE",
                    ["recognizer_loop:utterance"])
    agent.hm_protocol.clients = {"ws://alice": alice}

    for mt in ("ovos.utterance.handled", "ovos.intent.unmatched"):
        alice.send.reset_mock()
        agent.handle_internal_mycroft(_msg(mt, ["skills"], "NONCE:s1"))
        alice.send.assert_called_once()


def test_empty_allowed_still_delivers(agent, make_client):
    """(b) empty allowed_types does not block session-owned delivery."""
    agent.hm_protocol.db = None
    alice = _client(make_client, "ws://alice", "NONCE", [])
    agent.hm_protocol.clients = {"ws://alice": alice}

    agent.handle_internal_mycroft(_msg("speak", ["skills"], "NONCE:s1"))

    alice.send.assert_called_once()
    sent = alice.send.call_args[0][0]
    assert sent.payload.context["session"]["session_id"] == "s1"


def test_double_send_guard(agent, make_client):
    """(c) destination has A's peer AND A owns the session => sent once."""
    agent.hm_protocol.db = None
    alice = _client(make_client, "ws://alice", "NONCE", ["speak"])
    agent.hm_protocol.clients = {"ws://alice": alice}

    agent.handle_internal_mycroft(_msg("speak", ["ws://alice"], "NONCE:s1"))

    alice.send.assert_called_once()


def test_ownership_isolation(agent, make_client):
    """(d) only B connected, B does not own A's session => not forwarded."""
    agent.hm_protocol.db = None
    bob = _client(make_client, "ws://bob", "OTHER", ["speak"])
    agent.hm_protocol.clients = {"ws://bob": bob}

    agent.handle_internal_mycroft(_msg("speak", ["skills"], "NONCE:s1"))

    bob.send.assert_not_called()


def test_peer_id_path_unchanged(agent, make_client):
    """(e) ordinary peer-id destination still delivers via the existing path,
    with no allowed_types check (empty allowed_types still delivers)."""
    agent.hm_protocol.db = None
    alice = _client(make_client, "ws://alice", "NONCE", [])
    agent.hm_protocol.clients = {"ws://alice": alice}

    agent.handle_internal_mycroft(_msg("speak", "ws://alice", "NONCE:s1"))

    alice.send.assert_called_once()
    sent = alice.send.call_args[0][0]
    assert sent.payload.context["session"]["session_id"] == "s1"


def test_no_session_dict_is_safe(agent, make_client):
    """(e) a message with no session dict does not crash or session-forward."""
    agent.hm_protocol.db = None
    alice = _client(make_client, "ws://alice", "NONCE", ["speak"])
    agent.hm_protocol.clients = {"ws://alice": alice}

    msg = Message("speak", {}, {"destination": ["skills"]})
    agent.handle_internal_mycroft(msg.serialize())

    alice.send.assert_not_called()


def test_delivery_survives_reconnect_durable_namespace(agent, make_client):
    """(f) reconnect durability: a session minted before reconnect (keyed on
    the durable session_namespace) still routes after the connection's
    conn_nonce changes. The client's conn_nonce is deliberately DIFFERENT from
    its session_namespace here, proving ownership no longer keys on conn_nonce.
    """
    agent.hm_protocol.db = None
    # session was minted under namespace "NS" before the reconnect
    alice = _client(make_client, "ws://alice-reconnected", "NS", ["speak"])
    # conn_nonce rotated on reconnect; it must NOT be what ownership keys on
    alice.conn_nonce = "FRESH-NONCE-AFTER-RECONNECT"
    agent.hm_protocol.clients = {"ws://alice-reconnected": alice}

    agent.handle_internal_mycroft(_msg("speak", ["skills"], "NS:s1"))

    alice.send.assert_called_once()
    sent = alice.send.call_args[0][0]
    assert sent.payload.context["session"]["session_id"] == "s1"


def test_canonical_frame_reaches_legacy_allowed_client(agent, make_client):
    """(twin) the CANONICAL frame ovos.utterance.speak that actually arrives
    on the firehose reaches a satellite provisioned with the LEGACY spelling
    "speak". (Under the removed gate this passed only because the gate
    matched twins; now no gate stands and delivery is unconditional.)"""
    agent.hm_protocol.db = None
    alice = _client(make_client, "ws://alice", "NS", ["speak"])
    agent.hm_protocol.clients = {"ws://alice": alice}

    agent.handle_internal_mycroft(_msg("ovos.utterance.speak", ["skills"], "NS:s1"))

    alice.send.assert_called_once()


def test_type_with_no_twin_still_delivered(agent, make_client):
    """(twin) a type with no relation to allowed_types is still delivered:
    downward delivery reads only session ownership."""
    agent.hm_protocol.db = None
    alice = _client(make_client, "ws://alice", "NS", ["speak"])
    agent.hm_protocol.clients = {"ws://alice": alice}

    agent.handle_internal_mycroft(_msg("ovos.mic.listen", ["skills"], "NS:s1"))

    alice.send.assert_called_once()


def _msg_ctx(msg_type, ctx):
    return Message(msg_type, {}, ctx).serialize()


def test_peer_context_routes_when_no_destination_no_session(agent, make_client):
    """(peer) case B from utterance-routing-regression.md: a skill that emits
    speak echoing only the hub-stamped context (peer names the live peer id)
    and no destination and no session reaches the client the hub stamped.
    Fail-before: the return path reads destination then session-ownership and
    never context.peer, so this speak is dropped in silence."""
    agent.hm_protocol.db = None
    alice = _client(make_client, "HiveMessageBusClientV0.0.1::17::satellite_1::00000000-0000-0000-0000-000000000000",
                    "NS", [])
    agent.hm_protocol.clients = {alice.peer: alice}

    agent.handle_internal_mycroft(_msg_ctx("speak", {
        "source": "fake_skill.test", "peer": alice.peer,
    }))

    alice.send.assert_called_once()
    sent = alice.send.call_args[0][0]
    assert sent.payload.context["peer"] == alice.peer


def test_stale_peer_context_is_inert(agent, make_client):
    """(peer) case A from utterance-routing-regression.md: a stale peer uuid
    in context.peer must NOT deliver to anyone."""
    agent.hm_protocol.db = None
    alice = _client(make_client, "HiveMessageBusClientV0.0.1::17::satellite_1::11111111-1111-1111-1111-111111111111",
                    "NS", [])
    agent.hm_protocol.clients = {alice.peer: alice}

    agent.handle_internal_mycroft(_msg_ctx("speak", {
        "source": "fake_skill.test",
        "peer": "HiveMessageBusClientV0.0.1::17::satellite_1::00000000-0000-0000-0000-000000000000",
        "session": {"session_id": "deadbeef:00000000-0000-0000-0000-000000000000"},
    }))

    alice.send.assert_not_called()


def test_peer_context_does_not_cross_clients(agent, make_client):
    """(peer) a peer-id that names nobody connected delivers to nobody."""
    agent.hm_protocol.db = None
    alice = _client(make_client, "ws://alice-here", "NS", [])
    agent.hm_protocol.clients = {alice.peer: alice}

    agent.handle_internal_mycroft(_msg_ctx("speak", {
        "source": "fake_skill.test", "peer": "ws://gone",
    }))

    alice.send.assert_not_called()
