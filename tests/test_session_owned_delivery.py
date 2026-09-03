"""Session-ownership delivery in handle_internal_mycroft.

A hub bus message whose session belongs to a connected client must reach that
client even when the destination does not name the client's CURRENT peer id --
peer ids are per-message NAT-assigned and do not survive a satellite reconnect,
so a satellite-scheduled event replays the client's session but a stale/absent
peer id. The session (namespaced as ``f"{session_namespace}:{declared}"``,
where session_namespace is durable and survives a reconnect) is the durable
route. This inferred path is deny-by-default gated on the client's declared
``allowed_types`` contract (twin-aware for legacy/canonical spellings), unlike
the explicit peer-id path.
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


def test_deny_by_default_type_not_allowed(agent, make_client):
    """(b) msg_type not in allowed_types => not forwarded."""
    agent.hm_protocol.db = None
    alice = _client(make_client, "ws://alice", "NONCE", ["speak"])
    agent.hm_protocol.clients = {"ws://alice": alice}

    agent.handle_internal_mycroft(_msg("recognizer_loop:utterance", ["skills"], "NONCE:s1"))

    alice.send.assert_not_called()


def test_deny_by_default_empty_allowed(agent, make_client):
    """(b) empty allowed_types => not forwarded."""
    agent.hm_protocol.db = None
    alice = _client(make_client, "ws://alice", "NONCE", [])
    agent.hm_protocol.clients = {"ws://alice": alice}

    agent.handle_internal_mycroft(_msg("speak", ["skills"], "NONCE:s1"))

    alice.send.assert_not_called()


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


def test_twin_aware_legacy_allowed_admits_canonical_frame(agent, make_client):
    """(twin) a satellite provisioned with the LEGACY spelling "speak" in
    allowed_types receives the CANONICAL frame ovos.utterance.speak that
    actually arrives on the firehose."""
    agent.hm_protocol.db = None
    alice = _client(make_client, "ws://alice", "NS", ["speak"])
    agent.hm_protocol.clients = {"ws://alice": alice}

    agent.handle_internal_mycroft(_msg("ovos.utterance.speak", ["skills"], "NS:s1"))

    alice.send.assert_called_once()


def test_twin_aware_genuinely_not_allowed_still_denied(agent, make_client):
    """(twin) a type with no twin in allowed_types is still denied."""
    agent.hm_protocol.db = None
    alice = _client(make_client, "ws://alice", "NS", ["speak"])
    agent.hm_protocol.clients = {"ws://alice": alice}

    agent.handle_internal_mycroft(_msg("ovos.mic.listen", ["skills"], "NS:s1"))

    alice.send.assert_not_called()
