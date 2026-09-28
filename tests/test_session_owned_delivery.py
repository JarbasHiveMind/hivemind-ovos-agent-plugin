"""Return-path delivery in handle_internal_mycroft keys on the destination only.

HIVEMIND-AGENT-1 §3.2 (architecture dev 56f5f9e): "The server **MUST** deliver
a response **only** to the peer or peers named in the response's Layer-1
`destination`. A response that names no peer of this server is not delivered to
any peer." §3.3: "The server **MUST NOT** select a different peer to receive
the response by any identity other than the `destination` -- in particular not
by a shared session namespace, a shared credential, or a shared site", and
"**MUST** log the undelivered response at **WARNING**, naming the
`destination` it could not resolve, and the session when the response carries
one."

So neither ``context["peer"]`` nor the session namespace selects a receiver.
``context["peer"]`` only marks a message as a response to a peer, so that an
undelivered one is logged at WARNING; hub traffic addressed to services with no
peer behind it is not.

Delivery trusts the master: ``allowed_types`` gates only what a satellite sends
upward, so nothing on this path reads it.
"""

import logging
from types import SimpleNamespace

from ovos_bus_client.message import Message

import hivemind_ovos_agent_plugin as hmoap  # noqa: F401


def _client(make_client, peer, namespace, allowed_types):
    client = make_client(peer)
    client.session_namespace = namespace
    client.allowed_types = allowed_types
    client.sess = SimpleNamespace(session_id=None)
    return client


def _msg(msg_type, destination, session_id, peer=None):
    context = {"destination": destination,
               "session": {"session_id": session_id}}
    if peer is not None:
        context["peer"] = peer
    return Message(msg_type, {}, context).serialize()


def _warnings(caplog):
    return [r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING]


def test_destination_alice_with_bob_as_context_peer_reaches_alice_only(agent, make_client):
    """Regression (review of #65 at ce151ac): two peers on separate
    credentials. The response names alice; its context carries bob as the
    peer. Only alice may receive it (§3.2 bullets one and two, §3.3)."""
    agent.hm_protocol.db = None
    alice = _client(make_client, "ws://alice", "KA", ["speak"])
    bob = _client(make_client, "ws://bob", "KB", ["speak"])
    agent.hm_protocol.clients = {"ws://alice": alice, "ws://bob": bob}

    agent.handle_internal_mycroft(
        _msg("speak", ["ws://alice"], "KA:s1", peer="ws://bob"))

    alice.send.assert_called_once()
    bob.send.assert_not_called()


def test_service_destination_is_not_delivered_to_the_context_peer(agent, make_client, caplog):
    """A reply whose destination names no peer ("skills") is delivered to no
    peer, even when its context peer is connected, and the drop is logged at
    WARNING naming the destination and the session (§3.2, §3.3)."""
    agent.hm_protocol.db = None
    alice = _client(make_client, "ws://alice", "NS", ["speak"])
    agent.hm_protocol.clients = {"ws://alice": alice}

    with caplog.at_level(logging.WARNING):
        agent.handle_internal_mycroft(
            _msg("speak", ["skills"], "NS:s1", peer="ws://alice"))

    alice.send.assert_not_called()
    assert any("skills" in m and "NS:s1" in m for m in _warnings(caplog)), _warnings(caplog)


def test_gone_origin_warning_names_the_destination_and_session(agent, make_client, caplog):
    """The origin connection is gone and the destination names no peer. No
    one receives it, a live client on the same namespace is not substituted,
    and the WARNING names the destination (not only the origin) and the
    session (§3.3)."""
    agent.hm_protocol.db = None
    bob = _client(make_client, "ws://bob", "SITEKEY", ["speak"])
    agent.hm_protocol.clients = {"ws://bob": bob}

    with caplog.at_level(logging.WARNING):
        agent.handle_internal_mycroft(
            _msg("speak", ["skills"], "SITEKEY:s1", peer="ws://alice-gone"))

    bob.send.assert_not_called()
    assert any("skills" in m and "SITEKEY:s1" in m for m in _warnings(caplog)), _warnings(caplog)


def test_gone_destination_peer_warning_names_it_and_the_session(agent, make_client, caplog):
    """A destination that names a peer id with no connection: nobody receives
    it, and the WARNING names that destination and the session (§3.3)."""
    agent.hm_protocol.db = None
    bob = _client(make_client, "ws://bob::1", "SITEKEY", ["speak"])
    agent.hm_protocol.clients = {"ws://bob::1": bob}

    with caplog.at_level(logging.WARNING):
        agent.handle_internal_mycroft(
            _msg("speak", ["ws://alice::gone"], "SITEKEY:s1"))

    bob.send.assert_not_called()
    assert any("ws://alice::gone" in m and "SITEKEY:s1" in m
               for m in _warnings(caplog)), _warnings(caplog)


def test_service_traffic_with_no_peer_is_not_warned(agent, make_client, caplog):
    """Hub traffic addressed to a service with no peer behind it is ordinary
    OVOS routing, not an undelivered response: nothing is sent, nothing is
    logged at WARNING."""
    agent.hm_protocol.db = None
    alice = _client(make_client, "ws://alice", "NS", ["speak"])
    agent.hm_protocol.clients = {"ws://alice": alice}

    with caplog.at_level(logging.WARNING):
        agent.handle_internal_mycroft(Message("speak", {}, {"destination": ["skills"]}).serialize())

    alice.send.assert_not_called()
    assert not _warnings(caplog), _warnings(caplog)


def test_the_request_itself_is_not_warned_as_an_undelivered_response(agent, make_client, caplog):
    """hivemind-core forwards a satellite's request with source == peer. On
    this bus it names a service ("skills") and reaches no peer, but it is the
    request, not a response: it must not log the §3.3 WARNING, or every
    utterance would."""
    agent.hm_protocol.db = None
    alice = _client(make_client, "ws://alice", "NS", ["speak"])
    agent.hm_protocol.clients = {"ws://alice": alice}
    request = Message("recognizer_loop:utterance", {"utterances": ["hi"]},
                      {"destination": ["skills"], "source": "ws://alice",
                       "peer": "ws://alice", "session": {"session_id": "NS:s1"}})

    with caplog.at_level(logging.WARNING):
        agent.handle_internal_mycroft(request.serialize())

    alice.send.assert_not_called()
    assert not _warnings(caplog), _warnings(caplog)


def test_twin_on_one_key_receives_only_its_own_destination(agent, make_client):
    """Two satellites share one access key and one session namespace. A
    response addressed to bob reaches bob alone (§3.3: a shared credential
    does not select a receiver)."""
    agent.hm_protocol.db = None
    alice = _client(make_client, "ws://alice", "SITEKEY", ["speak"])
    bob = _client(make_client, "ws://bob", "SITEKEY", ["speak"])
    agent.hm_protocol.clients = {"ws://alice": alice, "ws://bob": bob}

    agent.handle_internal_mycroft(_msg("speak", ["ws://bob"], "SITEKEY:s1", peer="ws://bob"))

    bob.send.assert_called_once()
    alice.send.assert_not_called()


def test_namespace_alone_does_not_deliver(agent, make_client):
    """No peer in the destination and no context peer: the namespace does not
    stand in for one."""
    agent.hm_protocol.db = None
    alice = _client(make_client, "ws://alice", "NONCE", ["speak"])
    agent.hm_protocol.clients = {"ws://alice": alice}

    agent.handle_internal_mycroft(_msg("speak", ["skills"], "NONCE:s1"))

    alice.send.assert_not_called()


def test_destination_and_context_peer_the_same_is_sent_once(agent, make_client):
    """The destination names alice and alice is the context peer: one send."""
    agent.hm_protocol.db = None
    alice = _client(make_client, "ws://alice", "NONCE", ["speak"])
    agent.hm_protocol.clients = {"ws://alice": alice}

    agent.handle_internal_mycroft(_msg("speak", ["ws://alice"], "NONCE:s1", peer="ws://alice"))

    alice.send.assert_called_once()


def test_peer_id_path_unchanged(agent, make_client):
    """A string peer-id destination still delivers, with no allowed_types check."""
    agent.hm_protocol.db = None
    alice = _client(make_client, "ws://alice", "NONCE", [])
    agent.hm_protocol.clients = {"ws://alice": alice}

    agent.handle_internal_mycroft(_msg("speak", "ws://alice", "NONCE:s1"))

    alice.send.assert_called_once()
    sent = alice.send.call_args[0][0]
    assert sent.payload.context["session"]["session_id"] == "s1"


def test_no_session_dict_is_safe(agent, make_client):
    """A message with no session dict and no peer does not crash or forward."""
    agent.hm_protocol.db = None
    alice = _client(make_client, "ws://alice", "NONCE", ["speak"])
    agent.hm_protocol.clients = {"ws://alice": alice}

    agent.handle_internal_mycroft(Message("speak", {}, {"destination": ["skills"]}).serialize())

    alice.send.assert_not_called()


def test_destination_delivery_reverse_nats_the_session(agent, make_client):
    """The delivered payload carries the client's own declared session id,
    not the hub-namespaced one."""
    agent.hm_protocol.db = None
    alice = _client(make_client, "ws://alice", "NS", ["speak"])
    agent.hm_protocol.clients = {"ws://alice": alice}

    agent.handle_internal_mycroft(_msg("speak", ["ws://alice"], "NS:s1", peer="ws://alice"))

    alice.send.assert_called_once()
    sent = alice.send.call_args[0][0]
    assert sent.payload.context["session"]["session_id"] == "s1"


def test_types_outside_allowed_types_are_still_delivered(agent, make_client):
    """allowed_types never gates master-to-satellite delivery: a client
    allowed only recognizer_loop:utterance, or nothing at all, still receives
    responses addressed to it, including the canonical ovos.utterance.speak."""
    agent.hm_protocol.db = None
    alice = _client(make_client, "ws://alice", "NS", ["recognizer_loop:utterance"])
    bob = _client(make_client, "ws://bob", "NB", [])
    agent.hm_protocol.clients = {"ws://alice": alice, "ws://bob": bob}

    agent.handle_internal_mycroft(_msg("speak", ["ws://alice"], "NS:s1", peer="ws://alice"))
    agent.handle_internal_mycroft(_msg("ovos.utterance.speak", ["ws://bob"], "NB:s2", peer="ws://bob"))
    agent.handle_internal_mycroft(_msg("ovos.mic.listen", ["ws://bob"], "NB:s2", peer="ws://bob"))

    assert alice.send.call_count == 1
    assert bob.send.call_count == 2
