"""Outbound reverse-NAT of the Layer-1 session_id back to the client's name.

hivemind-core derives the Layer-1 session id as ``f"{conn_nonce}:{declared}"``
per connection, so two clients (or two multiplexed sessions on one
connection) never collide on the OVOS bus. Outbound, the server must recover
the client's declared name per message by stripping its own nonce prefix --
not by reading the connection's current ``sess.session_id``, which would be
wrong for a multiplexing client juggling more than one declared session.
"""

from types import SimpleNamespace

from ovos_bus_client.message import Message

import hivemind_ovos_agent_plugin as hmoap


def _make_nonce_client(make_client, peer, nonce, current_session=None):
    client = make_client(peer)
    client.conn_nonce = nonce
    client.sess = SimpleNamespace(session_id=current_session)
    return client


def _ovos_internal(msg_type, destination, session_id):
    msg = Message(
        msg_type,
        {},
        {"destination": destination, "session": {"session_id": session_id}},
    )
    return msg.serialize()


def test_outbound_session_id_mapped_back_to_client_name(agent, make_client):
    alice = _make_nonce_client(make_client, "ws://alice", "NONCE")
    agent.hm_protocol.clients = {"ws://alice": alice}

    agent.handle_internal_mycroft(_ovos_internal("speak", "ws://alice", "NONCE:myname"))

    sent = alice.send.call_args[0][0]
    assert sent.payload.context["session"]["session_id"] == "myname"


def test_two_clients_different_nonces_only_the_matching_one_gets_stripped(agent, make_client):
    alice = _make_nonce_client(make_client, "ws://alice", "N1")
    bob = _make_nonce_client(make_client, "ws://bob", "N2")
    agent.hm_protocol.clients = {"ws://alice": alice, "ws://bob": bob}

    raw = _ovos_internal("speak", ["ws://alice", "ws://bob"], "N1:call-7")
    original = Message.deserialize(raw)
    original_session_id = original.context["session"]["session_id"]

    agent.handle_internal_mycroft(raw)

    alice_sent = alice.send.call_args[0][0]
    bob_sent = bob.send.call_args[0][0]

    assert alice_sent.payload.context["session"]["session_id"] == "call-7"
    # bob's nonce is N2, the prefix doesn't match, so it passes through unchanged
    assert bob_sent.payload.context["session"]["session_id"] == "N1:call-7"

    # per-peer copies: rewriting alice's payload must not mutate bob's, nor
    # any shared source object.
    assert alice_sent.payload is not bob_sent.payload
    assert original.context["session"]["session_id"] == original_session_id


def test_same_client_multiplexed_sessions_recovered_per_message(agent, make_client):
    """The key multiplex-fix test: a single connection can carry more than one
    declared session over time (or in flight), and the CURRENT
    client.sess.session_id must never be consulted -- each outbound message
    carries its own session id, recovered independently."""
    alice = _make_nonce_client(make_client, "ws://alice", "NONCE", current_session="whatever-is-current")
    agent.hm_protocol.clients = {"ws://alice": alice}

    agent.handle_internal_mycroft(_ovos_internal("speak", "ws://alice", "NONCE:call-1"))
    first_sent = alice.send.call_args[0][0]

    agent.handle_internal_mycroft(_ovos_internal("speak", "ws://alice", "NONCE:call-2"))
    second_sent = alice.send.call_args[0][0]

    assert first_sent.payload.context["session"]["session_id"] == "call-1"
    assert second_sent.payload.context["session"]["session_id"] == "call-2"


def test_non_matching_nonce_prefix_passed_through(agent, make_client):
    alice = _make_nonce_client(make_client, "ws://alice", "NONCE")
    agent.hm_protocol.clients = {"ws://alice": alice}

    agent.handle_internal_mycroft(_ovos_internal("speak", "ws://alice", "someone-elses-id"))

    sent = alice.send.call_args[0][0]
    assert sent.payload.context["session"]["session_id"] == "someone-elses-id"


def test_no_session_context_is_safe(agent, make_client):
    alice = _make_nonce_client(make_client, "ws://alice", "NONCE")
    agent.hm_protocol.clients = {"ws://alice": alice}

    msg = Message("speak", {}, {"destination": "ws://alice"})
    agent.handle_internal_mycroft(msg.serialize())

    sent = alice.send.call_args[0][0]
    assert "session" not in sent.payload.context
