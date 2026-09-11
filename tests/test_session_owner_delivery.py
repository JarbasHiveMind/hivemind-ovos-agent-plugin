"""One satellite's response does not reach another that shares its access key.

The session-ownership delivery path exists so a scheduled event -- an alarm --
still reaches the device that set it after a reconnect, when the peer id in
the message is stale. It used to match on the session namespace, which
HIVEMIND-BRIDGE-1 §4 keys by site and access key, so two devices at one site
on one credential share it. Matching the namespace alone therefore delivered
one device's response to both, which AGENT-1 §3.2 forbids: "A peer MUST NOT
receive a response generated for a different peer."

Delivery is now by ``client.peer``, the id hivemind-core stamps as
``context["peer"]`` on every message it forwards and which core itself keeps
unique per live connection (forcing a colliding peer apart with a suffix at
HELLO time). The namespace fallback is kept, because it is the only route a
scheduled event has once its session is gone, and it announces itself in the
log.

The connections below are real ``hivemind_core.protocol.HiveMindClientConnection``
instances, registered through the real ``handle_hello_message``, so every
``peer`` and ``session_namespace`` used in an assertion is a value hivemind-core
produced -- never one set on the test double to match what the plugin is
expected to do with it.
"""
from types import SimpleNamespace

import pytest
from hivemind_bus_client.message import HiveMessage, HiveMessageType
from hivemind_core.protocol import HiveMindClientConnection, HiveMindListenerProtocol
from ovos_bus_client.message import Message

from hivemind_ovos_agent_plugin import OVOSAgentProtocol


class _FakeClientDatabase:
    """The two lookups a connection's ACL and ``session_namespace`` need.

    ``session_namespace`` hashes the hub's salt with the DB client_id, and
    the ACL gate reads ``allowed_types`` off the same row, so a database is
    needed to exercise either as real code instead of the conn_nonce
    fallback that would otherwise make two devices on one credential land on
    different namespaces.
    """

    def __init__(self):
        self._by_key = {}
        self._by_id = {}

    def register(self, key, client_id, allowed_types):
        user = SimpleNamespace(client_id=client_id, allowed_types=list(allowed_types))
        self._by_key[key] = user
        self._by_id.setdefault(client_id, user)

    def get_client_by_api_key(self, key):
        return self._by_key.get(key)

    def refresh(self, client_id):
        return self._by_id.get(client_id)


@pytest.fixture
def hub():
    """A real listener protocol, built without opening a socket or a key file."""
    hm = object.__new__(HiveMindListenerProtocol)
    hm.clients = {}
    hm.identity = SimpleNamespace(public_key="test-hub-salt")
    hm.db = _FakeClientDatabase()
    return hm


@pytest.fixture
def connect(hub):
    """Register a real connection through the real HELLO handler.

    Returns ``(client, sent)`` where ``sent`` collects the frames handed to
    ``send_msg`` -- the connection's own wire-send seam, not a stub of
    anything the plugin does.
    """

    def _connect(key, name, session_id, client_id, allowed_types=("speak",)):
        hub.db.register(key, client_id, allowed_types)
        sent = []
        client = HiveMindClientConnection(
            key=key,
            send_msg=lambda payload, is_bin: sent.append(payload),
            disconnect=lambda *a, **k: None,
            name=name,
            handshake=SimpleNamespace(),  # skips RSA key material never used here
            hm_protocol=hub,
            allowed_types=list(allowed_types),
        )
        hello = HiveMessage(HiveMessageType.HELLO,
                             payload={"session": {"session_id": session_id}})
        hub.handle_hello_message(hello, client)
        return client, sent

    return _connect


@pytest.fixture
def proto(hub):
    p = OVOSAgentProtocol.__new__(OVOSAgentProtocol)
    p.hm_protocol = hub
    return p


def _deliver(proto, origin_peer, sid):
    msg = Message("speak", {"utterance": "your alarm"},
                  {"peer": origin_peer, "session": {"session_id": sid}})
    proto.handle_internal_mycroft(msg.serialize())


def test_two_devices_on_one_key_get_forced_apart_and_only_the_addressed_one_answers(
        connect, hub, proto):
    """The security property: a shared credential is not a shared inbox."""
    a, sent_a = connect("shared-key", "satellite", "same-declared-session", client_id=1)
    b, sent_b = connect("shared-key", "satellite", "same-declared-session", client_id=1)

    assert a.peer != b.peer, "core must force a colliding peer id apart at HELLO"
    assert set(hub.clients) == {a.peer, b.peer}

    # The session id core stamps on the forwarded message, built the way
    # protocol.py does: the namespace half is derived from the credential, so
    # both connections carry it. A test that invents a session id here would
    # match no namespace, the fallback would engage for nobody, and the old
    # lookup would fail this test by delivering to neither device instead of to
    # both. Delivering to both is the defect under test.
    assert a.session_namespace == b.session_namespace
    stamped_sid = f"{a.session_namespace}:same-declared-session"

    _deliver(proto, a.peer, stamped_sid)

    assert sent_a and not sent_b, (
        f"a message addressed to {a.peer!r} reached sent_a={sent_a!r} "
        f"sent_b={sent_b!r}; satellite B shares only the credential and must "
        "not receive it"
    )


def test_the_namespace_fallback_still_reaches_a_client_whose_old_peer_is_gone(
        connect, proto, caplog):
    """A scheduled event whose session predates a reconnect still lands."""
    a, sent_a = connect("shared-key", "satellite", "current-session", client_id=1)
    stale_sid = f"{a.session_namespace}:the-session-that-set-the-alarm"

    with caplog.at_level("INFO"):
        _deliver(proto, "satellite::a-connection-that-no-longer-exists", stale_sid)

    assert sent_a, "the namespace fallback did not reach the reconnected device"
    assert any("delivered to 1 client(s)" in r.message for r in caplog.records), (
        "a fan-out that is not peer-exact must announce itself in the log"
    )


def test_the_fallback_count_is_what_was_delivered_not_what_matched(
        connect, proto, caplog):
    """A candidate dropped by the allowed_types gate is not a delivery."""
    a, sent_a = connect("key-a", "satellite", "a-session", client_id=1,
                         allowed_types=("speak",))
    b, sent_b = connect("key-b", "satellite", "b-session", client_id=1,
                         allowed_types=())  # admits nothing

    assert a.session_namespace == b.session_namespace, (
        "both devices are issued to the same account and must share a namespace "
        "for this to be a meaningful test of the gate"
    )

    shared_sid = f"{a.session_namespace}:a-session-nobody-holds"
    with caplog.at_level("INFO"):
        _deliver(proto, "stale-peer-nobody-owns", shared_sid)

    assert sent_a and not sent_b, f"sent_a={sent_a!r} sent_b={sent_b!r}"
    assert any("delivered to 1 client(s)" in r.message for r in caplog.records), (
        "two clients matched the namespace and one was dropped by the type "
        f"gate; the line must say 1. records={[r.message for r in caplog.records]}"
    )
