"""Downstream site targeting, the shape HIVEMIND-MSG-1 §4 defines.

A `BROADCAST`, `PROPAGATE` or `ESCALATE` carrying `target_site_id` travels
the mesh under the rules of its outer type, and only a node whose own site
identifier is equal delivers the inner `BUS` message to its Layer-1 bus. An
unset key means no node delivers it.

The same clause forbids the other shape: a server must not select the
recipient of a `BUS` message by a site identifier, because a site identifier
is self-declared and unbound to credentials — a routing key, not an
authorisation boundary. So the key belongs on the routed types only.

Before this, `handle_send` never read a site identifier, so every envelope
went out with the field unset: the fan-out happened and nothing landed.
"""
from unittest.mock import MagicMock

from hivemind_bus_client.message import HiveMessage, HiveMessageType
from ovos_bus_client.message import Message

import hivemind_ovos_agent_plugin as hap

def _send_msg(msg_type, peer=None, payload=None, site_id=None):
    data = {"msg_type": msg_type, "peer": peer, "payload": payload}
    if site_id is not None:
        data["site_id"] = site_id
    return Message("hive.send.downstream", data)


def _sent(client):
    assert client.send.called, "nothing was sent to this client"
    return client.send.call_args[0][0]


class TestTheSiteKeyReachesTheEnvelope:

    def test_a_broadcast_carries_the_site_id(self, agent, make_client):
        c1, c2 = make_client("ws://a"), make_client("ws://b")
        agent.hm_protocol.clients = {"ws://a": c1, "ws://b": c2}

        agent.handle_send(_send_msg(HiveMessageType.BROADCAST,
                                    payload={"x": 1}, site_id="garage"))

        # site targeting restricts delivery, not travel: both connections get
        # the envelope, and each node decides for itself
        for client in (c1, c2):
            sent = _sent(client)
            assert isinstance(sent, HiveMessage)
            assert sent.target_site_id == "garage"

    def test_a_propagate_carries_the_site_id(self, agent, make_client):
        client = make_client("ws://a")
        agent.hm_protocol.clients = {"ws://a": client}

        agent.handle_send(_send_msg(HiveMessageType.PROPAGATE,
                                    payload={}, site_id="kitchen"))

        assert _sent(client).target_site_id == "kitchen"

    def test_without_a_site_id_the_key_stays_unset(self, agent, make_client):
        client = make_client("ws://a")
        agent.hm_protocol.clients = {"ws://a": client}

        agent.handle_send(_send_msg(HiveMessageType.BROADCAST, payload={}))

        assert not _sent(client).target_site_id


class TestABusMessageIsNeverSiteAddressed:
    """The clause's MUST NOT, held by a test.

    A directly addressed BUS frame must be ignored by the receiver if it
    carries the key at all; not setting it keeps this side from looking like
    site-addressed BUS delivery.
    """

    def test_a_direct_bus_message_drops_a_site_id(self, agent, make_client):
        peer = "ws://a"
        client = make_client(peer)
        agent.hm_protocol.clients = {peer: client}

        agent.handle_send(_send_msg(HiveMessageType.BUS, peer=peer,
                                    payload={}, site_id="garage"))

        assert not _sent(client).target_site_id


class TestTheDropIsNoLongerSilent:
    """The forwarding path uses its own cached logger, so the records are
    taken from it rather than from caplog, which never sees it."""

    @staticmethod
    def _capture(monkeypatch):
        log = MagicMock()
        monkeypatch.setattr(hap, "_forward_logger", lambda: log)
        return log

    @staticmethod
    def _text(call_list):
        return " ".join(str(a) for call in call_list for a in call[0])

    def test_a_message_naming_a_site_says_where_site_targeting_lives(
            self, agent, make_client, monkeypatch):
        log = self._capture(monkeypatch)
        client = make_client("ws://a")
        client.session_namespace = "ns"
        agent.hm_protocol.clients = {"ws://a": client}
        message = Message("speak", {},
                          {"destination": [], "session": {"session_id": "other",
                                                          "site_id": "garage"}})

        agent.handle_internal_mycroft(message.serialize())

        client.send.assert_not_called()
        text = self._text(log.info.call_args_list)
        assert "garage" in text
        assert "hive.send.downstream" in text

    def test_an_empty_destination_says_why_at_debug(
            self, agent, make_client, monkeypatch):
        log = self._capture(monkeypatch)
        client = make_client("ws://a")
        client.session_namespace = "ns"
        agent.hm_protocol.clients = {"ws://a": client}
        message = Message("speak", {},
                          {"destination": [], "session": {"session_id": "not-ours"}})

        agent.handle_internal_mycroft(message.serialize())

        client.send.assert_not_called()
        assert "not delivered" in self._text(log.debug.call_args_list)

    def test_a_named_but_unknown_destination_is_not_logged_twice(
            self, agent, make_client, monkeypatch):
        """The unmatched loop already explains a destination that named
        something; the summary must not repeat it on every bus message."""
        log = self._capture(monkeypatch)
        client = make_client("ws://a")
        client.session_namespace = "ns"
        agent.hm_protocol.clients = {"ws://a": client}
        message = Message("speak", {}, {"destination": ["skills"]})

        agent.handle_internal_mycroft(message.serialize())

        client.send.assert_not_called()
        assert "not delivered" not in self._text(log.debug.call_args_list)

    def test_a_delivered_message_logs_no_drop(self, agent, make_client, monkeypatch):
        log = self._capture(monkeypatch)
        peer = "ws://a"
        client = make_client(peer)
        client.session_namespace = "ns"
        agent.hm_protocol.clients = {peer: client}
        message = Message("speak", {}, {"destination": [peer]})

        agent.handle_internal_mycroft(message.serialize())

        client.send.assert_called_once()
        assert "not delivered" not in self._text(log.debug.call_args_list)
        log.info.assert_not_called()
