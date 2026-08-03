"""Client isolation invariant: a client must only receive messages targeted at it."""

from unittest.mock import MagicMock

from hivemind_bus_client.message import HiveMessageType
from ovos_bus_client.message import Message

import hivemind_ovos_agent_plugin as hmoap


def _ovos_internal(msg_type, destination=None, data=None):
    """Build the serialized JSON that handle_internal_mycroft expects."""
    msg = Message(msg_type, data or {}, {"destination": destination} if destination is not None else {})
    return msg.serialize()


class TestClientIsolation:
    def test_message_addressed_to_one_client_only_reaches_that_client(self, agent, make_client):
        alice = make_client("ws://alice")
        bob = make_client("ws://bob")
        agent.hm_protocol.clients = {"ws://alice": alice, "ws://bob": bob}

        agent.handle_internal_mycroft(_ovos_internal("speak", destination="ws://alice", data={"utterance": "hi"}))

        alice.send.assert_called_once()
        bob.send.assert_not_called()

    def test_destination_can_be_a_list(self, agent, make_client):
        alice = make_client("ws://alice")
        bob = make_client("ws://bob")
        carol = make_client("ws://carol")
        agent.hm_protocol.clients = {"ws://alice": alice, "ws://bob": bob, "ws://carol": carol}

        agent.handle_internal_mycroft(_ovos_internal("speak", destination=["ws://alice", "ws://bob"]))

        alice.send.assert_called_once()
        bob.send.assert_called_once()
        carol.send.assert_not_called()

    def test_message_with_no_destination_is_dropped(self, agent, make_client):
        alice = make_client("ws://alice")
        bob = make_client("ws://bob")
        agent.hm_protocol.clients = {"ws://alice": alice, "ws://bob": bob}

        agent.handle_internal_mycroft(_ovos_internal("speak", destination=None, data={"utterance": "hi"}))

        alice.send.assert_not_called()
        bob.send.assert_not_called()

    def test_message_addressed_to_unknown_peer_is_dropped(self, agent, make_client):
        alice = make_client("ws://alice")
        agent.hm_protocol.clients = {"ws://alice": alice}

        agent.handle_internal_mycroft(_ovos_internal("speak", destination="ws://stranger"))

        alice.send.assert_not_called()

    def test_message_addressed_to_unknown_peer_logs_a_warning(self, agent, make_client, monkeypatch):
        """A stale/reconnected peer with no live client must be searchable in
        the logs instead of silently swallowing the reply. A reconnect mints a
        new peer id because the session id is part of it."""
        log = MagicMock()
        monkeypatch.setattr(hmoap, "LOG", log)
        alice = make_client("voice_sat::c0ffee")
        agent.hm_protocol.clients = {"voice_sat::c0ffee": alice}
        stale_peer = "voice_sat::deadbeef"

        agent.handle_internal_mycroft(_ovos_internal("speak", destination=stale_peer))

        assert log.warning.call_count == 1
        assert stale_peer in log.warning.call_args[0][0]

    def test_ordinary_ovos_destinations_do_not_warn(self, agent, make_client, monkeypatch):
        """OVOS routes internal labels through context["destination"] on every
        reply and every spoken line. They are not peers and must not be
        reported as disconnected ones, or the real stale-peer case drowns."""
        log = MagicMock()
        monkeypatch.setattr(hmoap, "LOG", log)
        alice = make_client("voice_sat::c0ffee")
        agent.hm_protocol.clients = {"voice_sat::c0ffee": alice}

        for destination in (["audio"], ["enclosure"], ["skills"], ["ovos.gui"],
                            ["ovos-skill-date-time.openvoiceos"]):
            agent.handle_internal_mycroft(_ovos_internal("speak", destination=destination))

        assert not log.warning.called
        alice.send.assert_not_called()

    def test_forwarded_message_is_wrapped_as_bus_hivemessage(self, agent, make_client):
        alice = make_client("ws://alice")
        agent.hm_protocol.clients = {"ws://alice": alice}

        agent.handle_internal_mycroft(_ovos_internal("speak", destination="ws://alice"))

        sent = alice.send.call_args[0][0]
        assert sent.msg_type == HiveMessageType.BUS
        # payload is the Mycroft Message
        assert sent.payload.msg_type == "speak"

    def test_forwarded_message_marks_source_as_hive(self, agent, make_client):
        """Downstream relays must rewrite source so the client sees it came from the hive."""
        alice = make_client("ws://alice")
        agent.hm_protocol.clients = {"ws://alice": alice}

        agent.handle_internal_mycroft(_ovos_internal("speak", destination="ws://alice"))

        sent = alice.send.call_args[0][0]
        assert sent.payload.context.get("source") == "hive"
