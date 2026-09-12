"""site_id is a routing key for BROADCAST, not an authorisation boundary.

A client declares its own ``site_id`` in HELLO, and hivemind-core does not tie
it to the access key. So a targeted bus message goes by peer id only: a
connection on another access key that claims a site must not receive that
site's messages. Site-targeted delivery is a BROADCAST sent through
``hive.send.downstream`` with ``target_site_id``; every connection receives
the envelope and each node delivers it only when the site is its own
(HIVEMIND-MSG-1 §5). ``allowed_types`` gates what a client sends, never what
it receives.

Issue #52 also reported a silent drop: a bus message with only
``target_site_id`` was dropped with no log. It now logs a warning that names
the supported route.
"""

from unittest.mock import MagicMock

from hivemind_bus_client.message import HiveMessage, HiveMessageType
from ovos_bus_client.message import Message

import hivemind_ovos_agent_plugin as hmoap


def _client(make_client, peer, site_id):
    client = make_client(peer)
    client.site_id = site_id
    return client


def _kitchen_and_spoofer(agent, make_client):
    kitchen = _client(make_client, "kitchen::c0ffee", "kitchen")
    # another access key whose HELLO claims the kitchen site
    spoofer = _client(make_client, "garage::deadbeef", "kitchen")
    agent.hm_protocol.clients = {
        "kitchen::c0ffee": kitchen, "garage::deadbeef": spoofer,
    }
    return kitchen, spoofer


def test_a_connection_claiming_a_site_receives_nothing_targeted(agent, make_client):
    """fail-before: the site_id branch sent this to every matching site."""
    kitchen, spoofer = _kitchen_and_spoofer(agent, make_client)

    agent.handle_internal_mycroft(Message(
        "speak", {"utterance": "door code 1234"},
        {"target_site_id": "kitchen"}).serialize())

    spoofer.send.assert_not_called()
    kitchen.send.assert_not_called()


def test_a_site_match_never_selects_a_bus_recipient_beside_the_peer(agent, make_client):
    kitchen, spoofer = _kitchen_and_spoofer(agent, make_client)

    agent.handle_internal_mycroft(Message(
        "speak", {},
        {"destination": "kitchen::c0ffee", "target_site_id": "kitchen"}).serialize())

    kitchen.send.assert_called_once()
    spoofer.send.assert_not_called()


def test_an_undeliverable_target_site_id_logs_a_warning(agent, make_client, monkeypatch):
    log = MagicMock()
    monkeypatch.setattr(hmoap, "_FORWARD_LOGGER", log)
    kitchen = _client(make_client, "kitchen::c0ffee", "kitchen")
    agent.hm_protocol.clients = {"kitchen::c0ffee": kitchen}

    agent.handle_internal_mycroft(Message(
        "speak", {}, {"target_site_id": "kitchen"}).serialize())

    kitchen.send.assert_not_called()
    assert log.warning.call_count == 1
    args = log.warning.call_args[0]
    text = args[0] % args[1:]
    assert "kitchen" in text and "broadcast" in text


def _downstream(msg_type, payload, **data):
    return Message("hive.send.downstream",
                   {"msg_type": msg_type, "payload": payload, **data})


def test_a_site_broadcast_reaches_every_connection_with_the_site_on_the_envelope(
        agent, make_client):
    kitchen, spoofer = _kitchen_and_spoofer(agent, make_client)
    bedroom = _client(make_client, "bedroom::0b0b", "bedroom")
    agent.hm_protocol.clients["bedroom::0b0b"] = bedroom
    inner = HiveMessage(HiveMessageType.BUS, payload=Message("speak", {}))

    agent.handle_send(_downstream(HiveMessageType.BROADCAST, inner,
                                  target_site_id="kitchen"))

    for client in (kitchen, spoofer, bedroom):
        client.send.assert_called_once()
        sent = client.send.call_args[0][0]
        assert sent.msg_type == HiveMessageType.BROADCAST
        # each node filters by its own site; the hub does not select by it
        assert sent.target_site_id == "kitchen"


def test_a_bus_send_with_only_a_site_sends_nothing(agent, make_client):
    kitchen, spoofer = _kitchen_and_spoofer(agent, make_client)

    agent.handle_send(_downstream(HiveMessageType.BUS, Message("speak", {}).serialize(),
                                  target_site_id="kitchen"))

    kitchen.send.assert_not_called()
    spoofer.send.assert_not_called()
