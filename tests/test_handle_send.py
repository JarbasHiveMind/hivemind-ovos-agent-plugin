"""Tests for OVOSAgentProtocol.handle_send (downstream dispatch from OVOS bus)."""

import threading
import time

from ovos_bus_client.message import Message
from hivemind_bus_client.message import HiveMessage, HiveMessageType


def _send_msg(msg_type, peer=None, payload=None):
    """Helper to build the ovos Message that triggers handle_send."""
    return Message("hive.send.downstream", {
        "msg_type": msg_type,
        "peer": peer,
        "payload": payload,
    })


class TestHandleSendDirect:
    def test_dispatch_to_connected_peer(self, agent, make_client):
        peer = "ws://1.2.3.4:5678"
        client = make_client(peer)
        agent.hm_protocol.clients = {peer: client}

        agent.handle_send(_send_msg(HiveMessageType.BUS, peer=peer, payload={"x": 1}))

        client.send.assert_called_once()
        sent = client.send.call_args[0][0]
        assert isinstance(sent, HiveMessage)
        assert sent.msg_type == HiveMessageType.BUS

    def test_stale_peer_send_is_dropped_without_raising(self, agent, make_client):
        peer = "ws://stale"
        client = make_client(peer)
        client.send.side_effect = RuntimeError("closed")
        agent.hm_protocol.clients = {peer: client}

        agent.handle_send(_send_msg(HiveMessageType.BUS, peer=peer, payload={}))

        client.send.assert_called_once()
        client.disconnect.assert_called_once()
        assert agent.hm_protocol.clients == {}

    def test_stale_peer_disconnect_failure_does_not_break_dispatch(self, agent, make_client):
        peer = "ws://stale"
        client = make_client(peer)
        client.send.side_effect = RuntimeError("closed")
        client.disconnect.side_effect = RuntimeError("already closed")
        agent.hm_protocol.clients = {peer: client}

        sent = agent._send_to_client(
            peer,
            client,
            HiveMessage(HiveMessageType.BUS, payload={}),
        )

        assert sent is False
        client.disconnect.assert_called_once()
        assert agent.hm_protocol.clients == {}

    def test_unknown_peer_emits_error_on_bus(self, agent, make_client, fake_bus):
        seen = []
        fake_bus.on("hive.client.send.error", lambda m: seen.append(m))

        agent.hm_protocol.clients = {}  # nobody connected
        agent.handle_send(_send_msg(HiveMessageType.BUS, peer="ws://missing:1234", payload={}))

        assert len(seen) == 1
        assert seen[0].data["peer"] == "ws://missing:1234"
        assert "not connected" in seen[0].data["error"].lower()

    def test_no_peer_no_dispatch(self, agent, make_client):
        c1 = make_client("ws://a")
        c2 = make_client("ws://b")
        agent.hm_protocol.clients = {"ws://a": c1, "ws://b": c2}

        # msg_type that isn't propagate/broadcast/escalate, with peer=None
        agent.handle_send(_send_msg(HiveMessageType.BUS, peer=None, payload={}))

        c1.send.assert_not_called()
        c2.send.assert_not_called()


class TestHandleSendFanout:
    def test_propagate_fans_out_to_all(self, agent, make_client):
        peers = [f"ws://{i}" for i in range(3)]
        clients = {p: make_client(p) for p in peers}
        agent.hm_protocol.clients = clients

        agent.handle_send(_send_msg(HiveMessageType.PROPAGATE, peer=peers[0], payload={}))

        for c in clients.values():
            c.send.assert_called_once()

    def test_propagate_continues_after_stale_peer(self, agent, make_client):
        peers = ["ws://stale", "ws://alive"]
        clients = {p: make_client(p) for p in peers}
        clients["ws://stale"].send.side_effect = RuntimeError("closed")
        agent.hm_protocol.clients = dict(clients)

        agent.handle_send(_send_msg(HiveMessageType.PROPAGATE, peer=peers[0], payload={}))

        clients["ws://stale"].send.assert_called_once()
        clients["ws://stale"].disconnect.assert_called_once()
        clients["ws://alive"].send.assert_called_once()
        assert agent.hm_protocol.clients == {"ws://alive": clients["ws://alive"]}

    def test_broadcast_fans_out_to_all(self, agent, make_client):
        peers = [f"ws://{i}" for i in range(3)]
        clients = {p: make_client(p) for p in peers}
        agent.hm_protocol.clients = clients

        agent.handle_send(_send_msg(HiveMessageType.BROADCAST, peer=peers[0], payload={}))

        for c in clients.values():
            c.send.assert_called_once()

    def test_escalate_is_silently_ignored(self, agent, make_client):
        c = make_client("ws://a")
        agent.hm_protocol.clients = {"ws://a": c}

        agent.handle_send(_send_msg(HiveMessageType.ESCALATE, peer="ws://a", payload={}))

        # ESCALATE goes upstream, not downstream — no client should be sent to
        c.send.assert_not_called()


class TestClientSendLocking:
    def test_client_writes_are_serialized_per_peer(self, agent):
        state_lock = threading.Lock()
        active = 0
        max_active = 0
        sent = []

        class _Client:
            def send(self, message):
                nonlocal active, max_active
                with state_lock:
                    active += 1
                    max_active = max(max_active, active)
                time.sleep(0.02)
                sent.append(message)
                with state_lock:
                    active -= 1

        client = _Client()
        hmessage = HiveMessage(HiveMessageType.BUS, payload={})
        threads = [
            threading.Thread(
                target=agent._send_to_client,
                args=("ws://alice", client, hmessage),
            )
            for _ in range(5)
        ]

        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert len(sent) == 5
        assert max_active == 1
