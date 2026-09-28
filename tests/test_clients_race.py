"""Regression test for a reproduced concurrency bug in handle_send fan-out.

self.clients (hm_protocol.clients) is a plain dict, mutated on the tornado
IOLoop thread (connect/reconnect/disconnect) while handle_send's
PROPAGATE/BROADCAST fan-out ran on the OVOS bus thread. With enough
satellites plus one reconnecting, direct dict iteration raced into
RuntimeError: dictionary changed size during iteration -- see the sibling
fix in HiveMind-core's HiveMindListenerProtocol fan-out loops.

handle_send now iterates a list(self.clients.values()) snapshot, so a
concurrent mutation can no longer raise mid-iteration.
"""

import sys
import threading

from hivemind_bus_client.message import HiveMessageType
from ovos_bus_client.message import Message


def _send_msg(msg_type, peer=None, payload=None):
    return Message("hive.send.downstream", {
        "msg_type": msg_type,
        "peer": peer,
        "payload": payload,
    })


def test_fan_out_survives_concurrent_client_mutation(agent, make_client):
    peers = [f"ws://{i}" for i in range(30)]
    clients = {p: make_client(p) for p in peers}
    agent.hm_protocol.clients = clients
    pre_existing = dict(clients)

    stop = threading.Event()

    def reconnect_storm(mutator_id):
        while not stop.is_set():
            peer = f"ws://reconnecting-{mutator_id}"
            agent.hm_protocol.clients.pop(peer, None)
            agent.hm_protocol.clients[peer] = make_client(peer)

    # lower the GIL switch interval so the mutator threads interleave with
    # the fan-out loop far more often, making the race reliable in a test
    old_interval = sys.getswitchinterval()
    sys.setswitchinterval(1e-6)
    threads = [threading.Thread(target=reconnect_storm, args=(i,), daemon=True)
               for i in range(8)]
    for t in threads:
        t.start()
    try:
        for _ in range(2000):
            agent.handle_send(_send_msg(HiveMessageType.BROADCAST, peer=peers[0], payload={}))
    finally:
        stop.set()
        for t in threads:
            t.join(timeout=5)
        sys.setswitchinterval(old_interval)

    for p, c in pre_existing.items():
        assert c.send.called, p


class VanishingClients(dict):
    """A peer that disconnects between the membership check and the lookup.

    `in` still sees it, the indexed read no longer does -- exactly what a
    tornado-thread disconnect does to a check-then-index on the bus thread.
    """

    def __contains__(self, key):
        return True

    def __getitem__(self, key):
        raise KeyError(key)

    def get(self, key, default=None):
        return default


def test_unicast_send_to_a_peer_that_just_disconnected(agent, fake_bus):
    agent.hm_protocol.clients = VanishingClients()
    errors = []
    fake_bus.on("hive.client.send.error", lambda m: errors.append(m))

    agent.handle_send(_send_msg(HiveMessageType.BUS, peer="ws://gone", payload={}))

    assert len(errors) == 1
    assert errors[0].data["peer"] == "ws://gone"
