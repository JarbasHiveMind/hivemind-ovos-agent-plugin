"""A request is never delivered to a peer, whatever its destination says.

T-6673. hivemind-core stops a CLIENT writing the Layer-1 ``destination`` of an
injected message. It cannot stop anything INSIDE the bus writing it afterwards:
a metadata transformer, a skill, any other bus writer. If such a value names a
connected peer, the relay used to deliver one peer's traffic to another peer's
socket, with ``source`` rewritten to "hive" so the receiver could not tell.

The test that separates the two cases is the one the relay already used for its
log level: hivemind-core stamps ``context["source"]`` with the peer id it minted
for the connection a message arrived on, so a message whose ``source`` names a
CONNECTED peer is that peer's own request in flight. A reply carries the peer in
``destination`` and no peer in ``source``, because ``Message.reply()`` swaps the
two.

HIVEMIND-AGENT-1 3.2 delivers a RESPONSE to the peers its destination names. A
request is not a response.
"""
from ovos_bus_client.message import Message

ATTACKER = "attacker::session-a"
VICTIM = "victim::session-b"


def _ovos_internal(msg_type, destination=None, source=None, peer=None,
                   data=None):
    context = {}
    if destination is not None:
        context["destination"] = destination
    if source is not None:
        context["source"] = source
    if peer is not None:
        context["peer"] = peer
    return Message(msg_type, data or {}, context).serialize()


class TestARequestIsNotDelivered:
    def test_a_peer_id_written_after_injection_does_not_reach_the_victim(
            self, agent, make_client):
        attacker = make_client(ATTACKER)
        victim = make_client(VICTIM)
        agent.hm_protocol.clients = {ATTACKER: attacker, VICTIM: victim}

        # the shape a transformer or a skill can produce: the attacker's
        # request, still carrying its own source, with the victim's peer id
        # written into destination
        agent.handle_internal_mycroft(_ovos_internal(
            "recognizer_loop:utterance", destination=VICTIM,
            source=ATTACKER, peer=ATTACKER, data={"utterances": ["hi"]}))

        victim.send.assert_not_called()
        attacker.send.assert_not_called()

    def test_the_same_with_the_victim_hidden_in_a_list(self, agent,
                                                       make_client):
        attacker = make_client(ATTACKER)
        victim = make_client(VICTIM)
        agent.hm_protocol.clients = {ATTACKER: attacker, VICTIM: victim}

        agent.handle_internal_mycroft(_ovos_internal(
            "speak", destination=[VICTIM, "audio"], source=ATTACKER,
            peer=ATTACKER, data={"utterance": "say this"}))

        victim.send.assert_not_called()

    def test_a_peer_cannot_address_itself_either(self, agent, make_client):
        # Not a security case: a peer's own request must not be echoed back to
        # it as though the agent had answered. It sends requests and receives
        # responses, and this is neither.
        attacker = make_client(ATTACKER)
        agent.hm_protocol.clients = {ATTACKER: attacker}

        agent.handle_internal_mycroft(_ovos_internal(
            "speak", destination=ATTACKER, source=ATTACKER, peer=ATTACKER,
            data={"utterance": "hi"}))

        attacker.send.assert_not_called()


class TestARealResponseStillArrives:
    """The negative control. The guard must not close the ordinary path."""

    def test_a_reply_reaches_the_peer_it_names(self, agent, make_client):
        victim = make_client(VICTIM)
        agent.hm_protocol.clients = {VICTIM: victim}

        # what Message.reply() produces: the peer in destination, and a service
        # name in source, because reply() swapped them
        agent.handle_internal_mycroft(_ovos_internal(
            "speak", destination=VICTIM, source="skills", peer=VICTIM,
            data={"utterance": "the answer"}))

        victim.send.assert_called_once()

    def test_a_reply_with_no_source_at_all_still_arrives(self, agent,
                                                         make_client):
        victim = make_client(VICTIM)
        agent.hm_protocol.clients = {VICTIM: victim}

        agent.handle_internal_mycroft(_ovos_internal(
            "speak", destination=VICTIM, peer=VICTIM,
            data={"utterance": "the answer"}))

        victim.send.assert_called_once()

    def test_a_list_valued_source_naming_the_peer_still_blocks(self, agent,
                                                               make_client):
        # hivemind_bus_client.protocol.handle_bus does
        # pload.context["source"] = pload.context.pop("destination"), and a
        # destination is routinely a list. The guard above tests source with
        # isinstance(source, str), so a list-valued source fails that test
        # and the request-vs-response guard above does not run, even though
        # destination is normalised to a list twelve lines above this guard.
        attacker = make_client(ATTACKER)
        agent.hm_protocol.clients = {ATTACKER: attacker}

        agent.handle_internal_mycroft(_ovos_internal(
            "speak", destination=ATTACKER, source=[ATTACKER], peer=ATTACKER,
            data={"utterance": "hi"}))

        attacker.send.assert_not_called()

    def test_a_source_naming_a_peer_that_is_not_connected_does_not_block(
            self, agent, make_client):
        # The guard tests CONNECTED peers, not the shape of the string. A
        # response whose source happens to look like a peer id that nobody
        # holds is still a response, and AGENT-1 3.2 delivers it to the peer
        # its destination names.
        victim = make_client(VICTIM)
        agent.hm_protocol.clients = {VICTIM: victim}

        agent.handle_internal_mycroft(_ovos_internal(
            "speak", destination=VICTIM, source="gone::session-z",
            peer=VICTIM, data={"utterance": "the answer"}))

        victim.send.assert_called_once()

    def test_two_peers_each_get_their_own_reply(self, agent, make_client):
        alice = make_client("alice::a")
        bob = make_client("bob::b")
        agent.hm_protocol.clients = {"alice::a": alice, "bob::b": bob}

        agent.handle_internal_mycroft(_ovos_internal(
            "speak", destination="alice::a", source="skills",
            peer="alice::a", data={"utterance": "for alice"}))
        agent.handle_internal_mycroft(_ovos_internal(
            "speak", destination="bob::b", source="skills",
            peer="bob::b", data={"utterance": "for bob"}))

        alice.send.assert_called_once()
        bob.send.assert_called_once()
