"""The catch-all bus handler must not pay for logging it discards.

``handle_internal_mycroft`` is registered on ``bus.on("message", ...)``, so it
runs for *every* message on the OVOS bus. ``LOG.debug``/``LOG.warning`` walk
``inspect.stack()`` to label the record before the level is consulted, so a
discarded record costs as much as an emitted one -- per message, on the bus
client's handler thread.
"""
import logging
from types import SimpleNamespace

import pytest
from ovos_bus_client.message import Message
from ovos_utils.log import LOG

import hivemind_ovos_agent_plugin as hap
from hivemind_ovos_agent_plugin import OVOSAgentProtocol


@pytest.fixture(autouse=True)
def _reset_forward_logger():
    hap._FORWARD_LOGGER = None
    yield
    hap._FORWARD_LOGGER = None


@pytest.fixture
def agent():
    a = object.__new__(OVOSAgentProtocol)
    a.hm_protocol = SimpleNamespace(clients={})
    return a


def _msg(destination):
    return Message("speak", {"utterance": "hello"},
                   {"destination": destination}).serialize()


def test_service_destination_does_not_use_the_stack_walking_logger(agent):
    """The common case: OVOS routes to "audio"/"skills", not to a peer."""
    with pytest.MonkeyPatch.context() as mp:
        calls = []
        mp.setattr(hap.LOG, "debug", lambda *a, **k: calls.append(a))
        agent.handle_internal_mycroft(_msg("audio"))

    assert calls == [], (
        "every bus message paid a LOG.debug that walks inspect.stack()"
    )


def test_matched_peer_does_not_use_the_stack_walking_logger(agent):
    sent = []
    agent.hm_protocol = SimpleNamespace(
        clients={"peer-1": SimpleNamespace(send=lambda m: sent.append(m))})

    with pytest.MonkeyPatch.context() as mp:
        calls = []
        mp.setattr(hap.LOG, "debug", lambda *a, **k: calls.append(a))
        agent.handle_internal_mycroft(_msg("peer-1"))

    assert calls == []
    assert len(sent) == 1, "the message must still be forwarded to the peer"


def test_forward_logger_is_resolved_once(agent):
    assert hap._forward_logger() is hap._forward_logger()


def test_forward_logger_follows_log_set_level():
    """Caching must not pin the level."""
    log = hap._forward_logger()
    assert log.name in LOG._loggers
    previous = LOG.level
    try:
        LOG.set_level("DEBUG")
        assert log.level == logging.DEBUG
        LOG.set_level("WARNING")
        assert log.level == logging.WARNING
    finally:
        LOG.set_level(previous)


def test_debug_output_is_unchanged_when_enabled(agent, caplog):
    log = hap._forward_logger()
    log.setLevel(logging.DEBUG)
    log.propagate = True
    try:
        with caplog.at_level(logging.DEBUG, logger=log.name):
            agent.handle_internal_mycroft(_msg("audio"))
    finally:
        log.propagate = False

    assert "destination is not a peer: audio" in caplog.text


def test_unconnected_peer_still_warns(agent, caplog):
    log = hap._forward_logger()
    log.propagate = True
    try:
        with caplog.at_level(logging.WARNING, logger=log.name):
            agent.handle_internal_mycroft(_msg("satellite::session-1"))
    finally:
        log.propagate = False

    assert "not connected" in caplog.text
