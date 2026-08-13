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
    previous = log.level
    log.setLevel(logging.DEBUG)
    log.propagate = True
    try:
        with caplog.at_level(logging.DEBUG, logger=log.name):
            agent.handle_internal_mycroft(_msg("audio"))
    finally:
        log.propagate = False
        log.setLevel(previous)

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


def _reset_forward_logger_state():
    """Drop the cache AND the underlying logging singleton's handlers.

    ``logging.getLogger(name)`` is process-wide; popping ``LOG._loggers``
    alone leaves its handlers attached, and the next resolve would stack a
    second one -- exactly the bug the lock exists to prevent.
    """
    name = f"{LOG.name} - {hap.__name__}"
    stale = logging.getLogger(name)
    for handler in list(stale.handlers):
        stale.removeHandler(handler)
    LOG._loggers.pop(name, None)
    hap._FORWARD_LOGGER = None
    hap._FORWARD_LOGGER_KEY = None


def test_logger_rewires_after_log_init_changes_base_path(tmp_path):
    """A logger resolved before LOG.init must pick up file logging after it.

    LOG.init sets base_path and later loggers get a RotatingFileHandler; a
    cached pre-init logger would silently keep stdout only.
    """
    hap._forward_logger()
    previous = LOG.base_path
    try:
        LOG.base_path = str(tmp_path)
        log = hap._forward_logger()
        kinds = {type(h).__name__ for h in log.handlers}
        assert "RotatingFileHandler" in kinds, (
            "cached logger kept its pre-init handlers; file logging was lost")
    finally:
        LOG.base_path = previous
        _reset_forward_logger_state()


def test_concurrent_first_use_attaches_handlers_once():
    """Two racing first calls must not double the handlers (duplicate log lines)."""
    import threading
    _reset_forward_logger_state()
    gate = threading.Event()

    def resolve():
        gate.wait()
        hap._forward_logger()

    threads = [threading.Thread(target=resolve) for _ in range(8)]
    for t in threads:
        t.start()
    gate.set()
    for t in threads:
        t.join(timeout=10)

    handlers = hap._forward_logger().handlers
    assert len(handlers) == len({id(h) for h in handlers})
    stream_handlers = [h for h in handlers
                       if type(h).__name__ == "StreamHandler"]
    assert len(stream_handlers) <= 1, (
        f"racing first use attached {len(stream_handlers)} stdout handlers")
