import dataclasses
from threading import Lock
from collections.abc import Iterator
from typing import Any

from hivemind_bus_client.message import HiveMessage, HiveMessageType
from hivemind_plugin_manager.protocols import AgentProtocol
from ovos_bus_client import MessageBusClient
from ovos_bus_client.message import Message
from ovos_config import Configuration
from ovos_utils.fakebus import FakeBus
from ovos_utils.log import LOG
from pyee import EventEmitter

from hivemind_ovos_agent_plugin.policy import (
    AddBlacklistedIntent,
    AddBlacklistedSkill,
    OVOSAgentPolicy,
    RewriteUtterance,
    SetContextField,
    SetSessionField,
)
from hivemind_ovos_agent_plugin.version import __version__


#: Forwarding-path logger, resolved once.
#:
#: ``LOG.debug``/``LOG.warning`` resolve the calling module, function and line
#: with ``inspect.stack()`` on every call, before the level is consulted, so a
#: record an INFO-level node discards costs as much as one it emits.
#: ``handle_internal_mycroft`` is registered as the catch-all ``message``
#: handler, so it runs for every message on the OVOS bus and pays that per
#: message, on the bus client's handler thread.
#:
#: ``LOG.create_logger`` returns the same OVOS-configured logger those calls
#: would have built -- same formatter and handlers -- and registers it in
#: ``LOG._loggers``, so ``LOG.init``/``LOG.set_level`` still retargets its
#: level. Only the per-call stack walk is dropped. Resolved lazily because
#: ``LOG.init`` usually runs after import.
_FORWARD_LOGGER = None
_FORWARD_LOGGER_KEY = None
_FORWARD_LOGGER_LOCK = Lock()


def _forward_logger():
    """Return the cached forwarding-path logger, rebuilding when LOG rewires.

    Cached against ``(LOG.name, LOG.base_path)``: ``LOG.init()`` normally runs
    after import, and a logger created before it would carry only the stdout
    handler -- configured file logging would silently vanish from this path.
    When the fingerprint changes, the stale entry and its handlers are dropped
    so ``create_logger`` rebuilds against the live config. The lock keeps two
    racing first calls from attaching duplicate handlers to the same
    process-wide ``logging.getLogger`` name.
    """
    global _FORWARD_LOGGER, _FORWARD_LOGGER_KEY
    key = (LOG.name, LOG.base_path)
    if _FORWARD_LOGGER is None or _FORWARD_LOGGER_KEY != key:
        with _FORWARD_LOGGER_LOCK:
            if _FORWARD_LOGGER is None or _FORWARD_LOGGER_KEY != key:
                name = f"{LOG.name} - {__name__}"
                stale = LOG._loggers.pop(name, None)
                if stale is not None:
                    for handler in list(stale.handlers):
                        stale.removeHandler(handler)
                        handler.close()
                _FORWARD_LOGGER = LOG.create_logger(name)
                _FORWARD_LOGGER_KEY = key
    if LOG.diagnostic_mode:
        # Mirror LOG._get_real_logger: diagnostic mode records the bus message
        # behind each log call. Costs one attribute check when it is off.
        try:
            from ovos_bus_client.message import dig_for_message
            message = dig_for_message()
            if message:
                _FORWARD_LOGGER.debug(
                    f"DIAGNOSTIC - source bus message {message.serialize()}")
        except ImportError:
            pass
    return _FORWARD_LOGGER


def _is_peer_id(destination: str) -> bool:
    """Whether a message destination names a HiveMind peer.

    HiveMindClientConnection.peer mints ids as "name::session_id", plus a
    "::suffix" when a second live connection claims the same string. OVOS
    routes plenty of other labels through context["destination"] - "audio",
    "skills", "ovos.gui", a skill_id - and none of them carry a session id.
    """
    return "::" in destination


@dataclasses.dataclass()
class OVOSAgentProtocol(AgentProtocol):
    """HiveMind agent protocol that bridges client messages to an OVOS bus."""
    bus: MessageBusClient = dataclasses.field(default_factory=FakeBus)
    config: dict[str, Any] = dataclasses.field(default_factory=lambda: Configuration().get("websocket", {}))
    _owned_bus: MessageBusClient | None = dataclasses.field(
        default=None, init=False, repr=False, compare=False
    )

    def __post_init__(self):
        if not self.bus or isinstance(self.bus, FakeBus):
            ovos_bus_address = self.config.get("host") or "127.0.0.1"
            ovos_bus_port = self.config.get("port") or 8181
            timeout = self._connection_timeout()
            self.bus = MessageBusClient(
                host=ovos_bus_address,
                port=ovos_bus_port,
                emitter=EventEmitter(),
            )
            self.bus.run_in_thread()
            self._owned_bus = self.bus
            # Bounded wait, then carry on. Blocking forever stalls whatever
            # hosts this protocol, and raising takes the whole node down over
            # a messagebus that is merely slow to boot — on a Pi that is the
            # normal case. MessageBusClient keeps reconnecting on its own, and
            # until it succeeds ``get_bus`` reports the bus as unavailable, so
            # Core answers its clients BACKEND_UNAVAILABLE instead of vanishing.
            if not self.bus.connected_event.wait(timeout):
                LOG.error(
                    f"Could not connect to the OVOS messagebus at "
                    f"ws://{ovos_bus_address}:{ovos_bus_port} within {timeout}s, "
                    f"retrying in the background. Is the OVOS messagebus running? "
                    f"Start it (e.g. 'ovos-messagebus'), or set the agent "
                    f"protocol's host/port/connection_timeout in the config."
                )
        self.register_bus_handlers()

    def register_bus_handlers(self):
        LOG.debug("registering internal OVOS bus handlers")
        self.bus.on("hive.send.downstream", self.handle_send)
        self.bus.on("message", self.handle_internal_mycroft)  # catch all

    def _connection_timeout(self) -> float:
        raw = self.config.get("connection_timeout", 10)
        try:
            return max(0.0, float(raw))
        except (TypeError, ValueError):
            return 10.0

    def get_bus(self, client=None) -> FakeBus | MessageBusClient:
        """Return the current bus, or raise at once if it is not connected.

        Core calls this inline on the single IOLoop thread that serves every
        satellite, so this method must never wait: one disconnected OVOS bus
        would otherwise freeze the whole node for the connection timeout, and
        each retrying satellite would pay that price again on the same thread.
        ``MessageBusClient`` owns websocket recovery and keeps reconnecting in
        the background; raising here lets Core answer BACKEND_UNAVAILABLE in
        milliseconds meanwhile. Callers that can afford to wait use
        ``wait_for_bus``. Caller-supplied buses remain entirely caller-owned.
        """
        bus = self.bus
        if bus is not self._owned_bus or bus.connected_event.is_set():
            return bus
        raise ConnectionError("OVOS messagebus is not connected")

    def wait_for_bus(self, timeout: float | None = None) -> bool:
        """Block until the plugin-created bus is connected, at most ``timeout``
        seconds. For setup code only — never call it from Core's IOLoop thread."""
        if self.bus is not self._owned_bus:
            return True
        if timeout is None:
            timeout = self._connection_timeout()
        return self.bus.connected_event.wait(timeout)

    def natural_language_query(self, utterance: str,
                               lang: str) -> "Iterator[str | None]":
        """Answer by injecting the utterance on the OVOS bus and streaming the
        ``speak`` replies until ``ovos.utterance.handled`` (or 10s inactivity),
        correlated by a fresh query-scoped session so they are not reverse-routed."""
        import queue
        import uuid
        qid = uuid.uuid4().hex
        q: queue.Queue = queue.Queue()

        def _on_speak(msg):
            if isinstance(msg, str):
                try:
                    msg = Message.deserialize(msg)
                # Ignore malformed third-party bus payloads.
                except Exception:  # noqa: BLE001
                    return
            if not isinstance(msg, Message):
                return
            if msg.msg_type == "speak" and msg.context.get("query_id") == qid:
                q.put(msg.data.get("utterance", ""))

        def _on_done(msg):
            if isinstance(msg, str):
                try:
                    msg = Message.deserialize(msg)
                # Ignore malformed third-party bus payloads.
                except Exception:  # noqa: BLE001
                    return
            if not isinstance(msg, Message):
                return
            if msg.context.get("query_id") == qid:
                q.put(None)

        bus = self.get_bus()
        bus.on("speak", _on_speak)
        bus.on("ovos.utterance.handled", _on_done)
        try:
            bus.emit(Message(
                "recognizer_loop:utterance",
                {"utterances": [utterance], "lang": lang},
                {"query_id": qid, "session": {"session_id": qid}},
            ))
            while True:
                try:
                    chunk = q.get(timeout=10.0)
                except queue.Empty:
                    yield None
                    return
                if chunk is None:
                    yield None
                    return
                yield chunk
        finally:
            bus.remove("speak", _on_speak)
            bus.remove("ovos.utterance.handled", _on_done)

    # mycroft handlers - from master -> slave
    def handle_send(self, message: Message):
        """ovos wants to send a HiveMessage.

        A device can be both a master and a slave; downstream messages are handled here.
        HiveMindSlaveInternalProtocol handles requests meant to go upstream.
        """
        payload = message.data.get("payload")
        peer = message.data.get("peer")
        msg_type = message.data["msg_type"]

        hmessage = HiveMessage(msg_type, payload=payload, target_peers=[peer])

        if msg_type in [HiveMessageType.PROPAGATE, HiveMessageType.BROADCAST]:
            # snapshot: connect/disconnect mutate self.clients from another thread
            for client in list(self.clients.values()):
                client.send(hmessage)
        elif msg_type == HiveMessageType.ESCALATE:
            # only slaves can escalate, ignore silently
            pass
        elif peer:
            # get() + None check instead of "in" + index: a disconnect between
            # the two would raise KeyError on the OVOS bus thread
            client = self.clients.get(peer)
            if client is not None:
                client.send(hmessage)
            else:
                LOG.error("That client is not connected")
                self.bus.emit(
                    message.forward(
                        "hive.client.send.error",
                        {"error": "That client is not connected", "peer": peer},
                    )
                )

    def handle_internal_mycroft(self, message: str):
        """Forward internal messages to clients if they are the target.

        Client isolation happens here: clients only get responses to their own messages.
        """
        message = Message.deserialize(message)
        target_peers = message.context.get("destination") or []
        if not isinstance(target_peers, list):
            target_peers = [target_peers]

        if target_peers:
            # snapshot: connect/disconnect mutate self.clients from another thread
            connected = list(self.clients.items())
            unmatched = set(target_peers)
            log = _forward_logger()
            for peer, client in connected:
                if peer in target_peers:
                    unmatched.discard(peer)
                    log.debug("%s - destination: %s", message.msg_type, peer)
                    message.context["source"] = "hive"
                    msg = HiveMessage(
                        HiveMessageType.BUS,
                        source_peer=peer,
                        target_peers=target_peers,
                        payload=message,
                    )
                    client.send(msg)
            for peer in unmatched:
                if _is_peer_id(peer):
                    log.warning("%s - destination peer not connected: %s",
                                message.msg_type, peer)
                else:
                    # The common case: OVOS routes to service names such as
                    # "audio" or "skills", so every such message reaches here.
                    log.debug("%s - destination is not a peer: %s",
                              message.msg_type, peer)


# back-compat alias for the old class name shipped from ovos-bus-client
OVOSProtocol = OVOSAgentProtocol


__all__ = [
    "AddBlacklistedIntent",
    "AddBlacklistedSkill",
    "OVOSAgentPolicy",
    "OVOSAgentProtocol",
    "OVOSProtocol",
    "RewriteUtterance",
    "SetContextField",
    "SetSessionField",
    "__version__",
]
