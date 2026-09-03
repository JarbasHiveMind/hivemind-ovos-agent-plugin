import copy
import dataclasses
from threading import Lock
from collections.abc import Iterator
from typing import Any

from hivemind_bus_client.message import HiveMessage, HiveMessageType
from ovos_spec_tools import migration_counterpart
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

    @staticmethod
    def _nat_outbound_session(message: Message, client) -> Message:
        """Recover the client's declared session name from its layer1 id.

        hivemind-core derives the layer1 session_id as
        ``f"{session_namespace}:{declared_session_id}"`` at the inbound
        boundary, namespacing each connection's declared session under the
        client's durable, identity-derived ``session_namespace`` so two clients
        (or two multiplexed sessions on one connection) never collide on the
        OVOS bus. That namespace is hub-salted and survives a reconnect, unlike
        the per-connection ``conn_nonce``. Outbound, the declared name is
        recovered per message by stripping this client's namespace prefix --
        not by reading ``client.sess.session_id``, which only holds the
        connection's *current* declared session and would be wrong for any
        earlier-declared, still in-flight session on a multiplexing client.

        Returns a per-client copy; the caller's ``message`` is shared across
        every matching peer and must not be mutated.
        """
        session = message.context.get("session")
        namespace = getattr(client, "session_namespace", None)
        sid = session.get("session_id") if session else None
        prefix = f"{namespace}:" if namespace else None
        if not session or not prefix or not isinstance(sid, str) or not sid.startswith(prefix):
            return message
        declared = sid[len(prefix):]
        if not declared:
            return message
        payload = copy.deepcopy(message)
        payload.context["session"]["session_id"] = declared
        return payload

    def handle_internal_mycroft(self, message: str):
        """Forward internal messages to clients if they are the target.

        Client isolation happens here: clients only get responses to their own messages.
        """
        message = Message.deserialize(message)
        target_peers = message.context.get("destination") or []
        if not isinstance(target_peers, list):
            target_peers = [target_peers]

        # snapshot: connect/disconnect mutate self.clients from another thread
        connected = list(self.clients.items())
        log = _forward_logger()
        delivered = set()

        if target_peers:
            unmatched = set(target_peers)
            for peer, client in connected:
                if peer in target_peers:
                    unmatched.discard(peer)
                    delivered.add(peer)
                    log.debug("%s - destination: %s", message.msg_type, peer)
                    message.context["source"] = "hive"
                    payload = self._nat_outbound_session(message, client)
                    msg = HiveMessage(
                        HiveMessageType.BUS,
                        source_peer=peer,
                        target_peers=target_peers,
                        payload=payload,
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

        # Session-ownership delivery: a hub bus message replaying a connected
        # client's session must reach that client even when destination does
        # not name its CURRENT peer id. Peer ids are per-message NAT-assigned
        # and do not survive a satellite reconnect, so a satellite-scheduled
        # event (e.g. an alarm firing later) carries the client's session but a
        # stale/absent peer id and the peer-id path above drops it. Ownership
        # keys on the client's durable, identity-derived session_namespace
        # (hub-salted, non-secret) which survives a reconnect -- conn_nonce
        # would not, so a session minted before a reconnect would lose its
        # route. The session is the durable path back to the client.
        session = message.context.get("session")
        sid = session.get("session_id") if isinstance(session, dict) else None
        if isinstance(sid, str):
            for peer, client in connected:
                if peer in delivered:
                    continue
                namespace = getattr(client, "session_namespace", None)
                if not (namespace and sid.startswith(f"{namespace}:")):
                    continue
                # ACL posture: the peer-id path above is explicit hub-decided
                # direct addressing (destination names this exact live
                # connection) and is trusted as-is -- allowed_types is a
                # send/receive contract, NOT an exhaustive receive filter, so
                # e.g. ovos.intent.unmatched still reaches peers over that
                # trusted path regardless of allowed_types. Session-ownership
                # delivery is INFERRED from the client owning the session, so
                # only this inferred path is gated deny-by-default by the
                # client's declared allowed_types -- forwarding only message
                # types the client admits.
                if not self._type_allowed(message.msg_type, client):
                    continue
                delivered.add(peer)
                log.debug("%s - session-owned delivery to %s",
                          message.msg_type, peer)
                message.context["source"] = "hive"
                payload = self._nat_outbound_session(message, client)
                msg = HiveMessage(
                    HiveMessageType.BUS,
                    source_peer=peer,
                    target_peers=[peer],
                    payload=payload,
                )
                client.send(msg)

    def _client_allowed_types(self, client) -> list:
        """Resolve a client's allowed message types, DB row winning.

        Mirrors hivemind-core's MessageTypeACLPolicy._allowed_types: the live
        DB row takes precedence so a grant/revocation applies without a
        reconnect; the connection-time snapshot (``client.allowed_types``) is
        used only when no DB is reachable. Any lookup error yields an empty
        list, which the deny-by-default caller treats as "forward nothing".
        """
        db = getattr(self.hm_protocol, "db", None) if self.hm_protocol else None
        if db is None:
            return list(getattr(client, "allowed_types", None) or [])
        try:
            user = client.resolve_user(db)
        except Exception:  # noqa: BLE001
            return []
        if user is None:
            return list(getattr(client, "allowed_types", None) or [])
        return list(getattr(user, "allowed_types", None) or [])

    def _type_allowed(self, msg_type: str, client) -> bool:
        """Deny-by-default, twin-aware allowed_types admission.

        The frame that actually arrives on the firehose is the canonical
        ``ovos.*`` spelling; bus-client does not re-emit a frame under its
        legacy twin. A satellite provisioned with the legacy spelling in
        ``allowed_types`` would therefore never match the canonical frame. The
        migration map (single lookup, no hardcoded type list) bridges the two:
        an ``allowed_types`` entry admits EITHER spelling of a migrated pair.
        Empty/unresolvable allowed_types forwards nothing.
        """
        allowed = self._client_allowed_types(client)
        if not allowed:
            return False
        if msg_type in allowed:
            return True
        twin = migration_counterpart(msg_type)
        return twin is not None and twin in allowed


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
