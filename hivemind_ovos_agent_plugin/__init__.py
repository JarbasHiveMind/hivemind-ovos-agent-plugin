import dataclasses
import itertools
import threading
from typing import Dict, Any, Iterator, Optional

from ovos_bus_client import MessageBusClient
from ovos_bus_client.message import Message
from ovos_config import Configuration
from ovos_utils.fakebus import FakeBus
from ovos_utils.log import LOG
from pyee import EventEmitter

from hivemind_bus_client.message import HiveMessage, HiveMessageType
from hivemind_plugin_manager.protocols import AgentProtocol

from hivemind_ovos_agent_plugin.policy import (AddBlacklistedIntent,
                                                AddBlacklistedSkill,
                                                OVOSAgentPolicy,
                                                RewriteUtterance,
                                                SetContextField,
                                                SetSessionField)
from hivemind_ovos_agent_plugin.version import __version__


@dataclasses.dataclass()
class OVOSAgentProtocol(AgentProtocol):
    """HiveMind agent protocol that bridges client messages to an OVOS bus."""
    bus: MessageBusClient = dataclasses.field(default_factory=FakeBus)
    config: Dict[str, Any] = dataclasses.field(default_factory=lambda: Configuration().get("websocket", {}))

    def __post_init__(self):
        if not self.bus or isinstance(self.bus, FakeBus):
            self.bus = self._connect_messagebus()
        pool_size = self._configured_pool_size()
        self._bus_pool = [self.bus]
        for _ in range(pool_size - 1):
            self._bus_pool.append(self._connect_messagebus())
        self._bus_cycle = itertools.cycle(self._bus_pool)
        self._bus_cycle_lock = threading.Lock()
        self._inflight_semaphore = threading.BoundedSemaphore(
            self._configured_max_inflight(pool_size)
        )
        self._inflight_timeout = self._configured_inflight_timeout()
        self._client_bus = {}
        self._client_send_locks = {}
        self._client_state_lock = threading.RLock()
        self.register_bus_handlers()

    def _configured_pool_size(self) -> int:
        try:
            pool_size = int(self.config.get("pool_size", 1))
        except (TypeError, ValueError):
            pool_size = 1
        return max(1, pool_size)

    def _configured_max_inflight(self, pool_size: Optional[int] = None) -> int:
        raw_limit = self.config.get("max_inflight", self.config.get("max_in_flight"))
        if raw_limit is not None:
            try:
                return max(1, int(raw_limit))
            except (TypeError, ValueError):
                pass
        try:
            per_bus = int(self.config.get("inflight_per_bus", 4))
        except (TypeError, ValueError):
            per_bus = 4
        return max(1, (pool_size or self._configured_pool_size()) * max(1, per_bus))

    def _configured_inflight_timeout(self) -> float:
        try:
            timeout = float(self.config.get("inflight_timeout", 10))
        except (TypeError, ValueError):
            timeout = 10.0
        return max(0.0, timeout)

    def _inflight_gate(self) -> tuple[threading.BoundedSemaphore, float]:
        semaphore = getattr(self, "_inflight_semaphore", None)
        if semaphore is None:
            pool_size = len(getattr(self, "_bus_pool", []) or [self.bus])
            semaphore = threading.BoundedSemaphore(self._configured_max_inflight(pool_size))
            self._inflight_semaphore = semaphore
        return semaphore, getattr(self, "_inflight_timeout", self._configured_inflight_timeout())

    def _connect_messagebus(self) -> MessageBusClient:
        ovos_bus_address = self.config.get("host") or "127.0.0.1"
        ovos_bus_port = self.config.get("port") or 8181
        timeout = self.config.get("connection_timeout", 10)
        bus = MessageBusClient(
            host=ovos_bus_address,
            port=ovos_bus_port,
            emitter=EventEmitter(),
        )
        bus.run_in_thread()
        # Fail fast instead of blocking forever: a bare ``connected_event.wait()``
        # hangs indefinitely when no OVOS messagebus is reachable, which silently
        # stalls whatever hosts this protocol (e.g. HiveMindService.run() never
        # binds its network listeners). Raise a clear, actionable error instead.
        if not bus.connected_event.wait(timeout):
            bus.close()
            raise ConnectionError(
                f"Could not connect to the OVOS messagebus at "
                f"ws://{ovos_bus_address}:{ovos_bus_port} within {timeout}s. "
                f"Is the OVOS messagebus running? Start it (e.g. 'ovos-messagebus'), "
                f"or set the agent protocol's host/port/connection_timeout in the config."
            )
        return bus

    def register_bus_handlers(self):
        LOG.debug("registering internal OVOS bus handlers")
        for bus in getattr(self, "_bus_pool", [self.bus]):
            bus.on("hive.send.downstream", self._bind_bus_handler(self.handle_send, bus))
            bus.on("message", self._bind_bus_handler(self.handle_internal_mycroft, bus))  # catch all

    def _bind_bus_handler(self, callback, bus):
        """Bind a handler to its OVOS bus so pooled replies keep client affinity."""

        def _handler(message, _bus=bus):
            return callback(message, bus=_bus)

        _handler.__name__ = callback.__name__
        _handler._hivemind_bus = bus
        return _handler

    def _state_lock(self):
        lock = getattr(self, "_client_state_lock", None)
        if lock is None:
            lock = threading.RLock()
            self._client_state_lock = lock
        if not hasattr(self, "_client_bus"):
            self._client_bus = {}
        if not hasattr(self, "_client_send_locks"):
            self._client_send_locks = {}
        return lock

    def _remember_client_bus(self, peer: str, bus: MessageBusClient):
        if not peer:
            return
        with self._state_lock():
            self._client_bus[peer] = bus
            self._client_send_locks.setdefault(peer, threading.RLock())

    def _peer_owns_bus(self, peer: str, bus: Optional[MessageBusClient]) -> bool:
        if bus is None:
            return True
        with self._state_lock():
            assigned_bus = self._client_bus.get(peer)
        return assigned_bus is None or assigned_bus is bus

    def _forget_client(self, peer: str, client):
        try:
            if self.hm_protocol and self.hm_protocol.clients.get(peer) is client:
                self.hm_protocol.clients.pop(peer, None)
                with self._state_lock():
                    self._client_bus.pop(peer, None)
                    self._client_send_locks.pop(peer, None)
        except Exception:
            LOG.exception(f"Failed to forget disconnected client: {peer}")

    def get_bus(self, client=None) -> MessageBusClient:
        """Return the next OVOS bus connection for an injected client message."""
        bus_pool = getattr(self, "_bus_pool", None)
        if not bus_pool or len(bus_pool) == 1:
            bus = self.bus
        else:
            with self._bus_cycle_lock:
                bus = next(self._bus_cycle)
        peer = getattr(client, "peer", None)
        if peer:
            self._remember_client_bus(peer, bus)
        return bus

    def _send_to_client(self, peer: str, client, hmessage: HiveMessage) -> bool:
        """Send a HiveMessage without letting stale sockets break bus dispatch."""
        with self._state_lock():
            lock = self._client_send_locks.setdefault(peer, threading.RLock())
        try:
            with lock:
                client.send(hmessage)
            return True
        except Exception as exc:
            LOG.warning(f"Could not send {hmessage.msg_type} to {peer}: {exc}")
            self._forget_client(peer, client)
            return False

    def natural_language_query(self, utterance: str,
                               lang: str) -> "Iterator[Optional[str]]":
        """Answer by injecting the utterance on the OVOS bus and streaming the
        ``speak`` replies until ``ovos.utterance.handled`` (or 10s inactivity),
        correlated by a fresh query-scoped session so they are not reverse-routed."""
        import queue
        import uuid
        semaphore, inflight_timeout = self._inflight_gate()
        if not semaphore.acquire(timeout=inflight_timeout):
            LOG.warning("Timed out waiting for an OVOS query slot")
            yield None
            return

        qid = uuid.uuid4().hex
        q: "queue.Queue" = queue.Queue()

        def _on_speak(msg):
            if isinstance(msg, str):
                try:
                    msg = Message.deserialize(msg)
                except Exception:
                    return
            if msg.msg_type == "speak" and msg.context.get("query_id") == qid:
                q.put(msg.data.get("utterance", ""))

        def _on_done(msg):
            if isinstance(msg, str):
                try:
                    msg = Message.deserialize(msg)
                except Exception:
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
            try:
                bus.remove("speak", _on_speak)
                bus.remove("ovos.utterance.handled", _on_done)
            finally:
                semaphore.release()

    # mycroft handlers - from master -> slave
    def handle_send(self, message: Message, bus: Optional[MessageBusClient] = None):
        """ovos wants to send a HiveMessage.

        A device can be both a master and a slave; downstream messages are handled here.
        HiveMindSlaveInternalProtocol handles requests meant to go upstream.
        """
        payload = message.data.get("payload")
        peer = message.data.get("peer")
        msg_type = message.data["msg_type"]

        hmessage = HiveMessage(msg_type, payload=payload, target_peers=[peer])

        if msg_type in [HiveMessageType.PROPAGATE, HiveMessageType.BROADCAST]:
            for peer, client in list(self.clients.items()):
                self._send_to_client(peer, client, hmessage)
        elif msg_type == HiveMessageType.ESCALATE:
            # only slaves can escalate, ignore silently
            pass
        elif peer:
            if not self._peer_owns_bus(peer, bus):
                return
            client = self.clients.get(peer)
            if client is not None:
                self._send_to_client(peer, client, hmessage)
            else:
                LOG.error("That client is not connected")
                self.bus.emit(
                    message.forward(
                        "hive.client.send.error",
                        {"error": "That client is not connected", "peer": peer},
                    )
                )

    def handle_internal_mycroft(self, message: str, bus: Optional[MessageBusClient] = None):
        """Forward internal messages to clients if they are the target.

        Client isolation happens here: clients only get responses to their own messages.
        """
        if isinstance(message, str):
            message = Message.deserialize(message)
        target_peers = message.context.get("destination") or []
        if not isinstance(target_peers, list):
            target_peers = [target_peers]

        if target_peers:
            for peer, client in list(self.clients.items()):
                if peer in target_peers:
                    if not self._peer_owns_bus(peer, bus):
                        continue
                    LOG.debug(f"{message.msg_type} - destination: {peer}")
                    message.context["source"] = "hive"
                    msg = HiveMessage(
                        HiveMessageType.BUS,
                        source_peer=peer,
                        target_peers=target_peers,
                        payload=message,
                    )
                    self._send_to_client(peer, client, msg)


# back-compat alias for the old class name shipped from ovos-bus-client
OVOSProtocol = OVOSAgentProtocol


__all__ = ["OVOSAgentProtocol", "OVOSProtocol", "__version__"]
