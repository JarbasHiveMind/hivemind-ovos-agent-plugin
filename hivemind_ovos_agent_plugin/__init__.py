import dataclasses
import itertools
import socket
import threading
from typing import Dict, Any, Iterator, Optional

from ovos_bus_client import MessageBusClient
from ovos_bus_client.client.client import _maybe_encrypt, json_dumps
from ovos_bus_client.message import Message
from ovos_bus_client.session import Session, SessionManager
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


DEFAULT_RESPONSE_EVENTS = (
    "speak",
    "ovos.utterance.speak",
    "ovos.utterance.handled",
    "complete_intent_failure",
    "hive.client.send.error",
)


@dataclasses.dataclass()
class OVOSAgentProtocol(AgentProtocol):
    """HiveMind agent protocol that bridges client messages to an OVOS bus."""
    bus: MessageBusClient = dataclasses.field(default_factory=FakeBus)
    config: Dict[str, Any] = dataclasses.field(default_factory=lambda: Configuration().get("websocket", {}))

    def __post_init__(self):
        endpoints = self._configured_bus_endpoints()
        self._bus_endpoints = endpoints
        if not self.bus or isinstance(self.bus, FakeBus):
            host, port = endpoints[0]
            self.bus = self._connect_messagebus(host=host, port=port)
        else:
            setattr(self.bus, "_hivemind_ovos_endpoint", ("provided", 0))
        self._bus_pool = [self.bus]
        for host, port in endpoints[1:]:
            self._bus_pool.append(self._connect_messagebus(host=host, port=port))
        self._bus_cycle = itertools.cycle(self._bus_pool)
        self._bus_cycle_lock = threading.Lock()
        self._bus_reconnect_locks = [
            threading.Lock() for _ in self._bus_pool
        ]
        self._inflight_semaphore = threading.BoundedSemaphore(
            self._configured_max_inflight(len(self._bus_pool))
        )
        self._inflight_timeout = self._configured_inflight_timeout()
        self._client_bus = {}
        self._client_send_locks = {}
        self._bus_emit_locks = {}
        self._query_waiters = {}
        self._query_waiter_lock = threading.RLock()
        self._client_state_lock = threading.RLock()
        self._log_bus_pool_ready()
        self.register_bus_handlers()

    def _configured_pool_size(self) -> int:
        try:
            pool_size = int(self.config.get("pool_size", 1))
        except (TypeError, ValueError):
            pool_size = 1
        return max(1, pool_size)

    def _configured_bus_endpoints(self) -> list[tuple[str, int]]:
        endpoints = self._configured_endpoint_list()
        if not endpoints:
            host = self.config.get("host") or "127.0.0.1"
            port = self._configured_bus_port()
            endpoints = self._resolve_bus_host(host, port)

        pool_size = max(self._configured_pool_size(), len(endpoints))
        return list(itertools.islice(itertools.cycle(endpoints), pool_size))

    def _configured_endpoint_list(self) -> list[tuple[str, int]]:
        raw = (
            self.config.get("endpoints")
            or self.config.get("hosts")
            or self.config.get("bus_hosts")
        )
        if not raw:
            return []
        if isinstance(raw, str):
            raw = [item.strip() for item in raw.split(",") if item.strip()]
        if not isinstance(raw, list):
            return []

        endpoints: list[tuple[str, int]] = []
        default_port = self._configured_bus_port()
        for item in raw:
            if isinstance(item, dict):
                host = item.get("host")
                port = item.get("port", default_port)
            else:
                host, port = self._split_endpoint(str(item), default_port)
            if host:
                try:
                    endpoints.append((str(host), int(port)))
                except (TypeError, ValueError):
                    endpoints.append((str(host), default_port))
        return endpoints

    def _split_endpoint(self, value: str, default_port: int) -> tuple[str, int]:
        if value.count(":") == 1:
            host, raw_port = value.rsplit(":", 1)
            try:
                return host, int(raw_port)
            except ValueError:
                pass
        return value, default_port

    def _configured_bus_port(self) -> int:
        try:
            return int(self.config.get("port") or 8181)
        except (TypeError, ValueError):
            return 8181

    def _resolve_bus_host(self, host: str, port: int) -> list[tuple[str, int]]:
        if not (self.config.get("resolve_hosts") or self.config.get("resolve_all")):
            return [(host, port)]
        try:
            infos = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
        except socket.gaierror as exc:
            LOG.warning(f"Could not resolve OVOS bus host {host}: {exc}")
            return [(host, port)]

        seen: set[tuple[str, int]] = set()
        endpoints: list[tuple[str, int]] = []
        for info in infos:
            address = info[4][0]
            endpoint = (address, port)
            if endpoint not in seen:
                seen.add(endpoint)
                endpoints.append(endpoint)
        return endpoints or [(host, port)]

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

    def _configured_reconnect_timeout(self) -> float:
        raw = (
            self.config.get("reconnect_timeout")
            or self.config.get("bus_reconnect_timeout")
        )
        try:
            timeout = float(raw if raw is not None else 5)
        except (TypeError, ValueError):
            timeout = 5.0
        return max(0.0, timeout)

    def _configured_response_timeout(self) -> float:
        raw = (
            self.config.get("response_timeout")
            or self.config.get("query_response_timeout")
            or self.config.get("utterance_timeout")
        )
        try:
            timeout = float(raw if raw is not None else 10)
        except (TypeError, ValueError):
            timeout = 10.0
        return max(0.0, timeout)

    def _configured_catch_all_responses(self) -> bool:
        raw = self.config.get(
            "catch_all_responses",
            self.config.get("catch_all_messages", True),
        )
        if isinstance(raw, str):
            return raw.strip().lower() not in {"0", "false", "no", "off"}
        return bool(raw)

    def _configured_response_events(self) -> tuple[str, ...]:
        raw = (
            self.config.get("response_events")
            or self.config.get("downstream_response_events")
            or DEFAULT_RESPONSE_EVENTS
        )
        if isinstance(raw, str):
            raw = [item.strip() for item in raw.split(",")]
        if not isinstance(raw, (list, tuple, set)):
            raw = DEFAULT_RESPONSE_EVENTS

        events: list[str] = []
        for event in raw:
            event = str(event).strip()
            if event and event not in events:
                events.append(event)
        return tuple(events or DEFAULT_RESPONSE_EVENTS)

    def _inflight_gate(self) -> tuple[threading.BoundedSemaphore, float]:
        semaphore = getattr(self, "_inflight_semaphore", None)
        if semaphore is None:
            pool_size = len(getattr(self, "_bus_pool", []) or [self.bus])
            semaphore = threading.BoundedSemaphore(self._configured_max_inflight(pool_size))
            self._inflight_semaphore = semaphore
        return semaphore, getattr(self, "_inflight_timeout", self._configured_inflight_timeout())

    def _connect_messagebus(self, host: Optional[str] = None, port: Optional[int] = None) -> MessageBusClient:
        ovos_bus_address = host or self.config.get("host") or "127.0.0.1"
        ovos_bus_port = port or self._configured_bus_port()
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
        setattr(bus, "_hivemind_ovos_endpoint", (ovos_bus_address, ovos_bus_port))
        return bus

    def _log_bus_pool_ready(self):
        pool = getattr(self, "_bus_pool", []) or [self.bus]
        endpoints = [
            getattr(bus, "_hivemind_ovos_endpoint", ("unknown", 0))
            for bus in pool
        ]
        LOG.info(
            "OVOS bus pool ready: "
            f"size={len(pool)} endpoints={endpoints} "
            f"max_inflight={self._configured_max_inflight(len(pool))}"
        )

    def register_bus_handlers(self):
        LOG.debug("registering internal OVOS bus handlers")
        for bus in getattr(self, "_bus_pool", [self.bus]):
            self._register_bus_handlers(bus)

    def _register_bus_handlers(self, bus: MessageBusClient):
        bus.on("hive.send.downstream", self._bind_bus_handler(self.handle_send, bus))
        for event in ("speak", "ovos.utterance.handled", "complete_intent_failure"):
            bus.on(event, self._bind_bus_handler(self._handle_query_response, bus))
        if self._configured_catch_all_responses():
            bus.on("message", self._bind_bus_handler(self.handle_internal_mycroft, bus))
            return
        for event in self._configured_response_events():
            bus.on(event, self._bind_bus_handler(self.handle_internal_mycroft, bus))

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
        if not hasattr(self, "_bus_emit_locks"):
            self._bus_emit_locks = {}
        return lock

    def _query_lock(self):
        lock = getattr(self, "_query_waiter_lock", None)
        if lock is None:
            lock = threading.RLock()
            self._query_waiter_lock = lock
        if not hasattr(self, "_query_waiters"):
            self._query_waiters = {}
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

    def _disconnect_client(self, peer: str, client, reason: str):
        disconnect = getattr(client, "disconnect", None)
        if not callable(disconnect):
            return
        try:
            LOG.info(f"Disconnecting stale HiveMind client {peer}: {reason}")
            disconnect()
        except Exception as exc:
            LOG.warning(
                "Failed to disconnect stale HiveMind client "
                f"{peer}: {type(exc).__name__}: {exc!r}"
            )

    def _forget_client(self, peer: str, client, *, disconnect: bool = False,
                       reason: str = "stale downstream socket"):
        try:
            if self.hm_protocol and self.hm_protocol.clients.get(peer) is client:
                self.hm_protocol.clients.pop(peer, None)
                with self._state_lock():
                    self._client_bus.pop(peer, None)
                    self._client_send_locks.pop(peer, None)
                if disconnect:
                    self._disconnect_client(peer, client, reason)
        except Exception:
            LOG.exception(f"Failed to forget disconnected client: {peer}")

    def get_bus(self, client=None) -> MessageBusClient:
        """Return the next OVOS bus connection for an injected client message."""
        _index, bus, candidates = self._next_connected_bus()
        if bus is None:
            errors = []
            for index, candidate in candidates:
                try:
                    bus = self._replace_bus(index, candidate)
                    if self._bus_is_connected(bus):
                        break
                except Exception as exc:
                    errors.append(exc)
                    bus = None
            if bus is None:
                detail = "; ".join(str(exc) for exc in errors[-3:])
                raise ConnectionError(
                    "No connected OVOS messagebus in the agent pool"
                    + (f": {detail}" if detail else "")
                )
        peer = getattr(client, "peer", None)
        if peer:
            self._remember_client_bus(peer, bus)
        return bus

    def _emit_lock_for_bus(self, bus: MessageBusClient):
        with self._state_lock():
            return self._bus_emit_locks.setdefault(id(bus), threading.RLock())

    def _event_is_set(self, event) -> bool:
        if event is None:
            return True
        is_set = getattr(event, "is_set", None)
        if callable(is_set):
            return bool(is_set())
        wait = getattr(event, "wait", None)
        if callable(wait):
            try:
                return bool(wait(0))
            except TypeError:
                return bool(wait())
        return True

    def _bus_socket_is_connected(self, bus: MessageBusClient) -> bool:
        client = getattr(bus, "client", None)
        sock = getattr(client, "sock", None)
        if sock is None:
            return True
        return bool(getattr(sock, "connected", True))

    def _bus_is_connected(self, bus: MessageBusClient) -> bool:
        if isinstance(bus, FakeBus):
            return True
        return (
            self._event_is_set(getattr(bus, "connected_event", None))
            and self._bus_socket_is_connected(bus)
        )

    def _mark_bus_disconnected(self, bus: MessageBusClient, reason: str = ""):
        event = getattr(bus, "connected_event", None)
        clear = getattr(event, "clear", None)
        if callable(clear):
            clear()
        try:
            bus.close()
        except Exception as exc:
            LOG.debug(f"Failed to close disconnected OVOS bus: {exc!r}")
        if reason:
            LOG.warning(f"Marked OVOS messagebus disconnected: {reason}")

    def _bus_reconnect_lock(self, index: int):
        locks = getattr(self, "_bus_reconnect_locks", None)
        pool_size = len(getattr(self, "_bus_pool", []) or [self.bus])
        if locks is None or len(locks) < pool_size:
            locks = [threading.Lock() for _ in range(pool_size)]
            self._bus_reconnect_locks = locks
        return locks[index]

    def _reset_bus_cycle(self):
        self._bus_cycle = itertools.cycle(self._bus_pool)

    def _bus_index(self, target: MessageBusClient) -> int:
        for index, bus in enumerate(getattr(self, "_bus_pool", []) or [self.bus]):
            if bus is target:
                return index
        return 0

    def _next_connected_bus(self):
        pool = getattr(self, "_bus_pool", None) or [self.bus]
        if not hasattr(self, "_bus_cycle_lock"):
            self._bus_cycle_lock = threading.Lock()
        if not hasattr(self, "_bus_cycle"):
            self._bus_cycle = itertools.cycle(pool)
        with self._bus_cycle_lock:
            disconnected = []
            for _ in range(len(pool)):
                bus = next(self._bus_cycle)
                index = self._bus_index(bus)
                if self._bus_is_connected(bus):
                    return index, bus, disconnected
                disconnected.append((index, bus))
            return None, None, disconnected

    def _replace_bus(self, index: int, old_bus: MessageBusClient) -> MessageBusClient:
        lock = self._bus_reconnect_lock(index)
        timeout = self._configured_reconnect_timeout()
        if not lock.acquire(timeout=timeout):
            bus = self._bus_pool[index]
            if self._bus_is_connected(bus):
                return bus
            raise ConnectionError("OVOS messagebus reconnect already in progress")

        try:
            current = self._bus_pool[index]
            if current is not old_bus and self._bus_is_connected(current):
                return current

            endpoints = getattr(self, "_bus_endpoints", None) or [
                getattr(bus, "_hivemind_ovos_endpoint", None)
                for bus in self._bus_pool
            ]
            endpoint = endpoints[index] if index < len(endpoints) else None
            if not endpoint or endpoint[0] == "provided":
                raise ConnectionError("OVOS messagebus endpoint is not reconnectable")

            host, port = endpoint
            LOG.warning(f"Replacing disconnected OVOS messagebus at ws://{host}:{port}")
            new_bus = self._connect_messagebus(host=host, port=port)
            self._register_bus_handlers(new_bus)
            self._bus_pool[index] = new_bus
            if index == 0 or self.bus is old_bus:
                self.bus = self._bus_pool[0]
            with self._state_lock():
                stale_peers = [
                    peer for peer, bus in self._client_bus.items()
                    if bus is old_bus
                ]
                for peer in stale_peers:
                    self._client_bus.pop(peer, None)
            if not hasattr(self, "_bus_cycle_lock"):
                self._bus_cycle_lock = threading.Lock()
            with self._bus_cycle_lock:
                self._reset_bus_cycle()
            try:
                old_bus.close()
            except Exception as exc:
                LOG.debug(f"Failed to close old OVOS bus: {exc!r}")
            return new_bus
        finally:
            lock.release()

    def _send_messagebus_checked(self, bus: MessageBusClient, message: Message) -> None:
        """Send over a real OVOS MessageBusClient without swallowing failures.

        MessageBusClient.emit logs websocket send errors internally and returns
        normally. For HiveMind upstream injection, core needs a truthful failure
        signal so it can avoid counting the message as observed/delivered.
        """
        emit_checked = getattr(bus, "emit_checked", None)
        if callable(emit_checked):
            emit_checked(message)
            return

        client = getattr(bus, "client", None)
        if client is None or not hasattr(client, "send"):
            bus.emit(message)
            return

        connected_event = getattr(bus, "connected_event", None)
        if connected_event is not None and not connected_event.wait(10):
            if not getattr(bus, "started_running", False):
                raise ValueError("Message bus is not running")
            connected_event.wait()

        if "session" not in message.context:
            session_id = getattr(bus, "session_id", "default")
            sess = SessionManager.sessions.get(session_id) or Session(session_id)
            message.context["session"] = sess.serialize()

        if hasattr(message, "serialize"):
            payload = message.serialize()
        else:
            payload = json_dumps(message.__dict__)
        client.send(_maybe_encrypt(payload))

    def _remember_query_waiter(self, query_id: str, queue):
        with self._query_lock():
            self._query_waiters[query_id] = queue

    def _forget_query_waiter(self, query_id: str):
        with self._query_lock():
            self._query_waiters.pop(query_id, None)

    def _handle_query_response(self, message: Message,
                               bus: Optional[MessageBusClient] = None):
        """Route query-scoped OVOS replies to their waiting API call directly."""
        if isinstance(message, str):
            message = Message.deserialize(message)
        query_id = message.context.get("query_id")
        if not query_id:
            return
        with self._query_lock():
            waiter = self._query_waiters.get(query_id)
        if waiter is None:
            return

        if message.msg_type == "speak":
            waiter.put(message.data.get("utterance", ""))
        elif message.msg_type in ("ovos.utterance.handled", "complete_intent_failure"):
            waiter.put(None)

    def emit_client_message(self, message: Message, client=None) -> bool:
        """Inject a HiveMind client message into OVOS with bus affinity.

        Newer HiveMind core versions call this hook when present. Older core
        versions still call ``get_bus(client).emit(message)`` directly.
        """
        last_error = None
        for attempt in range(2):
            bus = self.get_bus(client)
            try:
                with self._emit_lock_for_bus(bus):
                    self._send_messagebus_checked(bus, message)
                return True
            except Exception as exc:
                last_error = exc
                self._mark_bus_disconnected(
                    bus,
                    f"{type(exc).__name__} while injecting {message.msg_type}",
                )
                if attempt == 0:
                    continue
        raise last_error

    # Backwards/alternative hook names for core integrations.
    emit_agent_message = emit_client_message
    inject_agent_message = emit_client_message

    def _send_to_client(self, peer: str, client, hmessage: HiveMessage) -> bool:
        """Send a HiveMessage without letting stale sockets break bus dispatch."""
        with self._state_lock():
            lock = self._client_send_locks.setdefault(peer, threading.RLock())
        try:
            with lock:
                client.send(hmessage)
            return True
        except Exception as exc:
            LOG.warning(
                "Could not send "
                f"{hmessage.msg_type} to {peer}: {type(exc).__name__}: {exc!r}"
            )
            self._forget_client(
                peer,
                client,
                disconnect=True,
                reason=f"send failed for {hmessage.msg_type}",
            )
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
        response_timeout = self._configured_response_timeout()

        bus = None
        self._remember_query_waiter(qid, q)
        try:
            message = Message(
                "recognizer_loop:utterance",
                {"utterances": [utterance], "lang": lang},
                {"query_id": qid, "session": {"session_id": qid}},
            )
            last_error = None
            for attempt in range(2):
                bus = self.get_bus()
                try:
                    with self._emit_lock_for_bus(bus):
                        self._send_messagebus_checked(bus, message)
                    break
                except Exception as exc:
                    last_error = exc
                    self._mark_bus_disconnected(
                        bus,
                        f"{type(exc).__name__} while sending query {qid}",
                    )
                    bus = None
                    if attempt == 0:
                        continue
            else:
                LOG.warning(
                    "Could not send OVOS query "
                    f"for query_id={qid}: {last_error!r}"
                )
                yield None
                return
            while True:
                try:
                    chunk = q.get(timeout=response_timeout)
                except queue.Empty:
                    LOG.warning(
                        "Timed out waiting for OVOS response "
                        f"for query_id={qid} after {response_timeout}s"
                    )
                    yield None
                    return
                if chunk is None:
                    yield None
                    return
                yield chunk
        finally:
            self._forget_query_waiter(qid)
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
            routed_peers = []
            for peer in target_peers:
                if peer and peer not in routed_peers:
                    routed_peers.append(peer)
            for peer in routed_peers:
                if not self._peer_owns_bus(peer, bus):
                    continue
                client = self.clients.get(peer)
                if client is None:
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
