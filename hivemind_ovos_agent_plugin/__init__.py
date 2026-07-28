import dataclasses
import threading
import time
from collections.abc import Iterator
from typing import Any, Optional

from ovos_bus_client import MessageBusClient
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

try:
    # This helper is not present in every supported ovos-bus-client release.
    # Cache it once so the compatibility path does not import per message.
    from ovos_bus_client.client.client import _maybe_encrypt as _encrypt_bus_payload
except ImportError:
    _encrypt_bus_payload = None


@dataclasses.dataclass()
class OVOSAgentProtocol(AgentProtocol):
    """HiveMind agent protocol that bridges client messages to an OVOS bus."""
    bus: MessageBusClient = dataclasses.field(default_factory=FakeBus)
    config: dict[str, Any] = dataclasses.field(
        default_factory=lambda: Configuration().get("websocket", {})
    )
    _bus_state_lock: threading.RLock = dataclasses.field(
        default_factory=threading.RLock, init=False, repr=False, compare=False
    )
    _owned_bus: Optional[MessageBusClient] = dataclasses.field(
        default=None, init=False, repr=False, compare=False
    )
    _bus_endpoint: Optional[tuple[str, int]] = dataclasses.field(
        default=None, init=False, repr=False, compare=False
    )
    _reconnect_blocked_until: float = dataclasses.field(
        default=0.0, init=False, repr=False, compare=False
    )

    def __post_init__(self):
        if not self.bus or isinstance(self.bus, FakeBus):
            ovos_bus_address = self.config.get("host") or "127.0.0.1"
            ovos_bus_port = self.config.get("port") or 8181
            self._bus_endpoint = (ovos_bus_address, ovos_bus_port)
            self.bus = self._connect_messagebus(ovos_bus_address,
                                                ovos_bus_port)
            self._owned_bus = self.bus
        self.register_bus_handlers()

    def _connect_messagebus(self, host: str, port: int) -> MessageBusClient:
        timeout = self.config.get("connection_timeout", 10)
        bus = MessageBusClient(
            host=host,
            port=port,
            emitter=EventEmitter(),
        )
        bus.run_in_thread()
        # Fail fast instead of blocking forever: a bare
        # ``connected_event.wait()`` hangs indefinitely when no OVOS
        # messagebus is reachable, which silently stalls whatever hosts this
        # protocol (e.g. HiveMindService.run() never binds its listeners).
        if not bus.connected_event.wait(timeout):
            bus.close()
            raise ConnectionError(
                f"Could not connect to the OVOS messagebus at "
                f"ws://{host}:{port} within {timeout}s. "
                f"Is the OVOS messagebus running? Start it (e.g. "
                f"'ovos-messagebus'), or set the agent protocol's "
                f"host/port/connection_timeout in the config."
            )
        return bus

    def register_bus_handlers(self, bus: Optional[MessageBusClient] = None):
        LOG.debug("registering internal OVOS bus handlers")
        bus = bus or self.bus
        bus.on("hive.send.downstream", self.handle_send)
        bus.on("message", self.handle_internal_mycroft)  # catch all

    def _unregister_bus_handlers(self, bus: MessageBusClient) -> None:
        """Detach this agent before retiring a stale bus connection."""
        try:
            bus.remove("hive.send.downstream", self.handle_send)
            bus.remove("message", self.handle_internal_mycroft)
        except Exception as error:
            LOG.debug(f"Failed to detach stale OVOS bus handlers: {error!r}")

    def _delivery_timeout(self) -> float:
        raw = self.config.get("delivery_timeout", 10)
        try:
            return max(0.0, float(raw))
        except (TypeError, ValueError):
            return 10.0

    def _reconnect_cooldown(self) -> float:
        raw = self.config.get("reconnect_cooldown", 5)
        try:
            return max(0.0, float(raw))
        except (TypeError, ValueError):
            return 5.0

    def _send_messagebus_checked(self, bus: MessageBusClient,
                                 message: Message) -> None:
        """Send one message without swallowing a closed websocket error."""
        emit_checked = getattr(bus, "emit_checked", None)
        if callable(emit_checked):
            emit_checked(message)
            return

        client = getattr(bus, "client", None)
        if client is None or not hasattr(client, "send"):
            # FakeBus and compatible in-process buses have no websocket and
            # already propagate their own delivery errors.
            bus.emit(message)
            return

        connected = getattr(bus, "connected_event", None)
        if connected is not None and not connected.wait(
                self._delivery_timeout()):
            raise ConnectionError("OVOS messagebus is not connected")

        if "session" not in message.context:
            session_id = getattr(bus, "session_id", "default")
            session = (SessionManager.sessions.get(session_id)
                       or Session(session_id))
            message.context["session"] = session.serialize()

        payload = message.serialize()
        if _encrypt_bus_payload is not None:
            payload = _encrypt_bus_payload(payload)
        client.send(payload)

    def _replace_owned_bus(self, failed_bus: MessageBusClient
                           ) -> MessageBusClient:
        """Replace the exact bus created by this plugin.

        The state lock serializes ownership changes only; message writes stay
        outside it so one blocked socket cannot stall unrelated senders.
        """
        with self._bus_state_lock:
            if failed_bus is not self._owned_bus:
                # Another sender already replaced this formerly-owned bus.
                if self._owned_bus is not None and self.bus is self._owned_bus:
                    return self._owned_bus
                raise ConnectionError(
                    "Externally supplied OVOS messagebus cannot be replaced"
                )

            if self.bus is not failed_bus:
                # The application reassigned the active bus while this send
                # was in flight. Retire only the plugin-owned stale bus and
                # preserve the caller-supplied replacement.
                self._owned_bus = None
                self._unregister_bus_handlers(failed_bus)
                try:
                    failed_bus.close()
                except Exception as error:
                    LOG.debug(
                        f"Failed to close superseded OVOS bus: {error!r}"
                    )
                return self.bus

            endpoint = self._bus_endpoint
            if endpoint is None:
                raise ConnectionError(
                    "Externally supplied OVOS messagebus cannot be replaced"
                )

            now = time.monotonic()
            if now < self._reconnect_blocked_until:
                raise ConnectionError(
                    "OVOS messagebus reconnect cooldown is active"
                )

            try:
                self._unregister_bus_handlers(failed_bus)
                failed_bus.close()
            except Exception as error:
                LOG.debug(f"Failed to close disconnected OVOS bus: {error!r}")

            host, port = endpoint
            LOG.warning(f"Reconnecting OVOS messagebus at ws://{host}:{port}")
            try:
                replacement = self._connect_messagebus(host, port)
            except Exception:
                self._reconnect_blocked_until = (
                    time.monotonic() + self._reconnect_cooldown()
                )
                raise

            self.register_bus_handlers(replacement)
            self.bus = self._owned_bus = replacement
            self._reconnect_blocked_until = 0.0
            return replacement

    def emit_client_message(self, message: Message, client=None) -> bool:
        """Deliver an admitted HiveMind client message to the OVOS runtime.

        ``MessageBusClient.emit`` logs and swallows websocket send failures.
        Core needs a truthful result, so use a checked write and replace the
        plugin-owned connection once before reporting failure.
        """
        bus = self.get_bus(client)
        with self._bus_state_lock:
            owned_at_send = bus is self._owned_bus
            reconnect_blocked = (
                owned_at_send
                and time.monotonic() < self._reconnect_blocked_until
            )
        if reconnect_blocked:
            raise ConnectionError(
                "OVOS messagebus reconnect cooldown is active"
            )

        try:
            self._send_messagebus_checked(bus, message)
        except Exception as delivery_error:
            if not owned_at_send:
                raise
            LOG.warning(
                "OVOS messagebus delivery failed with "
                f"{type(delivery_error).__name__}; reconnecting once"
            )
            bus = self._replace_owned_bus(bus)
            try:
                self._send_messagebus_checked(bus, message)
            except Exception:
                close_owned_bus = False
                with self._bus_state_lock:
                    if bus is self._owned_bus:
                        self._reconnect_blocked_until = (
                            time.monotonic() + self._reconnect_cooldown()
                        )
                        close_owned_bus = True
                if close_owned_bus:
                    try:
                        bus.close()
                    except Exception as error:
                        LOG.debug("Failed to close replacement OVOS bus: "
                                  f"{error!r}")
                raise
        return True


    def natural_language_query(self, utterance: str,
                               lang: str) -> Iterator[Optional[str]]:
        """Answer by injecting the utterance on the OVOS bus and streaming the
        ``speak`` replies until ``ovos.utterance.handled`` (or 10s inactivity),
        correlated by a fresh query-scoped session so they are not reverse-routed."""
        import queue
        import uuid
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

        self.bus.on("speak", _on_speak)
        self.bus.on("ovos.utterance.handled", _on_done)
        try:
            self.bus.emit(Message(
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
            self.bus.remove("speak", _on_speak)
            self.bus.remove("ovos.utterance.handled", _on_done)

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
            for peer in self.clients:
                self.clients[peer].send(hmessage)
        elif msg_type == HiveMessageType.ESCALATE:
            # only slaves can escalate, ignore silently
            pass
        elif peer:
            if peer in self.clients:
                client = self.clients[peer]
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
            for peer, client in self.clients.items():
                if peer in target_peers:
                    LOG.debug(f"{message.msg_type} - destination: {peer}")
                    message.context["source"] = "hive"
                    msg = HiveMessage(
                        HiveMessageType.BUS,
                        source_peer=peer,
                        target_peers=target_peers,
                        payload=message,
                    )
                    client.send(msg)


# back-compat alias for the old class name shipped from ovos-bus-client
OVOSProtocol = OVOSAgentProtocol


__all__ = ["OVOSAgentProtocol", "OVOSProtocol", "__version__"]
