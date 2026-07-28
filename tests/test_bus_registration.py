"""Verify the plugin registers the expected handlers on the OVOS bus."""

import threading
from unittest.mock import MagicMock

from hivemind_ovos_agent_plugin import OVOSAgentProtocol
from ovos_bus_client.message import Message
import pytest


class TestBusRegistration:
    def test_registers_hive_send_downstream(self, agent, fake_bus):
        # FakeBus stores listeners on its internal emitter
        listeners = fake_bus.ee.listeners("hive.send.downstream")
        assert any(l.__func__ is OVOSAgentProtocol.handle_send for l in listeners) or \
               any(getattr(l, "__name__", "") == "handle_send" for l in listeners)

    def test_registers_catch_all_message_listener(self, agent, fake_bus):
        listeners = fake_bus.ee.listeners("message")
        assert any(getattr(l, "__name__", "") == "handle_internal_mycroft" for l in listeners) or \
               any(l.__func__ is OVOSAgentProtocol.handle_internal_mycroft for l in listeners)

    def test_bus_field_is_kept(self, agent, fake_bus):
        """The bus the plugin operates on is the one we wired in."""
        assert agent.bus is fake_bus

    def test_emit_client_message_prefers_checked_bus_api(self, agent):
        bus = MagicMock()
        bus.emit_checked = MagicMock()
        agent.bus = bus
        message = Message("recognizer_loop:utterance")

        assert agent.emit_client_message(message) is True

        bus.emit_checked.assert_called_once_with(message)

    def test_emit_client_message_propagates_external_bus_failure(self, agent):
        bus = MagicMock()
        bus.emit_checked.side_effect = RuntimeError("socket closed")
        agent.bus = bus

        with pytest.raises(RuntimeError, match="socket closed"):
            agent.emit_client_message(Message("speak"))

        bus.emit_checked.assert_called_once()

    def test_emit_client_message_reconnects_owned_bus_once(self, agent):
        failed_bus = MagicMock()
        failed_bus.emit_checked.side_effect = RuntimeError("socket closed")
        replacement = MagicMock()
        replacement.emit_checked = MagicMock()
        agent.bus = failed_bus
        agent._owned_bus = failed_bus
        agent._bus_endpoint = ("127.0.0.1", 8181)
        agent._connect_messagebus = MagicMock(return_value=replacement)
        agent.register_bus_handlers = MagicMock()
        message = Message("recognizer_loop:utterance")

        assert agent.emit_client_message(message) is True

        failed_bus.remove.assert_any_call(
            "hive.send.downstream", agent.handle_send
        )
        failed_bus.remove.assert_any_call(
            "message", agent.handle_internal_mycroft
        )
        failed_bus.close.assert_called_once_with()
        agent._connect_messagebus.assert_called_once_with("127.0.0.1", 8181)
        agent.register_bus_handlers.assert_called_once_with(replacement)
        replacement.emit_checked.assert_called_once_with(message)
        assert agent.bus is replacement

    def test_reassigned_external_bus_is_not_replaced(self, agent):
        owned_bus = MagicMock()
        external_bus = MagicMock()
        external_bus.emit_checked.side_effect = RuntimeError("socket closed")
        agent._owned_bus = owned_bus
        agent._bus_endpoint = ("127.0.0.1", 8181)
        agent.bus = external_bus
        agent._connect_messagebus = MagicMock()

        with pytest.raises(RuntimeError, match="socket closed"):
            agent.emit_client_message(Message("speak"))

        external_bus.close.assert_not_called()
        agent._connect_messagebus.assert_not_called()
        assert agent.bus is external_bus

    def test_replacement_failure_is_closed_and_propagated(self, agent):
        failed_bus = MagicMock()
        failed_bus.emit_checked.side_effect = RuntimeError("socket closed")
        replacement = MagicMock()
        replacement.emit_checked.side_effect = RuntimeError(
            "replacement socket closed"
        )
        agent.bus = agent._owned_bus = failed_bus
        agent._bus_endpoint = ("127.0.0.1", 8181)
        agent._connect_messagebus = MagicMock(return_value=replacement)
        agent.register_bus_handlers = MagicMock()

        with pytest.raises(RuntimeError, match="replacement socket closed"):
            agent.emit_client_message(Message("speak"))

        replacement.close.assert_called_once_with()
        assert agent.bus is replacement
        assert agent._reconnect_blocked_until > 0

    def test_reconnect_wait_does_not_hold_state_lock(self, agent):
        failed_bus = MagicMock()
        failed_bus.emit_checked.side_effect = RuntimeError("socket closed")
        replacement = MagicMock()
        agent.bus = agent._owned_bus = failed_bus
        agent._bus_endpoint = ("127.0.0.1", 8181)
        agent.register_bus_handlers = MagicMock()

        def connect_without_state_lock(host, port):
            assert agent._bus_state_lock.acquire(blocking=False)
            agent._bus_state_lock.release()
            return replacement

        agent._connect_messagebus = MagicMock(
            side_effect=connect_without_state_lock
        )

        assert agent.emit_client_message(Message("speak")) is True

    def test_bus_retirement_waits_for_inflight_write(self, agent):
        send_started = threading.Event()
        release_send = threading.Event()
        retirement_started = threading.Event()
        bus_closed = threading.Event()
        failed_bus = MagicMock()
        replacement = MagicMock()

        def blocking_send(message):
            send_started.set()
            assert release_send.wait(2)

        failed_bus.emit_checked.side_effect = blocking_send
        failed_bus.remove.side_effect = (
            lambda *args: retirement_started.set()
        )
        failed_bus.close.side_effect = bus_closed.set
        agent.bus = agent._owned_bus = failed_bus
        agent._bus_endpoint = ("127.0.0.1", 8181)
        agent._connect_messagebus = MagicMock(return_value=replacement)
        agent.register_bus_handlers = MagicMock()

        sender = threading.Thread(
            target=agent.emit_client_message,
            args=(Message("speak"),),
        )
        replacer = threading.Thread(
            target=agent._replace_owned_bus,
            args=(failed_bus,),
        )
        sender.start()
        assert send_started.wait(1)
        replacer.start()
        assert retirement_started.wait(1)
        assert not bus_closed.is_set()

        release_send.set()
        sender.join(2)
        replacer.join(2)

        assert not sender.is_alive()
        assert not replacer.is_alive()
        assert bus_closed.is_set()

    def test_failed_reconnect_enters_cooldown(self, agent):
        failed_bus = MagicMock()
        failed_bus.emit_checked.side_effect = RuntimeError("socket closed")
        agent.bus = agent._owned_bus = failed_bus
        agent._bus_endpoint = ("127.0.0.1", 8181)
        agent.config["reconnect_cooldown"] = 30
        agent._connect_messagebus = MagicMock(
            side_effect=ConnectionError("OVOS bus unavailable")
        )

        with pytest.raises(ConnectionError, match="OVOS bus unavailable"):
            agent.emit_client_message(Message("speak"))
        with pytest.raises(ConnectionError, match="cooldown"):
            agent.emit_client_message(Message("speak"))

        agent._connect_messagebus.assert_called_once_with("127.0.0.1", 8181)
        failed_bus.emit_checked.assert_called_once()

    def test_raw_websocket_fallback_propagates_send_error(self, agent):
        bus = MagicMock(spec=[])
        bus.client = MagicMock()
        bus.client.send.side_effect = RuntimeError("socket closed")
        bus.connected_event = MagicMock()
        bus.connected_event.wait.return_value = True
        bus.session_id = "test-session"
        agent.bus = bus

        with pytest.raises(RuntimeError, match="socket closed"):
            agent.emit_client_message(Message("speak"))

        bus.client.send.assert_called_once()
