"""Verify the plugin registers the expected handlers on the OVOS bus."""

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
        agent._bus_endpoint = ("127.0.0.1", 8181)
        agent._connect_messagebus = MagicMock(return_value=replacement)
        agent.register_bus_handlers = MagicMock()
        message = Message("recognizer_loop:utterance")

        assert agent.emit_client_message(message) is True

        failed_bus.close.assert_called_once_with()
        agent._connect_messagebus.assert_called_once_with("127.0.0.1", 8181)
        agent.register_bus_handlers.assert_called_once_with(replacement)
        replacement.emit_checked.assert_called_once_with(message)
        assert agent.bus is replacement

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
