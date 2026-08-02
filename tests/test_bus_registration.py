"""Verify the plugin registers the expected handlers on the OVOS bus."""

from unittest.mock import MagicMock

import pytest

from hivemind_ovos_agent_plugin import OVOSAgentProtocol


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

    def test_get_bus_returns_connected_owned_bus(self, agent):
        bus = MagicMock()
        bus.connected_event.wait.return_value = True
        agent.bus = agent._owned_bus = bus
        agent.config["connection_timeout"] = 3

        assert agent.get_bus(MagicMock()) is bus

        bus.connected_event.wait.assert_called_once_with(3.0)

    def test_get_bus_returns_external_bus_untouched(self, agent):
        owned_bus = MagicMock()
        external_bus = MagicMock()
        agent._owned_bus = owned_bus
        agent.bus = external_bus

        assert agent.get_bus(MagicMock()) is external_bus

        external_bus.connected_event.wait.assert_not_called()
        owned_bus.connected_event.wait.assert_not_called()

    def test_get_bus_raises_when_owned_bus_does_not_reconnect(self, agent):
        bus = MagicMock()
        bus.connected_event.wait.return_value = False
        agent.bus = agent._owned_bus = bus
        agent.config["connection_timeout"] = 3

        with pytest.raises(ConnectionError, match="within 3.0s"):
            agent.get_bus()

        bus.connected_event.wait.assert_called_once_with(3.0)
        bus.close.assert_not_called()
