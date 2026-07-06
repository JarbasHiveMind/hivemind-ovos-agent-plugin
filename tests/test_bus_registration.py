"""Verify the plugin registers the expected handlers on the OVOS bus."""

import hivemind_ovos_agent_plugin as plugin_module
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

    def test_pool_size_opens_and_round_robins_bus_connections(self, monkeypatch):
        created = []

        class _ConnectedEvent:
            def wait(self, _timeout):
                return True

        class _Bus:
            def __init__(self, host=None, port=None, emitter=None):
                self.host = host
                self.port = port
                self.emitter = emitter
                self.connected_event = _ConnectedEvent()
                self.handlers = []
                created.append(self)

            def run_in_thread(self):
                return None

            def on(self, event, handler):
                self.handlers.append((event, handler))

            def close(self):
                return None

        monkeypatch.setattr(plugin_module, "MessageBusClient", _Bus)

        agent = OVOSAgentProtocol(config={"host": "ovos-bus", "port": 8181, "pool_size": 3})

        assert len(created) == 3
        assert agent.bus is created[0]
        assert [bus.host for bus in created] == ["ovos-bus"] * 3
        assert all(("hive.send.downstream", agent.handle_send) in bus.handlers for bus in created)
        assert all(("message", agent.handle_internal_mycroft) in bus.handlers for bus in created)
        assert [agent.get_bus() for _ in range(4)] == [created[0], created[1], created[2], created[0]]

    def test_inflight_limit_defaults_from_pool_size(self, fake_bus):
        agent = OVOSAgentProtocol.__new__(OVOSAgentProtocol)
        agent.bus = fake_bus
        agent.config = {"pool_size": 3}

        assert agent._configured_max_inflight(pool_size=3) == 12

    def test_inflight_limit_can_be_overridden(self, fake_bus):
        agent = OVOSAgentProtocol.__new__(OVOSAgentProtocol)
        agent.bus = fake_bus
        agent.config = {"pool_size": 3, "max_inflight": 7}

        assert agent._configured_max_inflight(pool_size=3) == 7
