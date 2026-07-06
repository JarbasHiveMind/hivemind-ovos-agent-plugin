"""Verify the plugin registers the expected handlers on the OVOS bus."""

import hivemind_ovos_agent_plugin as plugin_module
from hivemind_ovos_agent_plugin import OVOSAgentProtocol


class TestBusRegistration:
    def test_registers_hive_send_downstream(self, agent, fake_bus):
        # FakeBus stores listeners on its internal emitter
        listeners = fake_bus.ee.listeners("hive.send.downstream")
        assert any(getattr(l, "__func__", None) is OVOSAgentProtocol.handle_send for l in listeners) or \
               any(getattr(l, "__name__", "") == "handle_send" for l in listeners)

    def test_registers_catch_all_message_listener(self, agent, fake_bus):
        listeners = fake_bus.ee.listeners("message")
        assert any(getattr(l, "__name__", "") == "handle_internal_mycroft" for l in listeners) or \
               any(getattr(l, "__func__", None) is OVOSAgentProtocol.handle_internal_mycroft for l in listeners)

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
        assert all(any(
            event == "hive.send.downstream"
            and getattr(handler, "__name__", "") == "handle_send"
            and getattr(handler, "_hivemind_bus", None) is bus
            for event, handler in bus.handlers
        ) for bus in created)
        assert all(any(
            event == "message"
            and getattr(handler, "__name__", "") == "handle_internal_mycroft"
            and getattr(handler, "_hivemind_bus", None) is bus
            for event, handler in bus.handlers
        ) for bus in created)
        assert [agent.get_bus() for _ in range(4)] == [created[0], created[1], created[2], created[0]]

        client = type("Client", (), {"peer": "ws://alice"})()
        selected = agent.get_bus(client)
        assert agent._client_bus["ws://alice"] is selected

    def test_endpoint_list_spreads_pool_across_hosts(self, monkeypatch):
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

        agent = OVOSAgentProtocol(config={
            "endpoints": [
                {"host": "ovos-bus-0", "port": 8181},
                "ovos-bus-1:8182",
            ],
            "pool_size": 4,
        })

        assert [(bus.host, bus.port) for bus in created] == [
            ("ovos-bus-0", 8181),
            ("ovos-bus-1", 8182),
            ("ovos-bus-0", 8181),
            ("ovos-bus-1", 8182),
        ]
        assert [agent.get_bus() for _ in range(5)] == [
            created[0],
            created[1],
            created[2],
            created[3],
            created[0],
        ]

    def test_resolve_hosts_expands_dns_addresses(self, monkeypatch):
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

        def _getaddrinfo(host, port, *args, **kwargs):
            assert host == "ovos-headless"
            assert port == 8181
            return [
                (None, None, None, None, ("10.42.1.10", port)),
                (None, None, None, None, ("10.42.1.11", port)),
                (None, None, None, None, ("10.42.1.10", port)),
            ]

        monkeypatch.setattr(plugin_module, "MessageBusClient", _Bus)
        monkeypatch.setattr(plugin_module.socket, "getaddrinfo", _getaddrinfo)

        OVOSAgentProtocol(config={
            "host": "ovos-headless",
            "port": 8181,
            "resolve_hosts": True,
        })

        assert [(bus.host, bus.port) for bus in created] == [
            ("10.42.1.10", 8181),
            ("10.42.1.11", 8181),
        ]

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
