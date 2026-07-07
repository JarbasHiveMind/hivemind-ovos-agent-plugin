"""Verify the plugin registers the expected handlers on the OVOS bus."""

import hivemind_ovos_agent_plugin as plugin_module
from hivemind_ovos_agent_plugin import OVOSAgentProtocol
from ovos_bus_client.message import Message
import pytest


class TestBusRegistration:
    def test_registers_hive_send_downstream(self, agent, fake_bus):
        # FakeBus stores listeners on its internal emitter
        listeners = fake_bus.ee.listeners("hive.send.downstream")
        assert any(
            getattr(listener, "__func__", None) is OVOSAgentProtocol.handle_send
            for listener in listeners
        ) or any(
            getattr(listener, "__name__", "") == "handle_send"
            for listener in listeners
        )

    def test_registers_catch_all_message_listener(self, agent, fake_bus):
        listeners = fake_bus.ee.listeners("message")
        assert any(
            getattr(listener, "__name__", "") == "handle_internal_mycroft"
            for listener in listeners
        ) or any(
            getattr(listener, "__func__", None) is OVOSAgentProtocol.handle_internal_mycroft
            for listener in listeners
        )

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

    def test_get_bus_skips_disconnected_pool_member(self):
        class _Event:
            def __init__(self, connected):
                self.connected = connected

            def is_set(self):
                return self.connected

        class _Bus:
            def __init__(self, connected):
                self.connected_event = _Event(connected)
                self.client = type(
                    "Client",
                    (),
                    {"sock": type("Sock", (), {"connected": connected})()},
                )()

        closed = _Bus(False)
        open_bus = _Bus(True)
        agent = OVOSAgentProtocol.__new__(OVOSAgentProtocol)
        agent.bus = closed
        agent.config = {}
        agent._bus_pool = [closed, open_bus]
        agent._bus_cycle = plugin_module.itertools.cycle(agent._bus_pool)
        agent._bus_cycle_lock = plugin_module.threading.Lock()
        agent._bus_reconnect_locks = [
            plugin_module.threading.Lock(),
            plugin_module.threading.Lock(),
        ]
        agent._client_bus = {}
        agent._client_send_locks = {}
        agent._bus_emit_locks = {}
        agent._client_state_lock = plugin_module.threading.RLock()

        assert agent.get_bus() is open_bus

    def test_get_bus_replaces_disconnected_pool_member(self):
        class _Event:
            def __init__(self, connected):
                self.connected = connected

            def is_set(self):
                return self.connected

            def wait(self, _timeout):
                return self.connected

        class _Bus:
            def __init__(self, connected, name):
                self.connected_event = _Event(connected)
                self.client = type(
                    "Client",
                    (),
                    {"sock": type("Sock", (), {"connected": connected})()},
                )()
                self.name = name
                self.closed = False
                self.handlers = []

            def on(self, event, handler):
                self.handlers.append((event, handler))

            def close(self):
                self.closed = True

        old_bus = _Bus(False, "old")
        new_bus = _Bus(True, "new")
        agent = OVOSAgentProtocol.__new__(OVOSAgentProtocol)
        agent.bus = old_bus
        agent.config = {"bus_reconnect_timeout": 0.01}
        agent._bus_pool = [old_bus]
        agent._bus_endpoints = [("ovos-bus", 8181)]
        agent._bus_cycle = plugin_module.itertools.cycle(agent._bus_pool)
        agent._bus_cycle_lock = plugin_module.threading.Lock()
        agent._bus_reconnect_locks = [plugin_module.threading.Lock()]
        agent._client_bus = {"ws://stale": old_bus}
        agent._client_send_locks = {}
        agent._bus_emit_locks = {}
        agent._client_state_lock = plugin_module.threading.RLock()
        agent._connect_messagebus = lambda host, port: new_bus

        selected = agent.get_bus()

        assert selected is new_bus
        assert agent.bus is new_bus
        assert agent._bus_pool == [new_bus]
        assert "ws://stale" not in agent._client_bus
        assert old_bus.closed is True
        assert any(event == "message" for event, _ in new_bus.handlers)

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

    def test_emit_client_message_uses_checked_send_and_affinity(self):
        sent = []

        class _ConnectedEvent:
            def wait(self, _timeout):
                return True

        class _Client:
            def send(self, payload):
                sent.append(payload)

        class _Bus:
            client = _Client()
            connected_event = _ConnectedEvent()
            session_id = "test-session"
            started_running = True

        bus = _Bus()
        agent = OVOSAgentProtocol.__new__(OVOSAgentProtocol)
        agent.bus = bus
        agent.config = {}
        agent._bus_pool = [bus]
        agent._client_bus = {}
        agent._client_send_locks = {}
        agent._bus_emit_locks = {}
        agent._client_state_lock = plugin_module.threading.RLock()

        client = type("Client", (), {"peer": "ws://alice"})()
        message = Message("recognizer_loop:utterance", {"utterances": ["hi"]}, {})

        assert agent.emit_client_message(message, client) is True
        assert agent._client_bus["ws://alice"] is bus
        assert sent
        assert "recognizer_loop:utterance" in sent[0]

    def test_emit_client_message_prefers_bus_emit_checked(self):
        class _Bus:
            def __init__(self):
                self.messages = []

            def emit_checked(self, message):
                self.messages.append(message)

        bus = _Bus()
        agent = OVOSAgentProtocol.__new__(OVOSAgentProtocol)
        agent.bus = bus
        agent.config = {}
        agent._bus_pool = [bus]
        agent._client_bus = {}
        agent._client_send_locks = {}
        agent._bus_emit_locks = {}
        agent._client_state_lock = plugin_module.threading.RLock()

        message = Message("recognizer_loop:utterance", {"utterances": ["hi"]}, {})

        assert agent.emit_client_message(message, None) is True
        assert bus.messages == [message]

    def test_emit_client_message_raises_send_failures(self):
        class _ConnectedEvent:
            def wait(self, _timeout):
                return True

        class _Client:
            def send(self, _payload):
                raise RuntimeError("socket closed")

        class _Bus:
            client = _Client()
            connected_event = _ConnectedEvent()
            session_id = "test-session"
            started_running = True

        bus = _Bus()
        agent = OVOSAgentProtocol.__new__(OVOSAgentProtocol)
        agent.bus = bus
        agent.config = {}
        agent._bus_pool = [bus]
        agent._client_bus = {}
        agent._client_send_locks = {}
        agent._bus_emit_locks = {}
        agent._client_state_lock = plugin_module.threading.RLock()

        with pytest.raises(RuntimeError, match="socket closed"):
            agent.emit_client_message(Message("speak", {}, {}), None)
