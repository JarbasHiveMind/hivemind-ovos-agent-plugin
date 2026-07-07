# Configuration

The plugin is configured by the `hivemind-core` `agent_protocol` block.

```json
{
  "agent_protocol": {
    "hivemind-ovos-agent-plugin": {
      "host": "127.0.0.1",
      "port": 8181
    }
  }
}
```

## Keys

| Key    | Type   | Default        | Description                                         |
|--------|--------|----------------|-----------------------------------------------------|
| `host` | string | `127.0.0.1`    | Hostname or IP of the OVOS messagebus.              |
| `port` | int    | `8181`         | TCP port of the OVOS messagebus.                    |
| `pool_size` | int | `1` | Number of OVOS bus client connections to keep open. |
| `inflight_per_bus` | int | `4` | Default request gate per pooled bus connection. |
| `max_inflight` | int | `pool_size * inflight_per_bus` | Explicit total in-flight request cap. |
| `inflight_timeout` | float | `10` | Seconds to wait for a free request slot. |
| `endpoints` / `hosts` | list or comma string | unset | Explicit OVOS bus endpoints. Entries may be `host`, `host:port`, or `{ "host": "...", "port": 8181 }`. |
| `resolve_hosts` / `resolve_all` | bool | `false` | Resolve all DNS A records for `host` and spread the pool across those addresses. Useful with a headless Kubernetes service. |
| `catch_all_responses` / `catch_all_messages` | bool | `true` | Listen to the OVOS bus catch-all `message` event for maximum skill compatibility. |
| `response_events` / `downstream_response_events` | list or comma string | `speak`, `ovos.utterance.speak`, `ovos.utterance.handled`, `complete_intent_failure`, `hive.client.send.error` | Direct reply event names to listen to when catch-all response handling is disabled. |

If no `host`/`port` are supplied, the plugin falls back to the
`websocket` section of the global OVOS `Configuration()`, which is also the standard
location for OVOS bus client settings. This means an OVOS install that already has
`mycroft.conf` configured will work without any extra config in `hivemind-core`.

## Reusing an existing bus connection

If you instantiate `OVOSAgentProtocol` programmatically and pass a non-default `bus`
argument, the plugin will skip its own bus-client setup and use the one you supply.
This is useful for tests and for OVOS deployments that already manage their own bus
client lifecycle.

```python
from ovos_bus_client import MessageBusClient
from hivemind_ovos_agent_plugin import OVOSAgentProtocol

bus = MessageBusClient(host="ovos.lan", port=8181)
bus.run_in_thread()
bus.connected_event.wait()

agent = OVOSAgentProtocol(bus=bus)
```

`hivemind-core` does not currently expose a way to inject a custom bus instance; that
path is for advanced/embedded use only.

## OVOS config interaction

When the plugin falls back to `Configuration().get("websocket", {})` it reads the
same keys OVOS itself reads. There is no separate "hivemind" section in `mycroft.conf`.

## Pooling across runtime endpoints

For horizontally scaled runtimes, prefer explicit endpoints or a headless service:

```json
{
  "agent_protocol": {
    "hivemind-ovos-agent-plugin": {
      "host": "ovos-messagebus-headless.example.svc.cluster.local",
      "port": 8181,
      "resolve_hosts": true,
      "pool_size": 8,
      "inflight_per_bus": 4
    }
  }
}
```

If `resolve_hosts` returns four runtime pod addresses and `pool_size` is `8`, the
plugin opens two bus connections per address and round-robins client traffic across
the full pool. This avoids depending on a ClusterIP service to balance long-lived
websocket connections.

At startup the plugin logs the final bus pool size, resolved endpoints, and request
gate. That line is useful in Kubernetes because it proves whether the listener is
actually connected across runtime pods or only talking to one messagebus backend.

## Direct response-event mode

By default the plugin listens to OVOS' catch-all `message` event so any skill can
reply with a destination-aware custom event. Under high assistant-query load this
also means the HiveMind bridge wakes up for unrelated internal OVOS traffic.

For deployments where downstream replies are known event types, disable the catch-all
handler and register only those response events:

```json
{
  "agent_protocol": {
    "hivemind-ovos-agent-plugin": {
      "host": "ovos-messagebus-headless.example.svc.cluster.local",
      "port": 8181,
      "resolve_hosts": true,
      "pool_size": 8,
      "max_inflight": 64,
      "catch_all_responses": false,
      "response_events": [
        "speak",
        "ovos.utterance.speak",
        "ovos.utterance.handled",
        "complete_intent_failure"
      ]
    }
  }
}
```

This keeps direct replies routed by `context.destination`, but avoids catch-all bus
fanout in the bridge. Keep `catch_all_responses` enabled if installed skills emit
custom destination-aware events that are not listed in `response_events`.
