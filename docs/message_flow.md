# Message Flow

This document traces messages through the system. The plugin only handles the downstream half (OVOS to client) and a small slice of fan-out and direct dispatch. Upstream traffic (client to OVOS bus) is `hivemind-core`'s job.

## Upstream: HiveMind client to OVOS bus

`hivemind-core` handles this entirely. The plugin is not involved.

```
client                hivemind-core                 OVOS bus
  |                        |                            |
  |---HiveMessage(BUS)---->|                            |
  |                        |  decrypt, verify, policy   |
  |                        |---Mycroft Message--------->|
  |                        |                            |--> skills, pipelines, ...
```

## Downstream A: skill replies, TTS, and similar

OVOS skills typically emit responses with `context["destination"]` set to the original client peer. The plugin's catch-all `message` handler picks these up.

```
OVOS skill                  OVOSAgentProtocol             hivemind-core            client
    |                              |                           |                     |
    |---emit Message --------------|                           |                     |
    |    context.destination=peer  |                           |                     |
    |                              |--match peer in self.clients                     |
    |                              |--wrap as HiveMessage(BUS)----------------------->|
```

### Client isolation guarantee

`handle_internal_mycroft` filters by `context["destination"]`:

```python
target_peers = message.context.get("destination") or []
if target_peers:
    for peer, client in self.clients.items():
        if peer in target_peers:
            ...
            client.send(msg)
```

The plugin drops a message with no `destination`. It also drops a message whose `destination` does not match any connected client. A connected client never sees a message addressed to a different client. This is the single security-critical invariant this plugin enforces, and tests in `tests/test_isolation.py` exercise it.

## Downstream B: explicit `hive.send.downstream`

An OVOS component, or any code on the bus, can explicitly push a HiveMessage to a peer:

```python
bus.emit(Message("hive.send.downstream", {
    "peer": "ws://1.2.3.4:5678",
    "msg_type": "speak",
    "payload": some_mycroft_message,
}))
```

`handle_send` routes by `msg_type`:

| `msg_type`                       | Behavior                                                            |
|----------------------------------|----------------------------------------------------------------------|
| `HiveMessageType.PROPAGATE`      | Broadcasts to all connected clients.                              |
| `HiveMessageType.BROADCAST`      | Broadcasts to all connected clients.                              |
| `HiveMessageType.ESCALATE`       | Silently ignored. Only slaves can escalate; this is handled elsewhere.       |
| Anything else with `peer` set    | Sent to that peer if connected; emits `hive.client.send.error` if not.|
| Anything else with `peer` unset  | Silently dropped.                                                    |

### Error reporting

When a requested `peer` is not connected, the plugin emits a Mycroft `hive.client.send.error` message back onto the OVOS bus, carrying `{error, peer}`. Callers can listen for this message to find out whether dispatch succeeded.

## Out of scope

- **Binary payloads** (audio streaming, files): a separate `BinaryDataHandlerProtocol` plugin owns these.
- **Hive routing types** (`HANDSHAKE`, `HELLO`): these never reach this plugin. `hivemind-core` consumes them internally.
- **Policy decisions**: `hivemind-core`'s policy chain runs before this plugin sees a message. By the time the OVOS bus emits anything, the message has already been authorized.

---
[← Configuration](configuration.md) · [Home](README.md) · [Development →](development.md)
