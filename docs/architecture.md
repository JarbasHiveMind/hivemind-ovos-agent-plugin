# Architecture

```
                                                +-----------------------+
                                                |   OVOS Skills /       |
                                                |   pipelines / TTS     |
                                                +----------^------------+
                                                           |
                                              (mycroft Message objects)
                                                           |
+------------------+    HiveMessage     +------------+   OVOS bus    +-----+
| HiveMind client  | <----------------> | hivemind-  | <-----------> | OVOS|
| (satellite, IoT) |   (encrypted ws)   | core       |  (websocket)  | bus |
+------------------+                    +-----^------+               +-----+
                                              |
                                              | AgentProtocol contract
                                              v
                                  +-------------------------+
                                  |  OVOSAgentProtocol      |
                                  |  (this package)         |
                                  +-------------------------+
```

## Responsibilities

This plugin is the **bridge** between `hivemind-core` and a running OVOS bus. It is
loaded by `hivemind-core` via the `hivemind.agent.protocol` entry point group and
fulfils the `AgentProtocol` contract from `hivemind-plugin-manager`.

It owns exactly two responsibilities:

1. **Downstream dispatch**: when an OVOS component emits `hive.send.downstream` on the
   OVOS bus, forward the payload to the correct HiveMind client (or fan out for
   `PROPAGATE`/`BROADCAST` types).
2. **Response routing with client isolation**: when any internal OVOS bus message has
   `context["destination"]` set to a connected HiveMind peer, wrap it as a
   `HiveMessageType.BUS` message and forward to that peer — and **only** that peer.

It does **not** own:

- Decryption, handshake, authentication — `hivemind-core` does this.
- ACL enforcement / policy admission — orchestrated by `hivemind-core`'s policy
  chain (see [issue #85](https://github.com/JarbasHiveMind/HiveMind-core/issues/85)).
  This package contributes `OVOSAgentPolicy` (entry point `hivemind.policy /
  hivemind-ovos-agent-policy`) to that chain; see [`policy.md`](policy.md).
- Binary payload routing — handled by a separate `BinaryDataHandlerProtocol` plugin.
- Upstream traffic (client → OVOS bus) — that is `hivemind-core`'s direct
  responsibility, not the agent protocol's.

## Why this lives in its own package

This module used to be `ovos_bus_client.hpm`, shipped as part of the `ovos-bus-client`
library. That created a dependency-direction smell:

- `ovos-bus-client` is a foundational lib used across the OVOS ecosystem.
- `hpm.py` imports `hivemind-core` and `hivemind-bus-client`.
- That means `ovos-bus-client`, at the bottom of the stack, knew about HiveMind, at the
  top of the stack.

Extracting it makes the layering explicit:

```
ovos-bus-client        <- foundational, OVOS only
hivemind-plugin-manager
hivemind-bus-client
hivemind-core
hivemind-ovos-agent-plugin   <- depends on all of the above; nothing depends on it
```

## Plugin lifecycle

1. `hivemind-core` reads its `agent_protocol` config block at startup.
2. `AgentProtocolFactory.create("hivemind-ovos-agent-plugin", config=...)` resolves the
   entry point to `OVOSAgentProtocol` and instantiates it with the given config.
3. `__post_init__` connects to the OVOS bus and registers the two bus handlers.
4. `hivemind-core` injects the instance as the `agent_protocol` field of its
   `HiveMindListenerProtocol`.
5. From that point on the plugin is purely event-driven: it reacts to OVOS bus
   messages and dispatches HiveMessages.

## Threading

The plugin runs the OVOS bus client on its own background thread (started in
`__post_init__` via `MessageBusClient.run_in_thread()`). Both registered handlers
(`handle_send`, `handle_internal_mycroft`) execute on that bus thread.

The plugin holds no mutable state of its own; the `self.clients` mapping is owned by
the `HiveMindListenerProtocol` and is safe to read from any thread.
