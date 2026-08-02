# HiveMind OVOS Agent Plugin

This plugin connects [HiveMind-core](https://github.com/JarbasHiveMind/HiveMind-core) to an [OpenVoiceOS](https://github.com/OpenVoiceOS) message bus.

HiveMind clients connect to a `hivemind-core` listener. This plugin forwards their Mycroft `Message` payloads to a local OVOS bus, and it routes OVOS responses back to the client that sent the original message.

## Installation

```bash
pip install hivemind-ovos-agent-plugin
```

Or install it from source:

```bash
git clone https://github.com/JarbasHiveMind/hivemind-ovos-agent-plugin
cd hivemind-ovos-agent-plugin
pip install -e .
```

## Usage

The plugin registers itself as a HiveMind agent protocol through the `hivemind.agent.protocol` entry point group, under the name `hivemind-ovos-agent-plugin`.

Add it to your `hivemind-core` configuration (`~/.config/hivemind-core/server.json`):

```json
{
  "agent_protocol": {
    "module": "hivemind-ovos-agent-plugin",
    "hivemind-ovos-agent-plugin": {
      "host": "127.0.0.1",
      "port": 8181
    }
  }
}
```

`hivemind-core` discovers the plugin through its entry point and creates the `OVOSAgentProtocol` class with the config you supply. The plugin then connects to the OVOS message bus at the given host and port. The default host and port are `127.0.0.1:8181`.

This configuration is also the default. If `hivemind-core` runs on the same host as OVOS, you do not need to change the config. `hivemind-core` ships with `hivemind-ovos-agent-plugin` pre-selected. It falls back to the `websocket` section of the global OVOS `mycroft.conf` for the bus address.

### Direct programmatic use

```python
from hivemind_ovos_agent_plugin import OVOSAgentProtocol

agent = OVOSAgentProtocol(config={"host": "127.0.0.1", "port": 8181})
# pass `agent` to your HiveMindListenerProtocol
```

## How it works

The plugin owns two callbacks on the OVOS bus:

- **`hive.send.downstream`**: OVOS components emit this message to push a `HiveMessage` to a connected HiveMind client. The plugin wraps the payload in a `HiveMessage` and sends it to the right peer, or fans it out for `PROPAGATE` and `BROADCAST` types.
- **`message`** (catch-all): the plugin inspects every internal OVOS bus message. If its `context["destination"]` lists a connected HiveMind peer, the plugin forwards the message back to that peer, wrapped as a `HiveMessageType.BUS` message. This is where the plugin enforces **client isolation**: a client never sees responses meant for another client.

Upstream traffic (client to OVOS bus) is handled by `hivemind-core` itself. This plugin only handles the downstream half.

## Policy plugin

This package also registers as a `hivemind.policy` provider under the name `hivemind-ovos-agent-policy`. `hivemind-core` runs the policy chain before it forwards any inbound client message. The built-in `OVOSAgentPolicy` reads the per-client `skill_blacklist` and `intent_blacklist` from the credential store, and it injects them into `message.context["session"]` as `AddBlacklistedSkill` and `AddBlacklistedIntent` mutations.

Five concrete `Mutation` subclasses are available for custom policy plugins:

| Class | Purpose |
|---|---|
| `AddBlacklistedSkill` | Append to `session["blacklisted_skills"]` |
| `AddBlacklistedIntent` | Append to `session["blacklisted_intents"]` |
| `SetSessionField` | Set any key in `message.context["session"]` |
| `SetContextField` | Set a nested path in `message.context` |
| `RewriteUtterance` | Replace utterance text in `recognizer_loop:utterance` messages |

You can import all these types directly from `hivemind_ovos_agent_plugin`. See [`docs/policy.md`](docs/policy.md) for full details.

## Related projects

- [HiveMind-core](https://github.com/JarbasHiveMind/HiveMind-core): the server this plugin connects to.
- [OpenVoiceOS](https://github.com/OpenVoiceOS): the OVOS bus and skill ecosystem this plugin bridges to.

## Documentation

Full developer documentation lives in [`docs/`](docs/):

- [`docs/architecture.md`](docs/architecture.md): how the plugin fits between HiveMind and OVOS.
- [`docs/configuration.md`](docs/configuration.md): every config option.
- [`docs/message_flow.md`](docs/message_flow.md): the end-to-end message lifecycle.
- [`docs/development.md`](docs/development.md): running tests and releasing.
- [`docs/policy.md`](docs/policy.md): the policy plugin and mutation classes.

## License

Apache 2.0. See [LICENSE.md](LICENSE.md).
