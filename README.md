# HiveMind OVOS Agent Plugin

OVOS agent protocol plugin for [HiveMind-core](https://github.com/JarbasHiveMind/HiveMind-core).

Bridges HiveMind client messages to an [OpenVoiceOS](https://github.com/OpenVoiceOS) message bus.
HiveMind clients connect to a `hivemind-core` listener, and this plugin forwards their
Mycroft `Message` payloads to a local OVOS bus (and routes OVOS responses back to the
originating client).

## Installation

```bash
pip install hivemind-ovos-agent-plugin
```

Or from source:

```bash
git clone https://github.com/JarbasHiveMind/hivemind-ovos-agent-plugin
cd hivemind-ovos-agent-plugin
pip install -e .
```

## Usage

The plugin registers itself as a HiveMind agent protocol via the
`hivemind.agent.protocol` entry point group, with name `hivemind-ovos-agent-plugin`.

In your `hivemind-core` configuration (`~/.config/hivemind-core/server.json`):

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

`hivemind-core` will discover the plugin via its entry point and instantiate the
`OVOSAgentProtocol` class with the supplied config. The plugin connects to the OVOS
message bus at the given host/port (defaulting to `127.0.0.1:8181`).

This is also the default — if `hivemind-core` is running on the same host as OVOS,
no config change is needed. `hivemind-core` ships with `hivemind-ovos-agent-plugin`
pre-selected and falls back to the `websocket` section of the global OVOS
`mycroft.conf` for the bus address.

### Direct programmatic use

```python
from hivemind_ovos_agent_plugin import OVOSAgentProtocol

agent = OVOSAgentProtocol(config={"host": "127.0.0.1", "port": 8181})
# pass `agent` to your HiveMindListenerProtocol
```

## How it works

The plugin owns two callbacks on the OVOS bus:

- **`hive.send.downstream`** — emitted by OVOS components that want to push a
  `HiveMessage` to a connected HiveMind client. The plugin wraps the payload in a
  `HiveMessage` and dispatches it to the right peer, or fans it out for
  `PROPAGATE`/`BROADCAST` types.
- **`message`** (catch-all) — every internal OVOS bus message is inspected. If its
  `context["destination"]` lists a connected HiveMind peer, the plugin forwards it back
  to that peer wrapped as a `HiveMessageType.BUS` message. This is where **client
  isolation** is enforced: a client never sees responses meant for another client.

Upstream traffic (client → OVOS bus) is handled by `hivemind-core` itself; this plugin
only handles the downstream half.


## Policy plugin

This package also registers as a `hivemind.policy` provider under the name
`hivemind-ovos-agent-policy`. `hivemind-core` runs the policy chain before
forwarding any inbound client message; the built-in `OVOSAgentPolicy` reads
per-client `skill_blacklist` and `intent_blacklist` from the credential store
and injects them into `message.context["session"]` as
`AddBlacklistedSkill` / `AddBlacklistedIntent` mutations.

Six concrete `Mutation` subclasses are available for custom policy plugins:

| Class | Purpose |
|---|---|
| `AddBlacklistedSkill` | Append to `session["blacklisted_skills"]` |
| `AddBlacklistedIntent` | Append to `session["blacklisted_intents"]` |
| `SetSessionField` | Set any key in `message.context["session"]` |
| `SetContextField` | Set a nested path in `message.context` |
| `RewriteUtterance` | Replace utterance text in `recognizer_loop:utterance` messages |

All types are importable from `hivemind_ovos_agent_plugin` directly. See
[`docs/policy.md`](docs/policy.md) for full details.

## Documentation

Full developer documentation lives in [`docs/`](docs/):

- [`docs/architecture.md`](docs/architecture.md) — how the plugin fits between HiveMind
  and OVOS.
- [`docs/configuration.md`](docs/configuration.md) — every config knob.
- [`docs/message_flow.md`](docs/message_flow.md) — end-to-end message lifecycle.
- [`docs/development.md`](docs/development.md) — running tests, releasing.
- [`docs/policy.md`](docs/policy.md) — policy plugin and mutation classes.

## License

Apache 2.0. See [LICENSE.md](LICENSE.md).

## Credits

Developed by [TigreGótico](https://tigregotico.pt) for
[OpenVoiceOS](https://openvoiceos.org).

[![NGI0 Commons Fund](./ngi.png)](https://nlnet.nl/project/OpenVoiceOS)

This project was funded through the [NGI0 Commons Fund](https://nlnet.nl/commonsfund),
a fund established by [NLnet](https://nlnet.nl) with financial support from the
European Commission's [Next Generation Internet](https://ngi.eu) programme, under
the aegis of [DG Communications Networks, Content and Technology](https://commission.europa.eu/about-european-commission/departments-and-executive-agencies/communications-networks-content-and-technology_en)
under grant agreement No [101135429](https://cordis.europa.eu/project/id/101135429).
