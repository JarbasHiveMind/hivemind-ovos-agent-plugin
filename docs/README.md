# Developer Documentation

Documentation for contributors and integrators of `hivemind-ovos-agent-plugin`.

For end-user installation and usage, see the [top-level README](../README.md).

## Contents

- [`architecture.md`](architecture.md) — where this plugin sits between HiveMind and
  OVOS, and what responsibilities it owns versus delegates.
- [`configuration.md`](configuration.md) — every configuration key the plugin reads,
  with defaults and behaviour notes.
- [`message_flow.md`](message_flow.md) — end-to-end traces of how a client utterance
  reaches OVOS skills and how responses are routed back, including the multi-client
  isolation guarantee.
- [`development.md`](development.md) — local development, testing, and release
  workflow.
