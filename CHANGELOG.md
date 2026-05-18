# Changelog

## 0.1.0a1

Initial release. Code extracted from `ovos_bus_client.hpm` to its own package.

- `OVOSAgentProtocol` (formerly `OVOSProtocol`) — HiveMind agent protocol that bridges
  HiveMind client messages to an OVOS messagebus.
- `OVOSProtocol` retained as an alias for backwards compatibility with code that
  imported `from ovos_bus_client.hpm import OVOSProtocol`.
- Entry point `hivemind-ovos-agent-plugin` registered under
  `hivemind.agent.protocol` (unchanged from the previous location).
