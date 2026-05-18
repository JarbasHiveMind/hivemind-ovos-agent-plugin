"""Sanity tests: public surface and version."""

import hivemind_ovos_agent_plugin as pkg


def test_public_class_exported():
    assert hasattr(pkg, "OVOSAgentProtocol")


def test_backcompat_alias_exists():
    """The old name shipped by ovos-bus-client[hivemind] must still resolve."""
    assert pkg.OVOSProtocol is pkg.OVOSAgentProtocol


def test_version_string():
    assert isinstance(pkg.__version__, str)
    assert pkg.__version__.split(".")[0].isdigit()


def test_is_agent_protocol_subclass():
    from hivemind_plugin_manager.protocols import AgentProtocol
    assert issubclass(pkg.OVOSAgentProtocol, AgentProtocol)


def test_entry_point_registered():
    """The plugin must be discoverable via the hivemind.agent.protocol entry point."""
    try:
        from importlib.metadata import entry_points
    except ImportError:  # pragma: no cover
        from importlib_metadata import entry_points  # type: ignore

    eps = entry_points()
    # python 3.10+ select API
    group = eps.select(group="hivemind.agent.protocol") if hasattr(eps, "select") else eps.get("hivemind.agent.protocol", [])
    names = {ep.name for ep in group}
    assert "hivemind-ovos-agent-plugin" in names
