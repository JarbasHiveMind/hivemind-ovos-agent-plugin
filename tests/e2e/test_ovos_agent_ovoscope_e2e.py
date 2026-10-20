"""Live-OVOS e2e: OVOSAgentProtocol.natural_language_query answers via a REAL
OVOS stack (ovoscope's MiniCroft running an actual skill through the full intent
pipeline), proving the streaming agent contract against genuine skills.

Heavy: boots a real OVOS instance (tens of seconds). Needs ovoscope, ovos-core
and the test skill, all from the `test` extra.
"""
import pytest

_SKILL = "ovos-skill-hello-world.openvoiceos"


@pytest.mark.slow
def test_ovos_agent_answers_via_real_skill():
    from ovoscope import get_minicroft

    from hivemind_ovos_agent_plugin import OVOSAgentProtocol
    craft = get_minicroft([_SKILL])
    try:
        # Supply MiniCroft's in-process bus directly. __post_init__ would
        # reconnect to a ws hub on a FakeBus (and hang without one), so build
        # the instance bare and just hand natural_language_query the live bus.
        agent = OVOSAgentProtocol.__new__(OVOSAgentProtocol)
        agent.bus = craft.bus
        agent._owned_bus = None
        chunks = [c for c in agent.natural_language_query("how are you", "en-US") if c]
        assert chunks, "OVOS agent received no answer from the real skill"
    finally:
        try:
            craft.stop()
        # Cleanup should not replace the test's primary assertion failure.
        except Exception:  # noqa: BLE001,S110
            pass
