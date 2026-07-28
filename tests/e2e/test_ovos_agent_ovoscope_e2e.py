"""Live-OVOS e2e: OVOSAgentProtocol.natural_language_query answers via a REAL
OVOS stack (ovoscope's MiniCroft running an actual skill through the full intent
pipeline), proving the streaming agent contract against genuine skills.

Heavy: boots a real OVOS instance (tens of seconds). Skipped where
ovoscope/ovos-core/the test skill aren't installed.
"""
from importlib.metadata import entry_points

import pytest

pytest.importorskip("ovoscope")

_SKILL = "ovos-skill-hello-world.openvoiceos"


def _has_skill():
    return _SKILL in [e.name for e in entry_points(group="opm.skill")]


@pytest.mark.skipif(not _has_skill(), reason="needs ovos-skill-hello-world")
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
        chunks = [c for c in agent.natural_language_query("how are you", "en-US") if c]
        assert chunks, "OVOS agent received no answer from the real skill"
    finally:
        try:
            craft.stop()
        # Cleanup should not replace the test's primary assertion failure.
        except Exception:  # noqa: BLE001,S110
            pass
