"""Tests for hivemind_ovos_agent_plugin.policy — OVOS-specific mutations
and the OVOSAgentPolicy built-in.

The mutation tests cover behaviour previously held in
hivemind-plugin-manager#27 (before the OVOS-specific concrete classes
moved here).
"""
from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock

from hivemind_ovos_agent_plugin.policy import (AddBlacklistedIntent,
                                                AddBlacklistedMessageType,
                                                AddBlacklistedSkill,
                                                OVOSAgentPolicy,
                                                RewriteUtterance,
                                                SetContextField,
                                                SetSessionField)


class _FakeMessage:
    """Minimal stand-in for ovos_bus_client.message.Message."""

    def __init__(self, msg_type="t", data=None, context=None):
        self.msg_type = msg_type
        self.data = data if data is not None else {}
        self.context = context if context is not None else {}


# ---------------------------------------------------------------------------
# Mutations
# ---------------------------------------------------------------------------

class TestAddBlacklistedSkill(unittest.TestCase):
    def test_creates_list_when_missing(self):
        msg = _FakeMessage()
        AddBlacklistedSkill("weather.skill").apply(msg, client=None)
        self.assertEqual(
            msg.context["session"]["blacklisted_skills"],
            ["weather.skill"],
        )

    def test_appends_to_existing(self):
        msg = _FakeMessage(context={"session": {"blacklisted_skills": ["a"]}})
        AddBlacklistedSkill("b").apply(msg, client=None)
        self.assertEqual(
            msg.context["session"]["blacklisted_skills"], ["a", "b"],
        )

    def test_dedupes(self):
        msg = _FakeMessage(context={"session": {"blacklisted_skills": ["a"]}})
        AddBlacklistedSkill("a").apply(msg, client=None)
        self.assertEqual(msg.context["session"]["blacklisted_skills"], ["a"])

    def test_recovers_from_non_dict_session(self):
        msg = _FakeMessage(context={"session": "garbage"})
        AddBlacklistedSkill("x").apply(msg, client=None)
        self.assertEqual(msg.context["session"], {"blacklisted_skills": ["x"]})

    def test_recovers_from_non_dict_context(self):
        msg = _FakeMessage(context="garbage")
        AddBlacklistedSkill("x").apply(msg, client=None)
        self.assertEqual(msg.context, {"session": {"blacklisted_skills": ["x"]}})


class TestAddBlacklistedIntent(unittest.TestCase):
    def test_dedupes_and_appends(self):
        msg = _FakeMessage()
        AddBlacklistedIntent("intent.a").apply(msg, client=None)
        AddBlacklistedIntent("intent.b").apply(msg, client=None)
        AddBlacklistedIntent("intent.a").apply(msg, client=None)
        self.assertEqual(
            msg.context["session"]["blacklisted_intents"],
            ["intent.a", "intent.b"],
        )


class TestAddBlacklistedMessageType(unittest.TestCase):
    def test_appends(self):
        msg = _FakeMessage()
        AddBlacklistedMessageType("speak").apply(msg, client=None)
        self.assertEqual(
            msg.context["session"]["blacklisted_message_types"], ["speak"],
        )


class TestSetSessionField(unittest.TestCase):
    def test_sets_field(self):
        msg = _FakeMessage()
        SetSessionField("lang", "pt-pt").apply(msg, client=None)
        self.assertEqual(msg.context["session"]["lang"], "pt-pt")

    def test_overwrites_existing(self):
        msg = _FakeMessage(context={"session": {"lang": "en-us"}})
        SetSessionField("lang", "de-de").apply(msg, client=None)
        self.assertEqual(msg.context["session"]["lang"], "de-de")


class TestSetContextField(unittest.TestCase):
    def test_sets_top_level_key(self):
        msg = _FakeMessage()
        SetContextField(("source",), "policy").apply(msg, client=None)
        self.assertEqual(msg.context["source"], "policy")

    def test_creates_nested_path(self):
        msg = _FakeMessage()
        SetContextField(("a", "b", "c"), 1).apply(msg, client=None)
        self.assertEqual(msg.context["a"]["b"]["c"], 1)

    def test_overwrites_non_dict_intermediate(self):
        msg = _FakeMessage(context={"a": "scalar"})
        SetContextField(("a", "b"), 1).apply(msg, client=None)
        self.assertEqual(msg.context["a"], {"b": 1})

    def test_empty_path_is_noop(self):
        msg = _FakeMessage(context={"k": "v"})
        SetContextField((), "anything").apply(msg, client=None)
        self.assertEqual(msg.context, {"k": "v"})

    def test_non_dict_context_is_replaced(self):
        msg = _FakeMessage(context="garbage")
        SetContextField(("k",), "v").apply(msg, client=None)
        self.assertEqual(msg.context, {"k": "v"})


class TestRewriteUtterance(unittest.TestCase):
    def test_rewrites_recognizer_loop_utterance(self):
        msg = _FakeMessage(
            "recognizer_loop:utterance",
            data={"utterances": ["old"], "lang": "en-us"},
        )
        RewriteUtterance("new").apply(msg, client=None)
        self.assertEqual(msg.data["utterances"], ["new"])
        self.assertEqual(msg.data["lang"], "en-us")

    def test_noop_for_other_msg_types(self):
        msg = _FakeMessage("speak", data={"utterance": "hi"})
        RewriteUtterance("new").apply(msg, client=None)
        self.assertEqual(msg.data, {"utterance": "hi"})

    def test_noop_when_data_is_not_dict(self):
        msg = _FakeMessage("recognizer_loop:utterance", data=None)
        msg.data = ["not", "a", "dict"]
        RewriteUtterance("x").apply(msg, client=None)
        self.assertEqual(msg.data, ["not", "a", "dict"])


# ---------------------------------------------------------------------------
# OVOSAgentPolicy
# ---------------------------------------------------------------------------

def _user(skill_bl=None, intent_bl=None, msg_bl=None):
    return SimpleNamespace(
        skill_blacklist=skill_bl,
        intent_blacklist=intent_bl,
        message_blacklist=msg_bl,
    )


def _stub_hm_protocol(user=None, sync_raises=False, get_raises=False):
    db = MagicMock()
    db.sync = MagicMock(side_effect=Exception("sync") if sync_raises else None)
    if get_raises:
        db.get_client_by_api_key = MagicMock(side_effect=Exception("get"))
    else:
        db.get_client_by_api_key = MagicMock(return_value=user)
    return SimpleNamespace(db=db)


def _client(key="k"):
    c = SimpleNamespace(
        key=key, skill_blacklist=[], intent_blacklist=[], msg_blacklist=[],
    )
    return c


class TestOVOSAgentPolicy(unittest.TestCase):
    def test_no_hm_protocol_allows(self):
        v = OVOSAgentPolicy().review(_FakeMessage(), _client())
        self.assertFalse(v.denied)
        self.assertEqual(v.mutations, [])

    def test_no_user_allows(self):
        p = OVOSAgentPolicy(hm_protocol=_stub_hm_protocol(user=None))
        v = p.review(_FakeMessage(), _client())
        self.assertFalse(v.denied)
        self.assertEqual(v.mutations, [])

    def test_emits_skill_and_intent_mutations(self):
        user = _user(skill_bl=["s1", "s2"], intent_bl=["i1"], msg_bl=["m1"])
        p = OVOSAgentPolicy(hm_protocol=_stub_hm_protocol(user=user))
        client = _client()

        v = p.review(_FakeMessage(), client)

        self.assertFalse(v.denied)
        kinds = [type(m).__name__ for m in v.mutations]
        self.assertEqual(kinds, ["AddBlacklistedSkill",
                                 "AddBlacklistedSkill",
                                 "AddBlacklistedIntent"])

    def test_caches_outbound_msg_blacklist_on_connection(self):
        """Side-effect contract: client.msg_blacklist is updated so the
        existing send()-side filter in hivemind-core keeps working."""
        user = _user(msg_bl=["speak", "audio.play"])
        p = OVOSAgentPolicy(hm_protocol=_stub_hm_protocol(user=user))
        client = _client()

        p.review(_FakeMessage(), client)

        self.assertEqual(client.msg_blacklist, ["speak", "audio.play"])

    def test_caches_skill_and_intent_lists_on_connection(self):
        user = _user(skill_bl=["s"], intent_bl=["i"])
        p = OVOSAgentPolicy(hm_protocol=_stub_hm_protocol(user=user))
        client = _client()

        p.review(_FakeMessage(), client)

        self.assertEqual(client.skill_blacklist, ["s"])
        self.assertEqual(client.intent_blacklist, ["i"])

    def test_sync_failure_is_tolerated(self):
        user = _user(skill_bl=["s"])
        p = OVOSAgentPolicy(
            hm_protocol=_stub_hm_protocol(user=user, sync_raises=True),
        )
        v = p.review(_FakeMessage(), _client())
        self.assertFalse(v.denied)
        # still emits mutation from the user we returned
        self.assertEqual(len(v.mutations), 1)

    def test_get_client_failure_allows_without_mutations(self):
        p = OVOSAgentPolicy(
            hm_protocol=_stub_hm_protocol(get_raises=True),
        )
        v = p.review(_FakeMessage(), _client())
        self.assertFalse(v.denied)
        self.assertEqual(v.mutations, [])

    def test_none_blacklists_are_handled(self):
        user = _user(skill_bl=None, intent_bl=None, msg_bl=None)
        p = OVOSAgentPolicy(hm_protocol=_stub_hm_protocol(user=user))
        client = _client()
        v = p.review(_FakeMessage(), client)
        self.assertEqual(v.mutations, [])
        self.assertEqual(client.skill_blacklist, [])
        self.assertEqual(client.intent_blacklist, [])
        self.assertEqual(client.msg_blacklist, [])

    def test_review_binary_default_allows(self):
        v = OVOSAgentPolicy().review_binary(b"x", _client())
        self.assertFalse(v.denied)


if __name__ == "__main__":
    unittest.main()
