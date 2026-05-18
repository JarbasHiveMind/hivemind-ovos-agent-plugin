"""OVOS-specific policy primitives.

Concrete :class:`Mutation` subclasses for the OVOS bus and the built-in
:class:`OVOSAgentPolicy` that migrates the legacy skill/intent/message
blacklist injection out of ``hivemind-core`` (``_update_blacklist`` side
effects) into a proper policy plugin.

These types are OVOS-specific because they manipulate the shape of
``message.context["session"]`` (an OVOS ``Session`` serialisation) and
``recognizer_loop:utterance`` payloads. They live here rather than in
the generic ``hivemind-plugin-manager`` so non-OVOS agents (or
agent-less HiveMind deployments) don't pull in OVOS-shape assumptions.

Spec: https://github.com/JarbasHiveMind/HiveMind-core/issues/85
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Tuple

from hivemind_plugin_manager import Mutation, PolicyPlugin, Verdict
from ovos_utils.log import LOG


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ensure_session(message) -> Dict[str, Any]:
    """Return ``message.context["session"]``, creating it if missing.

    Robust to non-dict context/session values — the chain runner can
    feed in messages whose ``context`` was tampered with upstream;
    coerce rather than blow up.
    """
    if not isinstance(message.context, dict):
        message.context = {}
    session = message.context.get("session")
    if not isinstance(session, dict):
        session = {}
        message.context["session"] = session
    return session


# ---------------------------------------------------------------------------
# Mutations
# ---------------------------------------------------------------------------

@dataclass
class AddBlacklistedSkill(Mutation):
    """Add a ``skill_id`` to ``message.context["session"]["blacklisted_skills"]``."""
    skill_id: str

    def apply(self, message, client) -> None:
        bl = _ensure_session(message).setdefault("blacklisted_skills", [])
        if self.skill_id not in bl:
            bl.append(self.skill_id)


@dataclass
class AddBlacklistedIntent(Mutation):
    """Add an intent name to ``message.context["session"]["blacklisted_intents"]``."""
    intent_name: str

    def apply(self, message, client) -> None:
        bl = _ensure_session(message).setdefault("blacklisted_intents", [])
        if self.intent_name not in bl:
            bl.append(self.intent_name)


@dataclass
class AddBlacklistedMessageType(Mutation):
    """Add a Mycroft message-type to a session-level message blacklist."""
    msg_type: str

    def apply(self, message, client) -> None:
        bl = _ensure_session(message).setdefault("blacklisted_message_types", [])
        if self.msg_type not in bl:
            bl.append(self.msg_type)


@dataclass
class SetSessionField(Mutation):
    """Set a single key in ``message.context["session"]``."""
    key: str
    value: Any

    def apply(self, message, client) -> None:
        _ensure_session(message)[self.key] = self.value


@dataclass
class SetContextField(Mutation):
    """Set a key path in ``message.context``.

    ``path`` is a tuple of dict keys. Intermediate dicts are created if
    missing or replaced if they were non-dicts.
    """
    path: Tuple[str, ...]
    value: Any

    def apply(self, message, client) -> None:
        if not self.path:
            return
        target = message.context
        if not isinstance(target, dict):
            target = {}
            message.context = target
        for key in self.path[:-1]:
            nxt = target.get(key)
            if not isinstance(nxt, dict):
                nxt = {}
                target[key] = nxt
            target = nxt
        target[self.path[-1]] = self.value


@dataclass
class RewriteUtterance(Mutation):
    """Replace the utterance text in a ``recognizer_loop:utterance``
    Mycroft message. Silent no-op on any other ``msg_type``."""
    text: str

    def apply(self, message, client) -> None:
        if getattr(message, "msg_type", None) != "recognizer_loop:utterance":
            return
        if not isinstance(message.data, dict):
            return
        message.data["utterances"] = [self.text]


# ---------------------------------------------------------------------------
# OVOSAgentPolicy — built-in
# ---------------------------------------------------------------------------

class OVOSAgentPolicy(PolicyPlugin):
    """Built-in policy that injects per-client OVOS skill / intent
    blacklists into the session and refreshes the outbound message-type
    filter on the connection.

    Replaces the side-effecting block at the bottom of the legacy
    ``hivemind-core`` ``_update_blacklist`` method (which read
    ``Client.skill_blacklist`` / ``intent_blacklist`` /
    ``message_blacklist`` from the DB and either injected them into
    ``message.context["session"]`` or cached them on the connection
    object).

    Reads the same DB fields as before — no schema change. Outbound
    ``message_blacklist`` is cached on the connection object (still
    enforced at ``send()`` socket-write time in ``hivemind-core``); skill
    and intent blacklists are emitted as :class:`AddBlacklistedSkill` /
    :class:`AddBlacklistedIntent` mutations so the chain runner records
    what changed.
    """

    def review(self, message, client) -> Verdict:
        db = getattr(self.hm_protocol, "db", None)
        if db is None:
            return Verdict.allow()

        try:
            db.sync()
        except Exception:
            LOG.debug("db.sync() failed in OVOSAgentPolicy", exc_info=True)

        try:
            user = db.get_client_by_api_key(client.key)
        except Exception:
            LOG.debug("db.get_client_by_api_key failed in OVOSAgentPolicy",
                      exc_info=True)
            return Verdict.allow()

        if user is None:
            return Verdict.allow()

        skills = list(user.skill_blacklist or [])
        intents = list(user.intent_blacklist or [])
        msg_types = list(user.message_blacklist or [])

        # cache outbound filter on the connection — same as the old
        # _update_blacklist side effect; HiveMindClientConnection.send()
        # filters against client.msg_blacklist at socket-write time.
        client.skill_blacklist = skills
        client.intent_blacklist = intents
        client.msg_blacklist = msg_types

        muts = [AddBlacklistedSkill(s) for s in skills]
        muts += [AddBlacklistedIntent(i) for i in intents]
        return Verdict.allow(*muts)


__all__ = [
    "AddBlacklistedSkill",
    "AddBlacklistedIntent",
    "AddBlacklistedMessageType",
    "SetSessionField",
    "SetContextField",
    "RewriteUtterance",
    "OVOSAgentPolicy",
]
