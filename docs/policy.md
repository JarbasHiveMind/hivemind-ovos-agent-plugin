# Policy Admission Chain

This package ships an OVOS-specific policy plugin that participates in the
HiveMind-core policy admission chain (spec:
[HiveMind-core#85, comment 4478944429](https://github.com/JarbasHiveMind/HiveMind-core/issues/85)).

## Background

`hivemind-core` runs every inbound message through a chain of `PolicyPlugin`
instances before forwarding it to the OVOS bus. Each plugin returns a
`Verdict`. A `Verdict.allow()` may carry zero or more `Mutation` objects — the
chain runner applies them to the message in the order they were emitted.

The OVOS-specific mutations (those that manipulate `message.context["session"]`)
live here rather than in the generic `hivemind-plugin-manager` package so that
non-OVOS agents do not inherit OVOS session-shape assumptions.

Source: `hivemind_ovos_agent_plugin/policy.py`

## Entry point

```
group: hivemind.policy
name:  hivemind-ovos-agent-policy
class: hivemind_ovos_agent_plugin.policy.OVOSAgentPolicy
```

Declared in `pyproject.toml:47`.

## Mutations

All mutations are `@dataclass` subclasses of `hivemind_plugin_manager.Mutation`.
Each exposes an `apply(message, client) -> None` method called by the chain runner.

| Class | Field(s) | Effect |
|---|---|---|
| `AddBlacklistedSkill` | `skill_id: str` | Appends to `session["blacklisted_skills"]`; no-op if already present. |
| `AddBlacklistedIntent` | `intent_name: str` | Appends to `session["blacklisted_intents"]`; no-op if already present. |
| `AddBlacklistedMessageType` | `msg_type: str` | Appends to `session["blacklisted_message_types"]`; no-op if already present. |
| `SetSessionField` | `key: str`, `value: Any` | Sets `session[key] = value`. |
| `SetContextField` | `path: Tuple[str, ...]`, `value: Any` | Traverses `message.context` along `path`, creating missing intermediate dicts, then sets the leaf. |
| `RewriteUtterance` | `text: str` | Replaces `message.data["utterances"]` with `[text]`; silent no-op on any `msg_type` other than `recognizer_loop:utterance`. |

All six mutations guard against a non-dict `message.context` or
`message.context["session"]` by coercing to an empty dict rather than raising.
`_ensure_session` — `hivemind_ovos_agent_plugin/policy.py:29` — is the shared
helper that provides this guarantee.

## OVOSAgentPolicy

`OVOSAgentPolicy(PolicyPlugin)` — `hivemind_ovos_agent_plugin/policy.py:136`

The built-in policy. On every inbound message it:

1. Calls `db.sync()` to pick up any credential changes made since the listener
   started.
2. Looks up the connecting client via `db.get_client_by_api_key(client.key)`.
3. Reads `user.skill_blacklist` and `user.intent_blacklist` — property shims
   backed by `Client.metadata` after the HPM migration.
4. Emits one `AddBlacklistedSkill` per entry and one `AddBlacklistedIntent`
   per entry; returns `Verdict.allow(*mutations)`.

If `db` is unavailable, the client lookup fails, or the user record is absent,
the policy returns `Verdict.allow()` with no mutations — it never denies.

`message_blacklist` is **not** consulted. `hivemind-core` dropped outbound
message blacklisting in favour of a whitelist-only model (`allowed_types`);
there is no consumer for a message-type blacklist in the admission chain.
`AddBlacklistedMessageType` is provided for custom downstream policy plugins
that need it, not used by `OVOSAgentPolicy` itself.

## All-at-once import

All six mutation classes and `OVOSAgentPolicy` are re-exported from the package
top-level (`hivemind_ovos_agent_plugin/__init__.py:14-20`):

```python
from hivemind_ovos_agent_plugin import (
    AddBlacklistedSkill,
    AddBlacklistedIntent,
    AddBlacklistedMessageType,
    SetSessionField,
    SetContextField,
    RewriteUtterance,
    OVOSAgentPolicy,
)
```
