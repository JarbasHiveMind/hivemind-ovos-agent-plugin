"""Tests for OVOSAgentProtocol natural language query backpressure."""

import threading
import time
from unittest.mock import MagicMock

from hivemind_ovos_agent_plugin import OVOSAgentProtocol
from ovos_bus_client.message import Message


class _SessionLike:
    session_id = "wanted"


class _QueryBus:
    def __init__(
        self,
        fail_send=False,
        emit_done=True,
        speak_event="speak",
        done_first=False,
        respond_to=None,
        api_result=None,
    ):
        self.fail_send = fail_send
        self.emit_done = emit_done
        self.speak_event = speak_event
        self.done_first = done_first
        self.respond_to = respond_to
        self.api_result = api_result
        self.handlers = {}
        self.closed = False
        self.messages = []

    def on(self, event, handler):
        self.handlers.setdefault(event, []).append(handler)

    def remove(self, event, handler):
        if handler in self.handlers.get(event, []):
            self.handlers[event].remove(handler)

    def emit_checked(self, message):
        self.messages.append(message)
        if self.fail_send:
            raise RuntimeError("bus closed")
        if "query_id" not in message.context:
            return
        if self.respond_to is not None and message.msg_type not in self.respond_to:
            return
        if message.data.get("args") is not None:
            response = Message(
                f"{message.msg_type}.response",
                {"result": self.api_result or "api ok"},
                {"query_id": message.context["query_id"]},
            )
            for handler in list(self.handlers.get(response.msg_type, [])):
                handler(response)
            return
        speak = Message(
            "speak",
            {"utterance": "ok"},
            {"query_id": message.context["query_id"]},
        )
        done = Message(
            "ovos.utterance.handled",
            {},
            {"query_id": message.context["query_id"]},
        )
        if self.done_first and self.emit_done:
            for handler in list(self.handlers.get("ovos.utterance.handled", [])):
                handler(done)
            time.sleep(0.01)
        for handler in list(self.handlers.get(self.speak_event, [])):
            handler(Message(self.speak_event, speak.data, speak.context))
        if self.emit_done and not self.done_first:
            for handler in list(self.handlers.get("ovos.utterance.handled", [])):
                handler(done)

    def close(self):
        self.closed = True


def test_query_returns_none_when_inflight_gate_is_full():
    agent = OVOSAgentProtocol.__new__(OVOSAgentProtocol)
    agent.bus = MagicMock()
    agent.config = {"inflight_timeout": 0}
    agent._inflight_semaphore = threading.BoundedSemaphore(1)
    agent._inflight_semaphore.acquire()

    assert list(agent.natural_language_query("what time is it", "en-us")) == [None]
    agent.bus.emit.assert_not_called()

    agent._inflight_semaphore.release()


def test_query_response_timeout_can_be_configured():
    agent = OVOSAgentProtocol.__new__(OVOSAgentProtocol)
    agent.config = {"response_timeout": 42}

    assert agent._configured_response_timeout() == 42


def test_query_response_timeout_has_compatible_aliases():
    agent = OVOSAgentProtocol.__new__(OVOSAgentProtocol)
    agent.config = {"query_response_timeout": 12}

    assert agent._configured_response_timeout() == 12

    agent.config = {"utterance_timeout": 7}
    assert agent._configured_response_timeout() == 7


def test_query_retries_when_selected_bus_closes_on_send():
    first = _QueryBus(fail_send=True)
    second = _QueryBus()
    buses = iter([first, second])
    agent = OVOSAgentProtocol.__new__(OVOSAgentProtocol)
    agent.config = {"response_timeout": 0.1}
    agent._inflight_semaphore = threading.BoundedSemaphore(1)
    agent._inflight_timeout = 0
    agent._client_state_lock = threading.RLock()
    agent._bus_emit_locks = {}
    agent.get_bus = lambda *_: next(buses)
    agent._register_bus_handlers(first)
    agent._register_bus_handlers(second)

    assert list(agent.natural_language_query("what time is it", "en-us")) == ["ok", None]
    assert first.closed is True
    assert second.closed is False


def test_query_reply_dispatch_uses_waiter_map_not_per_query_handlers():
    bus = _QueryBus()
    agent = OVOSAgentProtocol.__new__(OVOSAgentProtocol)
    agent.config = {"response_timeout": 0.1}
    agent._inflight_semaphore = threading.BoundedSemaphore(1)
    agent._inflight_timeout = 0
    agent._client_state_lock = threading.RLock()
    agent._bus_emit_locks = {}
    agent._query_waiters = {}
    agent.get_bus = lambda *_: bus
    agent._register_bus_handlers(bus)

    speak_handlers = len(bus.handlers.get("speak", []))
    done_handlers = len(bus.handlers.get("ovos.utterance.handled", []))

    assert list(agent.natural_language_query("what time is it", "en-us")) == ["ok", None]
    assert len(bus.handlers.get("speak", [])) == speak_handlers
    assert len(bus.handlers.get("ovos.utterance.handled", [])) == done_handlers
    assert agent._query_waiters == {}


def test_query_dispatch_handles_ovos_utterance_speak_event():
    bus = _QueryBus(speak_event="ovos.utterance.speak")
    agent = OVOSAgentProtocol.__new__(OVOSAgentProtocol)
    agent.config = {"response_timeout": 0.1}
    agent._inflight_semaphore = threading.BoundedSemaphore(1)
    agent._inflight_timeout = 0
    agent._client_state_lock = threading.RLock()
    agent._bus_emit_locks = {}
    agent._query_waiters = {}
    agent.get_bus = lambda *_: bus
    agent._register_bus_handlers(bus)

    assert list(agent.natural_language_query("what time is it", "en-us")) == ["ok", None]


def test_query_completion_waits_briefly_for_late_speech():
    bus = _QueryBus(done_first=True)
    agent = OVOSAgentProtocol.__new__(OVOSAgentProtocol)
    agent.config = {"response_timeout": 0.1, "query_done_grace": 0.2}
    agent._inflight_semaphore = threading.BoundedSemaphore(1)
    agent._inflight_timeout = 0
    agent._client_state_lock = threading.RLock()
    agent._bus_emit_locks = {}
    agent._query_waiters = {}
    agent.get_bus = lambda *_: bus
    agent._register_bus_handlers(bus)

    assert list(agent.natural_language_query("what time is it", "en-us")) == ["ok", None]


def test_query_can_complete_after_first_response():
    bus = _QueryBus(emit_done=False)
    agent = OVOSAgentProtocol.__new__(OVOSAgentProtocol)
    agent.config = {"complete_on_first_response": True, "response_timeout": 10}
    agent._inflight_semaphore = threading.BoundedSemaphore(1)
    agent._inflight_timeout = 0
    agent._client_state_lock = threading.RLock()
    agent._bus_emit_locks = {}
    agent._query_waiters = {}
    agent.get_bus = lambda *_: bus
    agent._register_bus_handlers(bus)

    assert list(agent.natural_language_query("what time is it", "en-us")) == ["ok", None]
    assert agent._query_waiters == {}


def test_direct_query_route_sends_matching_prompt_to_skill_request():
    bus = _QueryBus()
    agent = OVOSAgentProtocol.__new__(OVOSAgentProtocol)
    agent.config = {
        "complete_on_first_response": True,
        "direct_query_routes": [
            {
                "event": "ovos.skills.fallback.thalovant-skill-date-time.thalovant.request",
                "patterns": [r"\btime\b"],
            }
        ],
        "response_timeout": 10,
    }
    agent._inflight_semaphore = threading.BoundedSemaphore(1)
    agent._inflight_timeout = 0
    agent._client_state_lock = threading.RLock()
    agent._bus_emit_locks = {}
    agent._query_waiters = {}
    agent.get_bus = lambda *_: bus
    agent._register_bus_handlers(bus)

    assert list(agent.natural_language_query("what time is it", "en-us")) == ["ok", None]
    assert bus.messages[0].msg_type == "ovos.skills.fallback.thalovant-skill-date-time.thalovant.request"
    assert bus.messages[0].data["utterances"] == ["what time is it"]
    assert bus.messages[0].data["utterance"] == "what time is it"
    assert bus.messages[0].context["lang"] == "en-us"


def test_direct_query_api_route_returns_skill_api_result():
    bus = _QueryBus(api_result="It is noon.")
    agent = OVOSAgentProtocol.__new__(OVOSAgentProtocol)
    agent.config = {
        "complete_on_first_response": True,
        "direct_query_routes": [
            {
                "api_event": "thalovant-skill-date-time.thalovant.preview_reply",
                "patterns": [r"\btime\b"],
            }
        ],
        "response_timeout": 10,
    }
    agent._inflight_semaphore = threading.BoundedSemaphore(1)
    agent._inflight_timeout = 0
    agent._client_state_lock = threading.RLock()
    agent._bus_emit_locks = {}
    agent._query_waiters = {}
    agent.get_bus = lambda *_: bus
    agent._register_bus_handlers(bus)

    assert list(agent.natural_language_query("what time is it", "en-us")) == ["It is noon.", None]
    assert bus.messages[0].msg_type == "thalovant-skill-date-time.thalovant.preview_reply"
    assert bus.messages[0].data["args"] == ["what time is it", "en-us"]
    assert bus.messages[0].data["kwargs"] == {}


def test_direct_query_route_accepts_flat_config_keys():
    agent = OVOSAgentProtocol.__new__(OVOSAgentProtocol)
    agent.config = {
        "direct_query_event": "ovos.skills.fallback.direct.request",
        "direct_query_patterns": r"\bphotosynthesis\b",
    }

    assert agent._direct_query_event_for("Explain photosynthesis", "en-us") == "ovos.skills.fallback.direct.request"


def test_direct_query_api_route_accepts_flat_config_keys():
    agent = OVOSAgentProtocol.__new__(OVOSAgentProtocol)
    agent.config = {
        "direct_query_api_event": "thalovant-skill-learning-lounge.thalovant.preview_reply",
        "direct_query_patterns": r"\bphotosynthesis\b",
    }

    route = agent._direct_query_route_for("Explain photosynthesis", "en-us")

    assert route["api_event"] == "thalovant-skill-learning-lounge.thalovant.preview_reply"
    assert agent._configured_direct_query_api_response_events() == (
        "thalovant-skill-learning-lounge.thalovant.preview_reply.response",
    )


def test_direct_query_route_falls_back_to_generic_pipeline_when_unanswered():
    bus = _QueryBus(respond_to={"recognizer_loop:utterance"})
    agent = OVOSAgentProtocol.__new__(OVOSAgentProtocol)
    agent.config = {
        "complete_on_first_response": True,
        "direct_query_timeout": 0.01,
        "direct_query_routes": [
            {
                "event": "ovos.skills.fallback.missing.request",
                "patterns": [r"\btime\b"],
            }
        ],
        "response_timeout": 10,
    }
    agent._inflight_semaphore = threading.BoundedSemaphore(1)
    agent._inflight_timeout = 0
    agent._client_state_lock = threading.RLock()
    agent._bus_emit_locks = {}
    agent._query_waiters = {}
    agent.get_bus = lambda *_: bus
    agent._register_bus_handlers(bus)

    assert list(agent.natural_language_query("what time is it", "en-us")) == ["ok", None]
    assert [message.msg_type for message in bus.messages] == [
        "ovos.skills.fallback.missing.request",
        "recognizer_loop:utterance",
    ]


def test_query_dispatch_ignores_unregistered_query_id():
    agent = OVOSAgentProtocol.__new__(OVOSAgentProtocol)
    agent._client_state_lock = threading.RLock()
    agent._query_waiters = {"wanted": MagicMock()}

    agent._handle_query_response(
        Message("speak", {"utterance": "wrong"}, {"query_id": "other"})
    )

    agent._query_waiters["wanted"].put.assert_not_called()


def test_query_dispatch_accepts_session_id_when_query_id_is_missing():
    agent = OVOSAgentProtocol.__new__(OVOSAgentProtocol)
    agent._client_state_lock = threading.RLock()
    waiter = MagicMock()
    agent._query_waiters = {"wanted": waiter}

    agent._handle_query_response(
        Message("speak", {"utterance": "ok"}, {"session": {"session_id": "wanted"}})
    )

    waiter.put.assert_called_once_with(("speak", "ok"))


def test_query_dispatch_accepts_serialized_session_id_when_query_id_is_missing():
    agent = OVOSAgentProtocol.__new__(OVOSAgentProtocol)
    agent._client_state_lock = threading.RLock()
    waiter = MagicMock()
    agent._query_waiters = {"wanted": waiter}

    agent._handle_query_response(
        Message("speak", {"utterance": "ok"}, {"session": '{"session_id":"wanted"}'})
    )

    waiter.put.assert_called_once_with(("speak", "ok"))


def test_query_dispatch_accepts_session_object_when_query_id_is_missing():
    agent = OVOSAgentProtocol.__new__(OVOSAgentProtocol)
    agent._client_state_lock = threading.RLock()
    waiter = MagicMock()
    agent._query_waiters = {"wanted": waiter}

    agent._handle_query_response(
        Message("speak", {"utterance": "ok"}, {"session": _SessionLike()})
    )

    waiter.put.assert_called_once_with(("speak", "ok"))


def test_emit_client_message_retries_when_selected_bus_closes_on_send():
    first = _QueryBus(fail_send=True)
    second = _QueryBus()
    buses = iter([first, second])
    agent = OVOSAgentProtocol.__new__(OVOSAgentProtocol)
    agent._client_state_lock = threading.RLock()
    agent._bus_emit_locks = {}
    agent.get_bus = lambda *_: next(buses)

    message = Message("recognizer_loop:utterance")
    assert agent.emit_client_message(message) is True
    assert first.closed is True
    assert second.closed is False
