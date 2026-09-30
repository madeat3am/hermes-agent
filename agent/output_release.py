"""Experimental final-only release boundary. Not a complete managed-egress gate.

Canonical payload encoding is UTF-8 sorted compact JSON, no NaN. Raw replay is
trusted; public projections never contain drafts. See docs/output-release-slice.md.
"""
import copy
import hashlib
import json
import re
import uuid
from registration_lifecycle import replacement_coordinator

NOTICE = "Output withheld."
POLICY_TIMEOUT = 5.0


# Wrap at assignment so provider adapters and captured worker callbacks share
# the same deny-only sink. Publication happens only via the authority result.
MANAGED_CALLBACKS = frozenset({
    "stream_delta_callback", "_stream_callback", "interim_assistant_callback",
    "reasoning_callback", "thinking_callback", "tool_gen_callback",
    "tool_progress_callback", "tool_start_callback", "tool_complete_callback",
    "status_callback", "notice_callback", "notice_clear_callback", "event_callback",
    "reaction_callback", "step_callback", "_print_fn",
})


def guarded(agent):
    return getattr(agent, "_required_output_release_policy", None) is not None


def guarded_sink(agent, callback):
    def deliver(*args, **kwargs):
        if not guarded(agent):
            return callback(*args, **kwargs)
    return deliver


def read_batch_allowed(agent, calls):
    from agent.tool_result_classification import tool_may_have_side_effect
    turn = getattr(agent, "_output_release_turn", None)
    # Native no-effect classification includes display/aux-model readers that are
    # not audited publication boundaries. Only these two read paths are traced.
    return bool(turn and turn.current() and calls and all(
        tc.function.name in {"web_search", "web_extract"}
        and not tool_may_have_side_effect(tc.function.name) for tc in calls))


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode("utf-8")


def register_policy(ctx, *, id, version, prepare, decide):
    if version != 1 or not re.fullmatch(r"[a-zA-Z0-9_-]+", id) or not callable(prepare) or not callable(decide):
        raise ValueError("Invalid output release policy")
    manager = ctx._manager
    with replacement_coordinator.transaction():
        mapping = manager.__dict__.setdefault("_output_release_policies", {})
        key = ctx.plugin_id + "/" + id
        if key in mapping:
            raise ValueError("Duplicate output release policy")
        entry = {"generation": uuid.uuid4().hex, "prepare": prepare, "decide": decide}
        return ctx._track_mapping_entry("output_release_policy", key, mapping, entry)


def public_result(text=NOTICE, *, released=False, digest=None):
    return {"final_response": text, "messages": [{"role": "assistant", "content": text}],
            "failed": not released, "completed": released,
            "output_release": {"version": 1, "state": "released" if released else "denied",
                               "payload_digest": digest if released else None}}


def display_projection(db, session_id):
    return db.get_messages_as_conversation(session_id)


class ReleaseTurn:
    def __init__(self, agent, policy_key, task_id, turn_id, request):
        from hermes_cli.plugins import get_plugin_manager
        self.agent, self.manager = agent, get_plugin_manager()
        self.key, self.closed = policy_key, False
        self.entry = getattr(self.manager, "_output_release_policies", {}).get(policy_key) if isinstance(policy_key, str) else None
        self.binding = {"version": 1, "profile_key": self.manager.scope_key,
                        "session_id": agent.session_id, "task_id": task_id, "turn_id": turn_id,
                        "request_digest": hashlib.sha256(canonical(request)).hexdigest(),
                        "policy_key": policy_key, "generation": self.entry["generation"] if self.entry else None,
                        "nonce": uuid.uuid4().hex}
        self.state = None

    def current(self):
        return (not self.closed and self.entry is not None and
                getattr(self.manager, "_output_release_policies", {}).get(self.key) is self.entry)

    def call(self, phase, **payload):
        if not self.current():
            return None
        try:
            value = self.manager._run_hook_callback_bounded(
                "output_release_" + phase, self.entry[phase], copy.deepcopy(payload), POLICY_TIMEOUT)
            # A timed-out worker never receives the holder; only detached JSON.
            return copy.deepcopy(value) if self.current() and isinstance(value, dict) else None
        except Exception:
            return None

    def prepare(self, request):
        db = getattr(self.agent, "_session_db", None)
        if not db or not self.current():
            return False
        db.begin_output_release(self.agent.session_id, self.binding["turn_id"], self.binding)
        value = self.call("prepare", binding=self.binding, request=request)
        if not value or set(value) - {"version", "action", "binding", "policy_state"} or value.get("version") != 1 or value.get("action") != "ready" or value.get("binding") != self.binding:
            return False
        self.state = value.get("policy_state")
        return len(canonical(self.state)) <= 65536

    def finish(self, result):
        if not self.current() or any(result.get(k) for k in ("failed", "partial", "interrupted", "cleanup_errors")):
            self.closed = True
            return public_result()
        text = result.get("final_response")
        if not isinstance(text, str) or not text or self.agent.session_id != self.binding["session_id"]:
            self.closed = True
            return public_result()
        payload = {"final_response": text, "visible_messages": [{"role": "assistant", "content": text}], "attachments": []}
        db = self.agent._session_db
        raw_final = next((m for m in reversed(result.get('messages') or [])
                          if m.get('role') == 'assistant' and not m.get('tool_calls')), None)
        matches = [m for m in db.get_messages(self.binding['session_id'], trusted_raw=True)
                   if raw_final and m.get('role') == 'assistant' and m.get('content') == raw_final.get('content')]
        try:
            receipt = db.stage_output_release(self.binding['session_id'], self.binding['turn_id'],
                self.binding, text, message_id=matches[0]['id'] if len(matches) == 1 else None)
        except ValueError:
            self.closed = True
            return public_result()
        digest, candidate_id = receipt['payload_digest'], receipt['candidate_id']
        value = self.call("decide", binding=self.binding, candidate_id=candidate_id,
                          payload_digest=digest, payload=payload, policy_state=self.state)
        expected = {"version": 1, "action": "release", "binding": self.binding,
                    "candidate_id": candidate_id, "payload_digest": digest}
        with replacement_coordinator.transaction():
            released = (self.current() and value == expected and
                        self.agent.session_id == self.binding['session_id'] and self.agent._session_db is db)
            if released:
                try:
                    db.publish_output_release(self.binding['session_id'], self.binding['turn_id'],
                                              self.binding, text, **receipt)
                except ValueError:
                    released = False
            self.closed = True
        return public_result(text, released=True, digest=digest) if released else public_result()
