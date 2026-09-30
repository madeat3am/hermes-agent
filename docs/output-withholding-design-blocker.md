# Per-turn output withholding: design blocker, not a shipped API

Scope inspected: `agent/turn_context.py`, `agent/stream_delivery.py`,
`agent/turn_finalizer.py`, and `agent/turn_tool_round.py`, at core revision
`53303bd5bd8ed69504f6013a723224bf5ded230d`.

**Disposition: do not implement a release contract in these four files alone.**
No hold directive or structured authorization API is implemented by this work.
The Research writer must not treat a transform string, `pre_verify` result,
or an invented `pre_llm_call` dictionary key as a withholding guarantee.

## Decisive boundary outside ownership

`agent/turn_final_response.py:226-237` appends the final assistant candidate and
calls `_flush_messages_to_session_db` **before** returning to the finalizer.
A finalizer which subsequently substitutes a body-free refusal cannot undo the
interval in which a concurrent reader can load the candidate, nor retract copies
already read. Moving only the finalizer's own persist call does not close this.

The store is not an exclusively model-private transcript:

- `agent/session_persistence.py:187-222` projects and writes new message rows;
  `:337-366` is the common incremental flush path. There is no release-state
  check there. It persists content and reasoning/provider sidecars.
- `tui_gateway/server.py:2522` reads canonical conversation history for resume.
- `tui_gateway/session_history.py:181-243` projects those rows into UI messages.
  Assistant content becomes `text`; reasoning and Codex message/reasoning items
  are also forwarded. Thus a preserved raw candidate can be user-visible after
  reconnect, not merely private input to a future model request.
- `display_kind="hidden"` is an existing UI projection primitive, not a complete
  release policy. New assistant messages would need to be stamped before every
  persistence path; raw conversation readers and other surfaces would still need
  review. Stamping it only in the finalizer is too late.

Do not work around this by monkeypatching an agent instance's persistence methods,
setting `_persist_disabled` for the whole turn, or rewriting the model transcript.
Those approaches bypass ownership, compromise crash-resilient tool accounting,
or violate transcript/prompt-cache fidelity. An API/display projection must remain
separate from the exact model-facing transcript.

## Additional paths that invalidate a finalizer-only gate

| Path | Observed behavior |
|---|---|
| `turn_context.py:657-709` | `pre_llm_call` has session/task/turn identity, but only consumes context strings. No output hold is interpreted. |
| `stream_delivery.py:33-36,163-203,279-334` | Display, TTS, interim, reasoning and observer stream/end payloads are delivered before final transforms. Scrubber tails also deliver through reset/flush at `:47-75`. |
| `turn_tool_round.py` | Has direct halt callback delivery and an incremental tool-call persist before execution. Guarding callbacks alone does not prevent history replay. |
| `turn_finalizer.py:459-510` | Trajectory save and canonical persistence precede transforms. Interrupted responses skip output hooks. |
| `turn_finalizer.py:400-430` | First nonempty transform string wins; the transform event lacks task/turn fields, while post-call carries them. A string transformation is not authorization. |
| `turn_finalizer.py:536-548` | Result exports raw `messages`, `last_reasoning`, and `pre_transform_response`; replacing only `final_response` leaves candidate copies. |
| `agent/turn_tool_validation.py:53-66` | Partial exits persist and return their own envelope; the docstring explicitly states they never reach `finalize_turn`. Tool-round ownership can intercept this returned envelope, but cannot undo the persist that already occurred. |
| `agent/turn_final_response.py:80-97` | Empty-response recovery can return a result directly rather than go through finalization; that caller is outside the four-file ownership. |

## Native plugin dispatch limitation

`hermes_cli/plugins_dispatch.py:41-48,174-207` bounds `pre_llm_call` and
`transform_llm_output`, but skips timeout results and exception results. Its result
list has no callback-owner or failure envelope. Only `pre_tool_call` timeout is
currently fail-closed.

Once a hold is successfully armed, a missing final authorization could safely mean
refusal. However, a pre-call timeout before a hold declaration is returned is
indistinguishable to the caller from no opt-in. Blocking every such turn would
also block ordinary Default/depth-none traffic. If failure to declare a required
hold must itself be fail-closed, the requirement must be known before executing
that fallible callback. This requires a reviewed registration/dispatch contract,
not a second callback runner hidden in turn context. Existing events may be
extended; a broad new plugin event engine is neither proposed nor implemented.

## Minimum follow-up design decisions

1. Widen ownership to the common persistence/projection boundary and the outer
   turn result boundary (including direct recovery/partial returns). Audit all
   persistence and UI/history readers before calling stored candidates private.
2. Bind the hold to core-issued session/task/turn identity before the first model
   byte. Keep it monotonic for that turn, clear it at turn start, and fence late
   stream workers. Session rotation needs an explicit identity policy.
3. Reuse lifecycle dispatch, but define required authority provenance, failure
   outcomes, and whether pre-call declaration failure is covered. Multiple
   authorities must not be bypassed by another plugin's transform string.
4. Authorize the exact final payload after all ordinary transforms. Block, error,
   timeout, absent/malformed/mismatched decisions and interruptions must yield a
   fixed body-free notice. No raw candidate sidecars may leave in returned or
   replayed user-visible projections. Preserve model bytes separately.
5. Only then publish a versioned plugin API and give it to the Research writer.
   Offline RED/GREEN integration coverage must include actual stream mixins,
   finalizer, incremental persistence, resume projection, independent sessions,
   next-turn reset, late workers, failed authorization, and direct return paths.

## Executed verification

Canonical offline runner:

```sh
scripts/run_tests.sh \
  tests/agent/test_turn_finalizer_final_response_persistence.py \
  tests/agent/test_turn_finalizer_cleanup_guard.py
```

Result: **12 passed, 0 failed**, two files. A second canonical run,
`scripts/run_tests.sh tests/tui_gateway/test_session_history_codex_sidecar.py`,
returned **1 passed, 0 failed**, exercising native history-sidecar projection.
These are existing native behavior tests. They verify transcript durability,
cleanup, and replay behavior that a new release contract must preserve; they do
**not** establish output-withholding safety. No production implementation or RED/GREEN feature-completion claim is
made. No model calls, runtime configuration edits, restarts, staging or commits.

This slice does not solve arbitrary tool egress, `send_message`, network/shell
operations, attachments, or plugin-originated side effects. Those require their
own authorization boundaries; shell-command regexes are not a containment model.
