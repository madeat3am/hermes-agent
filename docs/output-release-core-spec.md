# Output release: root cause, executable RED, proposed core contract

Status: DESIGN ONLY; not an approved or shipped plugin API. Read alongside
`output-withholding-design-blocker.md`. No production/config changes.

## Executed proof

`scripts/run_tests.sh tests/run_agent/test_output_release_design_tracer.py`
returned **1 failed, 0 passed**, at the intended invariant:

```
AssertionError: unreleased marker escaped via ['history_during_denial', 'returned_result']
```

The test runs the real AIAgent facade, loop, final response, finalizer and native
persistence. Only the model transport and external initialization dependencies are
fake; no inference is made. A separate real SQLite connection reads through the
native `server.handle_request(session.history)` DURING a transform that returns
`Output withheld.`. It verifies one fake completion and the notice final_response
before asserting absence of the unique candidate marker. Thus neither a fake DB
proxy nor an invented hold flag explains the failure. This proves the present
transform-only approach unsafe; it does NOT establish any proposed gate as safe.
The runner also reported a DB read-in-flight teardown warning; eliminate the
background reader lifecycle in the eventual permanent fixture.

The tracer deliberately remains RED and untracked: do not merge it as a passing
feature test or silently xfail it. Convert its denial seam to the actual registered
policy when implementing, retain the observed-history timing assertion.

## Root cause and smallest useful ownership expansion

One transcript currently serves incompatible audiences: exact provider replay,
UI history, callback observers and run_conversation return consumers. Finalization
is not the outer boundary and SQLite commit already makes bytes observable.

Existing useful primitives (reuse, do not replace):

* `agent/turn_facade.py:54-62,75-191`: core task/relay-turn identity, durable turn
  lease, finally-balanced scopes, outer result before `finish_task_run`. Put the
  release holder here, not only in build_turn_context/finalize_turn. Even early
  admission results need safe projection when they contain history.
* `agent/session_persistence.py:119,187-222,337-376`: per-agent persist lock,
  canonical row projection and atomic append_messages_batch; stamp quarantine
  provenance before the first incremental commit, not at finalization.
* `hermes_state_messages.py:21-27`: ONE insert shape covers append, batch,
  replace, compact, import. `SessionDB._execute_write` is the transaction primitive.
  `get_messages_as_conversation:893-909` currently serves BOTH display and replay;
  `_rows_to_conversation:933+` restores api_content verbatim. Do not replace its
  model bytes with a notice.
* `agent/stream_delivery.py`: common display/TTS delivery, interim, reasoning and
  observer enqueue boundaries exist, but no SINGLE method covers all. Guard those
  existing sinks using the same holder. `_fire_stream_delta` currently records
  text only if delivered; keep an internal candidate accumulator independently
  of external delivery so withholding cannot destroy empty-terminal recovery.
* `registration_lifecycle.py:48` transaction plus replacement generation leases;
  `plugins_ledger.py` PluginRegistration/_track_scoped_registration; existing
  dispatch `_run_hook_callback_bounded` already carries timeout/abandoned-worker
  semantics. Extend that machinery rather than launch a second callback engine.
* Existing ContextVar patterns in turn_facade/relay_runtime/aux_accounting support
  scoped identity. A new release ContextVar carries a holder, not mutable process
  config. ContextVar alone does not propagate into a fresh thread or identify late
  workers: explicitly capture the holder in existing stream-writer scopes.

## Proposed canonical API (v1, names provisional)

Host config, frozen into each session's agent initialization:

```
agent.output_release.required_policy: null | 'canonical-plugin-key/policy-id'
```

Null is precisely today's Default behavior. Plugin availability must NEVER decide
whether the requirement exists. Invalid config blocks guarded session admission;
missing, disabled, failed-load or ambiguous named registration means a fixed
body-free notice BEFORE any model or auxiliary preflight call. For depth-none or
other Research exemptions, the selected authority explicitly admits/releases the
turn; an optional context hook cannot silently switch off the requirement.
Do not add prompt/tool schema fields or change the system prompt.

Plugin registration, using PluginContext ownership (plugin cannot assert owner):

```
ctx.register_output_release_policy(
    id='evidence', version=1, prepare=prepare, decide=decide,
) -> PluginRegistration
```

Implement registration in the existing manager/ledger, keyed by profile scope +
canonical manifest key + policy id; reject duplicate ambiguity. Acquire a stable
registration generation under replacement_coordinator.transaction. Reload/unload
revokes that generation: in-flight turns fail closed rather than adopting a new
callback. No replacement policy may authorize an old holder. Ordinary hooks keep
current first-transform and optional failure behavior.

Extend dispatch with a targeted authoritative call returning a host-owned outcome:

```
PolicyOutcome(owner, policy_id, registration_generation,
              status='ok'|'missing'|'revoked'|'timeout'|'error', value=None)
```

Reuse the bounded runner and its queue, enforce a finite positive policy timeout
independent of ordinary hooks' disabling of timeouts, and preserve failure status.
Never parse ownership from a plugin-returned string/dict. Never pass candidates to
all subscribers to solicit authorization. Preparation failure cancels admission;
final decision failure denies release. Late abandoned workers cannot mutate state
or supply a late valid decision.

Core-issued immutable binding:

```
ReleaseBinding(version=1, profile_key, conversation_root_id,
  origin_session_id, task_id, turn_id, request_id, request_digest,
  policy_key, registration_generation, holder_nonce)
```

Use the facade's existing task/relay turn identity as the canonical release turn;
thread it into loop hooks instead of accepting independently generated identities.
`request_id` is a core nonce for this user turn (not a provider retry id).
`request_digest` covers a documented canonical encoding of original user content
including multimodal reference identity; provider attempt ids are separate optional
metadata. Immutable content-addressed inputs are needed if external bytes must be
bound, not merely the URL. Compression may change storage session_id but MUST NOT
change the logical binding. Record child storage-session ids under the same holder.
Unverifiable rotation/clone provenance blocks release.

Preparation receives the binding and read-only request; returns
`{version:1, action:'ready'|'deny', binding:<exact echo>, policy_state:<bounded opaque JSON>}`.
The host already holds output BEFORE preparation. No `nohold` response exists.

After all ordinary transforms, footer additions, fallback shaping and surrogate
normalization, freeze an immutable delivery projection and calculate its SHA-256:

```
ReleaseCandidate(binding, candidate_id, payload_digest,
  payload={final_response, visible_messages, attachments},
  policy_state, completion={failed, partial, interrupted, exit_reason})
ReleaseDecision(version=1, action='release'|'deny', binding,
  candidate_id, payload_digest, reason_code)
```

Canonical digest encoding: UTF-8 JSON with sorted keys, compact separators,
ensure_ascii=False, allow_nan=False; only schema-approved JSON values. Include
EXACT bytes/metadata to be delivered; do not normalize again after authorization.
Attachments are empty in initial v1 unless immutable byte digest + safe delivery
path are implemented. Core validates every binding field, generation and digest;
unknown/missing keys, malformed decisions, wrong identity, stale decisions, None,
timeout, exception, interruption, partial/error completion all deny. `reason_code`
is bounded internal metadata, never plugin-controlled refusal prose. No mutation
of payload after release; any change requires another candidate and decision.

The first version should release only the final public answer, NOT replay buffered
reasoning, drafts, tool arguments or speculative stream fragments. Those were not
authorized merely because the final answer was. This keeps quarantine bounded and
avoids creating a general callback replay engine.

## Persistence and result contract

Proposed implementation requires explicit public/private projection separation.
Raw durable transcript remains intact for replay and crash-resilient tool accounting.
Add durable release provenance to each newly authored guarded row, including tool
rows/sidecars and internal scaffold api_content, before insertion. Do not infer
ownership from role or transcript index after compaction. A turn release record
contains binding, state, candidate digest and approved public projection; pending
or malformed/unknown state is publicly withheld forever unless explicitly released.

Add `SessionDB.get_messages_for_display(...)` and one canonical
`project_output_for_display(...)`; apply the latter to in-memory UI history too.
Public history must consult the durable release record in the SAME read snapshot
as its rows. Release only the approved projection, never flip all raw rows visible.
Keep `get_messages_as_conversation` explicitly model-internal. Audit ALL external
history/search/export/snippet/raw-message readers, not merely session.history; FTS
and trajectory exports can otherwise redisclose withheld bytes. Public readers
must default safe, while internal model restore explicitly opts into raw replay.
A display_kind='hidden' flag alone is insufficient (raw API readers/sidecars).

Use `_execute_write` for atomic pending-row provenance and final release-state plus
public projection commit; retain ordinary short incremental transactions. Do not
hold a SQL transaction through inference. No DB proxy, `_persist_disabled`, or
rewrite of existing content/api_content. Rewrites/compaction/import must preserve
provenance; old unmarked ordinary rows retain old behavior, guarded session rows
with missing provenance cannot be treated as ordinary. Storage failure denies
release; do not deliver an answer whose durable authorization did not commit.

At the facade return boundary, project EVERY return path, including partial,
recovery, early admission, interruption and exceptions. Public result uses an
allowlisted schema: final_response, safe visible messages, typed accounting/status,
release receipt. Omit raw messages, last_reasoning, pre_transform_response and raw
error/cleanup strings on guarded results. Preserve exact internal transcript in
agent._session_messages / explicit internal continuation access; never pass a
public projection back as the next model history. Existing hosts commonly retain
result['messages']; this is a REQUIRED caller migration, not a safe additive flag.

Place authorization before post_llm_call/finish_task_run/result observers; those
receive only public projection on guarded turns. Ordinary transforms and trusted
context engines may need raw transcript internally; their explicit trust is not
an external confidentiality guarantee. Do not accidentally sync raw messages to
an external memory plugin through post_llm_call before authorization. Export/log
helpers with transcript bodies must use public projection or explicit private
storage classification. A no-body notice is permitted, never a report with a
'degraded' footer.

## Holder lifetime and sink fence

States: PENDING -> PREPARED -> SEALED -> RELEASED or DENIED; any failure/interruption
before committed release -> DENIED. A terminal holder cannot be reused. Arm before
any model-capable preflight; bind to the existing lease; reset ContextVar token in
outer finally. Close/revoke holder BEFORE cleanup can reset callbacks or drain
scrubber tails. Existing worker scope captures immutable holder identity + writer
generation; sinks consult that captured holder, never the agent's newest holder.
On next turn create a fresh holder. Late workers from denied/released/closed turns
cannot emit new bytes; release permits only the sealed payload, not future deltas.
New threads without explicit propagation are not allowed to bypass a guarded
agent's sinks. Cancellation after the release commit cannot retract already
released output; commit is the linearization point. Pending records after a crash
remain denied for display; resume must never auto-release.

Guard `_deliver_to_stream_callbacks`, `_deliver_interim`, `_fire_reasoning_delta`,
`_enqueue_stream_hook`, and direct tool-round halt/preview callback paths. Continue
internal provider accumulation regardless. Guard model-derived tool progress args,
print/status bodies and observation payloads too; only fixed core progress states
may pass during a hold. Audit direct callback accesses outside these helpers and
route those through the EXISTING sink helpers, not a parallel callback registry.

## Minimal owner file set and honest remaining boundary

Initial core seam files:

* NEW `agent/output_release.py`: schemas, scoped holder, canonical candidate,
  validation/projection; no callback engine.
* `agent/agent_init.py` + `hermes_cli/config.py`: frozen explicit opt-in resolution.
* `hermes_cli/plugins.py`, `hermes_cli/plugins_dispatch.py`: owned registration and
  targeted bounded authoritative outcomes; reuse ledger/replacement helpers as-is
  where their current methods suffice.
* `agent/turn_facade.py`, `agent/turn_context.py`: arm/prepare, canonical identity,
  outer seal/project/finally; reuse existing turn lease.
* `agent/turn_finalizer.py`: separate transforms from post-call notification;
  authorize normalized final projection before observers, not by replacing raw
  transcript. Direct loop returns sealed at facade.
* `agent/stream_delivery.py`, `agent/turn_tool_round.py`: existing sink guards.
* `agent/session_persistence.py`, `hermes_state_messages.py`, `hermes_state.py`
  and actual schema/migration owner: atomic provenance + durable release projection.
* `tui_gateway/session_history.py` + its server history/restore call sites: public
  display read vs explicit raw continuation read.
* tests: this tracer, real callback/partial/recovery/registration matrix, existing
  native finalizer persistence and Codex history regressions.

This is the minimum seam list, NOT an exhaustively approved implementation file
list. A full controlled-output claim also requires enumerating and migrating every
public history/result caller, export/search consumer and callback bypass (gateway,
CLI, ACP, API, cron as applicable). Search tooling returned intermittent process/
permission failures on broad call-site enumeration during this timeboxed scout;
no claim that this consumer audit is complete. Do not touch the owner's reserved
inline_tool_executors.py, auxiliary_client.py, hermes_cli/main.py or
 tools/async_delegation.py. If closure requires them, ask owner to integrate.

## Next child's acceptance tests

1. Convert this executed RED to actual PluginContext registration/config admission;
   fake model must be untouched for missing/failed prepare. Real DB + native UI
   snapshot during decision, after denial, after reopen contain no marker, while
   explicit internal replay contains exact candidate and provider sidecars.
2. Stream fake provider chunks through actual stream mixin (text, reasoning,
   interim, scrubber tail, end observer), not directly calling a fake sink. Capture
   display/TTS/observers. Deny yields only fixed notice; allow yields exact approved
   final projection once, no draft replay. Include tool round followed by final
   and inspect DB while fake tool is entered; use harmless fake tool, no network.
3. Parameterize missing/disabled/failed-load/prepare timeout/decision exception,
   malformed/None/wrong task/turn/request/digest/registration, post-transform digest,
   partial invalid tool exit, empty-response recovery, cancellation and raw cleanup
   errors. Exercise real AIAgent and native dispatch, not a mocked gate.
4. Two independent agents/sessions, same-agent next turn, late worker via threading
   Events and reload revocation. No sleep race. Pending crash/reopen remains hidden;
   session rotation provenance is preserved. SQL commit failure never publishes.
5. Cache remains enabled: capture both fake provider requests and compare the
   unchanged prior prefix/system/tools; internal content/api_content/reasoning/Codex
   sidecars survive DB reopen exactly. Public result must never become replay input.
   Existing Default/no-required-policy tests remain byte/behavior compatible,
   including optional failed ordinary hook behavior.

## Stop: user/owner scope decision required

Recommend v1 **controlled assistant-output withholding**: display/TTS/callback
observers/public result/history/search/export projections are gated; raw local DB,
internal replay, trusted code/providers are inside the trusted computing boundary.
This is NOT a sandbox against a user/process reading state.db, nor against a model
using shell/network, browser, send_message, attachments or plugin-originated sends.
Blocking those needs separate capability/egress authorization and possibly a
sandbox, not shell-command regexes. If 'no report leaves anywhere' includes those
routes or raw DB inspection, do NOT implement this narrower promise and call it
sound. Obtain user approval of the trust boundary and internal/public continuation
API migration first. No Research plugin author should receive a release API until
that choice and the consumer audit are resolved.
