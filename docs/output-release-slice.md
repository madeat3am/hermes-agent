# Experimental output-release vertical slice — NOT READY TO INSTALL

This is implemented code, not the completed managed-output/publication boundary.
Do not enable `agent.output_release.required_policy` in any production profile.
No runtime configuration, staging, commits or deployment were performed. The
approved scope remains all managed output/publication, with local raw storage,
trusted internal replay and arbitrary shell/browser/plugin-originated egress out
of scope. This slice does NOT yet satisfy that scope.

## Executed seams

`tests/run_agent/test_output_release_design_tracer.py` runs the real AIAgent
facade/loop/finalizer, registered PluginContext authority, native SessionDB and
native `session.history` RPC from a separate SQLite connection DURING decision.
Only provider transport and external initialization are mocked. The same fake
transport calls actual stream mixin methods, including reasoning and scrubber
reset, and asserts raw internal storage still contains the candidate.

Receipts in `~/.hermes/cache/hermes-repair-20260912/`:
- `release-original-red.txt`: original history/result marker leak (failed).
- `release-api-red.txt`: converted registration/config test before implementation (failed).
- `release-green-attempt.txt`: initial real registered denial vertical slice (passed).
- `release-stream-red.txt`: added actual reasoning callback marker leak (failed).
- `release-green-compat.txt`: seven release cases plus four ordinary transform tests,
  11 passed. Cases: deny, allow exact transformed answer, wrong digest, unload
  during decision, missing registration, prepare exception, decision exception.
- `release-storage-compat.txt`: 288 passed, one FAILED. Failure is
  `TestFTS5Search.test_search_projection_skips_context_enrichment_queries`:
  expected one traced context query, observed zero. Root cause/baseline not yet
  established; do not label this compatibility green.
- `release-consumer-inventory.json`: 34 matching callsites across agent,
  tui_gateway, gateway, hermes_cli, ACP and cron. It is a discovery inventory,
  not proof that every public consumer was migrated.

The tracer still exhibits an existing background DB read-in-flight teardown
warning; fixture lifecycle cleanup needs work before accepting it as final QA.

## Concrete experimental consumer API

The host freezes `agent.output_release.required_policy` at agent initialization:
null/unset retains ordinary behavior; a string names `<canonical-plugin-key>/<id>`.
The PluginContext establishes ownership; callback-returned identity cannot confer
ownership. Registration uses the existing replacement coordinator and ownership
ledger. Disposal revokes the exact generation; no duplicate callback engine.

An actual executable consumer is in the tracer (real PluginContext registration,
not a fake hook API). An external policy can use this shape:

```python
def register(ctx):
    def prepare(*, binding, request):
        # Establish policy-specific evidence state here; no no-hold exemption.
        return {"version": 1, "action": "ready", "binding": binding}

    def decide(*, binding, candidate_id, payload_digest, payload, policy_state):
        # Replace this deny-only example with the plugin's evidence adjudicator.
        return {"version": 1, "action": "deny", "binding": binding,
                "candidate_id": candidate_id, "payload_digest": payload_digest}

    return ctx.register_output_release_policy(
        id="evidence", version=1, prepare=prepare, decide=decide)
```

Wire schemas implemented in `agent/output_release.py`:
- binding: version, profile_key, session_id, task_id, turn_id, request_digest,
  policy_key, generation, nonce. These are host-issued; exact echo required.
- prepare: version=1, action=ready, binding; optional policy_state (JSON, <=64KiB).
- candidate: binding, candidate_id, payload_digest, payload, policy_state.
- payload: final_response, visible_messages (only final assistant), attachments=[].
- decision: EXACT keys version=1, action=release, binding, candidate_id,
  payload_digest. Any mismatch/unknown key/exception/non-dict denies. Deny is fixed
  host text; plugin refusal prose is not delivered.
- encoding: UTF-8 JSON, sorted keys, compact separators, ensure_ascii=False,
  allow_nan=False. SHA-256 covers the final post-transform/post-surrogate payload.
- public result: final_response, messages, failed, completed, output_release
  receipt. Raw reasoning, raw transcript, errors and pre-transform response omitted.

Bounded runner is the existing manager `_run_hook_callback_bounded`, with a
positive five-second cap independent of optional-hook timeout disabling. Workers
receive detached payload copies, never the holder. The registration generation is
checked again under the replacement transaction at authorization commit.

## Persistence shape and known deliberate limitations

A lazily created `output_release_public` table stores session_id (primary key),
messages (public JSON), binding, digest. It is armed before the loop's first raw
write. Existing raw message rows and api_content remain untouched. Pending/denied
turns append no candidate to the public stream; release appends only the approved
final message in a short `_execute_write` transaction. There is no long-running
inference transaction and no draft-buffer replay.

`get_messages` and `get_messages_as_conversation` default to this public stream
for the directly guarded session. Explicit `trusted_raw=True` on the latter
retains native raw replay. The facade takes guarded continuation from that raw
reader rather than caller result['messages'] (which is public).

This is a SESSION quarantine, not the spec's required per-row provenance design.
Preexisting history is hidden when quarantine is first armed. Ancestors, clones,
compaction/import/adoption and session rotation are NOT safely migrated. Pagination
and row-id semantics of the projected stream are not complete. The table needs a
proper schema migration/storage-owner implementation before production. A changed
storage session denies final authorization, but raw child rows may already have
escaped other readers. This limitation is a release blocker, not an exemption.

## Gated paths in this slice

- Normal outer facade result, loop early/partial/error result projection, lease
  admission result, exceptions in the facade try body; finish_task_run sees the
  public result, not raw candidate.
- Missing authority and prepare failure before conversation loop/model work.
- `_deliver_to_stream_callbacks` (display/TTS), `_deliver_interim`,
  `_fire_reasoning_delta`, `_enqueue_stream_hook`, including scrubber flush through
  the shared callback sink. These remain closed even after final release; no late
  delta can be enabled merely because a holder was released. Raw stream text still
  accumulates for recovery. Final approved text is returned once, not stream-replayed.
- `post_llm_call` raw finalizer notification suppressed for guarded turns (public
  replacement notification has NOT yet been wired).
- Native history backed by the directly guarded SessionDB read path; public
  `get_messages` and `get_messages_as_conversation`; explicit internal raw replay.
- Final exact transformed payload authority and registration-disposal revocation.

## Unresolved managed boundaries — mandatory follow-up, not safe to enable

1. Direct tool-round previews/halt callbacks, tool arguments/progress printers,
   `_fire_tool_gen_started`, provider/Codex-specific direct sinks and observer
   payloads (`pre_api_request`, `post_api_request`, tool hooks). Stream worker
   captured per-turn identity must be integrated with the existing writer token;
   the current permanent guarded-agent sink closure is not a completed worker audit.
2. External memory sync (`_sync_external_memory_for_turn`), context engine
   observation, background review/title work, trajectory exports, result cleanup
   and exceptions raised by outer finally or before its try. Initialization can
   still run model-capable plugins before authority preparation. Config-load errors
   currently retain existing fallback behavior; malformed policy settings need a
   strict resolver independent of optional initialization fallback.
3. In-memory native session.history while running, gateway/CLI/ACP/API/cron result
   consumers retaining histories, independent restart/continuation paths. Only the
   guarded facade's continuation is migrated; no complete host migration claim.
4. `hermes_state_search.py`: search_messages/context enrichment, get_anchored_view,
   list_recent_user_messages and FTS/snippets. `hermes_state_messages.py`:
   get_messages_around, find_pr_url_messages, other direct SQL/lineage readers.
   `hermes_state_portability.py`: exports, imports and lineage adoption need explicit
   public/trusted classification. Export methods relying on get_messages may inherit
   projection, but no export security guarantee has been tested.
5. Managed publication tools including send_message and attachments ARE NOT GATED
   by this slice. Immutable artifact/hash authorization or explicit tool blocking
   through the same holder is still mandatory. Attachments=[] in the final schema
   does not prevent a tool from publishing during the turn.
6. Required authority timeout/reload/concurrent turns/late workers, denial after
   partial/recovery/interruption, commit failures, exact cache-prefix/provider
   sidecars across reopen, and crash/restart require the full executed matrix.
   Current tests cover real raw candidate retention, not all sidecar/cache invariants.
7. Binding still needs logical conversation-root/origin-session/request identity,
   multimodal immutable input identity, completion metadata and exact schema hardening
   (including bool-vs-int JSON types). Do not relay this as the finalized plugin API.

Do not install this gate alongside independently approved recall/vision changes.
Parent specs/security review and closure of these boundaries are prerequisites.
