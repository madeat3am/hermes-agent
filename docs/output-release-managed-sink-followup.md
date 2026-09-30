# Managed sink follow-up — NOT READY TO ENABLE

This is a bounded prototype, not a complete managed-publication gate. No runtime
configuration, deployment, model/job changes, staging or gate commit was performed.
Raw local DB/replay/cache prefixes remain trusted internal state.

## Verified closed in this follow-up

- Required named authority is checked against the existing profile-scoped plugin
  manager before client/tool/memory/context-engine initialization. Missing authority
  raises a fixed ValueError. Authority must already have registered via the normal
  PluginContext lifecycle; this does not introduce a second callback registry.
- Constructor context-engine on_session_start is deferred until the first successful
  per-turn prepare. Prepare uses the existing bounded plugin worker. A real timed-out
  prepare returns withheld, with no main model request or context-start callback.
  Revoked authority and malformed decisions continue failing closed.
- Blanket standard-tool rejection is narrowed to the explicit web_search/web_extract
  read allowlist, intersected with native tool_may_have_side_effect classification.
  Unknown tools, writes, sends, terminal and execute_code remain denied; a read_only
  annotation never authorizes arbitrary code. No shell/network sandbox is claimed.
- Real AIAgent/native tool dispatcher/native web tools/local HTTP server tracer runs
  search -> extract -> third model response -> exact candidate decision. A real HTTP
  page body reaches trusted model context. Deny and approved-digest release both work;
  private preview/body stays absent from tested callbacks and public SQLite history.
  Only the local test fixture's SSRF check is replaced; production URL safety is unchanged.
- Existing pre_tool_call veto still executes, blocks both reads before HTTP dispatch,
  and does not publish tool bodies. Final publication remains independently authorized.
- Shared deny-only callback sinks cover callbacks assigned on guarded agents, including
  captured provider callbacks, tool progress/cards, thinking/reasoning, status and Codex
  event-bridge callbacks. The raw status printer is guarded too. Ordinary callbacks are
  not wrapped. Approved output is returned through the authority result, not raw streaming.
- Existing plugin dispatch suppresses pre_api_request/post_api_request/post_tool_call
  observers in guarded active-parent contexts, without disabling pre_tool_call decisions.
- Previous tested guards remain: send_message entry, native live-history fallback,
  external-memory finalizer sync, tool generation, cleanup exception boundary.

Tests: tests/run_agent/test_output_release_authority_reads.py (8 cases), plus existing
output_release_design_tracer, managed_sinks and init_boundary files. New behavioral
changes were observed red before fixes; pre-existing veto/ordinary/classification
behavior was additionally checked. Exact candidate_id/payload_digest are obtained from
stage_output_release's current receipt API, not reconstructed from an old storage API.

## Remaining blockers — do not enable

1. This constructor checks already-registered authority, not full cold plugin discovery
   or the upstream config loader's malformed-file swallowing behavior. Required policy
   startup discovery/registration ordering across every host needs independent proof.
   Constructor memory/context initialize methods (distinct from on_session_start) can
   still do model work after presence check, before request-specific prepare.
2. Tool safety is proved for standard dispatch of the two named native readers only.
   Provider-managed Codex tools, direct native invocations, middleware rewrites/replacements,
   plugin overrides of allowlisted names and tools called outside active-parent context
   need host-owned effect enforcement. Pre-tool policy plugins remain trusted decision
   code and receive arguments; their arbitrary own network activity is not sandboxed.
3. Other observers/middleware, context-engine observation, background title/review,
   alternate Codex memory sync, commit_memory_session and trajectory/error logs remain
   unaudited. The tracer exposes raw bodies in trusted internal diagnostic logs; this pass
   does NOT prove an external-log-export boundary. No all-provider/all-sink proof exists.
4. Assignment-time callback wrappers are deny-only, not a complete per-turn ownership
   capability. Original callbacks retained outside the agent, replacement of internal
   methods, unpropagated worker contexts, stale workers, unload/reload and concurrent-turn
   combinations require end-to-end testing. Do not treat wrapper coverage as a sandbox.
5. Direct send internals/native platform sends without active-parent context remain
   bypasses. No immutable attachment/reference publication policy exists; attachment
   directives in final text and arbitrary mutable file publication remain blockers.
6. Storage-owner changes are separate. This pass does not certify all detached histories,
   search/export, gateway/CLI/ACP/cron readers, reopen/cache-sidecar recovery or raw-exception
   pathways. Public SQLite and live native-history tests are not the full reader matrix.

Files changed by this follow-up: agent/{agent_init,output_release,stream_delivery,
status_output,turn_facade,turn_tool_round}.py; hermes_cli/plugins_dispatch.py;
tests/run_agent/test_output_release_authority_reads.py; registration setup ordering in
the two existing design_tracer/managed_sinks tests; this document. No storage edits.
