# Managed output: storage slice and remaining boundaries

This patch is tested storage infrastructure, **not a completed production managed-egress gate**.

## Owner API

- `SessionDB.begin_output_release(session_id, turn_id, binding)` must commit before candidate persistence. Schema v31 owns `output_release_turns`, `output_release_raw`, and `messages.output_raw_id`; policy code creates no tables.
- Canonical message insertions keep public bytes in `messages` before native FTS triggers execute. Candidate content, tool arguments/results, reasoning, API sidecars, and display metadata are in the raw sidecar. Ordinary public read SQL is unchanged. Historical rows are not rewritten. New user rows remain public unless marked as derivatives. Guarding is sticky and follows parent lineage.
- `get_messages_as_conversation(..., trusted_raw=True)` explicitly restores sidecars before existing conversation decoding. Public reads, search snippets/context, anchored views, and exports use the public table.
- `publish_output_release(session_id, turn_id, binding, text)` is a **trusted gate-only** final-text publication API. It verifies the stored binding and selects the latest active assistant candidate in the turn. It is not a standalone authorization API or the final immutable candidate/digest-bound writer protocol.
- Native SQL tail clones carry the raw reference. Public-dict compaction copies preserve public bytes only with matching row provenance and matching public field values. Missing raw references raise rather than publish copied content. Export deliberately omits local raw references; importing a public export does not import raw material.

## Verification

Strict vertical red/green tests exercised real SQLite stores, native FTS, context enrichment, public history, row identities, export/import/reopen, derived child summaries, compaction, and trusted raw replay. The existing agent/RPC history release tracer passes all seven modes.

The native search compatibility failure was a test instrumentation bug: `_get_read_conn()` leased a connection without returning it, so later search used a different connection from the one traced. The test now traces the actual `_read_ctx()` lease; its zero-enrichment-versus-one-enrichment assertions remain unchanged.

## Must resolve before production rollout

- Upgrade of databases actually used by the old experimental `output_release_public` aggregate is not implemented; those rows lack trustworthy per-row start provenance. Do not open such a database with this patch and assume historical quarantine migrated.
- Audit and adapt direct SQL message **updates**, transcript repair, and every noncanonical writer. This boundary is not an arbitrary-SQL-write sandbox.
- Wire model-facing paired resume/compaction readers explicitly to trusted raw restoration; only the named `trusted_raw=True` conversation path is restored here. Other raw consumers must not accidentally receive the public placeholder.
- Guard generated session metadata (titles, prompts, model config, lineage metadata), not only message columns. Export includes session metadata unchanged.
- Finish immutable candidate-id/digest/decision binding, replay/idempotency checks, row-addressed publication, and private-sidecar retention/deletion. Current publication chooses the latest turn assistant row and can be called again by trusted code.
- A derivative without lineage, guard state, or any provenance marker cannot be recognized as guarded. Import/branch producer contracts must preserve that provenance. Destructive replacements can also lose the public-copy source row and conservatively withhold it.
- Ordinary read SQL retains existing performance; write-side lineage lookup and trusted-raw per-row hydration are not performance-optimized.

No deploy, config changes, or commit were performed.
