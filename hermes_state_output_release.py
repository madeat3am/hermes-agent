"""Owner-managed quarantine: messages/FTS contain public bytes only.

The raw sidecar is reachable only through explicit trusted replay. Guarding is
sticky for a session: later summaries with missing provenance cannot become public.
This is a managed storage boundary, not protection against arbitrary SQL writers.
"""
import json
import uuid

NOTICE = 'Output withheld.'
# Positions in the canonical message INSERT. Identity/ordering remain native.
_PRIVATE_FIELDS = {
    2: 'content', 3: 'tool_call_id', 4: 'tool_calls', 5: 'tool_name',
    6: 'effect_disposition', 9: 'finish_reason', 10: 'reasoning',
    11: 'reasoning_content', 12: 'reasoning_details', 13: 'codex_reasoning_items',
    14: 'codex_message_items', 15: 'platform_message_id', 19: 'api_content',
    20: 'display_kind', 21: 'display_metadata',
}


def public_session_metadata(data):
    # Preserve routing/identity/counters; generated free-form fields are not decisions.
    private = ('title', 'title_source', 'system_prompt', 'system_prompt_hash', 'model_config',
               'origin_json', 'display_name', 'last_activity_description', 'last_activity_provenance',
               'handoff_error', 'compression_failure_error', 'tool_names')
    return {k: None if k in private else v for k, v in data.items()}


class UnsupportedOutputReleaseSchemaError(RuntimeError):
    """Experimental aggregate quarantine has no safe per-row migration."""


def reject_legacy_output_release(cursor):
    if cursor.execute("SELECT 1 FROM sqlite_master WHERE name='output_release_public'").fetchone():
        raise UnsupportedOutputReleaseSchemaError('Legacy output_release_public requires explicit recovery')


def install_output_release_triggers(cursor):
    cursor.execute("""CREATE TRIGGER IF NOT EXISTS output_release_delete_raw AFTER DELETE ON messages
        WHEN old.output_raw_id IS NOT NULL
        BEGIN DELETE FROM output_release_raw WHERE id=old.output_raw_id
        AND NOT EXISTS (SELECT 1 FROM messages WHERE output_raw_id=old.output_raw_id); END""")
    # Also handles already-created v31 experimental tables without cascade FKs.
    cursor.execute("""CREATE TRIGGER IF NOT EXISTS output_release_delete_turns BEFORE DELETE ON sessions
        BEGIN DELETE FROM output_release_turns WHERE session_id=old.id; END""")
    cursor.execute("""CREATE TRIGGER IF NOT EXISTS output_release_inherit_guard AFTER INSERT ON sessions
        WHEN EXISTS (SELECT 1 FROM sessions p WHERE p.id=new.parent_session_id AND p.output_guarded=1)
        BEGIN UPDATE sessions SET output_guarded=1 WHERE id=new.id; END""")
    # Managed direct UPDATEs (including transcript repair) must not bypass quarantine.
    private_changes = ' OR '.join(f'new.{name} IS NOT old.{name}'
                                 for name in _PRIVATE_FIELDS.values() if name != 'content')
    cursor.execute(f'''CREATE TRIGGER IF NOT EXISTS output_release_update_guard
        BEFORE UPDATE ON messages WHEN old.output_raw_id IS NOT NULL AND (
            new.output_raw_id IS NOT old.output_raw_id OR new.session_id IS NOT old.session_id OR
            new.role IS NOT old.role OR {private_changes} OR
            (new.content IS NOT old.content AND NOT EXISTS (
                SELECT 1 FROM output_release_candidates c WHERE c.message_id=old.id
                AND c.published=1 AND c.public_text IS new.content)))
        BEGIN SELECT RAISE(ABORT, 'guarded message update requires release'); END''')


class SessionOutputReleaseMixin:
    def begin_output_release(self, session_id, turn_id, binding):
        """Arm BEFORE candidate persistence; never changes historical public rows."""
        if not session_id or not turn_id:
            raise ValueError('Release requires session and turn identity')
        encoded = json.dumps(binding, sort_keys=True, allow_nan=False)
        def arm(conn):
            conn.execute("UPDATE sessions SET output_guarded=1 WHERE id=?", (session_id,))
            conn.execute('INSERT INTO output_release_turns VALUES (?, ?, ?, '
                         '(SELECT COALESCE(MAX(id), 0) FROM messages))',
                         (session_id, turn_id, encoded))
        self._execute_write(arm)

    def _insert_public_message(self, conn, sql, params, msg):
        guard = conn.execute('WITH RECURSIVE lineage(id) AS (SELECT ? UNION '
                             'SELECT s.parent_session_id FROM sessions s JOIN lineage l ON s.id=l.id '
                             'WHERE s.parent_session_id IS NOT NULL) '
                             'SELECT t.turn_id FROM output_release_turns t JOIN lineage l ON l.id=t.session_id '
                             'ORDER BY t.start_message_id DESC, t.rowid DESC LIMIT 1', (params[0],)).fetchone()
        if guard is None and conn.execute('SELECT 1 FROM sessions WHERE id=? AND output_guarded=1',
                                          (params[0],)).fetchone():
            guard = ('inherited-guard',)
        raw_id = msg.get('output_raw_id')
        # Users remain public unless explicitly derivative (compaction/tool delivery).
        derived = params[1] != 'user' or params[17] or params[20]
        if not raw_id and not (guard and derived):
            return conn.execute(sql, params)
        values = list(params)
        payload = {name: values[index] for index, name in _PRIVATE_FIELDS.items()}
        public_copy = False
        if raw_id:
            source = conn.execute('SELECT payload FROM output_release_raw WHERE id=?', (raw_id,)).fetchone()
            if source is None:
                raise ValueError('Missing guarded message provenance')
            public_row = conn.execute('SELECT * FROM messages WHERE id=? AND output_raw_id=?',
                                      (msg.get('id') or msg.get('_row_id'), raw_id)).fetchone()
            public_copy = public_row is not None and all(
                public_row[name] == payload[name] for name in _PRIVATE_FIELDS.values())
        else:
            raw_id = uuid.uuid4().hex
            conn.execute('INSERT INTO output_release_raw VALUES (?, ?, ?, ?)',
                         (raw_id, params[0], guard[0], json.dumps(payload, ensure_ascii=True)))
        if not public_copy:
            for index in _PRIVATE_FIELDS:
                values[index] = NOTICE if index == 2 else None
        # INSERT public bytes before ANY FTS trigger observes the row.
        cursor = conn.execute(sql, values)
        conn.execute('UPDATE messages SET output_raw_id=? WHERE id=?', (raw_id, cursor.lastrowid))
        return cursor

    def stage_output_release(self, session_id, turn_id, binding, text, *, message_id=None):
        """Freeze the exact candidate before policy evaluation; ambiguous rows fail closed."""
        from agent.output_release import canonical
        import hashlib
        encoded = json.dumps(binding, sort_keys=True, allow_nan=False)
        if not isinstance(text, str) or not text:
            raise ValueError('Public output must be nonempty text')
        digest = hashlib.sha256(canonical({'final_response': text,
            'visible_messages': [{'role': 'assistant', 'content': text}], 'attachments': []})).hexdigest()
        def stage(conn):
            turn = conn.execute('SELECT binding FROM output_release_turns WHERE session_id=? AND turn_id=?',
                                (session_id, turn_id)).fetchone()
            if turn is None or turn[0] != encoded:
                raise ValueError('Release binding mismatch')
            rows = conn.execute("SELECT m.id, r.payload FROM messages m JOIN output_release_raw r ON r.id=m.output_raw_id "
                "WHERE m.session_id=? AND r.session_id=? AND r.turn_id=? AND m.role='assistant' AND m.active=1",
                (session_id, session_id, turn_id)).fetchall()
            rows = [r for r in rows if (r['id'] == message_id if message_id is not None
                    else json.loads(r['payload'])['content'] == text)]
            if len(rows) != 1:
                raise ValueError('Missing or ambiguous exact release candidate')
            row = rows[0]
            prior = conn.execute('SELECT * FROM output_release_candidates WHERE session_id=? AND turn_id=?',
                                 (session_id, turn_id)).fetchone()
            if prior:
                if (prior['message_id'], prior['binding'], prior['raw_payload'], prior['public_text']) != (
                        row['id'], encoded, row['payload'], text):
                    raise ValueError('Immutable release candidate changed')
                candidate_id = prior['candidate_id']
            else:
                candidate_id = uuid.uuid4().hex
                conn.execute('INSERT INTO output_release_candidates '
                    '(candidate_id, session_id, turn_id, message_id, binding, payload_digest, raw_payload, public_text) '
                    'VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
                    (candidate_id, session_id, turn_id, row['id'], encoded, digest, row['payload'], text))
            return {'candidate_id': candidate_id, 'message_id': row['id'], 'payload_digest': digest}
        return self._execute_write(stage)

    def publish_output_release(self, session_id, turn_id, binding, text, *,
                               candidate_id=None, message_id=None, payload_digest=None):
        """Gate-only writer: immutable receipt, exact row/bytes, idempotent retry."""
        encoded = json.dumps(binding, sort_keys=True, allow_nan=False)
        def publish(conn):
            row = conn.execute('SELECT c.*, r.payload AS current_raw, m.active, m.role, m.session_id AS current_session '
                'FROM output_release_candidates c JOIN messages m ON m.id=c.message_id '
                'JOIN output_release_raw r ON r.id=m.output_raw_id WHERE c.candidate_id=?', (candidate_id,)).fetchone()
            turn = conn.execute('SELECT binding FROM output_release_turns WHERE session_id=? AND turn_id=?',
                                (session_id, turn_id)).fetchone()
            if (row is None or turn is None or turn[0] != encoded or
                (row['session_id'], row['turn_id'], row['binding'], row['public_text'], row['message_id'], row['payload_digest']) !=
                (session_id, turn_id, encoded, text, message_id, payload_digest) or
                row['raw_payload'] != row['current_raw'] or row['current_session'] != session_id or
                row['role'] != 'assistant' or not row['active']):
                raise ValueError('Release candidate/binding mismatch')
            if not row['published']:
                conn.execute('UPDATE output_release_candidates SET published=1 WHERE candidate_id=?', (candidate_id,))
                conn.execute('UPDATE messages SET content=? WHERE id=?', (text, message_id))
            return message_id
        return self._execute_write(publish)

    def _restore_output_raw_rows(self, rows):
        restored = []
        with self._read_ctx() as conn:
            for row in rows:
                item = dict(row)
                ref = conn.execute('SELECT output_raw_id FROM messages WHERE id=?', (row['id'],)).fetchone()
                if ref and ref[0]:
                    raw = conn.execute('SELECT payload FROM output_release_raw WHERE id=?', (ref[0],)).fetchone()
                    if raw is None:
                        raise ValueError('Missing guarded message provenance')
                    item.update(json.loads(raw[0]))
                restored.append(item)
        return restored
