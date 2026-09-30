import json
import pytest


def test_candidate_is_exact_row_bound_and_immutable_across_connections(tmp_path):
    db = SessionDB(tmp_path / 'race.db')
    db.create_session(session_id='s', source='cli')
    db.begin_output_release('s', 't', {})
    first = db.append_message('s', 'assistant', 'approvedanswer')
    receipt = db.stage_output_release('s', 't', {}, 'approvedanswer')
    other = SessionDB(tmp_path / 'race.db')
    second = other.append_message('s', 'assistant', 'racingdraft')
    assert db.publish_output_release('s', 't', {}, 'approvedanswer', **receipt) == first
    assert db.publish_output_release('s', 't', {}, 'approvedanswer', **receipt) == first
    assert other.get_messages('s')[-1]['content'] == 'Output withheld.'
    for text, binding, changes in [('changed', {}, {}), ('approvedanswer', {'changed': 1}, {}),
                                   ('approvedanswer', {}, {'message_id': second})]:
        with pytest.raises(ValueError):
            db.publish_output_release('s', 't', binding, text, **{**receipt, **changes})
    other.close()
    db.close()


def test_native_update_cannot_republish_guarded_payload(tmp_path):
    import sqlite3
    db = SessionDB(tmp_path / 'updates.db')
    db.create_session(session_id='s', source='cli')
    db.begin_output_release('s', 't', {})
    row = db.append_message('s', 'assistant', 'nativeupdatesentinel')
    with pytest.raises(sqlite3.IntegrityError, match='guarded'):
        db._execute_write(lambda c: c.execute('UPDATE messages SET content=? WHERE id=?',
                                             ('nativeupdatesentinel', row)))
    assert not db.search_messages('nativeupdatesentinel')
    assert db.get_messages_as_conversation('s', trusted_raw=True)[0]['content'] == 'nativeupdatesentinel'
    db.close()


@pytest.mark.parametrize('read_only', [False, True])
def test_old_experimental_aggregate_is_rejected(tmp_path, read_only):
    import sqlite3
    path = tmp_path / 'legacy.db'
    db = SessionDB(path)
    db.close()
    with sqlite3.connect(path) as conn:
        conn.execute('CREATE TABLE output_release_public (session_id TEXT, payload TEXT)')
    from hermes_state_output_release import UnsupportedOutputReleaseSchemaError
    with pytest.raises(UnsupportedOutputReleaseSchemaError):
        SessionDB(path, read_only=read_only)


def test_guarded_metadata_export_and_import_remain_guarded(tmp_path):
    db = SessionDB(tmp_path / 'metadata.db')
    db.create_session(session_id='s', source='cli', model_config={'secret': 'metasentinel'},
                      system_prompt='metasentinel')
    db.begin_output_release('s', 't', {})
    db.set_session_title('s', 'metasentinel')
    assert 'metasentinel' not in json.dumps(db.get_session('s'))
    assert db.get_session_title('s') is None
    exported = db.export_session('s')
    assert 'metasentinel' not in json.dumps(exported)
    assert 'metasentinel' not in json.dumps(db.list_sessions_rich())
    assert db.get_session('s', trusted_raw=True)['system_prompt'] == 'metasentinel'
    other = SessionDB(tmp_path / 'importmeta.db')
    assert other.import_sessions([exported])['ok']
    other.append_message('s', 'assistant', 'importsentinel')
    assert 'importsentinel' not in json.dumps(other.export_session('s'))
    other.close()
    db.close()


def test_private_sidecars_follow_native_deletion(tmp_path):
    db = SessionDB(tmp_path / 'delete.db')
    db.create_session(session_id='s', source='cli')
    db.begin_output_release('s', 't', {})
    db.append_message('s', 'assistant', 'deletesentinel')
    db.delete_session('s')
    with db._read_ctx() as conn:
        assert not conn.execute('SELECT 1 FROM output_release_raw').fetchall()
        assert not conn.execute('SELECT 1 FROM output_release_turns').fetchall()
    db.close()


def test_guarded_lineage_and_missing_import_provenance_fail_closed(tmp_path):
    db = SessionDB(tmp_path / 'lineage.db')
    db.create_session(session_id='parent', source='cli')
    db.begin_output_release('parent', 'turn', {})
    db.append_message('parent', 'assistant', 'secretsentinel')
    db.create_session(session_id='child', source='cli', parent_session_id='parent')
    db.append_message('child', 'user', 'secretsentinel', _compressed_summary=True)
    assert 'secretsentinel' not in json.dumps(db.export_session('child'))
    assert db.get_messages_as_conversation('child', trusted_raw=True)[0]['content'] == 'secretsentinel'
    with pytest.raises(ValueError, match='provenance'):
        db.append_messages_batch('child', [{'role': 'assistant', 'content': 'secretsentinel', 'output_raw_id': 'missing'}])
    db._execute_write(lambda c: c.execute('UPDATE sessions SET parent_session_id=NULL WHERE id=?', ('child',)))
    db.append_message('child', 'assistant', 'detachedsentinel')
    assert 'detachedsentinel' not in json.dumps(db.export_session('child'))
    db.close()

from hermes_state import SessionDB


def test_released_public_copy_survives_compaction_without_raw_republication(tmp_path):
    db = SessionDB(tmp_path / 'compact.db')
    db.create_session(session_id='s', source='cli')
    db.begin_output_release('s', 'turn', {})
    row_id = db.append_message('s', 'assistant', 'compactionsentinel')
    receipt = db.stage_output_release('s', 'turn', {}, 'approvedanswer', message_id=row_id)
    assert db.publish_output_release('s', 'turn', {}, 'approvedanswer', **receipt) == row_id
    assert db.search_messages('approvedanswer')
    public = db.get_messages('s')
    db.archive_and_compact('s', public)
    assert db.get_messages('s')[0]['content'] == 'approvedanswer'
    assert db.get_messages_as_conversation('s', trusted_raw=True)[0]['content'] == 'compactionsentinel'
    assert not db.search_messages('compactionsentinel')
    assert 'compactionsentinel' not in json.dumps(db.get_anchored_view('s', row_id))
    db.close()


def test_guarded_storage_is_public_by_construction_and_raw_replay_exact(tmp_path):
    db = SessionDB(tmp_path / 'state.db')
    db.create_session(session_id='s', source='cli')
    old = db.append_message('s', 'assistant', 'historical public')
    db.begin_output_release('s', 'turn-1', {'policy': 'test'})
    user = db.append_message('s', 'user', 'public question')
    candidate = db.append_message('s', 'assistant', 'quarantinesentinel', reasoning='quarantinesentinel')
    assert [m['id'] for m in db.get_messages('s')] == [old, user, candidate]
    assert 'historical public' in json.dumps(db.get_messages('s'))
    assert 'quarantinesentinel' not in json.dumps(db.get_messages('s'))
    assert not db.search_messages('quarantinesentinel')
    assert 'quarantinesentinel' not in json.dumps(db.search_messages('question'))
    assert 'quarantinesentinel' not in json.dumps(db.get_messages_around('s', candidate))
    raw = db.get_messages_as_conversation('s', trusted_raw=True)
    assert raw[-1]['content'] == 'quarantinesentinel'
    assert raw[-1]['reasoning'] == 'quarantinesentinel'
    exported = db.export_session('s')
    assert 'quarantinesentinel' not in json.dumps(exported)
    db.close()
    db = SessionDB(tmp_path / 'state.db')
    assert db.get_messages_as_conversation('s', trusted_raw=True)[-1]['content'] == 'quarantinesentinel'
    other = SessionDB(tmp_path / 'import.db')
    assert other.import_sessions([exported])['ok']
    assert 'quarantinesentinel' not in json.dumps(other.export_session('s'))
    other.close()
    db.close()
