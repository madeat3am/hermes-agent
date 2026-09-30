"""Real process read/modify/replace regressions; all credentials are fixtures."""
import multiprocessing
import os
import sys
from pathlib import Path

import pytest


pytestmark = pytest.mark.macos_only if sys.platform == "darwin" else pytest.mark.linux_only


def _writer(home, key, remove, entered, release, attempted, done):
    os.environ['HERMES_HOME'] = str(home)
    from hermes_cli import config
    original = config._write_env_lines
    if entered is not None:
        def paused(*args, **kwargs):
            entered.set()
            assert release.wait(15), 'parent did not release writer'
            return original(*args, **kwargs)
        config._write_env_lines = paused
    attempted.set()
    if remove:
        config.remove_env_value(key)
    else:
        config.save_env_value(key, 'new')
    done.set()


@pytest.mark.parametrize('initial,remove', [('', False), ('KEEP=original\nFIRST=old\nSECOND=old\nDROP=old\n', False), ('KEEP=original\nFIRST=old\nDROP=old\n', True)])
def test_native_waiting_writer_reads_latest(tmp_path, initial, remove):
    home = tmp_path / 'profile'
    home.mkdir(mode=0o700)
    target = home / '.env'
    if initial:
        target.write_text(initial)
    ctx = multiprocessing.get_context('spawn')
    entered, release, attempted1, attempted2, done1, done2 = [ctx.Event() for _ in range(6)]
    first = ctx.Process(target=_writer, args=(home, 'FIRST', False, entered, release, attempted1, done1))
    second = ctx.Process(target=_writer, args=(home, 'DROP' if remove else 'SECOND', remove, None, release, attempted2, done2))
    first.start()
    try:
        assert entered.wait(15)
        second.start()
        assert attempted2.wait(15)
        assert not done2.wait(2), 'peer committed while first held a stale read/modify candidate'
    finally:
        release.set()
        for process in (first, second):
            if process.pid is not None:
                process.join(15)
                if process.is_alive():
                    process.kill()
                    process.join()
    assert first.exitcode == second.exitcode == 0
    text = target.read_text()
    assert text.count('FIRST=new\n') == 1
    assert 'FIRST=old' not in text
    if remove:
        assert 'DROP=' not in text
    else:
        assert 'SECOND=new\n' in text
    if initial:
        assert 'KEEP=original\n' in text


def test_mode_is_preserved_at_atomic_commit(tmp_path, monkeypatch):
    from hermes_cli import config
    monkeypatch.setenv('HERMES_HOME', str(tmp_path))
    target = tmp_path / '.env'
    target.write_text('# keep\nKEEP=original')
    target.chmod(0o640)
    original = config.atomic_replace
    def checked(source, destination):
        assert Path(source).stat().st_mode & 0o777 == 0o640
        assert target.read_text() == '# keep\nKEEP=original'
        original(source, destination)
    monkeypatch.setattr(config, 'atomic_replace', checked)
    config.save_env_value('SECOND', 'new')
    assert target.read_text() == '# keep\nKEEP=original\nSECOND=new\n'


def test_directory_is_synced_before_publish(tmp_path, monkeypatch):
    import stat
    from hermes_cli import config
    monkeypatch.setenv('HERMES_HOME', str(tmp_path))
    synced = []
    original = os.fsync
    def sync(fd):
        synced.append(stat.S_ISDIR(os.fstat(fd).st_mode))
        original(fd)
    def publish(key, value):
        assert synced == [False, True]
    monkeypatch.setattr(os, 'fsync', sync)
    monkeypatch.setattr(config, '_publish_env_value', publish)
    config.save_env_value('DURABLE', 'fixture')


@pytest.mark.parametrize('unsafe', ['symlink', 'mode', 'hardlink', 'directory'])
def test_unsafe_lock_refused(tmp_path, monkeypatch, unsafe):
    from hermes_cli import config
    monkeypatch.setenv('HERMES_HOME', str(tmp_path))
    target = tmp_path / '.env'
    target.write_text('KEEP=old\n')
    lock = tmp_path / '.env.lock'
    other = tmp_path / 'other'
    other.write_text('untouched')
    if unsafe == 'symlink':
        lock.symlink_to(other)
    elif unsafe == 'mode':
        lock.touch(mode=0o644)
    elif unsafe == 'directory':
        lock.mkdir()
    else:
        os.link(other, lock)
    with pytest.raises((OSError, ValueError)):
        config.save_env_value('KEEP', 'new')
    assert target.read_text() == 'KEEP=old\n'
    assert other.read_text() == 'untouched'


def test_reentrant_alias_thread_timeout_and_release(tmp_path, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    from hermes_cli import config
    from hermes_cli.env_transaction import env_transaction
    monkeypatch.setenv('HERMES_HOME', str(tmp_path))
    alias = tmp_path / 'alias'
    alias.symlink_to(tmp_path, target_is_directory=True)
    target = tmp_path / '.env'
    def competing():
        with env_transaction(alias / '.env', timeout=0.1):
            raise AssertionError('thread bypassed process exclusion')
    with ThreadPoolExecutor(1) as pool:
        with pytest.raises(RuntimeError, match='abort'):
            with env_transaction(target):
                with env_transaction(alias / '.env') as canonical:
                    assert canonical == target.resolve()
                    config.save_env_value('NESTED', 'value')
                    assert config.remove_env_value('NESTED')
                with pytest.raises(TimeoutError):
                    pool.submit(competing).result(timeout=5)
                raise RuntimeError('abort')
        pool.submit(config.save_env_value, 'AFTER', 'ok').result(timeout=5)
    assert target.read_text() == 'AFTER=ok\n'
    assert (tmp_path / '.env.lock').stat().st_mode & 0o777 == 0o600


def test_failed_commit_does_not_publish_and_releases(tmp_path, monkeypatch):
    from hermes_cli import config
    monkeypatch.setenv('HERMES_HOME', str(tmp_path))
    monkeypatch.setenv('KEEP', 'old')
    target = tmp_path / '.env'
    target.write_text('KEEP=old\n')
    original = config.atomic_replace
    def fail(*args):
        raise OSError('injected commit failure')
    monkeypatch.setattr(config, 'atomic_replace', fail)
    for operation in (lambda: config.save_env_value('KEEP', 'new'), lambda: config.remove_env_value('KEEP')):
        with pytest.raises(OSError, match='injected'):
            operation()
        assert target.read_text() == 'KEEP=old\n'
        assert os.environ['KEEP'] == 'old'
        assert not list(tmp_path.glob('.env_*.tmp'))
    monkeypatch.setattr(config, 'atomic_replace', original)
    config.save_env_value('KEEP', 'new')
    assert os.environ['KEEP'] == 'new'


def test_uncovered_write_and_unsupported_target_refused(tmp_path):
    from hermes_cli import config
    from hermes_cli.env_transaction import env_transaction
    target = tmp_path / '.env'
    with pytest.raises(RuntimeError, match='requires env_transaction'):
        config._write_env_lines(target, ['STALE=1\n'], preserve_mode=False)
    target.write_text('KEEP=old\n')
    os.link(target, tmp_path / 'hardlink')
    with pytest.raises(ValueError, match='non-hardlinked'):
        with env_transaction(target):
            raise AssertionError('hardlink accepted')


def test_file_symlink_alias_preserved(tmp_path, monkeypatch):
    from hermes_cli import config
    target = tmp_path / 'secrets'
    target.write_text('KEEP=old\n')
    alias = tmp_path / '.env'
    alias.symlink_to(target)
    monkeypatch.setenv('HERMES_HOME', str(tmp_path))
    config.save_env_value('KEEP', 'new')
    assert alias.is_symlink()
    assert target.read_text() == 'KEEP=new\n'
    assert (tmp_path / 'secrets.lock').is_file()
    assert not (tmp_path / '.env.lock').exists()


def test_missing_primitive_and_untrusted_directory_fail_closed(tmp_path, monkeypatch):
    import importlib
    module = importlib.import_module('hermes_cli.env_transaction')
    target = tmp_path / '.env'
    with monkeypatch.context() as patcher:
        patcher.setattr(module, 'fcntl', None)
        with pytest.raises(RuntimeError, match='require POSIX'):
            with module.env_transaction(target):
                raise AssertionError('missing lock backend accepted')
    assert not target.exists()
    tmp_path.chmod(0o777)
    try:
        with pytest.raises(PermissionError, match='owner-controlled'):
            with module.env_transaction(target):
                raise AssertionError('untrusted parent accepted')
    finally:
        tmp_path.chmod(0o700)



def _fork_probe(target, result):
    from hermes_cli.env_transaction import env_transaction
    try:
        with env_transaction(target, timeout=0.1):
            result.put('bypassed')
    except TimeoutError:
        result.put('excluded')


def test_fork_does_not_inherit_reentrancy(tmp_path):
    from hermes_cli.env_transaction import env_transaction
    ctx = multiprocessing.get_context('fork')
    result = ctx.Queue()
    target = tmp_path / '.env'
    with env_transaction(target):
        process = ctx.Process(target=_fork_probe, args=(target, result))
        process.start()
        try:
            assert result.get(timeout=5) == 'excluded'
        finally:
            process.join(5)
            if process.is_alive():
                process.kill()
                process.join()
        assert process.exitcode == 0
    with env_transaction(target, timeout=0):
        pass
    result.close()
