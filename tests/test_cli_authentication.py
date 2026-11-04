from pathlib import Path
import shutil
import subprocess

import pytest
from test_redis_authentication import protected  # noqa: F401


@pytest.fixture
def cli(protected):
    directory = Path('/var/run/redis/sonic-db')
    target = directory / 'database_config.json'
    directory.mkdir(parents=True, exist_ok=True)
    backup = directory / 'database_config.json.auth-test-backup'
    assert not backup.exists()
    had_original = target.exists() or target.is_symlink()
    if had_original:
        target.rename(backup)
    shutil.copyfile(protected['metadata'], target)
    try:
        binary = Path(__file__).resolve().parents[1] / 'sonic-db-cli/sonic-db-cli'
        yield lambda *args: subprocess.run([str(binary), *args],
            text=True, capture_output=True, timeout=5)
    finally:
        target.unlink()
        if had_original:
            backup.rename(target)


def test_cli_authentication_is_explicit(protected, cli):
    denied = cli('-s', 'STATE_DB', 'SET', 'cli-legacy', 'denied')
    assert 'NOPERM' in denied.stdout + denied.stderr
    assert protected['writer'].get('cli-legacy') is None
    allowed = cli('-s', '--authenticate', 'STATE_DB', 'SET', 'cli-writer', 'allowed')
    assert allowed.returncode == 0 and allowed.stdout.strip() == 'True'
    assert protected['writer'].get('cli-writer') == b'allowed'


def test_cli_authentication_does_not_select_transport(protected, cli):
    result = cli('--authenticate', 'STATE_DB', 'PING')
    assert result.returncode != 0
    assert 'explicitly selected local Unix socket' in result.stderr


def test_cli_all_instances_uses_authentication(protected, cli):
    assert cli('-s', '--authenticate', 'PING').returncode == 0
    protected['secret'].write_text('incorrect-test-password')
    rejected = cli('-s', '--authenticate', 'PING')
    assert rejected.returncode != 0
    assert 'authentication failed' in rejected.stdout + rejected.stderr
