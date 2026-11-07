"""Incremental connector coverage using the isolated native-auth fixture."""

import subprocess
import sys

import pytest
from swsscommon import swsscommon as swss
from test_redis_authentication import protected  # noqa: F401


def test_sonicv2_authentication_is_explicit(protected):
    legacy = swss.SonicV2Connector(use_unix_socket_path=True)
    legacy.connect("STATE_DB", retry_on=False)
    with pytest.raises(RuntimeError, match="NOPERM"):
        legacy.set("STATE_DB", "legacy", "field", "denied")
    writer = swss.SonicV2Connector(use_unix_socket_path=True, authenticate=True)
    writer.connect("STATE_DB", retry_on=False)
    writer.set("STATE_DB", "explicit", "field", "allowed")
    assert writer.get("STATE_DB", "explicit", "field") == "allowed"
    writer.close("STATE_DB")
    writer.connect("STATE_DB", retry_on=False)
    writer.set("STATE_DB", "reopened", "field", "allowed")


def test_sonicv2_keeps_transport_selection(protected):
    with pytest.raises(ValueError, match="explicit Unix socket"):
        swss.SonicV2Connector(authenticate=True)
    legacy = swss.SonicV2Connector()
    with pytest.raises(RuntimeError, match="Unable to connect to redis"):
        legacy.connect("STATE_DB", retry_on=False)


@pytest.mark.parametrize("authenticate", [False, True])
def test_dbinterface_explicit_connection(protected, authenticate):
    interface = swss.DBInterface()
    interface.set_redis_kwargs(protected["socket"], "", 0)
    interface.connect("STATE_DB", swss.SonicDBKey(), False, authenticate)
    if authenticate:
        interface.get_redis_client("STATE_DB").set("interface", "allowed")
    else:
        with pytest.raises(RuntimeError, match="NOPERM"):
            interface.get_redis_client("STATE_DB").set("interface", "denied")
    interface.close()


def test_dbinterface_requires_close_before_changing_authentication(protected):
    interface = swss.DBInterface()
    interface.set_redis_kwargs(protected["socket"], "", 0)
    interface.connect(6, "STATE_DB", False)
    with pytest.raises(ValueError, match="Close the existing connection"):
        interface.connect("STATE_DB", swss.SonicDBKey(), False, True)
    interface.close("STATE_DB")
    interface.connect("STATE_DB", swss.SonicDBKey(), False, True)
    interface.get_redis_client("STATE_DB").set("upgraded", "allowed")


def test_sonicv2_recovery_keeps_authentication(protected):
    writer = swss.SonicV2Connector(use_unix_socket_path=True, authenticate=True)
    writer.connect("STATE_DB", retry_on=False)
    writer.set("STATE_DB", "recovery", "field", "before")
    protected["writer"].execute_command("CLIENT", "KILL", "USER", "test-writer", "SKIPME", "yes")
    assert writer.get("STATE_DB", "recovery", "field") == "before"
    writer.set("STATE_DB", "recovery", "field", "after")
    assert protected["writer"].hget("recovery", "field") == b"after"


@pytest.mark.parametrize("recover", [False, True])
def test_connection_retries_share_the_operation_loop(protected, recover):
    # A separate process restarts Redis while the native call is blocked.
    # Recovery leaves Redis down past the first reconnect attempt.
    program = """
import subprocess, sys
import redis
from swsscommon import swsscommon as s
s.SonicDBConfig.initialize(sys.argv[1])
c = s.SonicV2Connector(use_unix_socket_path=True, authenticate=True)
recover = sys.argv[3] == 'True'
if recover:
    c.connect('STATE_DB', retry_on=False)
    c.set('STATE_DB', 'retry', 'field', 'before')
admin = redis.Redis(unix_socket_path=sys.argv[2], username='test-writer', password='test-only-password')
admin.shutdown(nosave=True)
restart = subprocess.Popen(['timeout', '30s', sys.executable, '-c',
    'import os,sys,time; time.sleep(float(sys.argv[1])); os.execlp("redis-server", "redis-server", sys.argv[2])',
    '12' if recover else '1', sys.argv[4]], stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
try:
    if not recover:
        c.connect('STATE_DB', retry_on=True)
    c.set('STATE_DB', 'retry', 'field', 'after')
    assert c.get('STATE_DB', 'retry', 'field') == 'after'
finally:
    restart.terminate()
    restart.wait(timeout=5)
"""
    subprocess.run([sys.executable, "-c", program, str(protected["metadata"]),
                    protected["socket"], str(recover), str(protected["work"] / "redis.conf")],
                   check=True, timeout=35)


@pytest.mark.parametrize("authenticate", [False, True])
def test_nonretryable_connection_errors_remain_fail_fast(protected, authenticate):
    if authenticate:
        protected["secret"].write_text("wrong-test-password")
    path = protected["socket"] if authenticate else str(protected["work"] / "missing.sock")
    program = """
import sys
from swsscommon import swsscommon as s
s.SonicDBConfig.initialize(sys.argv[1])
c = s.DBInterface()
c.set_redis_kwargs(sys.argv[2], '', 0)
authenticate = sys.argv[3] == 'True'
try:
    if authenticate:
        c.connect('STATE_DB', s.SonicDBKey(), True, True)
    else:
        c.connect(6, 'STATE_DB')
except RuntimeError as error:
    expected = 'authentication' if authenticate else 'Unable to connect to redis'
    assert expected in str(error)
else:
    raise AssertionError('connection unexpectedly succeeded')
"""
    subprocess.run([sys.executable, "-c", program, str(protected["metadata"]), path, str(authenticate)],
                   check=True, timeout=3)
