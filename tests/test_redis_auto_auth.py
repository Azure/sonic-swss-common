"""Real native Python logins. Run with tests/run-redis-auth-tests.sh."""

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time

import pytest
import redis
from swsscommon import swsscommon as swss


if "REDIS_AUTH_TEST_DIR" not in os.environ:
    pytest.skip("run tests/run-redis-auth-tests.sh", allow_module_level=True)

# The runner compiles this path into a separate test library. Only the fixture
# reads the environment; production connections have no runtime path override.
POLICY_DIR = Path(os.environ["REDIS_AUTH_TEST_DIR"]) / "policy"
POLICY = POLICY_DIR / "local.json"
SECRET = POLICY_DIR / "local-system/writer.secret"
pytestmark = pytest.mark.skipif(os.geteuid() != 0, reason="requires root for ownership and ordinary-user checks")


@pytest.fixture
def protected_redis(tmp_path):
    # Fail rather than replace any pre-existing runtime state.
    POLICY_DIR.mkdir(mode=0o755)
    SECRET.parent.mkdir(mode=0o755)
    server = None
    swss.SonicDBConfig.reset()
    try:
        # A separate directory lets the non-root child reach the test socket.
        server_dir = Path(tempfile.mkdtemp(prefix="redis-auto-python-"))
        server_dir.chmod(0o755)
        socket_path = server_dir / "redis.sock"
        acl = server_dir / "users.acl"
        acl.write_text("user default on nopass ~* +@read +select +ping\n"
                       "user python-writer on >python-password ~* +@all\n")
        acl.chmod(0o600)
        server = subprocess.Popen(
            ["redis-server", "--dir", str(server_dir), "--port", "0", "--unixsocket", str(socket_path), "--unixsocketperm", "777",
             "--save", "", "--appendonly", "no", "--aclfile", str(acl)],
            stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
        control = redis.Redis(unix_socket_path=str(socket_path),
                              username="python-writer", password="python-password")
        for attempt in range(200):
            try:
                control.ping()
                break
            except redis.ConnectionError:
                if server.poll() is not None:
                    pytest.fail("test Redis exited during startup")
                time.sleep(0.01)
        else:
            pytest.fail("test Redis did not start")

        policy = {"username": "python-writer",
                  "unix_socket": str(socket_path), "tcp_host": "127.0.0.1", "tcp_port": 0}
        POLICY.write_text(json.dumps(policy))
        POLICY.chmod(0o644)
        SECRET.write_bytes(b"python-password")
        SECRET.chmod(0o400)
        config = tmp_path / "database_config.json"
        config.write_text(json.dumps({
            "INSTANCES": {"redis": {"hostname": "127.0.0.1", "port": 0,
                                     "unix_socket_path": str(socket_path)}},
            "DATABASES": {
                name: {"id": dbid, "separator": "|" if name == "CONFIG_DB" else ":", "instance": "redis"}
                for name, dbid in (("APPL_DB", 0), ("COUNTERS_DB", 2), ("CONFIG_DB", 4), ("STATE_DB", 6))}}))
        swss.SonicDBConfig.initialize(str(config))
        yield control, str(socket_path), policy
    finally:
        swss.SonicDBConfig.reset()
        if server is not None:
            server.terminate()
            server.wait(timeout=10)
        POLICY.unlink(missing_ok=True)
        SECRET.unlink(missing_ok=True)
        SECRET.parent.rmdir()
        POLICY_DIR.rmdir()
        if "server_dir" in locals():
            for child in server_dir.iterdir():
                child.unlink()
            server_dir.rmdir()


def test_existing_python_apis_authenticate(protected_redis):
    control, socket_path, _ = protected_redis
    db = swss.DBConnector(6, socket_path, 1000)
    db.hset("DIRECT", "field", "value")
    child = db.newConnector(1000)
    child.hset("CHILD", "field", "value")
    interface = swss.DBInterface()
    interface.set_redis_kwargs(unix_socket_path=socket_path, host="127.0.0.1", port=0)
    interface.connect(dbId=6, dbName="STATE_DB", retry=False)
    interface.set("STATE_DB", "INTERFACE", "field", "value")
    sonic = swss.SonicV2Connector()
    sonic.connect(db_name="STATE_DB", retry_on=False)
    sonic.set("STATE_DB", "SONIC", "field", "value")
    config = swss.ConfigDBConnector()
    config.connect(wait_for_init=False, retry_on=False)
    config.set_entry("AUTO_AUTH", "config", {"field": "value"})
    pipe = swss.ConfigDBPipeConnector()
    pipe.connect(wait_for_init=False, retry_on=False)
    pipe.mod_config({"AUTO_AUTH": {"pipeline": {"field": "value"}}})
    control.execute_command("SELECT", 6)
    for key in ("DIRECT", "CHILD", "INTERFACE", "SONIC"):
        assert control.hget(key, "field") == b"value"
    control.execute_command("SELECT", 4)
    assert control.hget("AUTO_AUTH|config", "field") == b"value"
    assert control.hget("AUTO_AUTH|pipeline", "field") == b"value"


def test_wrong_secret_never_falls_back(protected_redis):
    control, socket_path, _ = protected_redis
    control.acl_setuser("default", enabled=True, commands=["+@all"])
    SECRET.chmod(0o600)
    SECRET.write_bytes(b"wrong-password")
    SECRET.chmod(0o400)
    with pytest.raises(RuntimeError, match="authentication failed"):
        swss.DBConnector(0, socket_path, 1000)


def test_non_root_reader_and_missing_secret(protected_redis):
    _, socket_path, _ = protected_redis
    code = """
import sys
from swsscommon import swsscommon as swss
db = swss.DBConnector(0, sys.argv[1], 1000)
assert not db.exists('absent')
try:
    db.set('forbidden', 'value')
except RuntimeError:
    sys.exit(0)
sys.exit(1)
"""
    result = subprocess.run([sys.executable, "-c", code, socket_path],
                            user=65534, group=65534, extra_groups=[],
                            capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, result.stderr
    SECRET.unlink()
    with pytest.raises(RuntimeError, match="Cannot open Redis writer credential"):
        swss.DBConnector(0, socket_path, 1000)


def test_two_selected_local_processes_use_the_boot_secret(protected_redis):
    _, primary_socket, primary_policy = protected_redis
    secondary_dir = Path(primary_socket).parent
    secondary_socket = secondary_dir / "secondary.sock"
    secondary_acl = secondary_dir / "secondary.acl"
    secondary_acl.write_text("user default on nopass ~* +@read +select +ping\n"
                             "user secondary-writer on >python-password ~* +@all\n")
    secondary_acl.chmod(0o600)
    server = subprocess.Popen(
        ["redis-server", "--dir", str(secondary_dir), "--port", "0", "--unixsocket", str(secondary_socket),
         "--unixsocketperm", "777", "--save", "", "--appendonly", "no",
         "--aclfile", str(secondary_acl)],
        stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
    try:
        control = redis.Redis(unix_socket_path=str(secondary_socket),
                              username="secondary-writer", password="python-password")
        for attempt in range(200):
            try:
                control.ping()
                break
            except redis.ConnectionError:
                if server.poll() is not None:
                    pytest.fail("secondary Redis exited during startup")
                time.sleep(0.01)
        else:
            pytest.fail("secondary Redis did not start")
        secondary_policy = {"username": "secondary-writer", "unix_socket": str(secondary_socket),
                            "tcp_host": "127.0.0.1", "tcp_port": 0}
        POLICY.write_text(json.dumps([primary_policy, secondary_policy]))
        primary = swss.DBConnector(6, primary_socket, 1000)
        secondary = swss.DBConnector(6, str(secondary_socket), 1000)
        assert primary.set("process", "primary")
        assert secondary.set("process", "secondary")
        copied = secondary.newConnector(1000)
        assert copied.set("copy", "secondary")
        assert primary.get("process") == "primary"
        assert secondary.get("process") == "secondary"
        code = """
import sys
from swsscommon import swsscommon as swss
for path in sys.argv[1:]:
    db = swss.DBConnector(6, path, 1000)
    assert db.get('process')
    try:
        db.set('forbidden', 'value')
    except RuntimeError:
        continue
    sys.exit(1)
"""
        result = subprocess.run([sys.executable, "-c", code, primary_socket, str(secondary_socket)],
                                user=65534, group=65534, extra_groups=[],
                                capture_output=True, text=True, timeout=10)
        assert result.returncode == 0, result.stderr
        SECRET.unlink()
        for path in (primary_socket, str(secondary_socket)):
            with pytest.raises(RuntimeError, match="Cannot open Redis writer credential"):
                swss.DBConnector(6, path, 1000)
    finally:
        server.terminate()
        server.wait(timeout=10)
        secondary_acl.unlink(missing_ok=True)
        secondary_socket.unlink(missing_ok=True)


@pytest.mark.parametrize("timeouts", [
    {},
    {"socket_timeout": 0.5},
    {"socket_timeout": 0.5, "socket_connect_timeout": 0.25},
])
def test_native_pool_preserves_binary_transactions_and_reconnect(protected_redis, timeouts):
    from swsscommon.redis import connection_pool
    control, socket_path, _ = protected_redis
    pool = connection_pool(unix_socket_path=socket_path, db=6, **timeouts)
    client = redis.Redis(connection_pool=pool)
    try:
        with client.pipeline() as transaction:
            transaction.set(b"binary\x00key", b"\x00\xffvalue")
            transaction.get(b"binary\x00key")
            assert transaction.execute() == [True, b"\x00\xffvalue"]
        pool.disconnect()
        assert client.get(b"binary\x00key") == b"\x00\xffvalue"
        assert control.execute_command("SELECT", 6)
        assert control.get(b"binary\x00key") == b"\x00\xffvalue"
    finally:
        pool.disconnect()


def test_native_pool_authentication_failure_is_not_anonymous_fallback(protected_redis):
    from swsscommon.redis import connection_pool
    _, socket_path, _ = protected_redis
    SECRET.write_bytes(b"wrong-password")
    pool = connection_pool(unix_socket_path=socket_path, db=6)
    try:
        with pytest.raises(RuntimeError, match="authentication failed"):
            redis.Redis(connection_pool=pool).ping()
    finally:
        pool.disconnect()


def test_native_rdb_export_preserves_replication_format_and_failure_target(protected_redis, tmp_path):
    control, socket_path, _ = protected_redis
    control.set('rdb-native-export', b'\x00\xffnative-value')
    target = tmp_path / 'snapshot.rdb'
    swss.exportRedisRdb(socket_path, str(target), 10000)
    assert target.read_bytes().startswith(b'REDIS')
    subprocess.run(['redis-check-rdb', str(target)], check=True, capture_output=True)
    saved = target.read_bytes()
    SECRET.write_bytes(b'wrong-password')
    with pytest.raises(RuntimeError):
        swss.exportRedisRdb(socket_path, str(target), 1000)
    assert target.read_bytes() == saved


def test_native_rdb_truncation_keeps_existing_destination(protected_redis, tmp_path):
    import socket
    import threading
    endpoint = str(tmp_path / 'partial.sock')
    destination = tmp_path / 'existing.rdb'
    destination.write_bytes(b'preserve-existing-snapshot')
    with socket.socket(socket.AF_UNIX) as server:
        server.bind(endpoint)
        server.listen(1)
        server.settimeout(3)

        def truncate():
            with server.accept()[0] as connection:
                payload = b''
                while not payload.endswith(b'SYNC\r\n'):
                    part = connection.recv(128)
                    if not part:
                        return
                    payload += part
                connection.sendall(b'$100\r\nshort')

        worker = threading.Thread(target=truncate)
        worker.start()
        try:
            with pytest.raises(RuntimeError, match='advertised length'):
                swss.exportRedisRdb(endpoint, str(destination), 1000)
        finally:
            worker.join(timeout=4)
        assert not worker.is_alive()
    assert destination.read_bytes() == b'preserve-existing-snapshot'
    assert list(tmp_path.glob('existing.rdb.*')) == []


def test_native_pool_ordinary_reader_cannot_write(protected_redis):
    control, socket_path, _ = protected_redis
    control.set('native-reader', 'readable')
    code = '''
import sys, redis
from swsscommon.redis import connection_pool
client = redis.Redis(connection_pool=connection_pool(unix_socket_path=sys.argv[1]))
assert client.get('native-reader') == b'readable'
try:
    client.set('native-reader', 'forbidden')
except redis.exceptions.ResponseError:
    pass
else:
    raise AssertionError('ordinary native-pool write was allowed')
'''
    result = subprocess.run([sys.executable, '-c', code, socket_path],
                            user=65534, group=65534, extra_groups=[], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert control.get('native-reader') == b'readable'
