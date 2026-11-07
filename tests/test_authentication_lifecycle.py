import json
import subprocess
import sys
import time

import pytest
import redis
from swsscommon import swsscommon as swss
from test_redis_authentication import protected  # noqa: F401
from test_configdb_authentication import config_metadata  # noqa: F401


@pytest.fixture
def secondary(protected):
    socket = str(protected["work"] / "secondary.sock")
    config = protected["work"] / "secondary.conf"
    config.write_text((protected["work"] / "redis.conf").read_text().replace(protected["socket"], socket))
    process = subprocess.Popen(["redis-server", str(config)], stdout=subprocess.DEVNULL,
                               stderr=subprocess.STDOUT)
    try:
        client = redis.Redis(unix_socket_path=socket, db=6, username="test-writer",
                             password="test-only-password")
        for _ in range(100):
            try:
                if client.ping():
                    break
            except redis.ConnectionError:
                time.sleep(0.02)
        else:
            pytest.fail("secondary Redis did not start")
        entry = json.loads(protected["policy"].read_text())
        protected["policy"].write_text(json.dumps([entry, {**entry, "unix_socket": socket}]))
        yield {**protected, "secondary_socket": socket, "secondary_client": client}
    finally:
        process.terminate()
        process.wait(timeout=5)


def test_recovery_uses_original_endpoint_after_another_database(secondary):
    metadata = json.loads(secondary["metadata"].read_text())
    metadata["INSTANCES"]["secondary"] = {"hostname": "127.0.0.1", "port": 0,
                                             "unix_socket_path": secondary["secondary_socket"]}
    metadata["DATABASES"]["SECOND_DB"] = {"id": 6, "separator": "|", "instance": "secondary"}
    secondary["metadata"].write_text(json.dumps(metadata))
    swss.SonicDBConfig.reset()
    swss.SonicDBConfig.initialize(str(secondary["metadata"]))
    connector = swss.SonicV2Connector(use_unix_socket_path=True, authenticate=True)
    for name, value in [("STATE_DB", "primary"), ("SECOND_DB", "secondary")]:
        connector.connect(name, retry_on=False)
        connector.set(name, "endpoint", "field", value)
    secondary["writer"].execute_command("CLIENT", "KILL", "USER", "test-writer", "SKIPME", "yes")
    assert connector.get("STATE_DB", "endpoint", "field") == "primary"
    connector.set("STATE_DB", "endpoint", "field", "recovered")
    assert secondary["writer"].hget("endpoint", "field") == b"recovered"
    assert secondary["secondary_client"].hget("endpoint", "field") == b"secondary"


@pytest.mark.parametrize("higher_level", [False, True])
def test_namespace_selects_its_configured_socket(secondary, higher_level):
    metadata = json.loads(secondary["metadata"].read_text())
    metadata["INSTANCES"]["redis"]["unix_socket_path"] = secondary["secondary_socket"]
    namespace_file = secondary["work"] / "namespace.json"
    namespace_file.write_text(json.dumps(metadata))
    global_file = secondary["work"] / "global.json"
    global_file.write_text(json.dumps({"INCLUDES": [
        {"include": secondary["metadata"].name},
        {"namespace": "asic0", "include": namespace_file.name},
    ], "VERSION": "1.0"}))
    swss.SonicDBConfig.reset()
    swss.SonicDBConfig.initializeGlobalConfig(str(global_file))
    if higher_level:
        connector = swss.SonicV2Connector(use_unix_socket_path=True, namespace="asic0", authenticate=True)
        connector.connect("STATE_DB", retry_on=False)
        db = connector.get_redis_client("STATE_DB")
    else:
        db = swss.DBConnector("STATE_DB", 500, False, "asic0", True)
    db.set("namespace", "secondary-only")
    assert secondary["secondary_client"].get("namespace") == b"secondary-only"
    assert secondary["writer"].get("namespace") is None


@pytest.mark.parametrize("configdb", [False, True])
def test_ordinary_users_remain_readers_through_higher_connectors(config_metadata, configdb):
    for name in ["STATE_DB", "CONFIG_DB"]:
        swss.DBConnector(name, 500, False, "", True).set("reader", "visible")
    program = """
import sys
from swsscommon import swsscommon as s
s.SonicDBConfig.initialize(sys.argv[1])
if sys.argv[2] == 'config':
    c=s.ConfigDBConnector(use_unix_socket_path=True,authenticate=True)
    c.connect(wait_for_init=False)
    db=c.get_redis_client('CONFIG_DB')
else:
    c=s.SonicV2Connector(use_unix_socket_path=True,authenticate=True)
    c.connect('STATE_DB',retry_on=False)
    db=c.get_redis_client('STATE_DB')
assert db.get('reader') == 'visible'
try:
    db.set('reader','denied')
except RuntimeError as error:
    assert 'NOPERM' in str(error)
else:
    raise AssertionError('ordinary-user write succeeded')
"""
    subprocess.run(["setpriv", "--reuid=65534", "--regid=65534", "--clear-groups",
                    sys.executable, "-c", program, str(config_metadata["metadata"]),
                    "config" if configdb else "v2"], check=True, timeout=5)


@pytest.mark.parametrize("connector", [swss.ConfigDBConnector, swss.ConfigDBPipeConnector])
def test_legacy_configdb_ignores_invalid_policy(config_metadata, connector):
    config_metadata["policy"].write_text("{")
    db = connector(use_unix_socket_path=True)
    db.connect(wait_for_init=False)
    assert db.get_entry("not-present", "key") == {}
