import json
import subprocess
import sys

import pytest
from swsscommon import swsscommon as swss
from test_redis_authentication import protected  # noqa: F401


@pytest.fixture
def config_metadata(protected):
    metadata = json.loads(protected["metadata"].read_text())
    metadata["DATABASES"]["CONFIG_DB"] = {"id": 4, "separator": "|", "instance": "redis"}
    protected["metadata"].write_text(json.dumps(metadata))
    swss.SonicDBConfig.reset()
    swss.SonicDBConfig.initialize(str(protected["metadata"]))
    return protected


@pytest.mark.parametrize("connector", [swss.ConfigDBConnector, swss.ConfigDBPipeConnector])
def test_configdb_explicit_writer(config_metadata, connector):
    db = connector(use_unix_socket_path=True, authenticate=True)
    db.connect(wait_for_init=False)
    db.set_entry("AUTH_TEST", "key", {"field": "value"})
    assert db.get_entry("AUTH_TEST", "key") == {"field": "value"}
    db.close()
    db.connect(wait_for_init=False)
    db.set_entry("AUTH_TEST", "key", {"field": "reopened"})
    assert db.get_entry("AUTH_TEST", "key") == {"field": "reopened"}


@pytest.mark.parametrize("connector", [swss.ConfigDBConnector, swss.ConfigDBPipeConnector])
def test_configdb_default_remains_reader(config_metadata, connector):
    db = connector(use_unix_socket_path=True)
    db.connect(wait_for_init=False)
    with pytest.raises(RuntimeError, match="NOPERM"):
        db.set_entry("AUTH_TEST", "key", {"field": "denied"})


def test_configdb_initialization_subscription(config_metadata):
    # A child bounds the blocking native wait; a second process publishes INIT.
    program = """
import subprocess, sys
from swsscommon import swsscommon as s
s.SonicDBConfig.initialize(sys.argv[1])
setter = subprocess.Popen([sys.executable, '-c', '''
import sys,time
from swsscommon import swsscommon as s
s.SonicDBConfig.initialize(sys.argv[1])
time.sleep(0.2)
c=s.DBConnector('CONFIG_DB',500,False,'',True)
c.set('CONFIG_DB_INITIALIZED','1')
''',sys.argv[1]])
try:
    db=s.ConfigDBConnector(use_unix_socket_path=True,authenticate=True)
    db.connect(wait_for_init=True)
    db.set_entry('AUTH_TEST','init',{'field':'ready'})
    assert db.get_entry('AUTH_TEST','init') == {'field':'ready'}
    assert setter.wait(timeout=2) == 0
finally:
    if setter.poll() is None:
        setter.kill()
        setter.wait()
"""
    subprocess.run([sys.executable, "-c", program, str(config_metadata["metadata"])],
                   check=True, timeout=6)
