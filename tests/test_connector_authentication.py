"""Incremental connector coverage using the isolated native-auth fixture."""

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


def test_dbinterface_explicit_connection(protected):
    interface = swss.DBInterface()
    interface.connectAuthenticated("STATE_DB", swss.SonicDBKey(), False)
    interface.get_redis_client("STATE_DB").set("interface", "allowed")
    interface.close()


def test_dbinterface_requires_close_before_changing_authentication(protected):
    interface = swss.DBInterface()
    interface.set_redis_kwargs(protected["socket"], "", 0)
    interface.connect(6, "STATE_DB", False)
    with pytest.raises(ValueError, match="Close the existing connection"):
        interface.connectAuthenticated("STATE_DB", swss.SonicDBKey(), False)
    interface.close("STATE_DB")
    interface.connectAuthenticated("STATE_DB", swss.SonicDBKey(), False)
    interface.get_redis_client("STATE_DB").set("upgraded", "allowed")


def test_sonicv2_recovery_keeps_authentication(protected):
    writer = swss.SonicV2Connector(use_unix_socket_path=True, authenticate=True)
    writer.connect("STATE_DB", retry_on=False)
    writer.set("STATE_DB", "recovery", "field", "before")
    protected["writer"].execute_command("CLIENT", "KILL", "USER", "test-writer", "SKIPME", "yes")
    assert writer.get("STATE_DB", "recovery", "field") == "before"
    writer.set("STATE_DB", "recovery", "field", "after")
    assert protected["writer"].hget("recovery", "field") == b"after"
