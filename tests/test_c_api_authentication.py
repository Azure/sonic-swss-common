import ctypes as C
import os
from pathlib import Path
import subprocess

import pytest
from test_redis_authentication import protected  # noqa: F401
from test_configdb_authentication import config_metadata  # noqa: F401


class Result(C.Structure):
    _fields_ = [("exception", C.c_int), ("message", C.c_void_p), ("location", C.c_void_p)]


class Field(C.Structure):
    _fields_ = [("field", C.c_char_p), ("value", C.c_void_p)]


class Fields(C.Structure):
    _fields_ = [("len", C.c_uint64), ("data", C.POINTER(Field))]


@pytest.fixture
def api():
    lib = C.CDLL("libswsscommon.so")
    pointer = C.POINTER(C.c_void_p)
    signatures = {
        "SWSSDBConnector_new_named_with_auth": [C.c_char_p, C.c_uint32, C.c_uint8, C.c_uint8, pointer],
        "SWSSDBConnector_new_keyed_with_auth": [C.c_char_p, C.c_uint32, C.c_uint8, C.c_char_p, C.c_char_p, C.c_uint8, pointer],
        "SWSSDBConnector_free": [C.c_void_p],
        "SWSSDBConnector_set": [C.c_void_p, C.c_char_p, C.c_void_p],
        "SWSSSonicV2Connector_new_with_auth": [C.c_uint8, C.c_char_p, C.c_uint8, pointer],
        "SWSSSonicV2Connector_connect": [C.c_void_p, C.c_char_p, C.c_uint8],
        "SWSSSonicV2Connector_get_redis_client": [C.c_void_p, C.c_char_p, pointer],
        "SWSSSonicV2Connector_free": [C.c_void_p],
        "SWSSConfigDBConnector_new_with_auth": [C.c_uint8, C.c_char_p, C.c_uint8, pointer],
        "SWSSConfigDBConnector_connect": [C.c_void_p, C.c_uint8, C.c_uint8],
        "SWSSConfigDBConnector_set_entry": [C.c_void_p, C.c_char_p, C.c_char_p, C.POINTER(Fields)],
        "SWSSConfigDBConnector_free": [C.c_void_p],
    }
    for name, arguments in signatures.items():
        getattr(lib, name).argtypes = arguments
        getattr(lib, name).restype = Result
    lib.SWSSString_free.argtypes = [C.c_void_p]
    lib.SWSSString_free.restype = None
    lib.SWSSString_new_c_str.argtypes = [C.c_char_p]
    lib.SWSSString_new_c_str.restype = C.c_void_p
    return lib


def code(api, result):
    value = result.exception
    api.SWSSString_free(result.message)
    api.SWSSString_free(result.location)
    return value


def write(api, db):
    value = api.SWSSString_new_c_str(b"value")
    try:
        return code(api, api.SWSSDBConnector_set(db, b"c-api", value))
    finally:
        api.SWSSString_free(value)


@pytest.mark.parametrize("keyed", [False, True])
@pytest.mark.parametrize("authenticate", [False, True])
def test_c_dbconnector_option(protected, api, keyed, authenticate):
    db = C.c_void_p()
    if keyed:
        result = api.SWSSDBConnector_new_keyed_with_auth(b"STATE_DB", 500, 0, None, None, authenticate, C.byref(db))
    else:
        result = api.SWSSDBConnector_new_named_with_auth(b"STATE_DB", 500, 0, authenticate, C.byref(db))
    assert code(api, result) == 0
    try:
        assert write(api, db) == (0 if authenticate else 1)
    finally:
        assert code(api, api.SWSSDBConnector_free(db)) == 0


@pytest.mark.parametrize("authenticate", [False, True])
def test_c_sonicv2_option(protected, api, authenticate):
    connector, db = C.c_void_p(), C.c_void_p()
    assert code(api, api.SWSSSonicV2Connector_new_with_auth(1, None, authenticate, C.byref(connector))) == 0
    try:
        assert code(api, api.SWSSSonicV2Connector_connect(connector, b"STATE_DB", 0)) == 0
        assert code(api, api.SWSSSonicV2Connector_get_redis_client(connector, b"STATE_DB", C.byref(db))) == 0
        assert write(api, db) == (0 if authenticate else 1)
    finally:
        assert code(api, api.SWSSSonicV2Connector_free(connector)) == 0


@pytest.mark.parametrize("authenticate", [False, True])
def test_c_configdb_option(config_metadata, api, authenticate):
    connector = C.c_void_p()
    assert code(api, api.SWSSConfigDBConnector_new_with_auth(1, None, authenticate, C.byref(connector))) == 0
    value = api.SWSSString_new_c_str(b"value")
    fields = Fields(1, (Field * 1)(Field(b"field", value)))
    try:
        assert code(api, api.SWSSConfigDBConnector_connect(connector, 0, 0)) == 0
        result = api.SWSSConfigDBConnector_set_entry(connector, b"AUTH_TEST", b"c-api", C.byref(fields))
        assert code(api, result) == (0 if authenticate else 1)
    finally:
        api.SWSSString_free(value)
        assert code(api, api.SWSSConfigDBConnector_free(connector)) == 0


@pytest.mark.parametrize("name", ["SWSSSonicV2Connector", "SWSSConfigDBConnector"])
def test_c_invalid_flags_clear_output(protected, api, name):
    output = C.c_void_p(1234)
    assert code(api, getattr(api, name + "_new_with_auth")(1, None, 2, C.byref(output))) == 1
    assert output.value is None
    assert code(api, getattr(api, name + "_new_with_auth")(1, None, 1, None)) == 1


def test_c_rejected_credentials_clear_output(protected, api):
    protected["secret"].write_text("wrong-test-password")
    output = C.c_void_p(1234)
    assert code(api, api.SWSSDBConnector_new_named_with_auth(b"STATE_DB", 500, 0, 1, C.byref(output))) == 1
    assert output.value is None


def test_rust_authentication(config_metadata):
    target = Path(os.environ.get('CARGO_TARGET_DIR', 'target'))
    binaries = [p for p in (target / 'debug/deps').glob('authentication-*')
                if p.is_file() and os.access(p, os.X_OK)]
    assert binaries, 'Run cargo test --workspace --all-features before this suite'
    binary = max(binaries, key=lambda p: p.stat().st_mtime)
    subprocess.run([str(binary.resolve()), '--ignored', '--nocapture'],
        env={**os.environ, 'SWSS_AUTH_DB_CONFIG': str(config_metadata['metadata'])},
        check=True, timeout=10)
