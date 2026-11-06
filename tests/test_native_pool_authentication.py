import fcntl
import json
import os
import socket
import threading

import pytest
import redis
from swsscommon import swsscommon as swss
from swsscommon.redis import connection_pool
from test_redis_authentication import protected  # noqa: F401


@pytest.mark.parametrize('authenticate', [False, True])
def test_pool_explicit_authentication(protected, authenticate):
    client = redis.Redis(connection_pool=connection_pool(
        unix_socket_path=protected['socket'], db=6, authenticate=authenticate))
    assert client.ping()
    if authenticate:
        with client.pipeline(transaction=True) as transaction:
            transaction.set(b'binary\0key', b'value\0bytes')
            transaction.get(b'binary\0key')
            assert transaction.execute() == [True, b'value\0bytes']
        client.connection_pool.disconnect()
        assert client.get(b'binary\0key') == b'value\0bytes'
    else:
        with client.pipeline(transaction=True) as transaction:
            transaction.get('pool-legacy')
            assert transaction.execute() == [None]
        with pytest.raises(redis.ResponseError, match='permissions'):
            client.set('pool-legacy', 'denied')


def test_pool_rejected_credentials(protected):
    protected['secret'].write_text('wrong-test-password')
    client = redis.Redis(connection_pool=connection_pool(
        unix_socket_path=protected['socket'], authenticate=True))
    with pytest.raises(redis.AuthenticationError):
        client.ping()


def test_factory_descriptor_ownership(protected):
    before = len(os.listdir('/proc/self/fd'))
    for _ in range(8):
        fd = swss.openRedisUnixSocket(protected['socket'], 500, True)
        assert fcntl.fcntl(fd, fcntl.F_GETFD) & fcntl.FD_CLOEXEC
        os.close(fd)
    assert len(os.listdir('/proc/self/fd')) == before


def test_pool_authentication_timeout(protected):
    path = str(protected['work'] / 'pool-stalled.sock')
    listener = socket.socket(socket.AF_UNIX)
    listener.bind(path)
    listener.listen(1)
    listener.settimeout(1)
    stop, received = threading.Event(), []
    def server():
        try:
            connection, _ = listener.accept()
            with connection:
                connection.settimeout(1)
                received.append(connection.recv(1024))
                stop.wait(3)
        except socket.timeout:
            # The client may fail before connecting or sending AUTH.
            pass
    thread = threading.Thread(target=server, daemon=True)
    try:
        data = json.loads(protected['policy'].read_text())
        data['unix_socket'] = path
        protected['policy'].write_text(json.dumps(data))
        thread.start()
        client = redis.Redis(connection_pool=connection_pool(unix_socket_path=path,
            authenticate=True, socket_connect_timeout=0.1, socket_timeout=1))
        with pytest.raises(redis.TimeoutError):
            client.ping()
    finally:
        stop.set()
        if thread.ident is not None:
            thread.join(timeout=4)
        listener.close()
    assert received and b'AUTH' in received[0]


def test_authentication_requires_explicit_unix():
    with pytest.raises(ValueError, match='explicit Unix socket'):
        connection_pool(authenticate=True)
