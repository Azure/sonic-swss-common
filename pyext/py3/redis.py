"""Explicit native connections for redis-py pools; credentials stay in C++."""
import os
import socket

from redis import ConnectionPool
from redis.connection import UnixDomainSocketConnection
from redis.exceptions import AuthenticationError, ConnectionError as RedisConnectionError, TimeoutError as RedisTimeoutError
from . import swsscommon


class UnixConnection(UnixDomainSocketConnection):
    def __init__(self, *args, socket_connect_timeout=None, **kwargs):
        super().__init__(*args, **kwargs)
        self._connect_timeout = self.socket_timeout if socket_connect_timeout is None else socket_connect_timeout

    def _connect(self):
        timeout = self._connect_timeout
        if timeout is not None and timeout < 0:
            raise ValueError('Connection timeout must be nonnegative')
        milliseconds = 0 if not timeout else max(1, int(timeout * 1000))
        try:
            fd = swsscommon.openRedisUnixSocket(self.path, milliseconds, True)
        except PermissionError as error:
            raise AuthenticationError(str(error)) from error
        except TimeoutError as error:
            raise RedisTimeoutError(str(error)) from error
        except ConnectionError as error:
            raise RedisConnectionError(str(error)) from error
        try:
            connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM, fileno=fd)
        except BaseException:
            os.close(fd)
            raise
        try:
            connection.settimeout(self.socket_timeout)
            return connection
        except BaseException:
            connection.close()
            raise


def connection_pool(unix_socket_path=None, authenticate=False, **options):
    """Select native Unix connection handling explicitly, before redis-py's SELECT."""
    if unix_socket_path is None:
        if authenticate:
            raise ValueError('Native authentication requires an explicit Unix socket')
        return ConnectionPool(**options)
    options.pop('host', None)
    options.pop('port', None)
    connection_class = UnixConnection if authenticate else UnixDomainSocketConnection
    return ConnectionPool(connection_class=connection_class, path=unix_socket_path, **options)
