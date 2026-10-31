"""Raw redis-py connection adapters using swss-common's native writer login."""

import os
import socket

from redis import Connection, ConnectionPool
from redis.connection import UnixDomainSocketConnection

from . import swsscommon


def _milliseconds(timeout):
    if timeout is None or timeout == 0:
        return 0
    if timeout < 0:
        raise ValueError("Redis timeout must not be negative")
    return max(1, int(timeout * 1000))


class UnixConnection(UnixDomainSocketConnection):
    def __init__(self, *args, socket_connect_timeout=None, **kwargs):
        # redis-py 3.x Unix connections do not accept or store this option.
        # Keep it in the adapter so both old and new clients reach the native
        # connector with the requested connect and read timeouts.
        super().__init__(*args, **kwargs)
        self.socket_connect_timeout = (
            self.socket_timeout if socket_connect_timeout is None
            else socket_connect_timeout)

    def _connect(self):
        fd = swsscommon.openRedisUnixSocket(
            self.path, _milliseconds(self.socket_connect_timeout),
            _milliseconds(self.socket_timeout))
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


class TcpConnection(Connection):
    def _connect(self):
        swsscommon.checkRedisTcpEndpoint(self.host, self.port)
        return super()._connect()


def connection_pool(unix_socket_path=None, **options):
    """Build a pool; each new Unix connection authenticates in C++ before SELECT.

    Pass database, encoding and timeout options here, rather than to a Redis
    instance constructed with this pool. No credentials are returned to Python.
    """
    if unix_socket_path is not None:
        options.pop('host', None)
        options.pop('port', None)
        return ConnectionPool(connection_class=UnixConnection,
                              path=unix_socket_path, **options)
    return ConnectionPool(connection_class=TcpConnection, **options)
