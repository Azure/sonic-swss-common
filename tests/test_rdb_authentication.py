import json
import os
import subprocess
import sys
import time

import pytest
import redis
from swsscommon import swsscommon as swss
from test_redis_authentication import protected  # noqa: F401


def test_native_rdb_snapshot(protected):
    socket = str(protected['work'] / 'rdb.sock')
    config = protected['work'] / 'rdb.conf'
    config.write_text((protected['work'] / 'redis.conf').read_text().replace(protected['socket'], socket))
    server = subprocess.Popen(['redis-server', str(config)], cwd=protected['work'], stdout=subprocess.DEVNULL)
    try:
        client = redis.Redis(unix_socket_path=socket, username='test-writer', password='test-only-password', db=6)
        for _ in range(100):
            try:
                if client.ping(): break
            except redis.ConnectionError:
                time.sleep(.02)
        client.set(b'rdb\0key', b'binary\0value')
        policy = json.loads(protected['policy'].read_text()); policy['unix_socket'] = socket
        protected['policy'].write_text(json.dumps(policy))
        output = protected['work'] / 'snapshot.rdb'
        swss.exportRedisRdb(socket, str(output), 10000, True)
        assert output.read_bytes().startswith(b'REDIS')
        assert os.stat(output).st_mode & 0o777 == 0o600
        subprocess.run(['redis-check-rdb', str(output)], check=True, capture_output=True)
    finally:
        server.terminate(); server.wait(timeout=5)


@pytest.mark.parametrize('header', ['$100\r\nshort', '$EOF:unsupported\r\n'])
def test_bad_rdb_stream_preserves_destination(protected, header):
    socket = str(protected['work'] / 'bad-rdb.sock')
    program = '''
import socket,sys
s=socket.socket(socket.AF_UNIX);s.bind(sys.argv[1]);s.listen(1)
c,_=s.accept();c.recv(1024);c.sendall(sys.argv[2].encode());c.close();s.close()
'''
    server = subprocess.Popen([sys.executable, '-c', program, socket, header])
    try:
        for _ in range(100):
            if os.path.exists(socket): break
            time.sleep(.01)
        output = protected['work'] / 'existing.rdb'; output.write_bytes(b'original')
        with pytest.raises(RuntimeError):
            swss.exportRedisRdb(socket, str(output), 500, False)
        assert output.read_bytes() == b'original'
        assert not list(protected['work'].glob('existing.rdb.*'))
    finally:
        if server.poll() is None: server.terminate()
        server.wait(timeout=5)
