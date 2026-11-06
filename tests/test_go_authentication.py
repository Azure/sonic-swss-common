import os
from pathlib import Path
import subprocess

from test_redis_authentication import protected  # noqa: F401


def test_go_authentication(protected):
    root = Path(__file__).resolve().parents[1]
    subprocess.run(['/usr/local/go/bin/go', 'test', '-count=1', '-v', './goext/redisconn'],
        cwd=root, env={**os.environ, 'SWSS_AUTH_UNIX_SOCKET': protected['socket'],
                      'CGO_LDFLAGS': '-L' + str(root / 'common/.libs')},
        check=True, timeout=120)
