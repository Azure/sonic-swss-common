#!/bin/bash
set -eu
cd "$(dirname "$0")/.."

# The fixture requires a disposable root container and refuses existing policy.
sudo --preserve-env=PATH,PYTHONPATH,LD_LIBRARY_PATH,CARGO_TARGET_DIR \
    env SWSS_AUTH_TEST_CONTAINER=1 python3 -B -m pytest --noconftest \
    -o addopts= -p no:cacheprovider --junitxml=authentication-tests.xml \
    --cov=. --cov-append --cov-report=xml:tests/coverage.xml \
    tests/test_*authentication*.py
