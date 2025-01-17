#!/bin/sh

# run the test
for test in algorithm/src/main/python/it/big/unibo/streamprofiling/paper/*.py; do
    if [ "$test" != "algorithm/src/main/python/it/big/unibo/streamprofiling/paper/common.py" ]; then
        python3 "$test"
    fi
done