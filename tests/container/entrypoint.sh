#!/bin/bash
set -eux

SRC=/src
OUTPUT_DIR=/src/.test_result

cd ${SRC}
uv run --python python3 --no-managed-python coverage run -m pytest --junit-xml=${OUTPUT_DIR}/pytest.xml ${@:-}
uv run --python python3 --no-managed-python coverage combine
uv run --python python3 --no-managed-python coverage xml -o ${OUTPUT_DIR}/coverage.xml