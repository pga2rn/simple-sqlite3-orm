#!/bin/bash
set -eu

TEST_ROOT=/test_root
SRC=/
OUTPUT_DIR="${OUTPUT_DIR:-/.test_result}"

# copy the source code as source is read-only
cp -R "${SRC}" "${TEST_ROOT}"

cd ${TEST_ROOT}
uv run --python python3 --no-managed-python coverage run -m pytest --junit-xml=${OUTPUT_DIR}/pytest.xml ${@:-}
uv run --python python3 --no-managed-python coverage combine
uv run --python python3 --no-managed-python coverage xml -o ${OUTPUT_DIR}/coverage.xml