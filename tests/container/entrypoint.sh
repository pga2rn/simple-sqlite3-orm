#!/bin/bash
set -eux

TEST_ROOT=/test_root
SRC=/src
OUTPUT_DIR="${OUTPUT_DIR:-/test_result}"

# copy the source code as source is read-only
cp -R "${SRC}/src" "${TEST_ROOT}"
cp -R "${SRC}/tests" "${TEST_ROOT}"
cp "${SRC}/pyproject.toml" "${TEST_ROOT}"
cp "${SRC}/ruff.toml" "${TEST_ROOT}" || true
cp "${SRC}/hatch.toml" "${TEST_ROOT}" || true
cp "${SRC}/uv.lock" "${TEST_ROOT}" || true

cd ${TEST_ROOT}
uv run --python python3 --no-managed-python coverage run -m pytest --junit-xml=${OUTPUT_DIR}/pytest.xml ${@:-}
uv run --python python3 --no-managed-python coverage combine
uv run --python python3 --no-managed-python coverage xml -o ${OUTPUT_DIR}/coverage.xml