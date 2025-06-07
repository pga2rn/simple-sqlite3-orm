#!/bin/bash
set -eux

TEST_ROOT=/test_root
SRC=/src
OUTPUT_DIR="${OUTPUT_DIR:-/test_result}"

# copy the source code as source is read-only
cp -R "${SRC}/src" "${TEST_ROOT}"
cp -R "${SRC}/tests"
cp "${SRC}/pyproject.toml"
cp "${SRC}/ruff.toml" || true
cp "${SRC}/hatch.toml" || true
cp "${SRC}/uv.lock" || true

cd ${TEST_ROOT}
uv run --python python3 --no-managed-python coverage run -m pytest --junit-xml=${OUTPUT_DIR}/pytest.xml ${@:-}
uv run --python python3 --no-managed-python coverage combine
uv run --python python3 --no-managed-python coverage xml -o ${OUTPUT_DIR}/coverage.xml