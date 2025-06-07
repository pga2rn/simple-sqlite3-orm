#!/bin/bash
set -eux

TEST_ROOT=/test_root
SRC=/src
OUTPUT_DIR=/test_result

mkdir -p ${TEST_ROOT}
# source code needed to be rw, so copy it to the test_root
cp -R ${SRC}/src ${TEST_ROOT}
# symlink all the other needed folders/files into test root
ln -s ${SRC}/tests ${TEST_ROOT}
ln -s ${SRC}/.git ${TEST_ROOT}
ln -s ${SRC}/pyproject.toml ${TEST_ROOT}
ln -s ${SRC}/hatch.toml ${TEST_ROOT}
ln -s ${SRC}/uv.lock ${TEST_ROOT}
ln -s ${SRC}/README.md ${TEST_ROOT}
ln -s ${SRC}/LICENSE ${TEST_ROOT}

cd ${TEST_ROOT}
uv run --python python3 --no-managed-python coverage run -m pytest --junit-xml=${OUTPUT_DIR}/pytest.xml ${@:-}
uv run --python python3 --no-managed-python coverage combine || true
uv run --python python3 --no-managed-python coverage xml -o ${OUTPUT_DIR}/coverage.xml
