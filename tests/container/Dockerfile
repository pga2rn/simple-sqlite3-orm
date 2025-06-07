ARG UBUNTU_BASE

FROM ${UBUNTU_BASE}

# copy and setup the entry_point.sh
COPY ./docker/test_base/entry_point.sh /entry_point.sh

# bootstrapping the python environment
RUN set -eux; \
    apt-get update; \
    PYTHON_PACKAGE="python3"; \
    if [ "${UBUNTU_BASE}" == "ubuntu:18.04" ]; then \
        PYTHON_PACKAGE="python3.8"; \
    fi; \
    apt-get install -y -qq --no-install-recommends \
        curl \
        ca-certificates \
        git \
        ${PYTHON_PACKAGE}; \
    curl -LsSf https://astral.sh/uv/install.sh | sh; \
    apt-get clean; \
    rm -rf \
        /tmp/* \
        /var/lib/apt/lists/* \
        /var/tmp/*

ENV PATH="/root/.local/bin:${PATH}"