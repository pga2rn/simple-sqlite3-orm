ARG UBUNTU_BASE

FROM ${UBUNTU_BASE}

ARG UBUNTU_BASE

# copy and setup the entry_point.sh
COPY --chmod=755 ./entrypoint.sh /entrypoint.sh

# bootstrapping the python environment
RUN set -eux; \
    apt-get update; \
    PYTHON_PACKAGE="python3 python3-distutils"; \
    if [ "${UBUNTU_BASE}" = "ubuntu:18.04" ]; then \
        PYTHON_PACKAGE="python3.8 python3-distutils"; \
    elif [ "${UBUNTU_BASE}" = "ubuntu:24.04" ]; then \
        PYTHON_PACKAGE="python3"; \
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
ENTRYPOINT [ "/entrypoint.sh" ]
