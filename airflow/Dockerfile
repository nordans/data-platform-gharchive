FROM apache/airflow:2.10.5-python3.11

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

COPY --chown=airflow:root pyproject.toml uv.lock /tmp/airflow-deps/

WORKDIR /tmp/airflow-deps

RUN uv sync --frozen --no-dev

ENV PATH="/tmp/airflow-deps/.venv/bin:${PATH}"