# set base image (host OS)
FROM python:3.13

# hadolint ignore=DL3008
RUN apt-get -y update && \
    apt-get -y --no-install-recommends install libgdal-dev sqlite3 curl ca-certificates && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Download the latest uv installer
ADD https://astral.sh/uv/install.sh /uv-installer.sh

# Run the installer then remove it
RUN sh /uv-installer.sh && rm /uv-installer.sh

# Ensure the installed binary is on the `PATH`
ENV PATH="/root/.local/bin/:$PATH"

WORKDIR /app

COPY pyproject.toml /app/pyproject.toml
COPY README.md /app/README.md

RUN uv sync --no-dev
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
RUN curl https://install.duckdb.org | sh
