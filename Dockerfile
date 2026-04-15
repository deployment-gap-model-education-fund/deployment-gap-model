# set base image (host OS)
FROM python:3.13

RUN apt-get -y update && apt-get -y install sqlite3
RUN apt-get -y install libgdal-dev
RUN apt-get -y install curl ca-certificates

# Download the latest uv installer
ADD https://astral.sh/uv/install.sh /uv-installer.sh

# Run the installer then remove it
RUN sh /uv-installer.sh && rm /uv-installer.sh

# Ensure the installed binary is on the `PATH`
ENV PATH="/root/.local/bin/:$PATH"

WORKDIR /app

COPY pyproject.toml /app/pyproject.toml

RUN uv sync
RUN curl https://install.duckdb.org | sh
