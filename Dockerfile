# set base image (host OS)
FROM python:3.13

RUN apt-get -y update && apt-get -y install sqlite3
RUN apt-get -y install libgdal-dev

RUN useradd -d /app/ dbcp
USER dbcp

WORKDIR /app

COPY requirements.txt /app/requirements.txt
COPY pyproject.toml /app/pyproject.toml


RUN python -m pip install --upgrade pip
RUN python -m pip install -r /app/requirements.txt
RUN rm requirements.txt

# Add the python packages to PATH
ENV PATH="/app/.local/bin:${PATH}"

# Add dbcp to PYTHONPATH
ENV PYTHONPATH="/app:${PYTHONPATH}"
