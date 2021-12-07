# set base image (host OS)
FROM python:3.8

WORKDIR /app

COPY requirements.txt /app/requirements.txt

RUN python -m pip install -r /app/requirements.txt
RUN rm requirements.txt

RUN apt-get update && apt-get install sqlite3
