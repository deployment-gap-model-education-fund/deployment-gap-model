# set base image (host OS)
FROM python:3.8

WORKDIR /app

COPY requirements.txt .

RUN pip install -r requirements.txt