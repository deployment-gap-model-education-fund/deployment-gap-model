# set base image (host OS)
FROM python:3.12

RUN apt-get -y update && apt-get -y install sqlite3
RUN apt-get -y install libgdal-dev

RUN useradd -d /app/ dbcp

# set working directory and copy code
WORKDIR /app
COPY . .

# Give dbcp user ownership of everything in /app
RUN chown -R dbcp:dbcp /app

# Switch to dbcp user
USER dbcp

# Install pip dependencies via pyproject.toml
RUN pip install --upgrade pip \
 && pip install -e .

# Add the python packages to PATH
ENV PATH="/app/.local/bin:${PATH}"

# Add dbcp to PYTHONPATH
ENV PYTHONPATH="/app:${PYTHONPATH}"
