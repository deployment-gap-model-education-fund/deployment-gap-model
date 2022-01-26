# down_ballot_climate
Repository for work with the Down Ballot Climate Project

# Setup
## Conda Environment
Make sure you have [conda installed](https://docs.conda.io/projects/conda/en/latest/user-guide/install/index.html). Once conda is installed, run:
```
conda env create --name dbcp-dev --file environment.yml
```
then activate the environment:
```
conda activate dbcp-dev
```
This conda environment has python, pip and pre-commit installed in it. This env is just for running pre-commits, the actual ETL development happens in docker.

## Docker
[Install docker](https://docs.docker.com/get-docker/). Once you have docker installed, make sure it is running.

Now we can build the docker images by running:
```
make build
```
This command create a docker image and installs all the packages in `requirements.txt` so it will take a couple minutes to complete.

If you get this error:
```
Cannot connect to the Docker daemon at unix:///var/run/docker.sock. Is the docker daemon running?
```
during this step it means docker is not running.

## Environment Variables
Once the image is created we need to set some environment variables. First, make a new file in the repo root directory called `.env` and enter these two lines to configure local ports:
```
POSTGRES_PORT=5432
JUPYTER_PORT=8890
```
If you have other services running on these ports, you can change them in `.env`.

Second, make a copy of `default.env` and call it `local.env`. Follow the instructions inside to set up API key access. `local.env` contains environment variables that can be accessed within the docker container. You can read more about docker environment variables [here](https://docs.docker.com/compose/environment-variables/).

## Run the ETL
Now that we’ve built the image and set the environment variables run:
```
make run_etl
```
to run the ETL and load the data to postgres.

# Makefile
Here are additional make commands you can run.
```
make build
```
to build the dbcp docker images.

```
make run_etl
```
to run the etl.

```
make sql_shell
```
starts a PostgreSQL interactive terminal.

```
make shell
```
starts a bash interactive terminal.

```
make run_etl_bq
```
runs the etl and loads the data to our BigQuery instance.

```
make jupyter_lab
```
starts a jupyter lab instance at http://127.0.0.1:8888/. If you have another jupyter service running at 8888 you can change the port by setting an environment variable in your shell before running this command:

```
export JUPYTER_PORT=8890
```
