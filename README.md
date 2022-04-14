# down_ballot_climate
Repository for work with the Down Ballot Climate Project

# Setup
## Conda Environment
Make sure you have [conda installed](https://docs.conda.io/projects/conda/en/latest/user-guide/install/index.html). Once conda is installed, run:
```
conda env create --name dbcp-dev --file environment.yml
```
Then activate the environment:
```
conda activate dbcp-dev
```
This conda environment has python, pip and pre-commit installed in it. This env is just for running pre-commits, the actual ETL development happens in docker.

## Git Pre-commit Hooks
Git hooks let you automatically run scripts at various points as you manage your source code. “Pre-commit” hook scripts are run when you try to make a new commit. These scripts can review your code and identify bugs, formatting errors, bad coding habits, and other issues before the code gets checked in. This gives you the opportunity to fix those issues before publishing them.

To make sure they are run before you commit any code, you need to enable the pre-commit hooks scripts with this command:
```
pre-commit install
```
The scripts that run are configured in the .pre-commit-config.yaml file.


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
Builds the dbcp docker images.

```
make etl_local
```
Runs the etl and loads the data warehouse table to postgres.

```
make etl_bq
```
Runs the etl and upload the data warehouse to BigQuery.

```
make data_mart_local
```
Loads the data mart tables into postgres.

```
make data_mart_bq
```
Loads the data mart tables to BigQuery.

```
make all_local
```
Creates and loads the data warehouse and data mart tables into postgres.

```
make all_bq
```
Creates and loads the data warehouse and data mart tables to BigQuery.

```
make sql_shell
```
starts a PostgreSQL interactive terminal. This is helpful for inspecting the loaded data.

```
make shell
```
starts a bash interactive terminal. This is helpful for debugging.

```
make run_etl_bq
```
runs the etl and loads the data to our BigQuery instance. Currently only @bendnorman has the permissions to load to BigQuery.

```
make jupyter_lab
```
starts a jupyter lab instance at `http://127.0.0.1:8888/`. If you have another jupyter service running at `8888` you can change the port by setting an environment variable in your shell before running this command:

```
export JUPYTER_PORT=8890
```
## Architecture
DBCP roughly follows an ETL(T) architecture. `dbcp.etl.etl()` extracts the raw data, cleans it then loads it into a data warehouse, a local postgres database in ourcase. The tables in the data warehouse are normalized to a certain degree (we need to define a clear data model).

Tableau doesn't handle normalized data tables very well so we create denomalized tables for specific dashboards we call "data marts". To create a new data mart, create a new python file in the `dbcp.data_mart` module and implement a `create_data_mart()` function that returns the data_mart as a pandas data frame.
