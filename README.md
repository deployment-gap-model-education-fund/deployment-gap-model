# down_ballot_climate
Repository for work with the Down Ballot Climate Project

# Usage
## Configure Environment Variables

There are two config steps that must be completed before this repo will function.
First, after cloning this repo, make a new file in the repo root directory called `.env` and enter these two lines to configure local ports:
```
POSTGRES_PORT=5432
JUPYTER_PORT=8890
```
These ports can be changed to fit your local needs.

Second, make a copy of `default.env` and call it `local.env`. Follow the instructions inside to set up API key access.

## Docker
The Down Ballot Climate Project uses [Docker](https://www.docker.com/) for development and deployment. To start working with DBCP, [install docker](https://docs.docker.com/get-docker/) and refer to the following make commands:

```
make build
```
to build the dbcp docker imagess.

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

## Conda
There are some packages that are helpful for local development that aren't necessary in the docker image like pre-commit. To manage these packages, create a conda environment using this command:

```
conda env create -f environment.yml
```
