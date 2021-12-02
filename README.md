# down_ballot_climate
Repository for work with the Down Ballot Climate Project

# Usage
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

## Conda
There are some packages that are helpful for local development that aren't necessary in the docker image like pre-commit. To manage these packages, create a conda environment using this command:

```
conda env create -f environment.yml
```
