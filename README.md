# Deployment Gap Model

Repository containing the ETL that produces the [Deployment Gap](https://www.deploymentgap.fund/) Model data and dashboards.

## Licensing

All data is subject to the terms of agreement individual to each data source:

| Data Source | Source | License and Terms of Use |
| ---- | ---- | ---- |
| ISO Queues | [LBNL](https://emp.lbl.gov/generation-storage-and-hybrid-capacity) | Ambiguous |
| Local Renewables Opposition Ordinances | [RELDI](https://climate.law.columbia.edu/sites/default/files/content/RELDI%20report%20updated%209.10.21.pdf) | Ambiguous |
| Fossil Infrastructure | [EIP Oil and Gas Watch](https://oilandgaswatch.org/) | Ambiguous |
| Marginal Cost of Energy | [PUDL](https://github.com/catalyst-cooperative/pudl) | CC-BY-4.0 |
| County FIPS codes | Census Bureau | Public Domain |
| State Wind Permits | [NCSL](https://www.ncsl.org/research/energy/state-wind-energy-siting.aspx) | Ambiguous |
| Climate and Economic Justice Screening Tool | [CEJST](https://screeningtool.geoplatform.gov/en/downloads#3/33.47/-97.5) | [CC0 1.0 Universal](https://github.com/usds/justice40-tool/blob/main/LICENSE.md) |
| Ballot Ready Upcoming Elections | [Ballot Ready](https://www.ballotready.org/) | Ambiguous |

There is no stated license for this repository's data input and output data because of upstream licensing ambiguities.

All other code and assets are published under the [MIT License](https://opensource.org/licenses/MIT).

## Data Access

To access the processed data, add the `dbcp-dev-350818` project to your Big Query instance. To do this, select Add Data > Pin project > Enter Project Name. There should be two datasets named `data_warehouse` and `data_mart`.

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

## GCP Authentication

The ETL requires access to some data stored in Google Cloud Platform (GCP).
To authenticate the docker container with GCP install the [gcloud utilities](https://cloud.google.com/sdk/docs/install) on your
computer. There are several ways to do this. We recommend using ``conda`` or its faster
sibling ``mamba``. If you're not using ``conda`` environments, there are other
ways to install the Google Cloud SDK explained in the link above.

```
conda install -c conda-forge google-cloud-sdk
```

Finally, use ``gcloud`` to establish application default credentials

```
gcloud auth application-default login
```

This will send you to an authentication page in your default browser. Once
authenticated, the command should print out a message:

```
Credentials saved to file: <path/to/your_credentials.json>
```

Set this path to a local environment varible called `GOOGLE_GHA_CREDS_PATH`

```
export GOOGLE_GHA_CREDS_PATH=<path/to/your_credentials.json>
```

`GOOGLE_GHA_CREDS_PATH` will be mounted into the container so
the GCP APIs in the container can access the data stored in GCP.

You'll also need to set an environment variable for the Geocodio API Key. This api key is stored
GCP project Secret Manager as `geocodio-api-key`.

```
export GEOCODIO_API_KEY={geocodio api key}
```

To set the environment variables each time the environment is activated,
run the following with the environment activated:

```bash
mkdir -p $CONDA_PREFIX/etc/conda/activate.d
```

then set environment variables:
```bash
echo 'export GOOGLE_GHA_CREDS_PATH="value"' > $CONDA_PREFIX/etc/conda/activate.d/env_vars.sh
```

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

## Run the ETL

Now that we’ve built the image and set the environment variables run:

```
make all
```

to create and load the data warehouse and data mart tables into postgres.

# Makefile

Here are additional make commands you can run.

```
make build
```

Builds the dbcp docker images.

```
make data_warehouse
```

Runs the etl and loads the data warehouse table to postgres.

```
make data_mart
```

Loads the data mart tables into postgres.

```
make all
```

Creates and loads the data warehouse and data mart tables into postgres.

```
make sql_shell
```

starts a PostgreSQL interactive terminal. This is helpful for inspecting the loaded data.

```
make shell
```

starts a bash interactive terminal. This is helpful for debugging.

```
make jupyter_lab
```

starts a jupyter lab instance at `http://127.0.0.1:8888/`. If you have another jupyter service running at `8888` you can change the port by setting an environment variable in your shell before running this command:

```
export JUPYTER_PORT=8890
```

# Development Process

## Git Branches
We have the following branch setup for updating the production and development tables:
- development schema (`data_warehouse_dev`, `data_mart_dev`): updated using the HEAD of the `main` branch
- production schema (`data_warehouse`, `data_mart`): updated using the most recent tagged version of the `main` branch

Developing new features or adding new data is done by making a new branch off of `main`, and merging into main allows us to see the results of these changes in the `_dev` schema tables.

### Deploying a new version to production
We use Git tags to deploy versioned releases to production. The format of these tags is `vYYYY-MM-xx`, where:

- `YYYY` = year
- `MM` = month
- `xx` = sequential release number (reset each month)

Only tagged commits are used to update the production schema (`data_warehouse`, `data_mart`). Follow the steps below to create and deploy a new version:

1. Make sure your main branch is up to date:
```bash
git checkout main
git pull origin main
```
2. Verify that everything is ready for release
Ensure that the code in main is what you want deployed to production, and tests have passed.

3. Create a new version tag
Replace the tag value with the appropriate version:
```bash
git tag v2025-08-01  # Example for the first release in August 2025
```
4. Push the tag to the remote repository:
```bash
git push origin v2025-08-01
```
This will trigger the deployment process to update the production schemas with the state of the code at the tagged commit.

## Comparing Branches During Development

In addition to inspecting the data warehouse and data mart tables that are loaded into postgres,
you may want to compare the data outputs between git branches. The `branch_compare_helper.py`
script automates parts of this process in order to make it easier to compare the data
in a target branch to a base branch (`dev` by default).

It checks out the target branch, and then the base branch, and for each branch:
* Runs `make all` to generate the data output files
* Copies the parquet files that are generated during the run to compare
to a newly created temporary data folder with the branch name as the subfolder

With these Parquet files, one can create a notebook that reads the data in these
folders and makes comparisons between branches.

With the comparison branch checked out, run the following to generate and compare
data outputs between two branches:

```bash
branch-compare [<target_branch>] [<base_branch>]
```
- `target_branch` – the feature or target branch to compare (defaults to the currently checked-out branch)
- `base_branch` – the base branch to compare against (defaults to `dev`)

## Architecture

DBCP roughly follows an ETL(T) architecture. `dbcp.etl.etl()` extracts the raw data, cleans it then loads it into a data warehouse, a local postgres database in ourcase. The tables in the data warehouse are normalized to a certain degree (we need to define a clear data model).

We then create denomalized tables for specific dashboards we call "data marts". To create a new data mart, create a new python file in the `dbcp.data_mart` module and implement a `create_data_mart()` function that returns the data_mart as a pandas data frame.
