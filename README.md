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

## Install Dev Environment

Make sure you have [uv installed](https://docs.astral.sh/uv/getting-started/installation/).
The ETL and tests will run in a docker container, so you only need to install a minimal dev environment
to run pre-commit hooks. You can do this with the following command:

```
uv sync --only-dev
```

## GCP Authentication

The ETL requires access to some data stored in Google Cloud Platform (GCP).
To authenticate with GCP install the [gcloud utilities](https://docs.cloud.google.com/sdk/docs/install-sdk#latest-version) on your
computer. Once complete, use ``gcloud auth application-default login`` to establish application default credentials

```
gcloud auth application-default login
```

This will send you to an authentication page in your default browser. Once
authenticated, the command should print out a message:

```
Credentials saved to file: <path/to/your_credentials.json>
```

You'll also need to set an environment variable for the Geocodio API Key. This api key is stored
GCP project Secret Manager as `geocodio-api-key`.

```
export GEOCODIO_API_KEY={geocodio api key}
```

## Postgres Authentication
Madrone is moving towards using a `postgres` instance for accessing data,
and we have configured the ETL to auto-publish data to this instance when
running the `update-data` job from github. We generally avoid accessing
this instance during local runs of the ETL, so setting up credentials is
not strictly necessary, but it can be useful for testing and validation
purposes. To access this instance you must first set the following environment
variables:

- `PROD_POSTGRES_HOST`
- `PROD_POSTGRES_USER`
- `PROD_POSTGRES_PASSWORD`
- `STAGING_POSTGRES_HOST`
- `STAGING_POSTGRES_USER`
- `STAGING_POSTGRES_PASSWORD`

See the secret `DBCP Postgres Credentials` in Bitwarden to access the
corresponding values for each of these variables.

To explore the data using SQL you can use the `make duckdb` target (see
details below).

## Git Pre-commit Hooks

Git hooks let you automatically run scripts at various points as you manage your source code. “Pre-commit” hook scripts are run when you try to make a new commit. These scripts can review your code and identify bugs, formatting errors, bad coding habits, and other issues before the code gets checked in. This gives you the opportunity to fix those issues before publishing them.

To make sure they are run before you commit any code, you need to enable the pre-commit hooks scripts with this command:

```
uv run pre-commit install
```

The scripts that run are configured in the .pre-commit-config.yaml file.

## Run the ETL

Now that we’ve built the image and set the environment variables run:

```
make all
```

to create and load the data warehouse and data mart tables into duckdb. You can also
selectively run only the `data_warehouse` or `data_mart` using:

```
make data_warehouse
```

or,

```
make data_mart
```

# Development Tools

We also include `Makefile` targets to provide useful tools during development.

### make duckdb

This target will open a `duckdb` shell instance and the [duckdb ui](https://duckdb.org/2025/03/12/duckdb-ui),
which can be used to access local and remote data. This will attach to the `duckdb`
file created by local ETL runs as well as BigQuery and the dev/prod postgres instances.
Data is organized into distinct schemas for each source, allowing for organized access
to each of these sources. Remote tables can be accessed with queries like:

```
SELECT * FROM {bq|pg_prod|pg_dev}.{schema_name}.{table_name}
```

While local tables don't require the `bq`/`pg` prefix schema.

Note that we currently publish all data to a single schema in the postgres instances,
so the `schema_name` can be dropped for these cases.


### make inspect_version
`make inspect_version VERSION_ID=$VERSION_ID` will open a `duckdb` interface to inspect
outputs corresponding to a specific version. The `VERSION_ID` is a `uuid` that is
generated by the `update-data` workflow. `update-data` saves a text file with the
`uuid` as an artifact during each successful run. This file can be downloaded to
get the `VERSION_ID` associated with the run and inspect its outputs. The `make`
target will generate a SQL view for each parquet file output by `update-data`.


# Data Builds / Deployment
## Builds
Data builds are performed by the `update-data` Github workflow. This workflow will
run the full ETL, all tests, then upload data as parquet files to the GCS bucket,
`gs://dgm-outputs/{VERSION_ID}/`. Within this GCS directory, there will be a file
called `etl-run-metadata.yaml`, and subdirectories for the `data_mart` and `data_warehouse`
schemas, which will each contain a set of parquet files (one per table).

Builds can be manually triggered, and they will run on new pushes to `main`, or when
tags are pushed with the pattern `v*`. Builds have an associated `target`, which can
be either `prod` or `dev`. This doesn't actually impact the build process, but will
be saved in the output metadata and used when we publish the outputs.


## Deployment
The `publish-data` workflow will deploy data output by `update-data` to BigQuery and
postgres. The `target` associated with the build will determine which BigQuery schema,
or Postgres instance we deploy to.

The `publish-data` workflow will run automatically when a tagged build, or build on
`main` is complete. It can also be manually triggered and just needs the `run_id` from
the associated `update-data` run that you want to publish.


## Architecture

DBCP roughly follows an ETL(T) architecture. `dbcp.etl.etl()` extracts the raw data, cleans it then loads it into a data warehouse, a local postgres database in ourcase. The tables in the data warehouse are normalized to a certain degree (we need to define a clear data model).

We then create denomalized tables for specific dashboards we call "data marts". To create a new data mart, create a new python file in the `dbcp.data_mart` module and implement a `create_data_mart()` function that returns the data_mart as a pandas data frame.
