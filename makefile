data_warehouse:
	uv run python -m dbcp.cli etl --data-warehouse

data_mart:
	uv run python -m dbcp.cli etl --data-mart

private_data_mart:
	uv run python -m dbcp.cli etl --private-data-mart

all:
	uv run python -m dbcp.cli etl --data-mart --data-warehouse

upload_outputs:
	uv run python -m dbcp.cli upload-outputs \
        --build-ref $(BUILD_REF) \
        --code-git-sha $(CODE_GIT_SHA) \
        --github-action-run-id $(GITHUB_ACTION_RUN_ID) \
        --target $(TARGET)

publish_outputs:
	uv run python -m dbcp.cli publish-outputs $(VERSION_ID)

inspect_version:
	uv run python -m dbcp.cli inspect-outputs $(VERSION_ID)

duckdb:
	uv run duckdb -c 'INSTALL ui;'
	uv run duckdb ./data/dbcp.duckdb \
		-cmd 'LOAD UI; CALL start_ui();' \
		-cmd 'INSTALL bigquery FROM community; LOAD bigquery;' \
		-cmd "ATTACH 'dbname=postgres user=$$PROD_POSTGRES_USER host=$$PROD_POSTGRES_HOST password=$$PROD_POSTGRES_PASSWORD port=6543 connect_timeout=0' AS pg_prod (TYPE postgres, SCHEMA catalyst, READ_ONLY);" \
		-cmd "ATTACH 'dbname=postgres user=$$STAGING_POSTGRES_USER host=$$STAGING_POSTGRES_HOST password=$$STAGING_POSTGRES_PASSWORD port=5432 connect_timeout=0' AS pg_dev (TYPE postgres, SCHEMA catalyst, READ_ONLY);" \
		-cmd "ATTACH 'project=dbcp-dev-350818' AS bq (TYPE bigquery, READ_ONLY);"

test:
	uv run pytest --ignore=input/w

validate:
	uv run python -m dbcp.validation.tests

jupyter_lab:
	uv run jupyter lab

archive_all:
	uv run python -m dbcp.cli run-archivers

save_settings:
	uv run python -m dbcp.cli save-settings

.PHONY: test # "test" collides with a directory name. This tells make to run the command even if there is a directory named "test"
