APP_RUN_COMMAND = docker compose run --rm app

build:
	docker compose build

shell:
	$(APP_RUN_COMMAND) /bin/bash

data_warehouse:
	$(APP_RUN_COMMAND) python -m dbcp.cli etl --data-warehouse

data_mart:
	$(APP_RUN_COMMAND) python -m dbcp.cli etl --data-mart

private_data_mart:
	$(APP_RUN_COMMAND) python -m dbcp.cli etl --private-data-mart

all:
	$(APP_RUN_COMMAND) python -m dbcp.cli etl --data-mart --data-warehouse

sql_shell:
	docker compose run --rm postgres bash -c 'psql -U $$POSTGRES_USER -h $$POSTGRES_HOST $$POSTGRES_DB'

duckdb:
	docker compose run --publish 4213:4213 --rm app  duckdb -ui \
		-cmd 'INSTALL bigquery FROM community; LOAD bigquery;' \
		-cmd "ATTACH 'dbname=postgres user=catalyst host=$$PROD_POSTGRES_HOST password=$$PROD_POSTGRES_PASSWORD port=6543 connect_timeout=0' AS pg_prod (TYPE postgres, SCHEMA catalyst, READ_ONLY);" \
		-cmd "ATTACH 'dbname=postgres user=$$POSTGRES_USER host=$$POSTGRES_HOST password=$$POSTGRES_PASSWORD port=5432 connect_timeout=0' AS pg_dev (TYPE postgres);" \
		-cmd "ATTACH 'project=dbcp-dev-350818' AS bq (TYPE bigquery, READ_ONLY);"

test:
	$(APP_RUN_COMMAND) pytest --ignore=input/w

clean:
	docker compose down -v

validate:
	$(APP_RUN_COMMAND) python -m dbcp.validation.tests

update_conda:
	conda env update --file environment.yml --name dbcp-dev --prune

jupyter_lab:
	docker compose up

archive_all:
	$(APP_RUN_COMMAND) python -m dbcp.cli run-archivers

save_settings:
	$(APP_RUN_COMMAND) python -m dbcp.cli save-settings

.PHONY: test # "test" collides with a directory name. This tells make to run the command even if there is a directory named "test"
