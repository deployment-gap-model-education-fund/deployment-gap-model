APP_RUN_COMMAND = docker compose run --rm app

build:
	docker compose build

shell:
	$(APP_RUN_COMMAND) /bin/bash

data_warehouse:
	$(APP_RUN_COMMAND) python -m dbcp.cli etl --data-warehouse

data_mart:
	$(APP_RUN_COMMAND) python -m dbcp.cli etl --data-mart

all:
	$(APP_RUN_COMMAND) python -m dbcp.cli etl --data-mart --data-warehouse

sql_shell:
	docker compose run --rm postgres bash -c 'psql -U $$POSTGRES_USER -h $$POSTGRES_HOST $$POSTGRES_DB'

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
