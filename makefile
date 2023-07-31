build:
	docker compose build

shell:
	docker compose run --rm app /bin/bash

etl_local:
	docker compose run --rm app python -m dbcp.cli --etl

data_mart_local:
	docker compose run --rm app python -m dbcp.cli --data-mart

all_local:
	docker compose run --rm app python -m dbcp.cli --data-mart --etl

etl_bq_dev:
	docker compose run --rm app python -m dbcp.cli --etl --upload-to-bigquery --bigquery-env dev

data_mart_bq_dev:
	docker compose run --rm app python -m dbcp.cli --data-mart --upload-to-bigquery --bigquery-env dev

all_bq_dev:
	docker compose run --rm app python -m dbcp.cli --data-mart --etl --upload-to-bigquery --bigquery-env dev

etl_bq_prod:
	docker compose run --rm app python -m dbcp.cli --etl --upload-to-bigquery --bigquery-env prod

data_mart_bq_prod:
	docker compose run --rm app python -m dbcp.cli --data-mart --upload-to-bigquery --bigquery-env prod

all_bq_prod:
	docker compose run --rm app python -m dbcp.cli --data-mart --etl --upload-to-bigquery --bigquery-env prod

sql_shell:
	docker compose run --rm postgres bash -c 'psql -U $$POSTGRES_USER -h $$POSTGRES_HOST $$POSTGRES_DB'

pudl_db:
	docker compose run --rm app bash -c 'sqlite3 /app/data/data_cache/$$PUDL_VERSION/pudl_data/sqlite/pudl.sqlite'

test:
	docker compose run --rm app pytest --ignore=input/w

clean:
	docker compose down -v

update_conda:
	conda env update --file environment.yml --name dbcp-dev --prune

jupyter_lab:
	docker compose up

.PHONY: test # "test" collides with a directory name. This tells make to run the command even if there is a directory named "test"
