build:
	docker compose build

shell:
	docker compose run --rm app /bin/bash

run_etl:
	docker compose run --rm app python -m dbcp.cli

run_etl_csv:
	docker compose run --rm app python -m dbcp.cli --csv

run_etl_bq:
	docker compose run --rm app python -m dbcp.cli --upload-to-bigquery

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
