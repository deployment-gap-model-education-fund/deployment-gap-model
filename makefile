build:
	docker compose build

shell:
	docker compose run --rm app /bin/bash

run_etl:
	docker compose run --rm app python -m dbcp.cli

sql_shell:
	docker compose run --rm postgres bash -c 'psql -U $$POSTGRES_USER -h $$POSTGRES_HOST $$POSTGRES_DB'

pudl_db:
	docker compose run --rm app bash -c 'sqlite3 /app/input/$$PUDL_VERSION/pudl_data/sqlite/pudl.sqlite'
