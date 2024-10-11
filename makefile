build:
	docker compose build

shell:
	docker compose run --rm app /bin/bash

data_warehouse:
	docker compose run --rm app python -m dbcp.cli --etl

data_mart:
	docker compose run --rm app python -m dbcp.cli --data-mart

all:
	docker compose run --rm app python -m dbcp.cli --data-mart --etl

sql_shell:
	docker compose run --rm postgres bash -c 'psql -U $$POSTGRES_USER -h $$POSTGRES_HOST $$POSTGRES_DB'

test:
	docker compose run --rm app pytest --ignore=input/w

clean:
	docker compose down -v

validate:
	docker compose run --rm app python -m dbcp.validation.tests

update_conda:
	conda env update --file environment.yml --name dbcp-dev --prune

jupyter_lab:
	docker compose up

.PHONY: test # "test" collides with a directory name. This tells make to run the command even if there is a directory named "test"
