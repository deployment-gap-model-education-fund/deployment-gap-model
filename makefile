build:
	docker compose build

shell:
	docker compose run --rm app /bin/bash

run_etl:
	docker compose -f docker-compose.yaml run --rm app python -m dbcp.cli
