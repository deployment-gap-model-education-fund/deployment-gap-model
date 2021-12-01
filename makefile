build:
	docker compose build

run_etl:
	docker compose -f docker-compose.yaml run --rm app python -m dbcp.cli