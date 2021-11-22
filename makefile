SHELL := /bin/bash
create_dotenv:
	#TODO: very brittle
	if [[ ! -e ./.env ]]; then tail -n +7 config.env > ./.env; fi

start_database: create_dotenv
	docker compose up -d

stop_database:
	docker compose down

delete_db_data: stop_database
	docker volume rm down_ballot_climate_postgres-data

delete_pgadmin_data: stop_database
	docker volume rm down_ballot_climate_pgadmin-data

delete_all_data: delete_db_data delete_pgadmin
	# use docker volume prune to remove build cache.
	# Careful because it removes ALL unused volumes, even from other projects