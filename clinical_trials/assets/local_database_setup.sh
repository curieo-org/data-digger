#!/bin/bash
set -eo pipefail

# Create the database docker image
docker build -t aact_db . -f assets/Dockerfile_DB
docker run -p 5430:5432 --name aact_db -d aact_db
ECHO "Postgres database is running on port 5430"

# Populate the database with the downloaded data
docker cp assets/postgres.dmp aact_db:/usr/local/bin/postgres.dmp
docker exec -it aact_db bash -c 'pg_restore --no-owner --no-privileges -d aact -U postgres /usr/local/bin/postgres.dmp'
ECHO "Database has been populated with the downloaded data"

# Create the additional tables and views
docker cp assets/local_setup.sql aact_db:/usr/local/bin/queries.sql
docker exec -it aact_db bash -c 'psql -U postgres -d aact -f /usr/local/bin/queries.sql'
ECHO "Additional tables and views have been created"