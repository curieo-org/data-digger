#!/bin/bash
set -eo pipefail

# Stop and remove the database docker container
docker stop aact_db
docker rm aact_db
ECHO "Postgres database container has been stopped and removed"

# Remove the database docker image
docker rmi aact_db
ECHO "Postgres database image has been removed"

# Clean up the all unused docker resources
docker system prune -f

# Drop the dump file
rm -f assets/postgres.dmp
ECHO "Dump file has been removed"