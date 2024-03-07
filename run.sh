#!/bin/bash
set -eo pipefail

# Download and Unzip the data
docker build -t aact_scraper . -f Dockerfile_Scraper
docker run --name aact_scraper -d aact_scraper
docker cp aact_scraper:/usr/src/app/postgres.dmp postgres.dmp

# Create the database docker image
docker build -t aact_db . -f Dockerfile_DB
docker run -p 5430:5432 --name aact_db -d aact_db

# Populate the database with the downloaded data
docker cp postgres.dmp aact_db:/usr/local/bin/postgres.dmp
docker exec -it aact_db bash -c 'pg_restore --no-owner --no-privileges -d aact -U postgres /usr/local/bin/postgres.dmp'

# Create the additional tables and views
docker cp queries.sql aact_db:/usr/local/bin/queries.sql
docker exec -it aact_db bash -c 'psql -U postgres -d aact -f /usr/local/bin/queries.sql'

# Create the roles
docker cp roles.sql aact_db:/usr/local/bin/roles.sql
docker exec -it aact_db bash -c 'psql -U postgres -d aact -f /usr/local/bin/roles.sql'

# Setup Docker Credentials
DOCKER_USER=<DOCKER_HUB_EMAIL>
DOCKER_PASSWORD=<DOCKER_HUB_PASSWORD>
TAG_NAME=$(date +"%Y%m%d")
USER_NAME=<DOCKER_HUB_USERNAME>
IMAGE_NAME=<DOCKER_HUB_IMAGE_NAME>

# Setup Image Config
pushing_image_name=''
pushing_image_name+=$USER_NAME && pushing_image_name+='/'
pushing_image_name+=$IMAGE_NAME && pushing_image_name+=':'
pushing_image_name+=$TAG_NAME
docker tag aact_db $pushing_image_name

# Push the database image to Docker Hub
echo $DOCKER_PASSWORD | docker login -u $DOCKER_USER --password-stdin
docker push $pushing_image_name