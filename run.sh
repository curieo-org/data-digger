#!/bin/bash

# Download and Unzip the data
python scraper.py

# Create the database docker image
docker build -t aact_db .
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
ECHO $pushing_image_name
docker tag aact_db $pushing_image_name

# Push the database image to Docker Hub
docker login -u $DOCKER_USER -p $DOCKER_PASS
docker push $pushing_image_name