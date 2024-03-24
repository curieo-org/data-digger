#!/bin/bash
set -eo pipefail

# Download and Unzip the data
docker build -t chembl_scraper . -f Dockerfile_Scraper
docker run --name chembl_scraper -d chembl_scraper
docker cp chembl_scraper:/usr/src/app/chembl_postgres.dmp chembl_postgres.dmp
docker cp chembl_scraper:/usr/src/app/chembl.h5 chembl.h5

# Create the docker network
docker network create chembl_network

# Create the database docker image
docker build -t chembl_db . -f Dockerfile_DB
docker run -p 5431:5432 --network chembl_network --network-alias chembl_db --name chembl_db -d chembl_db

# Populate the database with the downloaded data
docker cp chembl_postgres.dmp chembl_db:/usr/local/bin/chembl_postgres.dmp
docker exec -it chembl_db bash -c 'pg_restore --no-owner --no-privileges -d chembl -U postgres /usr/local/bin/chembl_postgres.dmp'


# Create Nebula Graph Database
git clone -b release-3.6 https://github.com/vesoft-inc/nebula-docker-compose.git
cd nebula-docker-compose && docker-compose up -d && cd ..


# Create the backend docker image
docker build -t chembl_backend . -f Dockerfile_Backend
docker create --network chembl_network --network-alias chembl_backend -p 8080:8080 --name chembl_backend chembl_backend
docker network connect nebula-docker-compose_nebula-net chembl_backend
docker start chembl_backend


# Pause for 5 seconds to allow the backend to start 
# converting the PostgreSQL database to Nebula Graph database
sleep 60
CURL http://localhost:8080/start-transfer-database