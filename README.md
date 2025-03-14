# Data Digger

## Overview

### AACT Folder
This directory contains the code and documentation for generating the AACT database into our own database which is deployed using Docker.

### ChEMBL Folder
This directory contains the code and documentation for converting the ChEMBL database into the nebula graph database using Docker and Python.

### Pubmed Folder
This directory contains code and documentation for ingesting all of [pubmed](https://pmc.ncbi.nlm.nih.gov/) and some of [Pubmed Central (PMC)](https://pmc.ncbi.nlm.nih.gov/) into a local, searchable database. [Detailed documentation in this folder](pubmed/README.md).

## Setup
Requirements:
- Docker
- Bash

## AACT Usage
1. Go to the `aact` directory using `cd aact`.
2. Make the `run.sh` file executable using `chmod +x run.sh`.
3. Provide Docker Hub credentials and configuration in end the `run.sh` file.
4. Run the `run.sh` file using `./run.sh`.
5. After the process is complete, the database will be deployed using Docker. You can access the database using the following credentials:
    - Username: `postgress`
    - Password: `P@ssword1`
    - Database: `aact`
    - Host: `localhost` (if you are running the Docker container locally)
    - Port: `5430`


## ChEMBL Usage
1. Go to the `chembl` directory using `cd chembl`.
2. Make the `run.sh` file executable using `chmod +x run.sh`.
3. Run the `run.sh` file using `./run.sh`.
4. After the process is complete, the nebula graph database will be deployed using Docker. You can access the database using the following credentials
    - Username: `root`
    - Password: `nebula`
    - Host: `localhost` (if you are running the Docker container locally)
    - Port: `9669`

## AACT Folder Structure
- `run.sh`: The main script to run the entire process.
- `requirements.txt`: The required python packages.
- `scraper.py`: The script to scrape the AACT database and store it in a `.dmp` file.
- `Dockefile_DB`: The Dockerfile to build the Docker image of `Postgres` with the AACT database.
- `Dockerfile_Scraper`: The Dockerfile to build the Docker image of the scraper.
- `roles.sql`: The SQL script to create the required roles for the database.
- `queries.sql`: The SQL script to create the required additional tables and views for the database.


## ChEMBL Folder Structure
- `run.sh`: The main script to run the entire process.
- `requirements.txt`: The required python packages.
- `scraper.py`: The script to scrape the ChEMBL database and store it in a `.dmp` and `.h5` file.
- `Dockefile_DB`: The Dockerfile to build the Docker image of `Postgres` with the ChEMBL database.
- `Dockerfile_Scraper`: The Dockerfile to build the Docker image of the scraper.
- `Dockerfile_Backend`: The Dockerfile to build the Docker image of the database converter.
- `./src`: The directory containing the source code for the database converter.


## Root Folder Structure
- `README.md`: The documentation for the code.
- `.gitignore`: The gitignore file to ignore the unnecessary files and credentials.