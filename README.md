# AACT Data Digger

This directory contains the code and documentation for generating the AACT database into our own database which is deployed using Docker.

## Setup
Requirements:
- Python 3.12 or higher
- Docker
- Bash

## Usage
1. Setup a virtual environment using `python -m venv venv`. (Optional)
2. Install the required python packages using `pip install -r requirements.txt`.
3. Make the `run.sh` file executable using `chmod +x run.sh`.
4. Run the `run.sh` file using `./run.sh`.
5. After the process is complete, the database will be deployed using Docker. You can access the database using the following credentials:
    - Username: `postgress`
    - Password: `P@ssword1`
    - Database: `aact`
    - Host: `localhost` (if you are running the Docker container locally)
    - Port: `5430`

## Structure
- `run.sh`: The main script to run the entire process.
- `requirements.txt`: The required python packages.
- `scraper.py`: The script to scrape the AACT database and store it in a `.dmp` file.
- `Dockefile`: The Dockerfile to build the Docker image of `Postgres` with the AACT database.
- `roles.sql`: The SQL script to create the required roles for the database.
- `queries.sql`: The SQL script to create the required additional tables and views for the database.
- `README.md`: The documentation for the code.