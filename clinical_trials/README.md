# Clinical Trial Data Pipeline

## Table of Contents
- [Clinical Trial Data Pipeline](#clinical-trial-data-pipeline)
  - [Table of Contents](#table-of-contents)
  - [Introduction](#introduction)
  - [Pipeline](#pipeline)
  - [Setup](#setup)
    - [Prerequisites](#prerequisites)
    - [Installation](#installation)
    - [Usage](#usage)
    - [Local Postgres Setup](#local-postgres-setup)
      - [Installation](#installation-1)
      - [Sync the dump to the temporary database](#sync-the-dump-to-the-temporary-database)
    - [Local Qdrant Setup](#local-qdrant-setup)
    - [Text Embedding Service](#text-embedding-service)
  - [Deployment](#deployment)
  - [Future Scope](#future-scope)

## Introduction
The clinical trial data pipeline is responsible for updating the clinical trial data in the existing postgres and qdrant vector database. The pipeline fetches the latest clinical trial data from the [AACT website](https://aact.ctti-clinicaltrials.org/snapshots), updates the existing data in the database, and finally updates the vector database with the latest data.

## Pipeline
The pipeline consists of the following steps:
1. Sync the new database dump to a temporary database. Follow [Sync DB Dump](./sync_db_dump/README.md) for more details.
2. Update the new data from the temporary database to the existing database.
3. Upsert the new data into the `Qdrant` vector database.

## Setup

### Prerequisites
- Python 3

### Installation
- Make sure `pyenv` is installed.
- Create a new virtual environment `python3 -m venv .venv`
- Activate the env `source .venv/bin/activate`
- Install poetry `pip install poetry`
- Install dependencies `poetry install`

### Usage
```bash
# Create a .env file and configure the environment variables
cp .env.template .env

# Run the pipeline
poetry run main
```

### Local Postgres Setup

#### Installation
```bash
# Install and run the postgresql server
# Docker image is available [here](https://hub.docker.com/_/postgres)

# Create a database
psql -U postgres -c "CREATE DATABASE clinical_trials"

# Run the table creation queries
psql -U postgres -d clinical_trials -f sql/table_creation.sql
```

#### Sync the dump to the temporary database
Follow the steps in the [Sync DB Dump](./sync_db_dump/README.md) for more details

### Local Qdrant Setup
```bash
# Run the Qdrant using docker
docker run -p 6333:6333 -p 6334:6334 \
    -v $(pwd)/qdrant_storage:/qdrant/storage:z \
    qdrant/qdrant

# Create a collection
curl --location --request PUT 'http://localhost:7333/collections/clinical_trials_vector_db' \
--data '{
    "vectors": {
        "text-dense": {
            "size": 1024,
            "on_disk": true,
            "distance": "Cosine"
        }
    },
    "sparse_vectors": {
        "text-sparse": {
            "index": {
                "on_disk": true,
                "full_scan_threshold": 5000
            }
        }
    },
    "optimizers_config": {
        "memmap_threshold": 20000
    },
    "quantization_config": {
        "scalar": {
            "type": "int8",
            "quantile": 0.99,
            "always_ram": true
        }
    }
}'
```

### Text Embedding Service
Setup the embedding service by following the instructions in the [text-embeddings-inference](https://github.com/huggingface/text-embeddings-inference/tree/main) repository.

If you want use in local then do the following steps:

```
git clone git@github.com:huggingface/text-embeddings-inference.git
cd text-embeddings-inference

curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# On x86
cargo install --path router -F candle -F mkl
# On M1 or M2
cargo install --path router -F candle -F metal

model=BAAI/bge-large-en-v1.5
revision=refs/pr/5
text-embeddings-router --model-id $model --revision $revision --port 8081
```

## Deployment
```bash
# Update the following line in the Makefile for the TAG
TAG = 

# Build and upload the image to the ECR
make

# Change the TAG in the deployment/update_fulltext_xml_job.yaml file in the root directory
image: 698471419283.dkr.ecr.eu-central-1.amazonaws.com/data-digger-clinical-trials-ingestion:<TAG>

# Deploy a Cronjob in the Kubernetes from the root directory
kubectl apply -f deployments/data_ingestion_job.yaml -n $NAMESPACE
```

## Future Scope
1. Research more about [database schema](https://aact.ctti-clinicaltrials.org/schema) and update the `sql/table_creation.sql` file with more tables.
2. Add more error handling and auto retry mechanism in the script.