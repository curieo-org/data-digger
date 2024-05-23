# Pubmed Ranking

## Table of Contents
- [Pubmed Ranking](#pubmed-ranking)
  - [Table of Contents](#table-of-contents)
  - [Introduction](#introduction)
  - [Steps to update the Pubmed Ranking](#steps-to-update-the-pubmed-ranking)
  - [Usage](#usage)
  - [Deployment](#deployment)
  - [Future Scope](#future-scope)
   
## Introduction
This project is designed to rank the Pubmed records based on citation count and it's percentile in every year.

## Steps to update the Pubmed Ranking
1. Update `citationcounts` and `citationcountswithoutyear` tables in the database.
2. Calculate the percentage of the citation count for each year.
3. Update the percentage in `pubmed_percentiles` table in the database.

## Usage
```bash
cargo run --release

# Use local database for development
```

## Deployment
```bash
# Update the following line in the Makefile for the TAG
TAG = 

# Build and upload the image to the ECR
make

# Change the TAG in the deployment/ranking_job.yaml file in the root directory
image: 698471419283.dkr.ecr.eu-central-1.amazonaws.com/data-digger-pubmed-ranking:<TAG>

# Deploy a Cronjob in the Kubernetes from the root directory
kubectl apply -f deployments/ranking_job.yaml -n $NAMESPACE
```

## Future Scope
1. Add the ranking updates inside the `data-digger-etl` project.
2. Update ranking for each records when updated in the database.