@@ -0,0 +1,63 @@
# Sync Clinical Trials Database Dump

## Table of Contents
- [Sync Clinical Trials Database Dump](#sync-clinical-trials-database-dump)
  - [Table of Contents](#table-of-contents)
  - [Introduction](#introduction)
  - [Steps to sync the clinical trials database dump](#steps-to-sync-the-clinical-trials-database-dump)
  - [Usage](#usage)
  - [Deployment](#deployment)
  - [Future Scope](#future-scope)

## Introduction
In the [AACT website](https://aact.ctti-clinicaltrials.org/snapshots), there are snapshots of the clinical trials database. These snapshots are updated daily. The snapshots are in the form of a zip file. The zip file contains database schema documentation and a `postgres.dmp` file containing the database dump.

As `postgres.dmp` file size is large, it is not feasible to sync this with the help of code. So, we are syncing it with the help of shell script and postgres `pg_restore` command.

## Steps to sync the clinical trials database dump
1. Fetch the latest snapshot url from the AACT website and download the zip file.
2. Unzip the latest snapshot and store the `postgres.dmp` file in the respective directory.
3. Restore the `postgres.dmp` file in the a temporary database.
4. Aggregate the tables of the temporary database using `queries.sql` file and store the aggregated data in the `public` schema.
5. Clean all the downloaded files.

## Usage
```bash
brew install postgresql

# Run the script
./sync_db_dump.sh
```

## Deployment
```bash
# Update the following line in the Makefile for the TAG
TAG = 

# Build and upload the image to the ECR
make

# Change the TAG in the deployment/update_fulltext_xml_job.yaml file in the root directory
image: 698471419283.dkr.ecr.eu-central-1.amazonaws.com/data-digger-clinical-trials-db-dump:<TAG>

# Deploy a Cronjob in the Kubernetes from the root directory
kubectl apply -f deployments/sync_db_dump_job.yaml -n $NAMESPACE
```

## Future Scope
1. Research more about [database schema](https://aact.ctti-clinicaltrials.org/schema) and update the `queries.sql` file with more tables.
2. Add loggings, error handling, and auto retry mechanism in the script.