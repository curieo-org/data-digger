# Pubmed Data Digger

## Pubmed Ingest

The ingestion of pubmed records consists of several parts:

* download / ETL of the title/abstract data: [data-digger-etl](data-digger-etl/README.md).
* computation of embeddings, ingest into Qdrant database: [pubmed_ingestion](pubmed_ingestion/README.md).
* ranking of pubmed records by citation count: [ranking](rankings.md).
* download of full-text data, based on a query that gives the records to be downloaded. 

## Deployment

#### Configuration
```bash
# Create environment file and populate with the required values
cp .env.template .env

# Set the namespace
NAMESPACE=<namespace>

# Delete the existing configmap if any
kubectl delete configmap pubmed-baseline-job-config -n $NAMESPACE

# Create a config
kubectl create configmap pubmed-baseline-job-config --from-env-file=.env -n $NAMESPACE
```

#### Baseline Job Deployment
```bash
# Set the namespace
NAMESPACE=<namespace>

# Delete existing deployments if any
kubectl delete jobs pubmed-baseline-job -n $NAMESPACE

# Image generation and deployment
make
kubectl apply -f deployments/baseline_job.yaml -n $NAMESPACE

# View the logs
kubectl logs jobs/pubmed-baseline-job -n $NAMESPACE -f
```

#### Updates CronJob Deployment
```bash
# Set the namespace
NAMESPACE=<namespace>

# Delete existing deployments if any
kubectl delete cronjobs pubmed-updates-cronjob -n $NAMESPACE

# Image generation and deployment
make
kubectl apply -f deployments/updates_cronjob.yaml -n $NAMESPACE

# View the logs
kubectl logs jobs/pubmed-updates-cronjob -n $NAMESPACE -f
```
