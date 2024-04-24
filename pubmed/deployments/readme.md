# Pubmed Data Digger Deployment

## Configuration
1. In the root directory, copy `.env.template` to `.env` and update the necessary variables. 

#### Baseline Job Deployment
From the root directory run the following commands:
```bash
# Set the namespace
NAMESPACE=<namespace>

# Delete existing deployments if any
kubectl delete jobs pubmed-baseline-job -n $NAMESPACE

# Image generation and deployment
./deployments/baseline_ecr.sh
kubectl apply -f deployments/baseline_job.yaml -n $NAMESPACE

# View the logs
kubectl logs jobs/pubmed-baseline-job -n $NAMESPACE -f
```

#### Updates CronJob Deployment
From the root directory run the following commands:
```bash
# Set the namespace
NAMESPACE=<namespace>

# Delete existing deployments if any
kubectl delete cronjobs pubmed-updates-cronjob -n $NAMESPACE

# Image generation and deployment
./deployments/updates_ecr.sh
kubectl apply -f deployments/updates_cronjob.yaml -n $NAMESPACE

# View the logs
kubectl logs jobs/pubmed-updates-cronjob -n $NAMESPACE -f
```