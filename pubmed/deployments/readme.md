# Pubmed Data Digger Deployment

## Configuration
1. Copy `.env.template` to `.env` and update the necessary variables. 

#### Baseline Job Deployment
From the root directory run the following commands:
```bash
./deployments/baseline_ecr.sh
kubectl apply -f deployments/baseline_job.yaml -n <namespace>
```

Make sure that the existing deployments is deleted before running the above command.