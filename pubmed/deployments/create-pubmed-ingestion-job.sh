#!/bin/bash

# Check if the correct number of arguments are provided
if [ "$#" -ne 7 ]; then
    echo "Usage: $0 <mode> <year> <higher-criteria> <lower-criteria> <image-tag> <memory> <cpu>"
    exit 1
fi

MODE=$1
YEAR=$2
HIGHERCRITERIA=$3
LOWERCRITERIA=$4
IMAGE_TAG=$5
MEMORY=$6
CPU=$7

JOB_NAME="pubmed-ingestion-${MODE}-${YEAR}-${HIGHERCRITERIA}-${LOWERCRITERIA}-${MEMORY}-${CPU}"

# Create a YAML file for the job
cat <<EOF > ${JOB_NAME}-job.yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: ${JOB_NAME}
spec:
  template:
    metadata:
      name: ${JOB_NAME}
    spec:
      containers:
      - name: pubmed-ingestion
        image: 698471419283.dkr.ecr.eu-central-1.amazonaws.com/data-digger-pubmed-ingestion:${IMAGE_TAG}
        resources:
          requests:
            memory: "${MEMORY}Gi"
            cpu: "${CPU}"
        command: ["sh", "-c", "poetry install && poetry run python app/main.py --year ${YEAR} --highercriteria ${HIGHERCRITERIA} --lowercriteria ${LOWERCRITERIA}"]
        env:
          - name: DEBUG
            value: "true"
          - name: PSQL__CONNECTION
            valueFrom:
              configMapKeyRef:
                name: pubmed-ingestion-datadigger-config
                key: PSQL__CONNECTION
                optional: false
          - name: QDRANT__API_KEY
            valueFrom:
              configMapKeyRef:
                name: pubmed-ingestion-datadigger-config
                key: QDRANT__API_KEY
                optional: false
          - name: EMBEDDING__API_KEY
            valueFrom:
              configMapKeyRef:
                name: pubmed-ingestion-datadigger-config
                key: EMBEDDING__API_KEY
                optional: false
          - name: SPLADEDOC__API_KEY
            valueFrom:
              configMapKeyRef:
                name: pubmed-ingestion-datadigger-config
                key: SPLADEDOC__API_KEY
                optional: false
      restartPolicy: Never
      nodeSelector:
        role: large-workloads
      tolerations:
      - key: "type"
        operator: "Equal"
        value: "large-workloads"
        effect: "NoSchedule"
EOF

# Apply the job YAML file
kubectl apply -f ${JOB_NAME}-job.yaml -n dev

# Clean up the YAML file
rm ${JOB_NAME}-job.yaml

echo "Job ${JOB_NAME} has been created."
