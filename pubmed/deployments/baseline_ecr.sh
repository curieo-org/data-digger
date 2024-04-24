aws ecr get-login-password --region eu-central-1 | docker login --username AWS --password-stdin 698471419283.dkr.ecr.eu-central-1.amazonaws.com

docker build -f deployments/Dockerfile_Baseline -t data-digger-pubmed-baseline .

docker tag data-digger-pubmed-baseline:latest 698471419283.dkr.ecr.eu-central-1.amazonaws.com/data-digger-pubmed-baseline:latest

docker push 698471419283.dkr.ecr.eu-central-1.amazonaws.com/data-digger-pubmed-baseline:latest
