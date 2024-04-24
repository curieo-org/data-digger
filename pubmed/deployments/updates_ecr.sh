aws ecr get-login-password --region eu-central-1 | docker login --username AWS --password-stdin 698471419283.dkr.ecr.eu-central-1.amazonaws.com

docker build -f deployments/Dockerfile_Updates -t data-digger-pubmed-updates .

docker tag data-digger-pubmed-updates:latest 698471419283.dkr.ecr.eu-central-1.amazonaws.com/data-digger-pubmed-updates:latest

docker push 698471419283.dkr.ecr.eu-central-1.amazonaws.com/data-digger-pubmed-updates:latest
