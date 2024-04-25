Image = data-digger-pubmed
ECR = 698471419283.dkr.ecr.eu-central-1.amazonaws.com/data-digger-pubmed
TAG = latest

ecr_deploy: login
	docker build -t $(Image) .
	docker tag $(Image):$(TAG) $(ECR):$(TAG)
	docker push $(ECR):$(TAG)

login:
	aws ecr get-login-password --region eu-central-1 | docker login --username AWS --password-stdin 698471419283.dkr.ecr.eu-central-1.amazonaws.com