Image = data-digger-clinical-trials-db-dump
ECR = 698471419283.dkr.ecr.eu-central-1.amazonaws.com/data-digger-clinical-trials-db-dump
TAG = 3.0

ecr_deploy: login
	docker buildx build --platform linux/amd64 -t $(Image) .
	docker tag $(Image) $(ECR):$(TAG)
	docker push $(ECR):$(TAG)

login:
	aws ecr get-login-password --region eu-central-1 | docker login --username AWS --password-stdin 698471419283.dkr.ecr.eu-central-1.amazonaws.com