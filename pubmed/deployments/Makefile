Dockerfile_Baseline = deployments/Dockerfile_Baseline
Image_Baseline = data-digger-pubmed-baseline
ECR_Baseline = 698471419283.dkr.ecr.eu-central-1.amazonaws.com/data-digger-pubmed-baseline
TAG_Baseline = latest

Dockerfile_Updates = deployments/Dockerfile_Updates
Image_Updates = data-digger-pubmed-updates
ECR_Updates = 698471419283.dkr.ecr.eu-central-1.amazonaws.com/data-digger-pubmed-updates
TAG_Updates = latest

baseline:
	login
	$(call build, $(Dockerfile_Baseline), $(Image_Baseline))
	$(call tag, $(Image_Baseline), $(ECR_Baseline), $(TAG_Baseline))
	$(call push, $(ECR_Baseline), $(TAG_Baseline))

updates: 
	login
	$(call build, $(Dockerfile_Updates), $(Image_Updates))
	$(call tag, $(Image_Updates), $(ECR_Updates), $(TAG_Updates))
	$(call push, $(ECR_Updates), $(TAG_Updates))

login:
	aws ecr get-login-password --region eu-central-1 | docker login --username AWS --password-stdin 698471419283.dkr.ecr.eu-central-1.amazonaws.com

define build
	docker build -f $(1) -t $(2) .
endef

define tag
	docker tag $(1):$(3) $(2):$(3)
endef

define push
	docker push $(1):$(2)
endef