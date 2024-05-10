VERSION = $(shell cat VERSION)

.ONESHELL:

.PHONY: docker_build
docker_build:
	DOCKER_BUILDKIT=1 docker build \
	--rm \
	-t data-digger-pubmed-ingestion:latest \
	-t data-digger-pubmed-ingestion:$(VERSION) \
	-f Dockerfile .

.PHONY: docker_push
docker_push:
	docker tag data-digger-pubmed-ingestion:latest 698471419283.dkr.ecr.eu-central-1.amazonaws.com/data-digger-pubmed-ingestion:latest || exit 1
	docker tag data-digger-pubmed-ingestion:$(VERSION) 698471419283.dkr.ecr.eu-central-1.amazonaws.com/data-digger-pubmed-ingestion:$(VERSION) || exit 1
	docker push 698471419283.dkr.ecr.eu-central-1.amazonaws.com/data-digger-pubmed-ingestion:latest || exit 1
	docker push 698471419283.dkr.ecr.eu-central-1.amazonaws.com/data-digger-pubmed-ingestion:$(VERSION) || exit 1
