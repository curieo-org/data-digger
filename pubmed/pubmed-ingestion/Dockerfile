# pull official base image
FROM python:3.11.8-slim

# install system dependencies
RUN apt-get update && apt-get -y install libpq-dev gcc git
RUN export USE_CUDA=0
ENV PIP_DEFAULT_TIMEOUT=1200

COPY . .