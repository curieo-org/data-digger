# pull official base image
FROM python:3.11.8-slim

# install system dependencies
RUN apt-get update && apt-get -y install libpq-dev gcc git
RUN export USE_CUDA=0
ENV PIP_DEFAULT_TIMEOUT=1200

# install python dependencies
COPY ./pyproject.toml /
COPY ./poetry.lock /
RUN pip install poetry
RUN poetry install

COPY . .

# CMD ["poetry", "run", "python", "app/main.py", "-y", "2020", "--pc", "40", "--cc", "80"]