# pull official base image
FROM python:3.11.8-slim

# install system dependencies
RUN apt-get update && apt-get -y install libpq-dev gcc git
RUN export USE_CUDA=0

# install python dependencies
COPY ./pyproject.toml /
RUN pip install poetry
RUN poetry config virtualenvs.create false
RUN poetry install

COPY . .

# CMD ["poetry", "run", "python", "app/main.py", "-y", "2020", "--pc", "40", "--cc", "80"]