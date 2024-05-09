# pull official base image
FROM python:3.11.8-slim

# install system dependencies
RUN apt-get update && apt-get -y install libpq-dev gcc git

# install python dependencies
COPY ./pyproject.toml /
COPY ./poetry.lock /
RUN pip install poetry
RUN poetry config virtualenvs.create false
RUN poetry install --compile

COPY ./app /app

# CMD ["poetry", "run", "python", "app/main.py", "-y", "2024"]