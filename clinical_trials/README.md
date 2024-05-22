# Clinical Trial Data Pipeline

## Overview
This directory contains the code and documentation for updating the new clinical trial data to the existing postgres and qdrant vector database.

## Setup

### Prerequisites
- Python 3

### Installation
- Make sure `pyenv` is installed.
- Create a new virtual environment `python -m venv .venv`
- Activate the env `source .venv/bin/activate`
- Install poetry `pip install poetry`
- Install dependencies `poetry install`

### Configuration
Setup the embedding service by following the instructions in the [text-embeddings-inference](https://github.com/huggingface/text-embeddings-inference/tree/main) repository.

If you want use in local then do the following steps:

```
git clone git@github.com:huggingface/text-embeddings-inference.git
cd text-embeddings-inference

curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# On x86
cargo install --path router -F candle -F mkl
# On M1 or M2
cargo install --path router -F candle -F metal

model=BAAI/bge-large-en-v1.5
revision=refs/pr/5
text-embeddings-router --model-id $model --revision $revision --port 8081
```

### Production
Create a `.env` file in the root directory of the project and copy the contents of the `.env.template` file into it. Update the values of the variables in the `.env` file as needed.
```
cp .env.template .env
```

### Local Development
Setup Qdrant vector database for local development by running the following command:
```
docker run -p 6333:6333 --name aact_vectordb -d qdrant/qdrant
```

### Running the pipeline
Run the following command to update the clinical trial data in the database:

```
poetry run main
```
