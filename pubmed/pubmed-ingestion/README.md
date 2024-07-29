# Pubmed Data Ingestion to Qdrant

## Introduction

The Framework is a powerful tool designed for extracting information from a Pubmed database, processing it, and storing the processed data in a vector format using Qdrant. It utilizes advanced NLP techniques to parse, index, and store large volumes of text data efficiently. This system is built to facilitate easy search and retrieval of complex document-based datasets for research and analysis.

## Processing Pubmed 

We index both Pubmed Titles/Abstract data, as well as Pubmed Central Full Text.
This scaling challenge has been dealt with by clustering child nodes (sections) of full text documents. This has been documented in detail here [Pubmed Ingest Process](./app/tests/testing-vectorization.md).


## Installation

Before you begin, ensure you have [Poetry](https://python-poetry.org/docs/) installed on your system. This project uses Poetry for dependency management and packaging.
And please make sure Python 3.8+ installed on your system. You'll also need Docker if you plan to use Qdrant as a container.

1. **Clone the repository:**

   ```bash
   git clone <repository-url>
   cd <repository-directory>
    ```

2. **Install the project dependencies using Poetry:**

   ```bash
   poetry install
    ```

This command will create a virtual environment and install all the necessary dependencies within it.

3. **Check the Database:**

  Get the connection string and put into the `.env` file

4. **Set up Qdrant (if not already running):**

   Using Docker, you can run Qdrant with the following command:
   
   ```bash
   docker run -p 6333:6333 qdrant/qdrant
   ```

5. **Set up Text Embedding Server (if not already running):**

   First install Rust.
   
   ```bash
   git clone git@github.com:huggingface/text-embeddings-inference.git
   cd text-embeddings-inference

   curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

   # On x86
   cargo install --path router -F candle -F mkl
   # On M1 or M2
   cargo install --path router -F candle -F metal

   model=BAAI/bge-large-en-v1.5
   revision=refs/pr/5
   text-embeddings-router --model-id $model --revision $revision --port 8080
   ```

6. **Install Poetry:**

   ```bash
   poetry install
   ```

## Usage

   ```bash
   poetry run python app/main.py -y 2024 -pc 40 -cc 10

   # Usage of profiler
   # Option 1
   poetry run python -m cProfile app/main.py -y 2024 -pc 40 -cc 10

   # Option 1
   pip install flameprof
   poetry run python -m cProfile -o main.prof app/main.py -y 2024 -pc 40 -cc 10
   flameprof  main.prof > main.svg
   ```
