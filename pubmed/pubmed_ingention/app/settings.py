# Load environment variables from .env file
from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class ProjectSettings(BaseSettings):
    name: str = "Pubmed Ingestion"
    environment: str = "production"
    version: str = "0.0.1"
    default_base_url: str = "http://127.0.0.1"
    port: int = 50051
    graceful_shutdown_period: int = 5
    max_grpc_workers: int = 10
    debug: bool = True
    testing: bool = True
    show_request_process_time_header: bool = True
    prompt_language: str = "en-US"


class JetsParserSettings(BaseSettings):
    bucket_name: str = "pubmed-fulltext"
    section_split_depth: int = 2


class DatabaseVectorsEngineSettings(BaseSettings):
    parent_chunk_size: int = 1024
    parent_chunk_overlap: int = 50
    child_chunk_size: int = 512
    child_chunk_overlap: int = 30
    tree_depth: int = 2


class QdrantSettings(BaseSettings):
    api_port: int = 6333
    api_url: str = "localhost"
    collection_name: str = "pubmed_hybrid_vector_dbV2"
    api_key: SecretStr


class EmbeddingSettings(BaseSettings):
    api_url: str = "http://text-embedding.dev.curieo.org"
    api_key: SecretStr
    embed_batch_size: int = 8


class SpladedocSettings(BaseSettings):
    api_url: str = "http://text-splade-doc.dev.curieo.org"
    api_key: SecretStr
    embed_batch_size: int = 8


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        extra="allow",
        env_file=".env",
        env_file_encoding="utf-8",
        env_nested_delimiter="__",
    )

    qdrant: QdrantSettings
    jetsparser: JetsParserSettings = JetsParserSettings()
    embedding : EmbeddingSettings
    spladedoc: SpladedocSettings
    d2vengine: DatabaseVectorsEngineSettings = DatabaseVectorsEngineSettings()