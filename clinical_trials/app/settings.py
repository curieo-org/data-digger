# Load environment variables from .env file
from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class ProjectSettings(BaseSettings):
    name: str = "Clinical Trials Ingestion"
    environment: str = "production"
    version: str = "0.0.1"
    debug: bool = True
    testing: bool = True
    prompt_language: str = "en-US"

class QdrantSettings(BaseSettings):
    api_port: int = 6333
    api_url: str = "http://qdrant.qdrant.svc.cluster.local"
    collection_name: str = "clinical_trials_vector_db"
    api_key: SecretStr
    https: bool = False

class EmbeddingSettings(BaseSettings):
    api_url: str = "http://text-embedding.dev.svc.cluster.local"
    api_key: SecretStr
    embed_batch_size: int = 4
    timeout: float = 60.0
    model_name: str = ""

class SpladedocSettings(BaseSettings):
    api_url: str = "http://text-splade-doc.dev.svc.cluster.local"
    api_key: SecretStr
    embed_batch_size: int = 2
    timeout: float = 60.0
    model_name: str = ""

class DatabaseVectorsEngineSettings(BaseSettings):
    chunk_size: int = 512
    chunk_overlap: int = 30
    max_workers: int = 20
    batch_size: int = 200

class CTDatabaseReaderSettings(BaseSettings):
    batch_size: int = 10000
    max_workers: int = 10
    pool_size: int = 10
    max_overflow: int = 0
    table_creation_queries_file: str = 'sql/table_creation.sql'
    
class PsqlSettings(BaseSettings):
    temporary_db: SecretStr
    main_db: SecretStr

class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        extra="allow",
        env_file=".env",
        env_file_encoding="utf-8",
        env_nested_delimiter="__",
    )

    qdrant: QdrantSettings
    embedding : EmbeddingSettings
    spladedoc: SpladedocSettings
    d2vengine: DatabaseVectorsEngineSettings = DatabaseVectorsEngineSettings()
    database_reader: CTDatabaseReaderSettings = CTDatabaseReaderSettings()
    psql: PsqlSettings