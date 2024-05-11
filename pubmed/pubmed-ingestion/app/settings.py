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


class JatsParserSettings(BaseSettings):
    bucket_name: str = "pubmed-fulltext"
    split_depth: int = 2


class DatabaseVectorsEngineSettings(BaseSettings):
    parent_chunk_size: int = 1024
    parent_chunk_overlap: int = 50
    child_chunk_size: int = 512
    child_chunk_overlap: int = 30
    tree_depth: int = 2
    s3_upload_obj: str = "vector-db-ingestion-details"


class QdrantSettings(BaseSettings):
    api_port: int = 6333
    api_url: str = "http://qdrant.qdrant.svc.cluster.local"
    collection_name: str = "pubmed_hybrid"
    api_key: SecretStr


class EmbeddingSettings(BaseSettings):
    api_url: str = "http://text-embedding.dev.svc.cluster.local"
    api_key: SecretStr
    embed_batch_size: int = 4


class SpladedocSettings(BaseSettings):
    api_url: str = "http://text-splade-doc.dev.svc.cluster.local"
    api_key: SecretStr
    embed_batch_size: int = 2


class PubmedDatabaseReaderSettings(BaseSettings):
    offset: int = 0
    batch_size: int = 100
    pubmed_percentiles_tbl: str = "pubmed_percentiles"
    percentile_select_query: str = "SELECT citationcount from pubmed_percentiles where year = {year} and percentile = {percentile}"
    record_select_query: str = "SELECT identifier FROM public.citationcounts where year = {year} and citationcount >= {citationcount}"
    records_fetch_details: str = "SELECT identifier, record FROM public.records where identifier in ({ids})"
    fulltext_fetch_query: str = "SELECT pubmed, {column} FROM public.{table} where pubmed in ({ids})"
    parsed_record_abstract_key: str = "abstractText"
    parsed_record_titles_key: str = "titles"
    parsed_record_publicationdate_key: str = "publicationDate"
    parsed_record_year_key: str = "year"
    parsed_record_authors_key: str = "authors"
    parsed_record_references_key: str = "references"
    parsed_record_identifiers_key: str = "identifiers"


class PsqlSettings(BaseSettings):
    connection: SecretStr


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        extra="allow",
        env_file=".env",
        env_file_encoding="utf-8",
        env_nested_delimiter="__",
    )

    qdrant: QdrantSettings
    jatsparser: JatsParserSettings = JatsParserSettings()
    embedding : EmbeddingSettings
    spladedoc: SpladedocSettings
    database_reader: PubmedDatabaseReaderSettings = PubmedDatabaseReaderSettings()
    d2vengine: DatabaseVectorsEngineSettings = DatabaseVectorsEngineSettings()
    psql: PsqlSettings