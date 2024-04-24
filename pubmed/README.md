# Pubmed Ingest

The ingestion of pubmed records consists of several parts:

* download / ETL of the title/abstract data: [data-digger-etl](data-digger-etl/README.md).
* computation of embeddings, ingest into Qdrant database: [pubmed_ingestion](pubmed_ingestion/README.md).
* ranking of pubmed records by citation count: [ranking](rankings.md).
* download of full-text data, based on a query that gives the records to be downloaded. 
