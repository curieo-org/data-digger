# Pubmed Ingest

The ingest of pubmed records consists of several parts:

* download / ETL of the data: [data-digger-etl](data-digger-etl/README.md).
* computation of embeddings, ingest into QDrant database: [pubmed_ingention](pubmed_ingention/README.md).
* ranking of pubmed records by citation count: [ranking](rankings.md).

