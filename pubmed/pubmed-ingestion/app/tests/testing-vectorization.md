# Testing vectorization

We want to offer functionality to search in full text articles. Article sizes can be anywhere from 8 to 20 or more pages, so this represents a challenge in storage and retrieval.

### Ingest of Pubmed documents into the search index
The concept is that each document within a certain profile (Year, Citation Ranking) is retrieved from the database.
On additional conditions (Year, Ranking, Availability) the full-text of documents is retrieved from the `S3` source. 

The documents are analyzed into sections, such that we get:
* parent node (title/abstract)
* child nodes (section 1.1, section 1.2, ... section n.n)

Each of these sections are submitted to two different vectorization algorithms:
* dense vectorization [BAAI/bge-large-en-v1.5](https://huggingface.co/BAAI/bge-large-en-v1.5)
* sparse vectorization (Splade)

Based on the _dense vectors_, the child sections are clustered (in utils/clustering.py, using [umap](https://umap-learn.readthedocs.io/en/latest/clustering.html)).
For each of these clusters, a _centroid_ is computed using `numpy`:

```python
# averaging multiple vectors into a single vector
cluster_dense_centroid = np.mean(np.array(embeddings), axis=0)
```

The sparse embeddings are also averaged by a bespoke algorithm `average_sparse` (there may be a library method for this, but I did not find it).



STEP 1
I'm going to pick 200 pubmed docs from 2024 with corresponding full text (that makes 155)

```sql
    SELECT pubmed, pmc, CONCAT('s3://pubmed-fulltext/bulk/PMC', 
                SUBSTRING(LPAD(SUBSTRING(pmc, 4, 10), 9, '0'), 1, 3), 
                'xxxxxx/', pmc, '.xml') AS FullTextLocation  FROM datadigger.linktable lt 
    JOIN datadigger.records r on lt.pubmed = r.identifier
    WHERE Year>2022 AND pmc LIKE 'PMC%'
    ORDER BY YEAR DESC
    LIMIT 200
```
And upload them into the database (#4841 rows)

STEP 2
I'm going to process these in dense and sparse embeddings

STEP 3
I'm going to cluster the child nodes
I'm going to put these in postgres databases

STEP 4
I'm going to pick random sentences from each document, and try to find them back.
