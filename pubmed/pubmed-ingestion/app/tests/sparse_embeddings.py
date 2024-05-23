import sys, os
dir_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(f"{dir_path}/..")
from typing import List, Generator, Tuple, Dict
from dotenv import load_dotenv
import asyncio
import numpy as np
from collections import defaultdict
import uuid
from sqlalchemy import create_engine
from llama_index.embeddings.text_embeddings_inference import TextEmbeddingsInference

load_dotenv()

from utils.splade_embedding import SpladeEmbeddingsInference, Embedding
from utils.process_jats import JatsXMLParser
# from database_vectordb_transform.process_nodes import parse_clean_fulltext
from settings import SpladedocSettings, Settings
from utils.database_utils import run_insert_sql, run_select_sql, run_query
from utils.clustering import get_clusters_dict
import json

class EmbeddingsTester:
    def __init__(self, sparse_settings, dense_settings):
        self.splade_model = SpladeEmbeddingsInference(
            model_name="",
            base_url=sparse_settings.api_url,
            auth_token=sparse_settings.api_key.get_secret_value(),
            timeout=60,
            embed_batch_size=sparse_settings.embed_batch_size)
        
        self.embed_model = TextEmbeddingsInference(
            model_name="",
            base_url=dense_settings.api_url,
            auth_token=dense_settings.api_key.get_secret_value(),
            timeout=60,
            embed_batch_size=dense_settings.embed_batch_size)
    
    async def compute_sparse_embeddings(self, texts:List[str]) -> List[Embedding]:
        return self.splade_model.get_text_embedding_batch(texts) # async does not work?
    
    async def compute_dense_embeddings(self, texts:List[str]) -> List[Embedding]:
        return await self.embed_model.aget_text_embedding_batch(texts)
    


PARENT_SECTIONS = set(["Title", "Abstract"])
def parse_clean_fulltext(
        fulltext: str,
        name: str,
        split_depth: int):
    jatParser = JatsXMLParser(name=name, xml_data=fulltext)
    parsed_details = jatParser.parse_root_node()

    child_sections = [
        section for section in parsed_details.get('body_sections', [])
        if section.get('title') and section['title'] not in PARENT_SECTIONS
    ]
    parent_sections = [
        section for section in parsed_details.get('body_sections', [])
        if section.get('title') and section['title'] in PARENT_SECTIONS
    ]

    # Process paragraphs
    grouped_ids = defaultdict(list)
    for section in child_sections:
        id_value = section.get('id', '')
        prefix = '.'.join(id_value.split('.')[:split_depth]) if '.' in id_value else id_value
        paragraph_texts = [
            content.get('text') for content in section.get('contents', []) 
            if content.get('tag') == "p"
        ]
        grouped_ids[prefix].extend(paragraph_texts)

    return parent_sections, grouped_ids




"""

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


"""
DATA_TABLE_NAME = "pmc_records"
DENSE_VECTOR_TABLE = "pmc_dense_vectors"
SPARSE_VECTOR_TABLE = "pmc_sparse_vectors"
CLUSTER_TABLE = "pmc_clusters"
PRE_QUERIES = [
"""CREATE TABLE IF NOT EXISTS %s
            (
                id SERIAL PRIMARY KEY,
                pmc VARCHAR(20) NOT NULL,
                section VARCHAR(50) NOT NULL,
				node VARCHAR(20) NOT NULL,
                text TEXT NOT NULL,
                updated timestamp default now()
            );"""%DATA_TABLE_NAME,
"""CREATE TABLE IF NOT EXISTS %s
            (
                id INT NOT NULL,
                pmc VARCHAR(20) NOT NULL,
                section VARCHAR(50) NOT NULL,
				vector TEXT NOT NULL
            );"""%DENSE_VECTOR_TABLE,
"""CREATE TABLE IF NOT EXISTS %s
            (
                id INT NOT NULL,
                pmc VARCHAR(20) NOT NULL,
                section VARCHAR(50) NOT NULL,
				vector TEXT NOT NULL
            );"""%SPARSE_VECTOR_TABLE,
"""CREATE TABLE IF NOT EXISTS %s
            (
                id SERIAL PRIMARY KEY,
                pmc VARCHAR(20) NOT NULL,
                cluster_id VARCHAR(50) NOT NULL,
                ids TEXT NOT NULL,
				centroid TEXT NOT NULL
            );"""%CLUSTER_TABLE
]

""" 
INSERT A SAMPLE SET OF DOCUMENTS INTO THE PMC TABLE
"""
def insert_all(psql_engine):

    for filename in os.listdir(dir_path + '/full_text/'):
        with open(dir_path + '/full_text/' + filename, 'r') as file:
            full_text = file.read()
            parents, children = parse_clean_fulltext(fulltext=full_text, name=file, split_depth=2)

            for parent in parents:
                for content in parent['contents']:
                    section = parent['title']
                    if 'text' in content:
                        text = content['text']
                        node = 'parent'
                        pmc = filename
                        run_insert_sql(engine=psql_engine, table_name=DATA_TABLE_NAME, data_dict={
                            'text':text,
                            'node' : node,
                            'pmc' : pmc,
                            'section' : section
                        })
            for id, child in children.items():
                for content in child:
                    node = 'child'
                    pmc=filename
                    run_insert_sql(engine=psql_engine, table_name=DATA_TABLE_NAME, data_dict={
                        'text': content,
                        'node' : node,
                        'pmc' : pmc,
                        'section' : id
                    })


def get_documents(psql_engine) -> Generator[List[Dict], None, None]:
    document = []
    for item in run_query(psql_engine, "SELECT * FROM %s ORDER BY pmc"%DATA_TABLE_NAME):
        if len(document) == 0 or document[0]['pmc'] == item['pmc']:
            document.append(item)
        else:
            yield document
            document = [item]
    if len(document) != 0:
        yield document


async def vectorize_all(psql_engine, et : EmbeddingsTester):
    for document in get_documents(psql_engine=psql_engine):
        texts = [node['text'] for node in document]
        ids = [node['id'] for node in document]
        text_map = {}
        for node in document:
            text_map[node['id']] = node['text']
        # get embeddings
        sparse_embeddings = await et.compute_sparse_embeddings(texts)
        dense_embeddings = await et.compute_dense_embeddings(texts)
        
        # store the embeddings
        for i in range(len(ids)):
            node = document[i]
            run_insert_sql(engine=psql_engine, table_name=DENSE_VECTOR_TABLE, data_dict={
                'id':   node['id'],
                'vector' : json.dumps(dense_embeddings[i]),
                'pmc' : node['pmc'],
                'section' : node['section']
            })
            
            run_insert_sql(engine=psql_engine, table_name=SPARSE_VECTOR_TABLE, data_dict={
                'id':   node['id'],
                'vector' : json.dumps(sparse_embeddings[i]),
                'pmc' : node['pmc'],
                'section' : node['section']
            })

        # compute the clusters
        embedding_map = {}
        for i in range(len(dense_embeddings)):
            embedding_map[ids[i]] = (dense_embeddings[i], sparse_embeddings[i])
        
        child_ids = [node['id'] for node in document if node['node'] == 'child']
        child_dense_embeddings = [embedding_map[id][0] for id in child_ids]

        # cluster: lists of identifiers
        cluster_map = {}
        cluster_centroid = {}
        embeddings = np.array([np.array(emb) for emb in child_dense_embeddings])
        if len(embeddings) != 0:
            for cluster in get_clusters_dict(embeddings=embeddings, ids=child_ids, texts = text_map):
                cluster_id = str(uuid.uuid4())
                cluster_map[cluster_id] = cluster
                # now compute the centroid for each cluster
                cluster_centroid[cluster_id] = np.mean(np.array([embedding_map[id][0] for id in cluster]))
            
                run_insert_sql(engine=psql_engine, table_name=CLUSTER_TABLE, data_dict={
                    'pmc':   node['id'],
                    'centroid' : json.dumps(cluster_centroid[cluster_id]),
                    'cluster_id' : cluster_id,
                    'ids' : json.dumps(cluster)
                })
        print("Done document %s"%(document[0]['pmc']))


if __name__ == "__main__":
    settings = Settings()
    connection_string = settings.psql.connection.get_secret_value();
    psql_engine = create_engine(connection_string)

    # create tables
    for query in PRE_QUERIES:
        run_select_sql(psql_engine, query)

    # insert_all(psql_engine)
    tester = EmbeddingsTester(settings.spladedoc, settings.embedding)
    # asyncio.run(single_test(tester))
    asyncio.run(vectorize_all(psql_engine, tester))
    print('done')


