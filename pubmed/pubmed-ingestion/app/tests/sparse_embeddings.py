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
CLUSTER_MATCH_TABLE = "pmc_cluster_matches"
DETAIL_MATCH_TABLE = "pmc_detail_matches"
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
				sparse_centroid TEXT NOT NULL,
				dense_centroid TEXT NOT NULL
            );"""%CLUSTER_TABLE,
"""CREATE TABLE IF NOT EXISTS %s
            (
                id SERIAL PRIMARY KEY,
                query_id INT NOT NULL,
                dense_matches TEXT NOT NULL,
                sparse_matches TEXT NOT NULL,
                overlap5 INT NOT NULL,
                overlap10 INT NOT NULL
            );"""%CLUSTER_MATCH_TABLE,
"""CREATE TABLE IF NOT EXISTS %s
            (   query_id INT NOT NULL,
                match_id INT NOT NULL,
                dense_score FLOAT NOT NULL,
                sparse_score FLOAT NOT NULL,
                in_dense SMALLINT NOT NULL,
                in_sparse SMALLINT NOT NULL
            );"""%DETAIL_MATCH_TABLE
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

def sparse_to_map(v1):
    map = {}
    for item in v1:
        map[item['index']] = item['value']
    return map

def sparse_dot_product(v1, v2):
    product = 0.0
    for k,v in v1.items():
        if k in v2:
            product = product + v*v2[k]
    v1_len = sum(v*v for v in v1.values())**(1/float(len(v1)))
    v2_len = sum(v*v for v in v1.values())**(1/float(len(v1)))
    return product/(v2_len*v1_len)


def average_sparse(vectors:List)->List:
    summated = {}
    vector_length_sum = 0.0
    dimension_count_sum = 0
    for vector in vectors:
        vector_length_squared = 0.0
        for item in vector:
            summated[item['index']] = summated.get(item['index'], 0.0) + item['value']
            vector_length_squared += item['value']*item['value']
        dimension_count = len(vector)
        vector_length_sum = vector_length_sum + vector_length_squared**(1/float(dimension_count))
        dimension_count_sum = dimension_count_sum + dimension_count
    avg_vector_length = vector_length_sum/float(len(vectors))
    avg_dimension_count = int(dimension_count_sum/float(len(vectors)))
    sum_list = [(k,v) for k,v in summated.items()]
    sum_list.sort(key=lambda x : -x[1])
    sum_list = sum_list[:avg_dimension_count] # chop off the excess
    vector_length = sum(v[1]*v[1] for v in sum_list)**(1/float(len(sum_list))) # current vector length

    # print(f"#vectors: {len(vectors)}, Avg vector length: {avg_vector_length}, Avg dim count {avg_dimension_count}")
    # print(f"len summated vector: {len(sum_list)}, summated vec length: {vector_length}")
    # normalization = divide by length = 1; multiply by avg_vector_length avg_vector_length/vector_length
    normalized = [(k,(v / vector_length) * avg_vector_length) for (k, v) in sum_list]
    average = []
    for (k, v) in normalized:
        average.append({ 'index' : k, 'value' : v })
    return average


async def vectorize_all(psql_engine, et : EmbeddingsTester):
    # to do:
    """
    executor = concurrent.futures.ProcessPoolExecutor(10)
    futures = [executor.submit(try_my_operation, item) for item in items]
    concurrent.futures.wait(futures)
    """
    for document in get_documents(psql_engine=psql_engine):
        # empty texts screw up the embeddings computation
        non_empty_nodes = [node for node in document if (node['text'] is not None and len(node['text']) > 0)]
        texts = [node['text'] for node in non_empty_nodes] 
        ids = [node['id'] for node in non_empty_nodes]
        text_map = {}
        for node in non_empty_nodes:
            text_map[node['id']] = node['text']
        # get embeddings
        sparse_embeddings = await et.compute_sparse_embeddings(texts)
        dense_embeddings = await et.compute_dense_embeddings(texts)
        
        # store the embeddings
        for i in range(len(ids)):
            node = non_empty_nodes[i]
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
        
        child_ids = [node['id'] for node in non_empty_nodes if node['node'] == 'child']
        child_dense_embeddings = [embedding_map[id][0] for id in child_ids]

        # cluster: lists of identifiers
        cluster_map = {}
        embeddings = np.array([np.array(emb) for emb in child_dense_embeddings])
        pmc = document[0]['pmc']
        if len(embeddings) != 0:
            for cluster in get_clusters_dict(embeddings=embeddings, ids=child_ids, texts = text_map):
                cluster_id = str(uuid.uuid4())
                cluster_map[cluster_id] = cluster
                # now compute the centroid for each cluster  # .tolist() for serialization
                cluster_dense_centroid = np.mean(np.array([embedding_map[id][0] for id in cluster]), axis=0).tolist() 
                cluster_sparse_centroid = average_sparse([embedding_map[id][1] for id in cluster])
            
                run_insert_sql(engine=psql_engine, table_name=CLUSTER_TABLE, data_dict={
                    'pmc':   pmc,
                    'dense_centroid' : json.dumps(cluster_dense_centroid),
                    'sparse_centroid' : json.dumps(cluster_sparse_centroid),
                    'cluster_id' : cluster_id,
                    'ids' : json.dumps(cluster)
                })
        print("Done document %s"%(pmc))


def read_cluster_vectors(psql_engine):
    query="SELECT cluster_id, dense_centroid, sparse_centroid, ids FROM %s"%CLUSTER_TABLE
    dense_vectors = {}
    sparse_vectors = {}
    detail_lists = {}
    for item in run_query(psql_engine, query):
        dc = json.loads(item['dense_centroid'])
        dense_vectors[item['cluster_id']] = np.array(dc)    
        sparse_vectors[item['cluster_id']] = json.loads(item['sparse_centroid'])
        detail_lists[item['cluster_id']] = json.loads(item['ids'])
    return dense_vectors, sparse_vectors, detail_lists


from sklearn.metrics.pairwise import cosine_similarity

def get_dense_vector(psql_engine, id):
    query="SELECT vector FROM %s WHERE id=%s"%(DENSE_VECTOR_TABLE, id)
   
    for item in run_query(psql_engine, query):
        return np.array(json.loads(item['vector']))
    return None

def get_sparse_vector(psql_engine, id):
    query="SELECT vector FROM %s WHERE id=%s"%(SPARSE_VECTOR_TABLE, id)
   
    for item in run_query(psql_engine, query):
        return json.loads(item['vector'])
    return None

"""
max score is always 1 after this normalization
"""
def normalize_scores(scores: List[Tuple]) -> List[Tuple]:
    max_score = max(score for (item, score) in scores)
    return [(item, score/max_score) for (item, score) in scores]

def to_dict(scores: List[Tuple]) -> Dict:
    ret = dict()
    for (item, score) in scores:
        ret[item] = score
    return ret

def test_search(psql_engine):
    """
    https://dev.to/daviducolo/implementing-vector-search-in-python-7dn
    """
    dense_vectors, sparse_vectors, detail_lists = read_cluster_vectors(psql_engine=psql_engine)
    sv = {}
    for k, v in sparse_vectors.items():
        sv[k] = sparse_to_map(v)
    sparse_vectors = sv

    five_random_snippets = [item for item in run_query(psql_engine, "SELECT * FROM pmc_records ORDER BY random() limit 5")]
    cluster_ids = []
    dense_vec_arr = []
    for k,v in dense_vectors.items():
        cluster_ids.append(k)
        dense_vec_arr.append(v)
    dense_vec_np = np.array(dense_vec_arr)
    for random_snippet in five_random_snippets:
        # Define a query vector
        dense_query_vector = get_dense_vector(psql_engine, random_snippet['id'])
        sparse_query_vector = get_sparse_vector(psql_engine, random_snippet['id'])
        sparse_query_vector = sparse_to_map(sparse_query_vector)

        # Calculate Cosine similarities between the query vector and the dataset
        dense_similarities = cosine_similarity(dense_vec_np, [dense_query_vector]).tolist() # .tolist() for serialization purposes
        # Find the closest vectors
        dense_similarities_list = [(cluster_ids[i], dense_similarities[i][0]) for i in range(len(dense_similarities))]
        dense_similarities_list.sort(key=lambda x : -x[1])
        
        sparse_similarities_list = [(cluster_id, sparse_dot_product(vector, sparse_query_vector)) for (cluster_id, vector) in sparse_vectors.items()]
        sparse_similarities_list.sort(key=lambda x : -x[1])

        top5_dense = set([x[0] for x in dense_similarities_list[0:5]])
        top5_sparse = set([x[0] for x in sparse_similarities_list[0:5]])
        top10_dense = set([x[0] for x in dense_similarities_list[0:5]])
        top10_sparse = set([x[0] for x in sparse_similarities_list[0:5]])

        run_insert_sql(engine=psql_engine, table_name=CLUSTER_MATCH_TABLE, data_dict={
            'dense_matches' : json.dumps(dense_similarities_list[0:5]),
            'sparse_matches' : json.dumps(sparse_similarities_list[0:5]),
            'query_id' : random_snippet['id'],
            'overlap5' : len(top5_dense & top5_sparse),
            'overlap10' : len(top10_dense & top10_sparse)
        })

        # now figure out the detail matches, i.e. all records contained in the cluster are matched separately
        matched_cluster_sparse_items = set(item[0] for item in sparse_similarities_list[0:5])
        matched_cluster_dense_items = set(item[0] for item in dense_similarities_list[0:5])
        detail_dense_items = set()
        for mcdi in matched_cluster_dense_items:
            detail_dense_items = detail_dense_items.union(detail_lists.get(mcdi))
        detail_sparse_items = set()
        for mcsi in matched_cluster_sparse_items:
            detail_sparse_items = detail_sparse_items.union(detail_lists.get(mcsi))
        matched_cluster_items = []
        matched_cluster_items.extend(matched_cluster_dense_items|matched_cluster_sparse_items)
        detail_items = []
        detail_items.extend(detail_sparse_items|detail_dense_items)

         # Find the closest vectors -- DENSE
        detail_dense_vectors = [get_dense_vector(psql_engine, detail_item) for detail_item in detail_items]
        detail_dense_vec_np = np.array(detail_dense_vectors)
        detail_dense_similarities = cosine_similarity(detail_dense_vec_np, [dense_query_vector])
        detail_dense_similarities_list = \
            [(detail_items[i], detail_dense_similarities[i][0]) for i in range(len(detail_dense_similarities))]
        detail_dense_similarities_list = normalize_scores(detail_dense_similarities_list)
        detail_dense_similarities_dict = to_dict(detail_dense_similarities_list)
        
         # Find the closest vectors -- SPARSE
        detail_sparse_vectors = [sparse_to_map(get_sparse_vector(psql_engine, detail_item)) for detail_item in detail_items]
        detail_sparse_similarities_list = \
            [(detail_items[i], sparse_dot_product(detail_sparse_vectors[i], sparse_query_vector)) for i in range(len(detail_sparse_vectors))]
        detail_sparse_similarities_list = normalize_scores(detail_sparse_similarities_list)
        detail_sparse_similarities_dict = to_dict(detail_sparse_similarities_list)

        for id in detail_items:
            run_insert_sql(engine=psql_engine, table_name=DETAIL_MATCH_TABLE, data_dict={
                'query_id' : random_snippet['id'],
                'match_id' : id,
                'dense_score' : detail_dense_similarities_dict.get(id),
                'sparse_score' : detail_sparse_similarities_dict.get(id),
                'in_dense' : 1 if id in detail_dense_items else 0,
                'in_sparse' : 1 if id in detail_sparse_items else 0
            })

def export_to_excel(psql_engine):
    query = """
SELECT dense_score, sparse_score, in_dense, in_sparse, query_id, match_id,
pr_q.text as query_text, 
pr_m.text as match_text,
pr_q.pmc AS QueryPMC,
pr_m.pmc AS MatchPMC
FROM
datadigger.pmc_detail_matches dm JOIN 
datadigger.pmc_records pr_q ON pr_q.id = dm.query_id JOIN
datadigger.pmc_records pr_m ON pr_m.id = dm.match_id 
ORDER BY query_id, dense_score + sparse_score DESC
"""
    data = [item for item in run_query(psql_engine, query)]
    import pandas as pd
    df = pd.DataFrame.from_records(data,index=[i for i in range(len(data))]) 
    df.to_excel("output.xlsx")

if __name__ == "__main__":
    load_dotenv()
    settings = Settings()
    connection_string = settings.psql.connection.get_secret_value();
    psql_engine = create_engine(connection_string)

    # create tables
    for query in PRE_QUERIES:
        run_select_sql(psql_engine, query)

    # 1. upload a bunch of full-text documents
    # insert_all(psql_engine)
    tester = EmbeddingsTester(settings.spladedoc, settings.embedding)
    # asyncio.run(single_test(tester))

    # 2. compute vectors
    # asyncio.run(vectorize_all(psql_engine, tester))

    # 3. search in the vectors
    # test_search(psql_engine=psql_engine)

    # 4. export the data to excel
    export_to_excel(psql_engine)
    print('done')


