import asyncio
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import time
import requests
import json
from loguru import logger

from tqdm import tqdm

logger.add("file.log", rotation="500 MB", format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}")

class VectorUpdate:
    def __init__(self):
        self.num_workers = 256
        self.get_url = "http://localhost:6333/collections/pubmed_parent_hybrid/points/scroll"
        self.push_url = "http://localhost:6333/collections/pubmed_parent_hybrid/points/payload"
        

    def hit_search_url(self, limit = 1000):
        payload = json.dumps({
        "filter": {
            "must": [
            {
                "key": "_node_type",
                "match": {
                "value": "TextNode"
                }
            }
            ]
        },
        "limit": limit,
        "with_payload": True,
        "with_vector": False
        })
        headers = {
        'api-key': 'ea1db3c7-5b92-45fe-a929-b12584897dd5',
        'Content-Type': 'application/json'
        }
        response = requests.request("POST", self.get_url, headers=headers, data=payload)
        return response.json()
    
    def process_single_parent_record(self, point: defaultdict):
        payload = {}
        payload["_node_type"] = "CURIEO_NODE"
        payload["authors"] = point.get('payload').get("authors")
        payload["id"] = point.get("id")
        payload["identifiers"] = point.get('payload').get("identifiers")
        payload["parent_id"] = point.get('payload').get("parent_id")
        payload["publicationDate"] = point.get('payload').get("publicationDate")
        payload["pubmedid"] = point.get('payload').get("pubmedid")
        payload["text"] = json.loads(point.get('payload').get("_node_content")).get("text")
        payload["year"] = point.get('payload').get("year")

        record = {
            "payload": payload,
            "points" : [point.get("id")]
        }
        headers = {
            'api-key': 'ea1db3c7-5b92-45fe-a929-b12584897dd5',
            'Content-Type': 'application/json'
            }
        requests.request("PUT", self.push_url, headers=headers, data=json.dumps(record))

    async def process_batch_parent_records(self, points) -> None:
        loop = asyncio.get_running_loop()
        with ThreadPoolExecutor(max_workers=self.num_workers) as executor:
            jobs = []
            for point in points:
                job = loop.run_in_executor(executor, self.process_single_parent_record, point)
                jobs.append(job)

            lock = asyncio.Semaphore(self.num_workers)
            # run the jobs while limiting the number of concurrent jobs to num_workers
            async with lock:
                results = await asyncio.gather(*jobs)
    
    async def batch_process_records_to_vectors(self,
                                         points, batch_size=100):
        total_batches = (len(points) + batch_size - 1) // batch_size
        for i in tqdm(range(total_batches), desc="Transforming batches"):
            start_index = i * batch_size
            end_index = start_index + batch_size
            batch_data = points[start_index:end_index]
            await self.process_batch_parent_records(batch_data)
            
    
vu = VectorUpdate()
i = 0
while True:
    response = vu.hit_search_url()
    points = response.get('result').get('points', [])
    if len(points) != 0:
        start_time = time.time()
        asyncio.run(vu.batch_process_records_to_vectors(points, batch_size=100))
        i = i + len(points)
        logger.info(f"Processed Batch size of {i} in {time.time() - start_time:.2f}s")
    else:
        break