
from typing import List
from loguru import logger

logger.add("file.log", rotation="500 MB", format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}")


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
    
    # Create a list of (index, summed_value) and sort by summed_value in descending order
    sum_list = sorted(summated.items(), key=lambda x: -x[1])[:avg_dimension_count]

    # Calculate the current vector length
    vector_length = sum(v[1]*v[1] for v in sum_list)**(1/float(len(sum_list))) # current vector length

    # print(f"#vectors: {len(vectors)}, Avg vector length: {avg_vector_length}, Avg dim count {avg_dimension_count}")
    # print(f"len summated vector: {len(sum_list)}, summated vec length: {vector_length}")
    # normalization = divide by length = 1; multiply by avg_vector_length avg_vector_length/vector_length
    normalized = [(k,(v / vector_length) * avg_vector_length) for (k, v) in sum_list]
    
    # Convert to the desired output format
    average = [{'index': k, 'value': v} for k, v in normalized]
    return average
