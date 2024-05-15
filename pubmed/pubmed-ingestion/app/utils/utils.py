from collections import defaultdict
from pathlib import Path
from enum import Enum
from typing import List
import boto3
import datetime
import json
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError 

class BaseNodeTypeEnum(Enum):
    PARENT = "parent"
    CHILD = "child"


class ProcessingResultEnum(Enum):
    VECTORDB_FAILED = -1
    PMC_RECORD_PARSING_FAILED = -2
    PMC_RECORD_NOT_FOUND = -3
    ID_ABSTRACT_BAD_DATA = -4
    SUCCESS = 1
    ONPROGRESS = 2

def update_result_status(pubmed_id: int,
                         status:int,
                         parent_id_nodes_count: int = 0,
                         parent_id: str = "",
                         children_nodes_count: int = 0,
                         parsed_fulltext: defaultdict = json.dumps({})) -> defaultdict:
        ip_dict = defaultdict()
        ip_dict["pubmed_id"] = pubmed_id
        ip_dict["status"] = status
        ip_dict["parent_id_nodes_count"] = parent_id_nodes_count
        ip_dict["parent_id"] = parent_id
        ip_dict["children_nodes_count"] = children_nodes_count
        ip_dict["parsed_fulltext_json"] = json.dumps(parsed_fulltext, ensure_ascii=True)
        return ip_dict
    
def get_project_root() -> Path:
    return Path(__file__).parent.parent

def download_s3_file(s3_bucket, s3_object):
        s3 = boto3.client("s3")

        try:
            # Download the XML file from S3
            s3_response = s3.get_object(Bucket=s3_bucket, Key=s3_object)
            content = s3_response['Body'].read()
            return content.decode("utf-8")
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchBucket':
                print(f"Error: The bucket {s3_bucket} does not exist.")
            elif e.response['Error']['Code'] == 'NoSuchKey':
                print(f"Error: The object {s3_object} does not exist in bucket {s3_bucket}.")
            else:
                print(f"An S3 error occurred: {e}")
            return "" 
        except NoCredentialsError:
            print("Error: AWS credentials not found. Please set up your AWS credentials.")
        except PartialCredentialsError:
            print("Error: Partial AWS credentials found. Please ensure your credentials are complete.")
        except Exception as e:
            print(f"An error occurred: {e}")

def upload_to_s3(bucket_name, log_json, year):
    # If S3 object_name was not specified, use file_name
    s3_client = boto3.resource('s3')
    timestamp = datetime.datetime.now().strftime('%Y%m%d-%H%M%S')
    file_name = f"{year}_{timestamp}.json"
    s3object = s3_client.Object(bucket_name, file_name)
    try:
        s3object.put(
            Body=(bytes(log_json.encode('UTF-8')))
        )
    except Exception as e:
        print("Error uploading: ", e)
        return False
    return True