import logging
from pathlib import Path
from enum import Enum
from typing import List
import boto3
import httpx
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError 

class BaseNodeTypeEnum(Enum):
    PARENT = "parent"
    CHILD = "child"
    
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

def upload_to_s3(bucket_name, file_name, object_name=None):
    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Create an S3 client
    s3_client = boto3.client('s3')

    # Upload the file
    try:
        response = s3_client.upload_file(file_name, bucket_name, object_name)
    except Exception as e:
        print("Error uploading: ", e)
        return False
    return True