import csv
import json
import os

import time

from datetime import datetime

from urllib.parse import urlparse

import logging

import boto3

from io import BytesIO

import zipfile

import sys

import math

from functools import reduce

from pyspark.sql.functions import lit, input_file_name, when, collect_list, concat_ws, col

from pyspark.sql.types import StringType

from pyspark.sql.utils import AnalysisException

import logging

from pyspark.sql.types import StructType, StructField, StringType

from delta.tables import DeltaTable

from pyspark.sql import SparkSession

from pyspark.sql.functions import current_timestamp

import logging

 

EXTRA_LOGGING = {

    "group_name": os.environ.get('group_name', "med-rpr"),

    "job_name": os.environ.get('job_name', "ivo_file_proecss")

}

 

s3_client = boto3.client('s3')

 

def format_size(size):

    for unit in ['B', 'KB', 'MB', 'GB']:

        if size < 1024.0:

            return f"{size:.2f} {unit}"

        size /= 1024

 

def process_folder(s3_bucket, folder_path):

    logging.info("process_folder...")

    response = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=folder_path)

    return response

 

def upload_to_s3(s3_bucket, upload_key, data):

    s3_client.put_object(Bucket=s3_bucket, Key=upload_key, Body=data)

    logging.info(f"upload to s3 completed for - {upload_key}")

 

def read_in_chunks(file, chunk_size=4096):

    while True:

        chunk = file.read(chunk_size)

        if not chunk:

            break

        yield chunk

 

def process_data_in_chunks(file, chunk_size):

    csv_data = ""

    for chunk in read_in_chunks(file, chunk_size):

        csv_data_chunk = chunk.decode('utf-8', errors='replace')

        csv_data += csv_data_chunk

    return csv_data

 

def process_and_upload_zipped_files_to_s3(s3_bucket, obj, max_retries=1, max_direct_read_size_mb=1024, retry_delay_seconds=10,

                     chunk_size=10240):

    file_path = obj['Key']

    file_name = file_path.split("/")[-2]

    start_time = time.time()

    for attempt in range(max_retries + 1):

        try:

            zip_obj = s3_client.get_object(Bucket=s3_bucket, Key=obj['Key'])

            buffer1 = BytesIO()

 

            for chunk in zip_obj['Body'].iter_chunks(chunk_size):

                buffer1.write(chunk)

 

            with zipfile.ZipFile(buffer1, mode='r') as zipf:

                for subfile in zipf.namelist():

                    with zipf.open(subfile, 'r') as myfile:

                        file_size = myfile.seek(0, 2)

                        myfile.seek(0)

                        logging.info(f"Size of {file_name}: AFTER UNZIP {format_size(file_size)}")

                        if file_size > max_direct_read_size_mb * 1024 * 1024:

                            logging.info(f"{file_size} is greater than {max_direct_read_size_mb}, so reading in chunks")

                            csv_data = myfile.read().decode('utf-8', errors='replace')

                            upload_key = f"{dh_unzip_files_prefix}{table_name}/{subfile}"

                            upload_to_s3(s3_bucket, upload_key, csv_data)

                            #csv_data = process_data_in_chunks(myfile, chunk_size) -- Comneting to sllit method for now to check error

                        else:

                            csv_data = myfile.read().decode('utf-8', errors='replace')

                        upload_key = f"{dh_unzip_files_prefix}{table_name}/{subfile}"

                        upload_to_s3(s3_bucket, upload_key, csv_data)

            break

 

        except (MemoryError, TimeoutError, IOError, ValueError) as e:

            # Log specific errors and retry

            logging.error(f"Error processing {file_name}: {str(e)}")

 

            if attempt < max_retries:

                logging.warning(f"Retrying after {retry_delay_seconds} seconds. Retry attempt: {attempt + 1}")

                time.sleep(retry_delay_seconds)

            else:

                logging.error(f"Failed after {max_retries} retries. Skipping file processing")

 

        except Exception as e:

            # Log other unknown errors

            logging.error(f"Unknown error processing {file_name}: {str(e)}")

        finally:

            buffer1.close()  # Close the buffer to free up resources

 

    end_time = time.time()

    elapsed_time = end_time - start_time

    logging.info(f"Time taken to unzip {file_name}: {elapsed_time:.2f} seconds")



if __name__ == "__main__": 

    # set logging level to trim down noise

    logging.getLogger("py4j").setLevel(logging.WARNING)
    logging.getLogger().setLevel(logging.INFO) 

    s3_bucket = "cds-ingest-raw-prod"

    folder_path = "med-rpr/nas/grpout/date=2024-06-07/"

    files_to_process = "input_merged.zip"

    dh_unzip_files_prefix = "med-rpr/nas/grpout/unizp/"

    table_name="provider_med_rpr"

    subfile="provider.csv"

 

    s3 = boto3.client('s3')

    response = process_folder(s3_bucket, folder_path)

    for obj in response.get('Contents', []):

        if obj['Key'].endswith('.zip'):

            file_key = obj['Key']

            file_name = os.path.splitext(os.path.basename(file_key))[0]

            file_size = obj['Size']

            if file_name in files_to_process:

              readable_size = format_size(file_size)

              logging.info(f"processed: {file_name}, Size: {readable_size}")

              process_and_upload_zipped_files_to_s3(s3_bucket, obj)

              logging.info("process_and_upload_zipped_files_to_s3 is completed")