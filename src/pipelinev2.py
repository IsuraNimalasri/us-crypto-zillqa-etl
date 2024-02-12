import argparse
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import bigquery
import logging
from datetime import datetime
import os
import yaml
from typing import Tuple, List, Any, Dict
import boto3


AWS_REGION = 'us-east-1'
AWS_PROFILE = 'localstack'
ENDPOINT_URL = os.environ.get('LOCALSTACK_ENDPOINT_URL')


class Pipeline:
    """Pipeline to extract data from BigQuery, transform, and load to S3."""
    
    def __init__(self, pipeline_config_path: str):
        """Initializes the Pipeline with a path to the configuration file."""
        self.pcgf_path = pipeline_config_path.replace(".yaml", ".yml")
        self.cfg_data = self.load_configs()

    def load_configs(self) -> Dict:
        """Loads pipeline configuration from a YAML file."""
        with open(self.pcgf_path, 'r') as cfg:
            return yaml.safe_load(cfg)

    def get_transformation_query(self, transformed_query_path: str) -> str:
        """Reads the SQL transformation query from a file."""
        with open(transformed_query_path, 'r') as td:
            return td.read().strip()

    def start(self):
        """Starts the ETL process defined in the pipeline configuration."""
        _prefix_pattern = self.cfg_data['pipeline']['s3']['prefix_pattern']
        _file_format = self.cfg_data['pipeline']['s3']['file_format']

        processing_date = datetime.today().strftime('%Y-%m-%d')

        for metric in self.cfg_data['pipeline']['query_metric']:
            file_name = f'{metric["name"]}.{_file_format}'
            s3_prefix_pattern = _prefix_pattern.format(metric['name'], processing_date)
            _query = self.get_transformation_query(transformed_query_path=metric["query_path"])

            rows, table_schema = self.extract_transformation_data(tquery=_query)

            # self.load_to_local(table_rows=rows, local_path=s3_prefix_pattern, schema=table_schema, filename=file_name)
            self.load_to_s3(table_rows=rows, s3_path=s3_prefix_pattern, schema=table_schema, filename=file_name)

    def extract_transformation_data(self, tquery: str) -> Tuple[List[Any], List]:
        """Executes the transformation query and extracts data along with schema."""
        client = bigquery.Client()
        query_job = client.query(tquery)
        table_id = client.get_table(query_job.destination)

        all_rows = []
        page_token = None
        max_results = self.cfg_data['pipeline']['meta'].get('max_results', 10000)

        schema = table_id.schema

        while True:
            rows_iter = client.list_rows(table_id, max_results=max_results, page_token=page_token)
            rows = [list(row.values()) for row in rows_iter]
            all_rows.extend(rows)
            logging.info(f'Processed {len(all_rows)} rows so far.')

            page_token = rows_iter.next_page_token
            if not page_token:
                logging.info('All pages have been processed.')
                break

        return all_rows, schema
    
    def load_to_local(self, table_rows: List[Any], local_path: str, schema: List, filename: str):
        """Loads the transformed data into S3 as a Parquet file."""
        columns = [field.name for field in schema]
        df = pd.DataFrame(data=table_rows, columns=columns)
        
        table = pa.Table.from_pandas(df)
        parquet_file_path = f"./tmp/{local_path}"
        os.makedirs(parquet_file_path, exist_ok=True)

        abs_path = os.path.abspath(f"{parquet_file_path}/{filename}")
        pq.write_table(table, abs_path)
        logging.info(f'Data written to {abs_path}.')


    def load_to_s3(self, table_rows: List[Any], s3_path: str, schema: List, filename: str):
        
        """Loads the transformed data into S3 (LocalStack) as a Parquet file."""
        columns = [field.name for field in schema]
        df = pd.DataFrame(data=table_rows, columns=columns)
        
        table = pa.Table.from_pandas(df)
        parquet_file_path = f"{s3_path}/{filename}"

        # Convert the PyArrow Table to a Parquet file in memory
        buffer = pa.BufferOutputStream()
        pq.write_table(table, buffer)
        parquet_bytes = buffer.getvalue()
        
        boto3.setup_default_session(profile_name=AWS_PROFILE)
        # Configure boto3 to use the LocalStack endpoint
        s3_client = boto3.client("s3", region_name=AWS_REGION,
                         endpoint_url=ENDPOINT_URL)

        # Assuming s3_path is in the format "bucket_name/path/to/file"
        bucket_name = self.cfg_data['pipeline']['s3']['bucket_name']
        object_name = parquet_file_path

        try:
            # Upload the Parquet file to S3 (LocalStack)
            s3_client.put_object(Bucket=bucket_name, Key=object_name, Body=parquet_bytes)
            logging.info(f'Data successfully uploaded to {parquet_file_path} in LocalStack.')
        except NoCredentialsError:
            logging.error('AWS credentials not found.')
        except Exception as e:
            logging.error(f'Failed to upload data to LocalStack S3: {e}')

def setup_logging():
    """Sets up logging to write logs to a specific file based on the current date and time."""
    log_filename = datetime.now().strftime("crypto_zilliqa_%Y%m%d%H%M.logs")
    log_dir = "./logs"
    os.makedirs(log_dir, exist_ok=True)
    logging.basicConfig(filename=os.path.join(log_dir, log_filename), 
                        level=logging.INFO, 
                        format='%(asctime)s - %(levelname)s - %(message)s')

if __name__ == '__main__':
    setup_logging()
    zillqa_etl = Pipeline(pipeline_config_path='./pipeline_configs/daily_volume_crypto_v2.yml')
    zillqa_etl.start()
