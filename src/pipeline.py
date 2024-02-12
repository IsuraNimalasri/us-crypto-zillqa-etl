
import argparse
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import bigquery
import logging
import boto3
from botocore.exceptions import NoCredentialsError
import os
import yaml

class Pipeline:
    def __init__(self, pipeline_config_path):  # Added 'self' here
        self.pcgf_path = pipeline_config_path

    def load_configs(self):
        self.pcgf_path = self.pcgf_path.replace(".yaml", ".yml")
        with open(f"{self.pcgf_path}", 'r') as cfg:
            cfg_data = yaml.safe_load(cfg)
        self.cfg_data = cfg_data  

    def get_transformation_query(self):
        self.load_configs()  # Ensure configs are loaded
        transformed_query_path = self.cfg_data['pipeline']['transformed']['query_path'] 

        with open(transformed_query_path, 'r') as td:
            self.tquery = td.read().strip()

    def start(self):

        self.load_configs()

        for matric in self.cfg_data['pipeline']['query_metric']:
            print(query_metric)

        self.get_transformation_query()

        self.extract_transfomration_data()

        self.load_to_s3()
    
    def extract_transfomration_data(self):

        # Extraction + Transformations
        client = bigquery.Client()
        query_job = client.query(self.tquery)
        # results = query_job.to_arrow() 
        destination = query_job.destination
        table_id = client.get_table(destination)

        self.all_rows = []
        page_token = self.cfg_data['pipeline']['meta']['page_token']   # Initialize page_token
        max_results = self.cfg_data['pipeline']['meta']['max_results']   # Define max_results or make it configurable

        while True:
            rows_iter = client.list_rows(table_id, max_results=max_results, page_token=page_token)
            rows = list(rows_iter)
            self.all_rows.extend(rows)
            logging.info(f'Processed {len(self.all_rows)} rows so far.')

            page_token = rows_iter.next_page_token

            if not page_token:
                logging.info('All pages have been processed.')
                break  # Exit loop if no more pages
            
    def load_to_s3(self):

      table = pa.Table.from_pandas(pd.DataFrame(self.all_rows))
      # Write the Table to a Parquet file
      parquet_file_path = f"./tmp/{self.cfg_data['pipeline']['s3']['prefix_pattern']}data.parquet"
      pq.write_table(table, parquet_file_path)


        

if __name__ == '__main__':
    # Configure logging
    logging.basicConfig(level=logging.INFO)

    zillqa_etl = Pipeline(pipeline_config_path='./pipeline_configs/example.yml')
    zillqa_etl.start()
