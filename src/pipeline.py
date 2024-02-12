
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
from datetime import datetime

class Pipeline:
    def __init__(self, pipeline_config_path):  # Added 'self' here
        self.pcgf_path = pipeline_config_path

    def load_configs(self):
        self.pcgf_path = self.pcgf_path.replace(".yaml", ".yml")
        with open(f"{self.pcgf_path}", 'r') as cfg:
            cfg_data = yaml.safe_load(cfg)
        self.cfg_data = cfg_data  

    def get_transformation_query(self,transformed_query_path):
        with open(transformed_query_path, 'r') as td:
            _tquery = td.read().strip()
            return _tquery

    def start(self):

        self.load_configs()
        _prefix_pattern = self.cfg_data['pipeline']['s3']['prefix_pattern']
        _file_format = self.cfg_data['pipeline']['s3']['file_format']

        # Get today's date , Format the date as YYYY-MM-DD
        today = datetime.today()
        processing_date = today.strftime('%Y-%m-%d')


        for matric in self.cfg_data['pipeline']['query_metric']:
            print(matric)
            file_name = f'{matric["name"]}.{_file_format}'
            s3_prefix_pattern = _prefix_pattern.format(
                matric['name'],processing_date
            )
            _query = self.get_transformation_query(transformed_query_path=matric["query_path"])

            rows,table_schema =  self.extract_transfomration_data(tquery=_query)

            self.load_to_s3(table_rows=rows , s3_path=s3_prefix_pattern,schema=table_schema,filename=file_name)
    
    def extract_transfomration_data(self,tquery):

        # Extraction + Transformations
        client = bigquery.Client()
        query_job = client.query(tquery)
        destination = query_job.destination
        table_id = client.get_table(destination)

        all_rows = []
        page_token = self.cfg_data['pipeline']['meta']['page_token']   # Initialize page_token
        max_results = self.cfg_data['pipeline']['meta']['max_results']   # Define max_results or make it configurable

        # Access the schema
        schema = table_id.schema
        

        while True:
            rows_iter = client.list_rows(table_id, max_results=max_results, page_token=page_token)
            _rows_iter = map(lambda x: x.values() ,rows_iter )
            rows = list(_rows_iter)
            all_rows.extend(rows)
            logging.info(f'Processed {len(all_rows)} rows so far.')
            page_token = rows_iter.next_page_token
            
            if not page_token:
                logging.info('All pages have been processed.')
                break  # Exit loop if no more pages
        return all_rows ,schema
            
    def load_to_s3(self,table_rows, s3_path, schema, filename):

        _columns = list(map(lambda x: x.to_api_repr()['name'] ,schema))
        df = pd.DataFrame(data=table_rows,columns=_columns)
        print(df.head(3))
        table = pa.Table.from_pandas(df)

        # Write the Table to a Parquet file
        parquet_file_path = f"./tmp/{s3_path}"

        os.makedirs(parquet_file_path, exist_ok=True)
        logging.info(f'Processed  {parquet_file_path} .')

        _abspath = os.path.abspath(f"{parquet_file_path}{filename}")
        pq.write_table(table, _abspath)


        

if __name__ == '__main__':
    # Configure logging
    logging.basicConfig(level=logging.INFO)

    zillqa_etl = Pipeline(pipeline_config_path='./pipeline_configs/daily_volume_crypto_v2.yml')
    zillqa_etl.start()
