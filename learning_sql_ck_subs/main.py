import requests
import logging
import pandas as pd
import os
import config as cfg
from datetime import date
import datetime as dt
from google.cloud import bigquery
from google.cloud import storage
import json

logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s', level=logging.INFO, datefmt='%I:%M:%S')

today = dt.date.today()

bq_client = bigquery.Client()

bucket = storage.Client().get_bucket("convert_kit")

blob = bucket.blob(cfg.lsql_creds)
KEY = blob.download_as_string()
KEY = json.loads(KEY)

secret = KEY["api_secret"]

url = "https://api.convertkit.com/v3/subscribers?api_secret="+secret

dt_updated = pd.Timestamp.now("US/Eastern")

def upload_to_bq(df: pd.DataFrame, dataset_id: str, table_id: str, schema: list):

    dataset_ref = bq_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)

    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition='WRITE_APPEND'
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.schema = schema
    job_config.ignore_unknown_values=True 

    job = bq_client.load_table_from_dataframe(
    df,
    table_ref,
    location='US',
    job_config=job_config)
    
    return job.result()

def make_request(url):
    
    logging.info("Making request to subscriber list end point...")

    req = requests.get(url)
    
    if req.status_code==200:
        
        response = req.json()
        
        return response
        
    else:
        logging.info(f"Invalid API call. Response is {req.status_code}")
        
def learning_sql_ck_subs(event, context):
    
    init_response = make_request(url)
    
    total_subs = init_response["total_subscribers"]
    
    data = {"total": total_subs, "dt_updated": dt_updated}

    total_sub_df = pd.DataFrame(data, index=[0])
    
    logging.info(f"Uploading to {cfg.dataset}.{cfg.lsql_ck_table}...")

    upload_to_bq(total_sub_df, cfg.dataset, cfg.lsql_ck_table, cfg.total_subs_schema)

    logging.info(f"lsql_total_subs updated as of {today}")

if __name__ == "__main__":
    learning_sql_ck_subs("","")
