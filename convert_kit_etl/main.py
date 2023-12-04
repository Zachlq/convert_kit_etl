import pandas as pd
import requests
import logging
import config as cfg
import datetime as dt
from datetime import date
import google.cloud.logging
from google.cloud import storage 
from google.cloud import bigquery
import json
import config as cfg

# Initialize logging

logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s', level=logging.INFO, datefmt='%I:%M:%S')

logging_client = google.cloud.logging.Client()
logging_client.setup_logging()

dt_updated = pd.Timestamp.now("US/Eastern")

today = dt.date.today()

bq_client = bigquery.Client()

storage_client = storage.Client()

bucket = storage.Client().get_bucket(cfg.bucket)

blob = bucket.blob(cfg.creds)
KEY = blob.download_as_string()
KEY = json.loads(KEY)

secret = KEY["api_secret"]

url = "https://api.convertkit.com/v3/subscribers?api_secret="+secret

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

def make_request():
    
    logging.info("Making request to subscriber list end point...")

    req = requests.get(url)
    
    if req.status_code==200:
        
        response = req.json()
        
        return response
        
    else:
        logging.info(f"Invalid API call. Response is {req.status_code}")
        
def convert_kit_etl(event, context):
    
    init_response = make_request()
    
    total_subs = init_response["total_subscribers"]
    
    data = {"total": total_subs, "dt_updated": dt_updated}

    total_sub_df = pd.DataFrame(data, index=[0])
    
    subs = init_response["subscribers"]
    
    info_df = pd.DataFrame()

    for i in subs:

        Id = i["id"]
        email_address = i["email_address"]
        state = i["state"]
        created_at = i["created_at"]

        info_df=info_df.append({"id": Id,
                       "email_address": email_address,
                       "state": state,
                       "created_at": created_at},
                      ignore_index=True)
        
    info_df["created_at"] = pd.to_datetime(info_df["created_at"])
    info_df["created_at"] = info_df["created_at"].dt.strftime("%Y-%m-%d")
    
    today_subs = info_df[info_df["created_at"] == today]
    
    if len(today_subs) > 0:
        
        logging.info(f"Uploading to {cfg.dataset}.{cfg.subscriptions_table}...")
        
        upload_to_bq(today_subs, cfg.dataset, cfg.subscriptions_table, cfg.all_subs_schema)
        
        logging.info(f"{cfg.subscriptions_table} updated as of {today}")
    
    else:
        logging.info(f"No new subscribers for {today}...")
        
    logging.info(f"Uploading to {cfg.dataset}.{cfg.total_subscriptions_table}...")

    upload_to_bq(total_sub_df, cfg.dataset, cfg.total_subscriptions_table, cfg.total_subs_schema)

    logging.info(f"{cfg.total_subscriptions_table} updated as of {today}")
        
if __name__ == "__main__":
    logging.info(f"Beginning execution for {today}...")
    convert_kit_etl("","")
