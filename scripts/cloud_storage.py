import os
import yaml
import json
import pandas as pd
from google.cloud import storage

with open("configuration.yaml", "r") as yml_file:
    config_file = yaml.load(yml_file, yaml.Loader)

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=config_file['secrets_dir'] + '/' + config_file['gcp_service_account_file']

def load_raw_data_to_gcs():
    """
    Upload raw parquet files to GCS bucket
    """
    all_files = os.listdir(config_file['data_dir'])
    for file in all_files:
        df = pd.read_parquet(os.path.join(config_file['data_dir'], file))
        df.to_parquet("gs://{}/raw_data/{}".format(config_file['bucket_name'], file))

def load_processed_data_to_gcs(df, file_name, file_type):
    """
    Upload processed data (parquet, csv or json) to GCS bucket
    """
    if file_type == 'parquet':
        df.to_parquet("gs://{}/processed_data/{}".format(config_file['bucket_name'], file_name + '.parquet'))
    elif file_type == 'csv':
        df.to_csv("gs://{}/processed_data/{}".format(config_file['bucket_name'], file_name + '.csv'), index=False)
    elif file_type == 'json':
        bucket = storage.Client().get_bucket(config_file['bucket_name'])
        with open(file_name + '.json') as f:
            df_json = json.load(f)
        blob = bucket.blob('processed_data/' + file_name + '.json')
        blob.upload_from_string(data=json.dumps(df_json)) 