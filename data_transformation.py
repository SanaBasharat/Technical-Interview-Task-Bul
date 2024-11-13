import pandas as pd
import yaml
import json
import os
import time
import logging
import argparse
from scripts.cloud_storage import load_processed_data_to_gcs
from scripts.database import DatabaseGeneral

LOG_FILE = "logs_data_transformation.log"

with open("configuration.yaml", "r") as yml_file:
    config_file = yaml.load(yml_file, yaml.Loader)

# Get log level from config and convert it to the logging level
log_level = config_file["log_level"]
log_level = logging._nameToLevel.get(log_level, logging.INFO)

#define logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# define handlers and formatter
stream_handler = logging.StreamHandler()
file_handler = logging.FileHandler(os.path.join(config_file['log_dir'], LOG_FILE))
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

# add formatter to handlers
stream_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)

# add both handlers to logger
logger.addHandler(stream_handler)
logger.addHandler(file_handler)

def read_station_data(filename):
    """Read station_data raw parquet file"""
    try:
        station_info_df = pd.read_parquet(os.path.join(config_file["data_dir"], filename))
        logger.info("Station data file successfully read.")
    except Exception as e:
        logger.error(e)
    return station_info_df

def read_files():
    """Read all raw parquet data files from data directory"""
    files = os.listdir(config_file["data_dir"])

    if len(files) == 0:
        return
    
    files = [f for f in files if config_file["station_data_file"] not in f]
    df = pd.DataFrame()

    try:
        for f in files:
            df = pd.concat([df, pd.read_parquet(os.path.join(config_file["data_dir"], f))])
        df.reset_index(drop = True, inplace = True)
        logger.info(f"{str(len(files))} data files successfully read.")
    except Exception as e:
        logger.error(e)

    df['Climate ID'] = df['Climate ID'].astype(str)
    df['Station Name'] = df['Station Name'].astype(str)
    df['station_id'] = df['station_id'].astype(int)

    # selecting relevent columns
    df = df[['station_id', 'Station Name', 'Climate ID', 'Latitude (y)', 'Longitude (x)', 'Date/Time (LST)', 'Year', 'Month', 'Day', 'Temp (°C)']]
    df.rename({'Longitude (x)': 'longitude', 'Latitude (y)': 'latitude', 'Station Name': 'station_name', 'Climate ID': 'climate_id',
            'Temp (°C)': 'temp', 'Month': 'month', 'Year': 'year', 'Day': 'day', 'Date/Time (LST)': 'date_time'}, axis = 1, inplace = True)
    
    # merge with weather station info
    station_info_df = read_station_data(config_file["station_data_file"])
    df = pd.merge(df, station_info_df, left_on=['station_id', 'climate_id', 'station_name'], right_on=['station_id', 'climate_id', 'station_name'], how='left')
    
    return df

def transform(df):
    """Apply grouping and transformations, and calculate year-on-year average"""
    # setting index for group by
    df = df.set_index(['station_id', 'station_name',  'climate_id', 'month', 'year', 'latitude', 'longitude', 'feature_id', 'map'])

    # group by
    df_grouped = df.groupby(level=['station_id', 'station_name', 'climate_id', 'month', 'year', 'latitude', 'longitude', 'feature_id', 'map']).agg({'temp':['mean','min', 'max']})
    df_grouped.columns = ['{} {}'.format(col[1], col[0]) for col in df_grouped.columns]
    df_grouped.reset_index(drop=False, inplace=True)
    df_grouped.rename({'mean temp': 'temperature_celsius_avg', 'min temp': 'temperature_celsius_min', 'max temp': 'temperature_celsius_max'}, axis=1, inplace=True)
    
    # if month row has nulls, remove it
    df_grouped = df_grouped.dropna(subset=['temperature_celsius_avg', 'temperature_celsius_min', 'temperature_celsius_max'], how='all').reset_index(drop=True)

    # adding date_month column as concat of month_year
    df_grouped['date_month'] = df_grouped['month'].astype(str) + '_' + df_grouped['year'].astype(str)

    # calculating year on year average
    df_grouped.sort_values(['station_id', 'month', 'year'], inplace=True)
    df_grouped['temperature_celsius_yoy_avg'] = df_grouped.groupby(['station_id', 'month'])['temperature_celsius_avg'].diff()
    return df_grouped

def clean(df):
    """Drop future months and fill nulls"""
    # drop future months
    indices_to_remove = df[(df['year'] == time.localtime().tm_year) & (df['month'] > time.localtime().tm_mon)].index
    df = df.drop(indices_to_remove, axis='index').reset_index(drop=True)

    # df.drop(['month', 'year'], axis = 1, inplace=True) # Not dropping these columns, as they can be helpful

    # replace nulls with 0
    df = df.fillna(0)
    return df

def add_ingest_timestamp(df):
    """Add ingestion timestamp as epoch to differentiate records from each other"""
    df['ingest_timestamp'] = pd.Timestamp.now().timestamp()
    return df

def save_to_file(df, file_name, file_type):
    """Save final processed file as parquet, csv or json"""
    if file_type == 'parquet':
        df.to_csv(file_name + '.parquet', index=False)
    elif file_type == 'csv':
        df.to_csv(file_name + '.csv', index=False)
    elif file_type == 'json':
        df_json = df.to_json(orient='records')
        with open(file_name + '.json', 'w') as outfile:
            json.dump(df_json, outfile)

def save_to_database(df, db):
    """Append final processed dataframe to database"""
    db.connect()
    db.insert_weather_data(df)
    db.close()

def argument_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--filename',
                        help="File name of final processed file\n",
                        required=False)
    parser.add_argument('-t', '--filetype',
                        help="Type of final processed file: csv, parquet or json\n",
                        required=False)
    args = parser.parse_args()
    parser.print_help()
    return args

if __name__ == "__main__":
    args = argument_parser()

    if args != None:
        file_name = args.filename
        file_type = args.filetype
        if file_type not in ['csv', 'json', 'parquet']:
            logger.warning("Provided file type is invalid. Defaulting to csv.") # error handling for invalid file type
            file_type = 'csv'
    else:
        file_name = 'final_dataset'
        file_type = 'csv'

    db = DatabaseGeneral(config_file["database_dir"], config_file["database_file"]) # initialize database object

    os.makedirs(config_file['log_dir'], exist_ok=True)
    
    logger.info("Data transformation started.")
    df = read_files()
    df = transform(df)
    logger.info("Data transformation done.")
    df = clean(df)
    logger.info("Data cleaning done.")
    df = add_ingest_timestamp(df)
    save_to_file(df, file_name, file_type)
    save_to_database(df, db)  
    try:
        load_processed_data_to_gcs(df, file_name, file_type)
        logger.info("Final data saved in file, database and Google Cloud Storage bucket.")
    except Exception as e:
            logger.info("Final data saved in file and database.")
            logger.error("Could not upload to GCS. Make sure you have the google service account credentials file in the secrets folder.")