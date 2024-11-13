import pandas as pd
import os
import time
import yaml
import logging
import argparse
import datetime
from scripts.cloud_storage import load_raw_data_to_gcs
from scripts.database import DatabaseGeneral

LOG_FILE = 'logs_data_extraction.log'
GEONAMES_API = 'http://geogratis.gc.ca/services/geoname/en/geonames.csv?lat={}&lon={}&radius={}'
WEATHER_API = 'https://climate.weather.gc.ca/climate_data/bulk_data_e.html?format={}&stationID={}&Year={}&Month={}&Day={}&time=LST&timeframe=1&submit=Download+Data'
DAY = 1     # constant, as weather API returns data for whole month
RADIUS = 1  # constant, as it will help locate the exact relevant weather station

with open("configuration.yaml", "r") as yml_file:
    config_file = yaml.load(yml_file, yaml.Loader)

# Get log level from config and convert it to the logging level
log_level = config_file["log_level"]
log_level = logging._nameToLevel.get(log_level, logging.INFO)

#define logger
logger = logging.getLogger(__name__)
logger.setLevel(log_level)

# define handlers and formatter
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.DEBUG)
file_handler = logging.FileHandler(os.path.join(config_file['log_dir'], LOG_FILE))
file_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

# add formatter to handlers
stream_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)

# add both handlers to logger
logger.addHandler(stream_handler)
logger.addHandler(file_handler)

def get_station_data(lat, long, radius):
    """
    Call geonames API with lat, long and radius to return exact weather station needed
    """
    station_df = pd.read_csv(GEONAMES_API.format(lat, long, radius))
    return station_df['feature.id'].values[0], station_df['map'].values[0]

def get_weather_data(station_id, year, month, format):
    """
    Downloads weather data from the API and stores it locally as parquet files
    """
    logger.debug("Entered get_weather_data function")
    try:
        weather_df = pd.read_csv(WEATHER_API.format(format, station_id, year, month, DAY))
        weather_df['station_id'] = station_id
        weather_df.to_parquet(os.path.join(config_file["data_dir"], 'weather_{}_{}_{}.parquet'.format(station_id, year, month)))
        logger.info(f"Extracted {str(weather_df.shape[0])} rows, {str(weather_df.shape[1])} columns for Station {station_id} for {month}-{year}.")
        return weather_df['Latitude (y)'].values[0], weather_df['Longitude (x)'].values[0], weather_df['Station Name'].values[0], weather_df['Climate ID'].values[0]
    except Exception as e:
        logger.error(e)
        return 0, 0, 0, 0

def argument_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--stations', nargs='+',
                        help="Station IDs, separate with <space> when multiple. For example 26953 31688\n",
                        required=True)
    parser.add_argument('-y', '--years', nargs='+',
                        help="Enter years, separate with <space> when multiple. For example 2024 2022 2021\n",
                        required=True)
    args = parser.parse_args()
    parser.print_help()
    return args

if __name__ == "__main__":
    args = argument_parser()
    if args != None:
        os.makedirs(config_file['data_dir'], exist_ok=True)

        db = DatabaseGeneral(config_file["database_dir"], config_file["database_file"])

        stations = args.stations
        years = args.years
        months = list(range(1, 13))
        format = 'csv'  # format for weather API

        station_info_list = []
        for station_id in stations:
            db.connect()
            month_present, year_present = db.get_latest_month_year(station_id)  # get latest month and year for this station in db
            db.close()
            if month_present == 0 and year_present == 0:        # if no data exists in db for this station
                logger.info(f"No data exists for Station {station_id}. Extracting for all given years.")
                for year in years:                              # extract for all years given in args
                    for month in months:
                        logger.info(f"Extracting data for Station {station_id} for {month}-{year}")
                        lat, long, station_name, climate_id = get_weather_data(station_id, year, month, format)
            else:                                               # if some data exists for this station
                month_now = datetime.datetime.now().month
                year_now = datetime.datetime.now().year
                if year_present < year_now or month_present < month_now:    # check difference with current month and year
                    logger.info(f"Previous data exists for Station {station_id}. Extracting after {month_present}-{year_present} uptil now.")
                    if month_present == 12: # increment the month by 1 to get data after that month
                        month_present = 1
                    else:
                        month_present = month_present + 1
                    to_extract = pd.date_range(f'{year_present}-{month_present}-01',f'{year_now}-{month_now}-01',freq='MS') # get list of month-years uptil now
                    years_to_extract = list(set(to_extract.strftime("%Y").astype(int)))     # get years to extract
                    months_to_extract = list(set(to_extract.strftime("%m").astype(int)))    # get months to extract
                    for year in years_to_extract:
                        for month in months_to_extract:
                            logger.info(f"Extracting data for Station {station_id} for {month}-{year}")
                            lat, long, station_name, climate_id = get_weather_data(station_id, year, month, format)
                else:
                    logger.info(f"Data for Station {station_id} is up to date.")    # No data to be extracted, as it is up to date
            
            feature_id, map = get_station_data(lat, long, RADIUS)   # get station data
            
            if all([lat, long, station_name, climate_id]):  # if valid weather station, add to list
                logger.info(f"Extracting metadata for Station {station_id}")
                station_info_list.append({'station_id': station_id, # append to list
                                        'station_name': station_name,
                                        'climate_id': climate_id,
                                        'feature_id': feature_id,
                                        'map': map})
        station_info_df = pd.DataFrame(station_info_list)   # convert station data to dataframe to store as raw parquet file
        station_info_df = station_info_df.astype({'station_id': int, 'station_name': str, 'climate_id': str, 'feature_id': str, 'map': str})
        station_info_df.to_parquet(os.path.join(config_file["data_dir"], 'station_data.parquet'))
        logger.info("Data extraction complete.")
        logger.info("Uploading to GCS bucket.")
        load_raw_data_to_gcs()
        logger.info("All data successfully uploaded to GCS bucket")
    else:
        logger.error("Please enter arguments or run with -h flag for help.")
                