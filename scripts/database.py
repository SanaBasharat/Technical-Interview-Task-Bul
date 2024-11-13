import os
import sqlite3
import logging
import yaml

LOG_FILE = "logs_database.log"

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

class DatabaseGeneral:
    def __init__(self, database_path, filename):
        os.makedirs(database_path, exist_ok=True)
        self.filename = filename
        self.database_path = database_path

        try:
            self.connection = sqlite3.connect(self.database_path + self.filename + '.sqlite3', check_same_thread=False, timeout=15)
            self.cursor = self.connection.cursor()

            # reading SQL query from .sql file
            f = open('sql/create_table.sql','r')
            query = f.read() 
            self.cursor.execute(query)
            logger.info("Table weather_data created successfully.")
        except sqlite3.Error as e:
            logger.error(f"Error creating table: {e}")
        self.connection.close()

    def connect(self):
        """Connect to the SQLite database."""
        try:
            self.connection = sqlite3.connect(self.database_path + self.filename + '.sqlite3', check_same_thread=False, timeout=15)
            self.cursor = self.connection.cursor()
            logger.info("Database connection established.")
        except sqlite3.Error as e:
            logger.error(f"Error connecting to database: {e}")

    def close(self):
        """Close the database connection"""
        if self.connection:
            self.connection.close()
            logger.info("Database connection closed.")

    def insert_weather_data(self, df):
        """Append processed weather data to db"""
        try:
            df.to_sql(name='weather_data', index=False, con=self.connection, if_exists='append')
            logger.info(f"weather_data inserted successfully.")
            self.connection.commit()
        except sqlite3.Error as e:
            logger.error(f"Could not insert weather_data: {e}")

    def get_latest_month_year(self, station_id):
        """Get the latest month and year for which data exists for a specific station_id"""
        try:
            # reading SQL query from .sql file
            f = open('sql/get_latest_month_year.sql','r')
            query = f.read()
            self.cursor.execute(query.format(station_id))
            row = self.cursor.fetchone()
            logger.info("Data fetched successfully.")
            if row == None:         # station data not found in database
                return 0, 0
            return row[0], row[1]   # month, year
        except sqlite3.Error as e:
            logger.error(f"Error fetching data: {e}")