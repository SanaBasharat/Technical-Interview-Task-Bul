CREATE TABLE IF NOT EXISTS weather_data (station_id INTEGER,
                                        station_name TEXT,
                                        climate_id TEXT,
                                        latitude REAL,
                                        longitude REAL,
                                        date_month TEXT,
                                        feature_id TEXT,
                                        map TEXT,
                                        temperature_celsius_avg REAL,
                                        temperature_celsius_min REAL,
                                        temperature_celsius_max REAL,
                                        temperature_celsius_yoy_avg REAL,
                                        year INTEGER,
                                        month INTEGER,
                                        ingest_timestamp REAL)