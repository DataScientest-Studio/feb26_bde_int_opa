import psycopg2
import pandas as pd
import select
import logging
import os
from model import predict
import threading
import time

latest_prediction = None
prediction_lock = threading.Lock()

def listen_for_updates():
    global latest_prediction

    DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD")
    }   

    conn = psycopg2.connect(connect_timeout=5, **DB_CONFIG)
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)    

    logging.info("Connected to database")

    cur = conn.cursor()
    cur.execute("LISTEN new_feature_table;")  # Listen for notifications when new features are available in the database
    logging.info("Listening for new feature updates...")

    last_seen = None  # To track the last processed timestamp of the live features
    
    while True:
        try: 
            select.select([conn], [], [])
            conn.poll()

            while conn.notifies:
                notify = conn.notifies.pop(0)
                logging.info(f"trigger received: {notify.payload}")

                if notify.channel == 'new_feature_table':
                    df = pd.read_sql_query(
                        "SELECT * FROM klines_15m_features ORDER BY open_time DESC LIMIT 1",
                        conn
                    )

                    current_ts = df['open_time'].iloc[0]
                
                    if current_ts == last_seen:
                        continue  # Skip if we have already processed this timestamp

                    last_seen = current_ts

                    with prediction_lock:
                        latest_prediction = predict(df)
                        logging.info(f"New prediction: {latest_prediction}")
        except Exception as e:
            logging.error(f"Error while reading live data or making prediction: {e}")
            time.sleep(5)

def get_latest_prediction():
    with prediction_lock:
        return latest_prediction