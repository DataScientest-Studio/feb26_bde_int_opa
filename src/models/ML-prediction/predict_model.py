import pandas as pd
import psycopg2
import logging
import os
import joblib
import time
import select


############### load the trained model and scaler, and make predictions on the live data ##############
MODEL_PATH = '/models/rf_trade_decision_model.joblib'
SCALER_PATH = '/models/scaler.joblib'

while not os.path.exists(MODEL_PATH) or not os.path.exists(SCALER_PATH):
    logging.info("Waiting for model and scaler...")
    time.sleep(5)  

logging.info("Model and scaler found, loading...")

rf_clf = joblib.load(MODEL_PATH)
scaler = joblib.load(SCALER_PATH)

logging.info("Loaded trained model and scaler successfully")

################ establish connection to SQL database to read in the transformed live data ################

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
        select.select([conn], [], [])  # Wait for a notification of new features
        conn.poll()

        while conn.notifies:
            notify = conn.notifies.pop(0)   
            logging.info(f"trigger received: {notify.payload}")

            if notify.channel == 'new_feature_table':
                # to make the trading decision about the next candle, only the last row of live_feats should be used
                live_feats = pd.read_sql_query("SELECT * FROM klines_15m_features ORDER BY open_time DESC LIMIT 1", conn)
                
                current_ts = live_feats['open_time'].iloc[0]

                if current_ts == last_seen:
                    continue  # Skip if we have already processed this timestamp

                last_seen = current_ts
                ###### prepare the data set for prediction ######
                live_feats['open_time'] = pd.to_datetime(live_feats['open_time'], utc=True)
                live_feats = live_feats.set_index('open_time')
                
                X_live = live_feats.drop(columns=['future_close', 'future_return','trade_decision'])  # Drop future info from features
                
                X_live_scaled = scaler.transform(X_live)  # Scale features using the same scaler as training data

                ########### make prediction using the trained model ###########
                trade_decision_pred = rf_clf.predict(X_live_scaled)

                print(f"Predicted trade decision for next candle: {trade_decision_pred[0]}")
                logging.info(f"Predicted trade decision for next candle: {trade_decision_pred[0]}")
            
    except Exception as e:
        logging.error(f"Error while reading live data or making prediction: {e}")
        time.sleep(5)