import joblib
import os
import time
import logging
import pandas as pd

MODEL_PATH = '/models/rf_trade_decision_model.joblib'
SCALER_PATH = '/models/scaler.joblib'

rf_clf = None
scaler = None

def load_model():
    global rf_clf, scaler

    while not os.path.exists(MODEL_PATH) or not os.path.exists(SCALER_PATH):
        logging.info("Waiting for model and scaler...")
        time.sleep(5)

    rf_clf = joblib.load(MODEL_PATH)
    scaler = joblib.load(SCALER_PATH)

    logging.info("Model loaded successfully")


def predict(features_df):
    features_df['open_time'] = pd.to_datetime(features_df['open_time'], utc=True)
    features_df = features_df.set_index('open_time')
    X_live = features_df.drop(columns=['future_close', 'future_return','trade_decision']) # Drop future info from features  
    X_scaled = scaler.transform(X_live)
    return int(rf_clf.predict(X_scaled)[0])