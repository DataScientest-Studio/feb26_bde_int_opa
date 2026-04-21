import requests
import pandas as pd
import time
from datetime import datetime, timedelta, timezone
import psycopg2
from psycopg2.extras import execute_values
import os
import logging

BASE_URL = "https://api.binance.com/api/v3/klines"

SYMBOL = "BTCUSDT"      
INTERVAL = "15m"
LIMIT = 1000            # max allowed by Binance

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD")
}

############################# Extract and collect historical data from Binance API #############################
# 3 years ago timestamp
end_time = datetime.now(timezone.utc)
start_time = end_time - timedelta(days=3*365)

start_ts = int(start_time.timestamp() * 1000)
end_ts = int(end_time.timestamp() * 1000)

all_data = []

print(f"Downloading {SYMBOL} {INTERVAL} data from {start_time} to {end_time}")

while start_ts < end_ts:
    params = {
        "symbol": SYMBOL,
        "interval": INTERVAL,
        "limit": LIMIT,
        "startTime": start_ts,
        "endTime": end_ts
    }

    response = requests.get(BASE_URL, params=params)
    data = response.json()

    if not data:
        break

    all_data.extend(data)

    # Move start forward to last candle + 1 ms
    last_open_time = data[-1][0]
    start_ts = last_open_time + 1

    print(f"Fetched up to {datetime.fromtimestamp(last_open_time / 1000)}")

    time.sleep(0.5)  # avoid rate limits

# Convert to DataFrame
columns = [
    "open_time", "open", "high", "low", "close", "volume",
    "close_time", "quote_asset_volume", "trades",
    "taker_base", "taker_quote", "ignore"
]

df = pd.DataFrame(all_data, columns=columns)

# Convert timestamps
df["open_time"] = pd.to_datetime(df["open_time"], unit='ms')
df["close_time"] = pd.to_datetime(df["close_time"], unit='ms')

# Drop last column 'ignore'
df.drop(columns=["ignore"], inplace=True)

############################# Load historical data into SQL Database #############################

# Establish database connection
conn = psycopg2.connect(connect_timeout=5, **DB_CONFIG)

conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

cur = conn.cursor()
logging.info("Connected to database")

# Insert data into database
cols = list(df.columns)

cur.execute("""
CREATE TABLE IF NOT EXISTS historical_klines_15m (
    open_time TIMESTAMPTZ PRIMARY KEY,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    close_time TIMESTAMPTZ,
    quote_asset_volume DOUBLE PRECISION,
    trades INT,
    taker_base DOUBLE PRECISION,
    taker_quote DOUBLE PRECISION
)
""")

query = f"""INSERT INTO historical_klines_15m ({",".join(cols)}) VALUES %s ON CONFLICT (open_time) DO NOTHING"""

execute_values(cur, query, df.values.tolist())

conn.commit()
logging.info(f"Inserted {len(df)} rows into historical_klines_15m")

cur.close()
conn.close()
logging.info("Database connection closed")  