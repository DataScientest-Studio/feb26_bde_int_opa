##############################################################################
#                        15m-Binance Kline Streamer                          #
##############################################################################
# This script connects to Binance's WebSocket API to stream live 15-minute kline 
# (candlestick) data for the BTCUSDT trading pair.
# It also backfills historical data on startup to ensure continuity. The data 
# is stored in a PostgreSQL database.
# Key features:
# - WebSocket connection with automatic reconnection and exponential backoff
# - Graceful shutdown handling
# - Backfill of historical data on startup
# - Robust error handling and logging
# - Configurable via environment variables for database connection
##############################################################################
import json
import time
import signal
import logging
import psycopg2
import websocket
from datetime import datetime, timezone
import os
import pandas as pd
import requests

# ---------------- CONFIG ----------------
WS_URL = "wss://stream.binance.com:9443/ws/btcusdt@kline_15m"
REST_URL = "https://api.binance.com/api/v3/klines"

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD")
}

RECONNECT_DELAY = 5
MAX_RECONNECT_DELAY = 60
SYMBOL = "BTCUSDT"
INTERVAL = "15m"
MAX_BACKFILL_LIMIT = 100

ws = None
running = True
last_message_time = time.time()
ws_connected = False
conn = None
cur = None

logging.basicConfig(level=logging.INFO)


# ---------------- DB ----------------
def get_conn():
    return psycopg2.connect(connect_timeout=5, **DB_CONFIG)

def wait_for_db(max_retries=10, delay=2):
    for attempt in range(max_retries):
        try:
            conn = get_conn()
            logging.info("DB is ready")
            return conn
        except Exception as e:
            logging.warning(f"DB not ready (attempt {attempt+1}): {e}")
            time.sleep(delay)

    raise RuntimeError("Database not reachable after retries")

def init_db(cur):

    cur.execute("""
        CREATE TABLE IF NOT EXISTS klines_15m (
            symbol TEXT,
            open_time TIMESTAMPTZ,
            open DOUBLE PRECISION,
            high DOUBLE PRECISION,
            low DOUBLE PRECISION,
            close DOUBLE PRECISION,
            volume DOUBLE PRECISION,
            close_time TIMESTAMPTZ,
            quote_asset_volume DOUBLE PRECISION,
            trades INT,
            taker_base DOUBLE PRECISION,
            taker_quote DOUBLE PRECISION,
            PRIMARY KEY (open_time)
        );
    """)


def insert_kline(k, cur):

    cur.execute("""
        INSERT INTO klines_15m (symbol, open_time, open, high, low, close, volume, close_time, quote_asset_volume, trades, taker_base, taker_quote)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (open_time) DO NOTHING;
    """, (
        k["s"],
        datetime.fromtimestamp(k["t"] / 1000, tz=timezone.utc),
        k["o"],
        k["h"],
        k["l"],
        k["c"],
        k["v"],
        datetime.fromtimestamp(k["T"] / 1000, tz=timezone.utc),
        k["q"],   
        k["n"],   
        k["V"],   
        k["Q"]    
    ))

    cur.connection.commit()


# ---------------- BACKFILL (run once) ----------------
def get_last_timestamp(cur):

    cur.execute("SELECT MAX(open_time) FROM klines_15m;")
    result = cur.fetchone()[0]

    if result:
        return int(result.timestamp() * 1000)
    return None

def backfill(cur):

    logging.info("Starting backfill...")

    last_ts = get_last_timestamp(cur)

    params = {
        "symbol": SYMBOL,
        "interval": INTERVAL,
        "limit": MAX_BACKFILL_LIMIT
    }

    if last_ts:
        params["startTime"] = last_ts + 1

    response = requests.get(REST_URL, params=params)

    if response.status_code != 200:
        logging.error(f"Backfill failed: {response.text}")
        return

    data = response.json()

    if not isinstance(data, list):
        logging.error(f"Unexpected response: {data}")
        return

    for k in data:
        insert_kline({
            "s": SYMBOL,
            "t": k[0],
            "o": k[1],
            "h": k[2],
            "l": k[3],
            "c": k[4],
            "v": k[5],
            "T": k[6],
            "q": k[7],   
            "n": k[8],   
            "V": k[9],   
            "Q": k[10]   
        }, cur)


    if data:
        logging.info(f"Backfilled {len(data)} candles")
    else:
        logging.info("No backfill needed")


    logging.info("Backfill done")


# ---------------- WEBSOCKET CALLBACKS ----------------
def on_open(ws):
    global ws_connected, reconnect_delay
    ws_connected = True
    reconnect_delay = RECONNECT_DELAY
    logging.info("WS connected")


def on_close(ws, *args):
    global ws_connected
    ws_connected = False
    logging.warning("WS closed")


def on_error(ws, error):
    logging.error(f"WS error: {error}")


def on_message(ws, message):
    global last_message_time, cur

    last_message_time = time.time()

    try:
        data = json.loads(message)

        k = data["k"]

        # only closed candles
        if not k["x"]:
            return

        insert_kline(k,cur)
        logging.info(f"Saved candle {k['t']}")

    except Exception as e:
        logging.error(f"Message processing error: {e}")




# ---------------- GRACEFUL SHUTDOWN ----------------
def stop_handler(sig, frame):
    global running, ws, conn, cur
    logging.info("Shutting down...")
    running = False

    if ws is not None:
        ws.close()
        cur.close()
        conn.close()

signal.signal(signal.SIGINT, stop_handler)


# ---------------- MAIN LOOP ----------------
def run():
    
    global ws_connected, last_message_time, reconnect_delay
    global conn, cur, ws

    logging.info("Connecting to database...")

    conn = wait_for_db()
    cur = conn.cursor()

    init_db(cur)
    conn.commit()

    backfill(cur)

    reconnect_delay = RECONNECT_DELAY

    while running:
        ws_connected = False
        last_message_time = time.time()

        ws = websocket.WebSocketApp(
            WS_URL,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close
        )

        logging.info("Starting WebSocket...")

        try:
            ws.run_forever(ping_interval=20, ping_timeout=10)

        except Exception as e:
            logging.error(f"WS crash: {e}")

        # reconnect logic with exponential backoff
        logging.warning(f"Reconnecting in {reconnect_delay}s...")
        time.sleep(reconnect_delay)

        reconnect_delay = min(reconnect_delay * 2, MAX_RECONNECT_DELAY)


# ---------------- ENTRY ----------------
if __name__ == "__main__":
    run()