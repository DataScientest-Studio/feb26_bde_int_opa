import psycopg2
import select
import pandas as pd
import os
import logging
from psycopg2.extras import execute_values

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD")
}

def compute_features(df):
    feats = pd.DataFrame()

    ################### open_time to datetime ###################
    feats['open_time'] = pd.to_datetime(df['open_time'], unit='ms', utc=True)
    ################### Basic price features ###################
    # Candle body and range
    feats['candle_body'] = df['close'] - df['open']       # Positive = bullish, Negative = bearish
    feats['candle_range'] = df['high'] - df['low']       # Total price movement in candle
    feats['upper_shadow'] = df['high'] - df[['close','open']].max(axis=1)
    feats['lower_shadow'] = df[['close','open']].min(axis=1) - df['low']


    ################### Returns and Momentum features ###################
    # Short-term returns
    feats['return_1'] = df['close'].pct_change(1)
    feats['return_3'] = df['close'].pct_change(3)
    feats['return_5'] = df['close'].pct_change(5)

    # Rolling momentum (mean of returns)
    feats['momentum_5'] = feats['return_1'].rolling(5).sum()
    feats['momentum_10'] = feats['return_1'].rolling(10).sum()


    ################### Trend indicator features (moving averages) ###################
    # Simple Moving Averages
    feats['ma5'] = df['close'].rolling(5).mean()
    feats['ma10'] = df['close'].rolling(10).mean()
    feats['ma20'] = df['close'].rolling(20).mean()
    feats['ma50'] = df['close'].rolling(50).mean()

    # Exponential Moving Averages
    feats['ema5'] = df['close'].ewm(span=5, adjust=False).mean()
    feats['ema10'] = df['close'].ewm(span=10, adjust=False).mean()
    feats['ema20'] = df['close'].ewm(span=20, adjust=False).mean()

    ################### Volatility features ###################
    # Rolling standard deviation of returns
    feats['volatility_5'] = feats['return_1'].rolling(5).std()
    feats['volatility_10'] = feats['return_1'].rolling(10).std()
    feats['volatility_20'] = feats['return_1'].rolling(20).std()

    # ATR (Average True Range)
    feats['tr'] = df[['high','low','close']].apply(lambda x: max(x['high']-x['low'],
                                                            abs(x['high']-x['close']),
                                                            abs(x['low']-x['close'])), axis=1)
    feats['atr_14'] = feats['tr'].rolling(14).mean()

    ################### Volume features ###################
    # Volume change and rolling averages
    feats['vol_change'] = df['volume'].pct_change()
    feats['vol_ma5'] = df['volume'].rolling(5).mean()
    feats['vol_ma10'] = df['volume'].rolling(10).mean()
    feats['vol_ma20'] = df['volume'].rolling(20).mean()

    ################### Trend strength features ###################
    # Difference between short-term and long-term moving averages
    feats['ma_diff_5_20'] = feats['ma5'] - feats['ma20']
    feats['ma_diff_10_50'] = feats['ma10'] - feats['ma50']

    # Price position relative to moving averages
    feats['close_vs_ma20'] = df['close'] - feats['ma20']
    feats['close_vs_ma50'] = df['close'] - feats['ma50']

    # Generate target variable (1 if next candle is bullish, 0 if bearish)
    n = 1  # number of periods to look ahead 
    # threshold = 0.002  # 0.2% price movement for buy/sell
    # make threshold a function of market volatility
    threshold = feats['atr_14'] / df['close'] 

    feats['future_close'] = df['close'].shift(-n)
    feats['future_return'] = (feats['future_close'] - df['close']) / df['close']

    # Define Buy/Hold/Sell target
    feats['trade_decision'] = 0  # default Hold
    feats.loc[feats['future_return'] > threshold, 'trade_decision'] = 1   # Buy
    feats.loc[feats['future_return'] < -threshold, 'trade_decision'] = -1 # Sell

    return feats


def rebuild_feature_table(conn, features):
    cur = conn.cursor()

    # 1. Ensure table exists (only created once)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS klines_15m_features (
                open_time TIMESTAMPTZ PRIMARY KEY,
                -- Price structure
                candle_body DOUBLE PRECISION,
                candle_range DOUBLE PRECISION,
                upper_shadow DOUBLE PRECISION,
                lower_shadow DOUBLE PRECISION,

                -- Returns & momentum
                return_1 DOUBLE PRECISION,
                return_3 DOUBLE PRECISION,
                return_5 DOUBLE PRECISION,
                momentum_5 DOUBLE PRECISION,
                momentum_10 DOUBLE PRECISION,

                -- Moving averages
                ma5 DOUBLE PRECISION,
                ma10 DOUBLE PRECISION,
                ma20 DOUBLE PRECISION,
                ma50 DOUBLE PRECISION,
                ema5 DOUBLE PRECISION,
                ema10 DOUBLE PRECISION,
                ema20 DOUBLE PRECISION,

                -- Volatility
                volatility_5 DOUBLE PRECISION,
                volatility_10 DOUBLE PRECISION,
                volatility_20 DOUBLE PRECISION,
                tr DOUBLE PRECISION,
                atr_14 DOUBLE PRECISION,

                -- Volume features
                vol_change DOUBLE PRECISION,
                vol_ma5 DOUBLE PRECISION,
                vol_ma10 DOUBLE PRECISION,
                vol_ma20 DOUBLE PRECISION,

                -- Trend strength
                ma_diff_5_20 DOUBLE PRECISION,
                ma_diff_10_50 DOUBLE PRECISION,
                close_vs_ma20 DOUBLE PRECISION,
                close_vs_ma50 DOUBLE PRECISION,

                -- Targets
                future_close DOUBLE PRECISION,
                future_return DOUBLE PRECISION,
                trade_decision INT
                );
    """)

    # 2. Clear table instead of dropping
    cur.execute("TRUNCATE klines_15m_features;")

    cols = list(features.columns)
    
    query = f"""
        INSERT INTO klines_15m_features ({",".join(cols)})
        VALUES %s
    """
    execute_values(cur, query, features.values.tolist())
    cur.execute("NOTIFY new_feature_table;")  # Notify that new features are available
    logging.info("Feature table rebuilt and new features inserted successfully")

## Establish connection and listen for updates to generate features from streaming data
conn = psycopg2.connect(connect_timeout=5, **DB_CONFIG)

conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

logging.info("Bootstrap run to avoid missing first candle")

df = pd.read_sql(
    "SELECT * FROM klines_15m ORDER BY open_time DESC LIMIT 100",
    conn
)

if not df.empty:
    df = df.sort_values('open_time')  # Ensure data is in chronological order
    features = compute_features(df)
    rebuild_feature_table(conn, features)

cur = conn.cursor()
cur.execute("LISTEN kline_update;")

logging.info("Listening for updates...")

while True:
    select.select([conn], [], [])
    conn.poll()

    while conn.notifies:
        notify = conn.notifies.pop(0)
        logging.info(f"Trigger received: {notify.payload}")

        if notify.channel == 'kline_update':
            df = pd.read_sql(
            "SELECT * FROM klines_15m ORDER BY open_time DESC LIMIT 100",
            conn
            )
        
            df = df.sort_values('open_time')  # Ensure data is in chronological order
        
            features = compute_features(df)

            rebuild_feature_table(conn, features)

        