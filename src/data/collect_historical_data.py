import requests
import pandas as pd
import time
from datetime import datetime, timedelta, timezone

BASE_URL = "https://api.binance.com/api/v3/klines"

SYMBOL = "BTCUSDT"      
INTERVAL = "15m"
LIMIT = 1000            # max allowed by Binance

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
    "close_time", "quote_asset_volume", "number_of_trades",
    "taker_buy_base", "taker_buy_quote", "ignore"
]

df = pd.DataFrame(all_data, columns=columns)

# Convert timestamps
df["open_time"] = pd.to_datetime(df["open_time"], unit='ms')
df["close_time"] = pd.to_datetime(df["close_time"], unit='ms')

# Drop last column 'ignore'
df.drop(columns=["ignore"], inplace=True)

# Save to CSV
filename = f"{SYMBOL}_{INTERVAL}_3years.csv"
df.to_csv(filename, index=False)

print(f"Saved {len(df)} rows to {filename}")

