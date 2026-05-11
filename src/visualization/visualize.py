import streamlit as st
import requests
import pandas as pd
import psycopg2
import os
import plotly.graph_objects as go
from streamlit_autorefresh import st_autorefresh

API_URL = "http://ML_prediction:8000/latest"

# Auto-refresh every 15 minutes (900,000 ms)
st_autorefresh(interval=15 * 60 * 1000, key="refresh")

st.title("📊 OPA Trading Dashboard")

st.caption(
    "BTC/USDT • Binance • 15-minute candles • ML-powered trading signals"
)

# ---------------------------
# ML Model
# ---------------------------
st.subheader("ML Model")

st.info("ML-Model : Random Forest Classifier")

st.markdown(
    """
    The ML model analyzes the latest market data and generates trading signals (Buy, Sell, Hold) for the next candle. 
    It is trained on historical 15-minute price data and technical indicators to make informed predictions.
    """
)


# ---------------------------
# SIGNAL DISPLAY
# ---------------------------
st.subheader("Trading Signal")

placeholder = st.empty()

try:
    response = requests.get(API_URL)

    if response.status_code == 200:
        data = response.json()

        signal = data["The trading decision for the next candle is "]

    else:
        placeholder.error("API error or unavailable")

except Exception as e:
    placeholder.error(str(e))


if signal == "buy":
    placeholder.success("The trading recommendation for the upcoming candle is: 🟢 BUY")
elif signal == "sell":
    placeholder.error("The trading recommendation for the upcoming candle is: 🔴 SELL")
elif signal == "hold":
    placeholder.warning("The trading recommendation for the upcoming candle is: ⚪ HOLD")
else:   
    placeholder.info("No signal available")

# ---------------------------
# PRICE CHART 
# ---------------------------
st.subheader("Market Overview")

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD")
    }   

try:
    conn = psycopg2.connect(connect_timeout=5, **DB_CONFIG)

    query = """
        SELECT
            open_time,
            open,
            high,
            low,
            close
        FROM klines_15m
        ORDER BY open_time DESC
        LIMIT 50
    """

    df = pd.read_sql_query(query, conn)

    conn.close()

    if df.empty:
        st.warning("No market data available yet")

    else:
        # Sort oldest → newest
        df = df.sort_values("open_time")

        # Convert timestamp
        df["open_time"] = pd.to_datetime(df["open_time"])

        # Create candlestick chart
        fig = go.Figure(data=[go.Candlestick(
            x=df["open_time"],
            open=df["open"].astype(float),
            high=df["high"].astype(float),
            low=df["low"].astype(float),
            close=df["close"].astype(float),
            increasing_line_color='green',
            decreasing_line_color='red'
        )])

        fig.update_layout(
            xaxis_title="Time",
            yaxis_title="Price",
            xaxis_rangeslider_visible=False,
            height=500
        )

        st.plotly_chart(fig, use_container_width=True)

except Exception as e:
    st.error(f"Error loading candlestick chart: {e}")

# ---------------------------
# LAST UPDATE
# ---------------------------
st.caption("Auto-refresh every 15 minutes (1 candle)")