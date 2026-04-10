import pandas as pd

df = pd.read_csv('BTCUSDT_15m_3years.csv')

feats = pd.DataFrame()

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

######## ADD CLEANING STEPS ########

# Drop rows where future_close is NaN (at the end of the dataset)
#feats = feats.dropna(subset=['future_close', 'future_return'])


features = [col for col in feats.columns if col not in ['future_close','future_return','trade_decision']]

ml_df = pd.DataFrame(columns=features + ['trade_decision']) 
ml_df[features] = feats[features]
ml_df['trade_decision'] = feats['trade_decision']

ml_df.to_csv("BTCUSDT_15m_ML_ready.csv", index=False)

print("ML-ready dataset saved as BTCUSDT_15m_ml_ready.csv")

