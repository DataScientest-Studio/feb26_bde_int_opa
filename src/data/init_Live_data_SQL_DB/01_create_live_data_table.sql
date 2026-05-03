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