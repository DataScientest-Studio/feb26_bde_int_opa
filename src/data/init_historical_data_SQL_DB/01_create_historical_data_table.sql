CREATE TABLE IF NOT EXISTS historical_klines_15m (
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