-- Function
CREATE OR REPLACE FUNCTION notify_kline_insert()
RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('kline_update', NEW.open_time::text);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger
CREATE TRIGGER kline_insert_trigger
AFTER INSERT ON klines_15m
FOR EACH ROW
EXECUTE FUNCTION notify_kline_insert();

-- Function
CREATE OR REPLACE FUNCTION notify_new_feature()
RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('new_feature_table', NEW.open_time::text);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger
CREATE TRIGGER new_feature_trigger
AFTER INSERT ON klines_15m_features
FOR EACH STATEMENT
EXECUTE FUNCTION notify_new_feature();