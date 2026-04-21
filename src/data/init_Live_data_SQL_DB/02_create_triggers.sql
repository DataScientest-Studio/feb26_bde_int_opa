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