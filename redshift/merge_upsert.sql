
-- Example upsert from staging to final aggregate table (adjust as needed)
CREATE TABLE IF NOT EXISTS rt_demo.orders_hourly_final (
    hr TIMESTAMP PRIMARY KEY,
    events BIGINT,
    amount_sum DOUBLE PRECISION
);

MERGE INTO rt_demo.orders_hourly_final f
USING public.orders_hourly_agg s
ON f.hr = s.hr
WHEN MATCHED THEN UPDATE SET events = s.events, amount_sum = s.amount_sum
WHEN NOT MATCHED THEN INSERT (hr, events, amount_sum) VALUES (s.hr, s.events, s.amount_sum);
