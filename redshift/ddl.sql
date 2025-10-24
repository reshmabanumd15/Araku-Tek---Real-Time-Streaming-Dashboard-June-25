
CREATE SCHEMA IF NOT EXISTS rt_demo;

CREATE TABLE IF NOT EXISTS public.orders_hourly_agg (
    hr TIMESTAMP,
    events BIGINT,
    amount_sum DOUBLE PRECISION
);
