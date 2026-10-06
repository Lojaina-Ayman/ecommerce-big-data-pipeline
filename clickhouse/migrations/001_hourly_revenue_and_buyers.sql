ALTER TABLE ecommerce.hourly_activity ADD COLUMN IF NOT EXISTS revenue Decimal(38, 2);

DROP VIEW IF EXISTS ecommerce.mv_hourly_activity;

CREATE MATERIALIZED VIEW ecommerce.mv_hourly_activity TO ecommerce.hourly_activity AS
SELECT event_date, hour_of_day, day_of_week, event_type,
       count() AS events,
       sumIf(price, event_type = 'purchase') AS revenue
FROM ecommerce.ecommerce_events
GROUP BY event_date, hour_of_day, day_of_week, event_type;

TRUNCATE TABLE ecommerce.hourly_activity;
INSERT INTO ecommerce.hourly_activity
SELECT event_date, hour_of_day, day_of_week, event_type,
       count(), sumIf(price, event_type = 'purchase')
FROM ecommerce.ecommerce_events FINAL
GROUP BY event_date, hour_of_day, day_of_week, event_type;

CREATE TABLE IF NOT EXISTS ecommerce.daily_buyers
(
    event_date Date,
    buyers     AggregateFunction(uniqExact, UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY event_date;

CREATE MATERIALIZED VIEW IF NOT EXISTS ecommerce.mv_daily_buyers TO ecommerce.daily_buyers AS
SELECT event_date, uniqExactState(user_id) AS buyers
FROM ecommerce.ecommerce_events
WHERE event_type = 'purchase'
GROUP BY event_date;

TRUNCATE TABLE ecommerce.daily_buyers;
INSERT INTO ecommerce.daily_buyers
SELECT event_date, uniqExactState(user_id)
FROM ecommerce.ecommerce_events FINAL
WHERE event_type = 'purchase'
GROUP BY event_date;