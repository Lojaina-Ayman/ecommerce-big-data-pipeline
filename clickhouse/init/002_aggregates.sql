-- Funnel counts + revenue by day / event type / category. Revenue = PURCHASE events only.
CREATE TABLE IF NOT EXISTS ecommerce.daily_funnel
(
    event_date       Date,
    event_type       LowCardinality(String),
    primary_category LowCardinality(String),
    events           UInt64,
    revenue          Decimal(38, 2)
)
ENGINE = SummingMergeTree
ORDER BY (event_date, event_type, primary_category);

CREATE MATERIALIZED VIEW IF NOT EXISTS ecommerce.mv_daily_funnel TO ecommerce.daily_funnel AS
SELECT event_date, event_type, primary_category,
       count() AS events,
       sumIf(price, event_type = 'purchase') AS revenue
FROM ecommerce.ecommerce_events
GROUP BY event_date, event_type, primary_category;

-- Activity by hour of day and weekday
CREATE TABLE IF NOT EXISTS ecommerce.hourly_activity
(
    event_date  Date,
    hour_of_day UInt8,
    day_of_week UInt8,
    event_type  LowCardinality(String),
    events      UInt64
)
ENGINE = SummingMergeTree
ORDER BY (event_date, hour_of_day, day_of_week, event_type);

CREATE MATERIALIZED VIEW IF NOT EXISTS ecommerce.mv_hourly_activity TO ecommerce.hourly_activity AS
SELECT event_date, hour_of_day, day_of_week, event_type, count() AS events
FROM ecommerce.ecommerce_events
GROUP BY event_date, hour_of_day, day_of_week, event_type;

-- Purchases by category
CREATE TABLE IF NOT EXISTS ecommerce.category_sales
(
    event_date       Date,
    primary_category LowCardinality(String),
    sub_category     LowCardinality(String),
    purchases        UInt64,
    revenue          Decimal(38, 2)
)
ENGINE = SummingMergeTree
ORDER BY (event_date, primary_category, sub_category);

CREATE MATERIALIZED VIEW IF NOT EXISTS ecommerce.mv_category_sales TO ecommerce.category_sales AS
SELECT event_date, primary_category, sub_category,
       count() AS purchases, sum(price) AS revenue
FROM ecommerce.ecommerce_events
WHERE event_type = 'purchase'
GROUP BY event_date, primary_category, sub_category;

-- Purchases by brand
CREATE TABLE IF NOT EXISTS ecommerce.brand_sales
(
    event_date Date,
    brand      LowCardinality(String),
    purchases  UInt64,
    revenue    Decimal(38, 2)
)
ENGINE = SummingMergeTree
ORDER BY (event_date, brand);

CREATE MATERIALIZED VIEW IF NOT EXISTS ecommerce.mv_brand_sales TO ecommerce.brand_sales AS
SELECT event_date, brand, count() AS purchases, sum(price) AS revenue
FROM ecommerce.ecommerce_events
WHERE event_type = 'purchase'
GROUP BY event_date, brand;

-- Distinct users / sessions per day
CREATE TABLE IF NOT EXISTS ecommerce.daily_uniques
(
    event_date Date,
    users      AggregateFunction(uniqExact, UInt64),
    sessions   AggregateFunction(uniqExact, Nullable(String))
)
ENGINE = AggregatingMergeTree
ORDER BY event_date;

CREATE MATERIALIZED VIEW IF NOT EXISTS ecommerce.mv_daily_uniques TO ecommerce.daily_uniques AS
SELECT event_date,
       uniqExactState(user_id) AS users,
       uniqExactState(nullIf(user_session, '')) AS sessions
FROM ecommerce.ecommerce_events
GROUP BY event_date;

-- One row per session
CREATE TABLE IF NOT EXISTS ecommerce.session_state
(
    user_session String,
    user_id      SimpleAggregateFunction(max, UInt64),
    first_event  SimpleAggregateFunction(min, DateTime('UTC')),
    last_event   SimpleAggregateFunction(max, DateTime('UTC')),
    events       SimpleAggregateFunction(sum, UInt64),
    views        SimpleAggregateFunction(sum, UInt64),
    carts        SimpleAggregateFunction(sum, UInt64),
    purchases    SimpleAggregateFunction(sum, UInt64),
    revenue      SimpleAggregateFunction(sum, Decimal(38, 2))
)
ENGINE = AggregatingMergeTree
ORDER BY user_session;

CREATE MATERIALIZED VIEW IF NOT EXISTS ecommerce.mv_session_state TO ecommerce.session_state AS
SELECT user_session,
       max(user_id)    AS user_id,
       min(event_time) AS first_event,
       max(event_time) AS last_event,
       count()                          AS events,
       countIf(event_type = 'view')     AS views,
       countIf(event_type = 'cart')     AS carts,
       countIf(event_type = 'purchase') AS purchases,
       sumIf(price, event_type = 'purchase') AS revenue
FROM ecommerce.ecommerce_events
WHERE user_session != ''
GROUP BY user_session;
