CREATE VIEW events AS
SELECT *
FROM file('__FILE__', 'CSVWithNames',
  'event_time String, event_type String, product_id String, category_id String,
   category_code String, brand String, price String, user_id String, user_session String');

-- 1. Size and cardinality
SELECT count() AS total_rows,
       uniqExact(user_id)      AS unique_users,
       uniqExact(user_session) AS unique_sessions,
       uniqExact(product_id)   AS unique_products
FROM events;

-- 2. Event types
SELECT event_type,
       count() AS events,
       round(100 * count() / (SELECT count() FROM events), 2) AS percent
FROM events GROUP BY event_type ORDER BY events DESC;

-- 3. Time range
SELECT min(parseDateTimeBestEffortOrNull(event_time)) AS first_event,
       max(parseDateTimeBestEffortOrNull(event_time)) AS last_event,
       uniqExact(toDate(parseDateTimeBestEffortOrNull(event_time))) AS distinct_days
FROM events;

-- 4. Data quality counters
SELECT
  countIf(parseDateTimeBestEffortOrNull(event_time) IS NULL) AS bad_timestamp,
  countIf(event_type NOT IN ('view','cart','purchase'))      AS invalid_event_type,
  countIf(product_id = '')    AS null_product_id,
  countIf(category_id = '')   AS null_category_id,
  countIf(category_code = '') AS null_category_code,
  countIf(brand = '')         AS null_brand,
  countIf(user_id = '')       AS null_user_id,
  countIf(user_session = '')  AS null_user_session,
  countIf(toFloat64OrNull(price) IS NULL)  AS unparsable_price,
  countIf(toFloat64OrNull(price) < 0)      AS negative_price,
  countIf(toFloat64OrNull(price) = 0)      AS zero_price
FROM events;

-- 5. Price statistics per event type
SELECT event_type,
       min(toFloat64OrNull(price))              AS min_price,
       round(avg(toFloat64OrNull(price)), 2)    AS avg_price,
       round(quantile(0.5)(toFloat64OrNull(price)), 2) AS median_price,
       max(toFloat64OrNull(price))              AS max_price,
       round(sumIf(toFloat64OrNull(price), event_type = 'purchase'), 2) AS purchase_revenue
FROM events GROUP BY event_type ORDER BY event_type;

-- 6. Depth of category_code
SELECT if(category_code = '', 0, length(splitByChar('.', category_code))) AS levels,
       count() AS events
FROM events GROUP BY levels ORDER BY levels;

-- 7. Duplicate events
SELECT count() AS rows_total,
       uniqExact(cityHash64(event_time, event_type, product_id, user_id, user_session)) AS distinct_rows,
       count() - uniqExact(cityHash64(event_time, event_type, product_id, user_id, user_session)) AS duplicate_rows
FROM events;