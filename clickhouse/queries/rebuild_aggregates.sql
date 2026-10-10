TRUNCATE TABLE ecommerce.daily_funnel;
INSERT INTO ecommerce.daily_funnel
SELECT event_date, event_type, primary_category, count(), sumIf(price, event_type = 'purchase')
FROM ecommerce.ecommerce_events FINAL
GROUP BY event_date, event_type, primary_category;

TRUNCATE TABLE ecommerce.hourly_activity;
INSERT INTO ecommerce.hourly_activity
SELECT event_date, hour_of_day, day_of_week, event_type, count()
FROM ecommerce.ecommerce_events FINAL
GROUP BY event_date, hour_of_day, day_of_week, event_type;

TRUNCATE TABLE ecommerce.category_sales;
INSERT INTO ecommerce.category_sales
SELECT event_date, primary_category, sub_category, count(), sum(price)
FROM ecommerce.ecommerce_events FINAL
WHERE event_type = 'purchase'
GROUP BY event_date, primary_category, sub_category;

TRUNCATE TABLE ecommerce.brand_sales;
INSERT INTO ecommerce.brand_sales
SELECT event_date, brand, count(), sum(price)
FROM ecommerce.ecommerce_events FINAL
WHERE event_type = 'purchase'
GROUP BY event_date, brand;

TRUNCATE TABLE ecommerce.daily_uniques;
INSERT INTO ecommerce.daily_uniques
SELECT event_date, uniqExactState(user_id), uniqExactState(nullIf(user_session, ''))
FROM ecommerce.ecommerce_events FINAL
GROUP BY event_date;

TRUNCATE TABLE ecommerce.daily_buyers;
INSERT INTO ecommerce.daily_buyers (event_date, unique_buyers) SELECT event_date, uniqCombined64State(user_id)
FROM ecommerce.ecommerce_events FINAL
WHERE event_type = 'purchase'
GROUP BY event_date;

TRUNCATE TABLE ecommerce.session_state;
INSERT INTO ecommerce.session_state
SELECT user_session, max(user_id), min(event_time), max(event_time), count(),
       countIf(event_type = 'view'), countIf(event_type = 'cart'), countIf(event_type = 'purchase'),
       sumIf(price, event_type = 'purchase')
FROM ecommerce.ecommerce_events FINAL
WHERE user_session != ''
GROUP BY user_session;


