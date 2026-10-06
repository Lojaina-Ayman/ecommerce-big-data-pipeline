SELECT 
    check_name,
    aggregate_value,
    raw_value,
    abs(aggregate_value - raw_value) < 0.005 AS ok
FROM (
    SELECT 'events_total' AS check_name,
           toFloat64((SELECT sum(events) FROM ecommerce.daily_funnel)) AS aggregate_value,
           toFloat64((SELECT count() FROM ecommerce.ecommerce_events FINAL)) AS raw_value
    UNION ALL
    SELECT 'hourly_events_total',
           toFloat64((SELECT sum(events) FROM ecommerce.hourly_activity)),
           toFloat64((SELECT count() FROM ecommerce.ecommerce_events FINAL))
    UNION ALL
    SELECT 'revenue_daily_funnel',
           toFloat64((SELECT sumIf(revenue, event_type = 'purchase') FROM ecommerce.daily_funnel)),
           toFloat64((SELECT sumIf(price, event_type = 'purchase') FROM ecommerce.ecommerce_events FINAL))
    UNION ALL
    SELECT 'revenue_hourly',
           toFloat64((SELECT sumIf(revenue, event_type = 'purchase') FROM ecommerce.hourly_activity)),
           toFloat64((SELECT sumIf(price, event_type = 'purchase') FROM ecommerce.ecommerce_events FINAL))
    UNION ALL
    SELECT 'revenue_by_category',
           toFloat64((SELECT sum(revenue) FROM ecommerce.category_sales)),
           toFloat64((SELECT sumIf(price, event_type = 'purchase') FROM ecommerce.ecommerce_events FINAL))
    UNION ALL
    SELECT 'revenue_by_brand',
           toFloat64((SELECT sum(revenue) FROM ecommerce.brand_sales)),
           toFloat64((SELECT sumIf(price, event_type = 'purchase') FROM ecommerce.ecommerce_events FINAL))
    UNION ALL
    SELECT 'unique_users',
           toFloat64((SELECT uniqExactMerge(users) FROM ecommerce.daily_uniques)),
           toFloat64((SELECT uniqExact(user_id) FROM ecommerce.ecommerce_events FINAL))
    UNION ALL
    SELECT 'unique_sessions',
           toFloat64((SELECT uniqExactMerge(sessions) FROM ecommerce.daily_uniques)),
           toFloat64((SELECT uniqExactIf(user_session, user_session != '') FROM ecommerce.ecommerce_events FINAL))
    UNION ALL
    SELECT 'buyers',
           toFloat64((SELECT uniqExactMerge(buyers) FROM ecommerce.daily_buyers)),
           toFloat64((SELECT uniqExactIf(user_id, event_type = 'purchase') FROM ecommerce.ecommerce_events FINAL))
    UNION ALL
    SELECT 'session_state_events',
           toFloat64((SELECT sum(events) FROM ecommerce.session_state)),
           toFloat64((SELECT countIf(user_session != '') FROM ecommerce.ecommerce_events FINAL))
);