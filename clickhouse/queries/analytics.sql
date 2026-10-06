-- A1. Event-count funnel (whole period)
SELECT views, carts, purchases, view_to_cart, cart_to_purchase, view_to_purchase, total_revenue
FROM ecommerce.v_kpi_summary;

-- A2. Ordered session funnel (drill-down; scans the raw table)
SELECT countIf(lvl >= 1) AS reached_view, countIf(lvl >= 2) AS then_cart, countIf(lvl >= 3) AS then_purchase
FROM (SELECT user_session,
             windowFunnel(86400)(event_time, event_type = 'view', event_type = 'cart', event_type = 'purchase') AS lvl
      FROM ecommerce.ecommerce_events WHERE user_session != '' GROUP BY user_session);

-- B1. Revenue by day (purchase events only)
SELECT event_date, purchases, purchase_revenue, avg_purchase_value
FROM ecommerce.v_revenue_daily ORDER BY event_date;

-- B2. Revenue by hour of day
SELECT hour_of_day, sum(purchases) AS purchases, sum(purchase_revenue) AS hourly_revenue
FROM ecommerce.v_hourly_activity GROUP BY hour_of_day ORDER BY hour_of_day;

-- B3. Revenue by weekday (1 = Monday)
SELECT day_of_week, sum(total_events) AS all_events, sum(purchase_revenue) AS weekday_revenue
FROM ecommerce.v_hourly_activity GROUP BY day_of_week ORDER BY day_of_week;

-- B4. Top 10 brands by revenue ('unknown' = brand missing in the data)
SELECT brand, sum(purchases) AS purchases, sum(revenue) AS brand_revenue
FROM ecommerce.brand_sales GROUP BY brand ORDER BY brand_revenue DESC LIMIT 10;

-- B5. Category revenue contribution
SELECT primary_category, cat_revenue, round(100 * cat_revenue / sum(cat_revenue) OVER (), 2) AS share_pct
FROM (SELECT primary_category, toFloat64(sum(revenue)) AS cat_revenue
      FROM ecommerce.category_sales GROUP BY primary_category)
ORDER BY cat_revenue DESC;

-- B6. Conversion by category (min. 100 views to avoid noise)
SELECT primary_category,
       sumIf(events, event_type = 'view')     AS views,
       sumIf(events, event_type = 'purchase') AS purchases,
       round(purchases / nullIf(views, 0), 4) AS view_to_purchase
FROM ecommerce.daily_funnel
GROUP BY primary_category HAVING views >= 100 ORDER BY view_to_purchase DESC LIMIT 15;

-- C1. Users, sessions, events per session, purchases per user / per buyer
SELECT unique_users, unique_sessions, buyers, events_per_session, purchases_per_user, purchases_per_buyer
FROM ecommerce.v_kpi_summary;

-- C2. Repeat buyers
SELECT count() AS buyers, countIf(n >= 2) AS repeat_buyers,
       round(countIf(n >= 2) / count(), 4) AS repeat_rate, round(avg(n), 2) AS avg_purchases, max(n) AS max_purchases
FROM (SELECT user_id, count() AS n FROM ecommerce.ecommerce_events WHERE event_type = 'purchase' GROUP BY user_id);

-- C3. Observed customer value: top 10 buyers (31-day history, NOT a lifetime-value forecast)
SELECT user_id, count() AS purchases, sum(price) AS spent,
       min(event_time) AS first_purchase, max(event_time) AS last_purchase
FROM ecommerce.ecommerce_events WHERE event_type = 'purchase'
GROUP BY user_id ORDER BY spent DESC LIMIT 10;

-- D1. Cart abandonment per day (session level)
SELECT * FROM ecommerce.v_cart_abandonment_daily ORDER BY event_date;

-- D2. Cart abandonment, user level (compare with the session-level rate)
SELECT countIf(c > 0) AS cart_users, countIf(c > 0 AND p = 0) AS abandoned_users,
       round(abandoned_users / nullIf(cart_users, 0), 4) AS user_level_rate
FROM (SELECT user_id, countIf(event_type = 'cart') AS c, countIf(event_type = 'purchase') AS p
      FROM ecommerce.ecommerce_events GROUP BY user_id);

-- E1. Bounce rate (sessions with exactly one event)
SELECT count() AS sessions, countIf(e = 1) AS single_event_sessions,
       round(countIf(e = 1) / count(), 4) AS bounce_rate
FROM (SELECT user_session, sum(events) AS e FROM ecommerce.session_state GROUP BY user_session);

-- E2. Hourly velocity with a rolling 3-row average (on the sample, hours are not contiguous)
SELECT event_hour, n_events, round(avg(n_events) OVER (ORDER BY event_hour ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 1) AS rolling_3h
FROM (SELECT event_hour, count() AS n_events FROM ecommerce.ecommerce_events GROUP BY event_hour)
ORDER BY event_hour;