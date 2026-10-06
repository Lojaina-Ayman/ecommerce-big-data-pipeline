CREATE OR REPLACE VIEW ecommerce.v_funnel_daily AS
SELECT
    event_date,
    sumIf(events, event_type = 'view')     AS views,
    sumIf(events, event_type = 'cart')     AS carts,
    sumIf(events, event_type = 'purchase') AS purchases,
    round(carts     / nullIf(views, 0), 4) AS view_to_cart,
    round(purchases / nullIf(carts, 0), 4) AS cart_to_purchase,
    round(purchases / nullIf(views, 0), 4) AS view_to_purchase
FROM ecommerce.daily_funnel
GROUP BY event_date;

CREATE OR REPLACE VIEW ecommerce.v_revenue_daily AS
SELECT
    event_date,
    sum(events)  AS purchases,
    sum(revenue) AS purchase_revenue,
    round(toFloat64(purchase_revenue) / nullIf(purchases, 0), 2) AS avg_purchase_value
FROM ecommerce.daily_funnel
WHERE event_type = 'purchase'
GROUP BY event_date;

CREATE OR REPLACE VIEW ecommerce.v_hourly_activity AS
SELECT
    event_date, hour_of_day, day_of_week,
    sumIf(events, event_type = 'view')     AS views,
    sumIf(events, event_type = 'cart')     AS carts,
    sumIf(events, event_type = 'purchase') AS purchases,
    sum(events)                            AS total_events,
    sumIf(revenue, event_type = 'purchase') AS purchase_revenue
FROM ecommerce.hourly_activity
GROUP BY event_date, hour_of_day, day_of_week;

CREATE OR REPLACE VIEW ecommerce.v_cart_abandonment_daily AS
SELECT
    toDate(fe) AS event_date,
    countIf(c > 0)             AS cart_sessions,
    countIf(c > 0 AND p = 0)   AS abandoned_sessions,
    round(abandoned_sessions / nullIf(cart_sessions, 0), 4) AS abandonment_rate
FROM
(
    SELECT user_session, min(first_event) AS fe, sum(carts) AS c, sum(purchases) AS p
    FROM ecommerce.session_state
    GROUP BY user_session
)
GROUP BY event_date;

CREATE OR REPLACE VIEW ecommerce.v_session_funnel AS
SELECT
    count()           AS sessions,
    countIf(v > 0)    AS sessions_with_view,
    countIf(c > 0)    AS sessions_with_cart,
    countIf(p > 0)    AS sessions_with_purchase,
    round(sessions_with_cart     / nullIf(sessions_with_view, 0), 4) AS session_view_to_cart,
    round(sessions_with_purchase / nullIf(sessions_with_cart, 0), 4) AS session_cart_to_purchase
FROM
(
    SELECT user_session, sum(views) AS v, sum(carts) AS c, sum(purchases) AS p
    FROM ecommerce.session_state
    GROUP BY user_session
);

CREATE OR REPLACE VIEW ecommerce.v_kpi_summary AS
SELECT
    f.total_events        AS total_events,
    f.views               AS views,
    f.carts               AS carts,
    f.purchases           AS purchases,
    f.purchase_revenue    AS total_revenue,
    u.unique_users        AS unique_users,
    u.unique_sessions     AS unique_sessions,
    b.buyers              AS buyers,
    round(f.carts     / nullIf(f.views, 0), 4) AS view_to_cart,
    round(f.purchases / nullIf(f.carts, 0), 4) AS cart_to_purchase,
    round(f.purchases / nullIf(f.views, 0), 4) AS view_to_purchase,
    round(s.session_events / nullIf(u.unique_sessions, 0), 2) AS events_per_session,
    round(f.purchases / nullIf(u.unique_users, 0), 4)         AS purchases_per_user,
    round(f.purchases / nullIf(b.buyers, 0), 2)               AS purchases_per_buyer,
    a.cart_sessions       AS cart_sessions,
    a.abandoned_sessions  AS abandoned_sessions,
    round(a.abandoned_sessions / nullIf(a.cart_sessions, 0), 4) AS cart_abandonment_rate
FROM
    (SELECT sum(events) AS total_events,
            sumIf(events, event_type = 'view')     AS views,
            sumIf(events, event_type = 'cart')     AS carts,
            sumIf(events, event_type = 'purchase') AS purchases,
            sumIf(revenue, event_type = 'purchase') AS purchase_revenue
     FROM ecommerce.daily_funnel) AS f
CROSS JOIN (SELECT uniqExactMerge(users) AS unique_users, uniqExactMerge(sessions) AS unique_sessions
            FROM ecommerce.daily_uniques) AS u
CROSS JOIN (SELECT uniqExactMerge(buyers) AS buyers FROM ecommerce.daily_buyers) AS b
CROSS JOIN (SELECT sum(events) AS session_events FROM ecommerce.session_state) AS s
CROSS JOIN (SELECT countIf(c > 0) AS cart_sessions, countIf(c > 0 AND p = 0) AS abandoned_sessions
            FROM (SELECT user_session, sum(carts) AS c, sum(purchases) AS p
                  FROM ecommerce.session_state GROUP BY user_session)) AS a;