# Apache Superset Dashboard

Connection: `clickhousedb://superset_ro:***@clickhouse:8123/ecommerce` (clickhouse-connect, HTTP, Docker service name).
User `superset_ro`: SELECT only, 60 s / 2 GB per query. Metric naming rule: all metrics are prefixed `m_`
(prevents ClickHouse alias/column clashes inside aggregates).

## Charts
| Chart | Business question | Dataset | Dimension | Metric | Filters | Logic | Why this visual |
|---|---|---|---|---|---|---|---|
| KPI cards (11) | How is the shop doing overall? | daily_funnel, events_drilldown, v_cart_abandonment_daily | - | m_events, m_revenue, m_users, m_sessions, m_views, m_carts, m_purchases, 3 rates, abandonment rate | date (+ event type, category, brand where available) | Rates = ratio of SUMMED counts. Revenue = purchase events only | Single numbers scan fastest |
| Events Over Time | Is traffic growing or dropping? Per event type? | daily_funnel | event_date, event_type | m_events | date, event type, category | SUM(events) by day | Trend over time |
| Conversion Funnel | Where do customers drop off? | daily_funnel | event_type | m_events | date, event type, category | event counts V -> C -> P | Funnels show drop-off proportionally |
| Revenue Over Time | How does revenue evolve? | daily_funnel | event_date | m_revenue | date, category | sum(revenue) of purchase events | Trend of money |
| Revenue by Category | Which categories earn most? | category_sales | primary_category, sub_category | m_revenue | date, categories | SUM(revenue) | Treemap shows part-to-whole across a hierarchy |
| Top Brands | Which brands earn most? | brand_sales | brand | m_revenue | date, brand | top 10, brand != 'unknown' | Ranked bars compare magnitudes |
| Activity by Hour | When are customers active? | v_hourly_activity | hour_of_day | m_events | date | SUM(total_events) | Distribution over a 24 h cycle |
| Activity Heatmap | Which weekday x hour is busiest? | v_hourly_activity | day_of_week x hour_of_day | m_avg_events_per_day | date | events / distinct dates | Two-dimensional pattern |
| Activity by Weekday | Which weekdays are busiest? | v_hourly_activity | day_of_week | m_avg_events_per_day | date | events / distinct dates (fair across 4 vs 5 occurrences) | Normalised comparison |
| Cart Abandonment | How many carts never convert? | v_cart_abandonment_daily | event_date | m_abandonment_rate | date | sessions with cart and no purchase / sessions with cart | Rate over time |
| Event Type Distribution | What share is view / cart / purchase? | daily_funnel | event_type | m_events | date, event type, category | SUM(events) | Few categories: pie is readable |
| Pipeline Health | Is the data trustworthy? | v_dq_latest | check_name | - | - | latest status per check from Airflow | Operational transparency |

## Filter scope
| Filter ↓ / Chart → | KPIs (daily_funnel) | Unique users/sessions | Funnel, events, pie | Revenue over time | Category treemap | Top brands | Hourly / weekday / heatmap | Abandonment |
|---|---|---|---|---|---|---|---|---|
| Date | ✔ | ✔ | ✔ | ✔ | ✔ | ✔ | ✔ | ✔ |
| Event type | ✔ | ✔ | ✔ | – | – | – | – | – |
| Primary category | ✔ | ✔ | ✔ | ✔ | ✔ | – | – | – |
| Sub category | – | ✔ | – | – | ✔ | – | – | – |
| Brand | – | ✔ | – | – | – | ✔ | – | – |

*(Note: "-" cells indicate that the underlying aggregate table does not carry that dimension by design).*