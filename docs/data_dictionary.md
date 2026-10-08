# Data dictionary

## Source CSV (`2019-Oct.csv`)

| Column | Meaning |
|---|---|
| event_time | UTC time, format `2019-10-01 00:00:00 UTC` |
| event_type | view, cart, purchase |
| product_id, category_id | Product and category identifiers |
| category_code | Dotted hierarchy (e.g. `electronics.audio.headphone`), often empty |
| brand | Lower-case brand, often empty |
| price | Item price (no quantity column: one purchase event = one unit) |
| user_id, user_session | User and session identifiers |

## `ecommerce_events`

| Column | Type | Derivation |
|---|---|---|
| event_time | DateTime UTC CODEC(DoubleDelta, ZSTD(1)) | parsed from the source |
| event_date, event_hour, hour_of_day | Date, DateTime CODEC(DoubleDelta, ZSTD(1)), UInt8 | from event_time |
| day_of_week | UInt8 | ISO, 1 = Monday |
| event_type | LowCardinality(String) | lower-cased, validated |
| product_id, category_id, user_id | UInt64, UInt64, UInt64 CODEC(ZSTD(1)) | `category_id` 0 = unknown |
| category_code | String | '' = unknown |
| primary_category, sub_category | LowCardinality(String) | first and second level of category_code, `unknown` if absent |
| category_depth | UInt8 | number of levels (0 if none) |
| brand | LowCardinality(String) | `unknown` if absent |
| price | Decimal(10,2) | 0 if missing (see dq_flags) |
| user_session | String CODEC(ZSTD(3)) | '' = unknown |
| dq_flags | Array(String) | soft problems of kept rows |
| kafka_partition, kafka_offset | UInt8, UInt64 CODEC(Delta, ZSTD(1)) | traceability |
| ingested_at | DateTime CODEC(DoubleDelta, ZSTD(1)) | write time, version column of the ReplacingMergeTree |

## Reject reasons (`events_quarantine.reject_reason`)

malformed_json, invalid_timestamp, invalid_event_type, missing_product_id, invalid_product_id, missing_user_id, invalid_user_id,
invalid_category_id, invalid_price, negative_price, missing_price_on_purchase.

## Flags (`dq_flags`)

missing_user_session, missing_brand, missing_category_code, missing_price, zero_price.

## KPI definitions

| KPI | Definition |
|---|---|
| Views / Carts / Purchases | counts of event_type |
| View to Cart, Cart to Purchase, View to Purchase | carts/views, purchases/carts, purchases/views (event counts; can exceed 100% on small data) |
| Revenue | sum of price over purchase events only |
| Unique users / sessions | distinct user_id / non-empty user_session |
| Events per session | events having a session / unique sessions |
| Purchases per user / per buyer | purchases / all users, purchases / users with at least one purchase |
| Cart abandonment | sessions with a cart and no purchase / sessions with a cart (session level) |
| Bounce rate | sessions with exactly one event / sessions |
| Observed customer value | revenue per buyer over 31 days (not a lifetime-value forecast) |

Abandonment limitation: a cart in one session and a purchase in another counts as abandoned. Ordered funnel (`windowFunnel`) is
stricter than the unordered counts.