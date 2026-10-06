$ch = "$PSScriptRoot\ch.ps1"

$tests = [ordered]@{
  'funnel_raw' = @'
SELECT countIf(event_type = 'view'), countIf(event_type = 'cart'), countIf(event_type = 'purchase') FROM ecommerce.ecommerce_events
'@
  'funnel_agg' = @'
SELECT sumIf(events, event_type = 'view'), sumIf(events, event_type = 'cart'), sumIf(events, event_type = 'purchase') FROM ecommerce.daily_funnel
'@
  'revenue_day_raw' = @'
SELECT event_date, sumIf(price, event_type = 'purchase') FROM ecommerce.ecommerce_events GROUP BY event_date ORDER BY event_date
'@
  'revenue_day_agg' = @'
SELECT event_date, purchase_revenue FROM ecommerce.v_revenue_daily ORDER BY event_date
'@
  'brands_raw' = @'
SELECT brand, sum(price) AS r FROM ecommerce.ecommerce_events WHERE event_type = 'purchase' GROUP BY brand ORDER BY r DESC LIMIT 10
'@
  'brands_agg' = @'
SELECT brand, sum(revenue) AS r FROM ecommerce.brand_sales GROUP BY brand ORDER BY r DESC LIMIT 10
'@
  'users_raw' = @'
SELECT uniqExact(user_id), uniqExactIf(user_session, user_session != '') FROM ecommerce.ecommerce_events
'@
  'users_agg' = @'
SELECT uniqExactMerge(users), uniqExactMerge(sessions) FROM ecommerce.daily_uniques
'@
  'abandon_raw' = @'
SELECT countIf(c > 0), countIf(c > 0 AND p = 0) FROM (SELECT user_session, countIf(event_type = 'cart') AS c, countIf(event_type = 'purchase') AS p FROM ecommerce.ecommerce_events WHERE user_session != '' GROUP BY user_session)
'@
  'abandon_agg' = @'
SELECT cart_sessions, abandoned_sessions FROM ecommerce.v_kpi_summary
'@
}

foreach ($name in $tests.Keys) {
  1..3 | ForEach-Object {
    $q = $tests[$name].Trim() + " SETTINGS log_comment = 'p7_$name' FORMAT Null"
    & $ch -Query $q | Out-Null
  }
}

& $ch -Query "SYSTEM FLUSH LOGS" | Out-Null
& $ch -Query "SELECT replaceOne(log_comment, 'p7_', '') AS test, count() AS runs, round(avg(query_duration_ms)) AS avg_ms, max(read_rows) AS read_rows, formatReadableSize(max(read_bytes)) AS read_bytes, formatReadableSize(max(memory_usage)) AS peak_mem FROM system.query_log WHERE type = 'QueryFinish' AND log_comment LIKE 'p7_%' AND event_time > now() - INTERVAL 30 MINUTE GROUP BY log_comment ORDER BY log_comment"