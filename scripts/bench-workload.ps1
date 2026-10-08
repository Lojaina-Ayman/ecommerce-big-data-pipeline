# Runs the raw-table workload against one table, appends results to bench\workload.csv
# Usage: scripts\bench-workload.ps1 -Table ecommerce.ecommerce_events -Label baseline1
param([Parameter(Mandatory = $true)][string]$Table, [Parameter(Mandatory = $true)][string]$Label, [int]$Runs = 3)

$root = (Resolve-Path "$PSScriptRoot\..").Path
$ch = "$PSScriptRoot\ch.ps1"
New-Item -ItemType Directory -Force -Path "$root\bench" | Out-Null

$uid = ("$(& $ch -Query "SELECT any(user_id) FROM $Table WHERE event_type = 'purchase' AND event_date = '2019-10-15' FORMAT TSV")").Trim()
if (-not $uid) { throw "Could not pick a sample user_id from $Table (is it loaded?)" }

$queries = [ordered]@{
  'W1_day_type_count'   = "SELECT count() FROM {T} WHERE event_type = 'purchase' AND event_date = '2019-10-15'"
  'W2_users_cat_brand'  = "SELECT uniqExact(user_id) FROM {T} WHERE primary_category = 'electronics' AND brand = 'apple'"
  'W3_distinct_all'     = "SELECT uniqExact(user_id), uniqExactIf(user_session, user_session != '') FROM {T}"
  'W4_user_journey'     = "SELECT event_time, event_type, product_id, price FROM {T} WHERE user_id = {UID} ORDER BY event_time LIMIT 100"
  'W5_abandon_sessions' = "SELECT countIf(c > 0), countIf(c > 0 AND p = 0) FROM (SELECT user_session, countIf(event_type = 'cart') AS c, countIf(event_type = 'purchase') AS p FROM {T} WHERE user_session != '' GROUP BY user_session)"
  'W6_final_count'      = "SELECT count() FROM {T} FINAL"
  'W7_revenue_range'    = "SELECT event_date, sum(price) FROM {T} WHERE event_type = 'purchase' AND event_date BETWEEN '2019-10-10' AND '2019-10-20' GROUP BY event_date"
}

foreach ($name in $queries.Keys) {
  $sql = $queries[$name].Replace('{T}', $Table).Replace('{UID}', $uid)
  & $ch -Query "$sql SETTINGS log_comment = 'p11_warm' FORMAT Null" | Out-Null
  1..$Runs | ForEach-Object { & $ch -Query "$sql SETTINGS log_comment = 'p11_${Label}_$name' FORMAT Null" | Out-Null }
}

& $ch -Query "SYSTEM FLUSH LOGS" | Out-Null
$csv = & $ch -Query "SELECT replaceOne(log_comment, 'p11_${Label}_', '') AS query_name, count() AS runs, round(quantile(0.5)(query_duration_ms)) AS median_ms, max(read_rows) AS read_rows, max(read_bytes) AS read_bytes, max(memory_usage) AS peak_mem FROM system.query_log WHERE type = 'QueryFinish' AND log_comment LIKE 'p11_${Label}_%' AND event_time > now() - INTERVAL 3 HOUR GROUP BY log_comment ORDER BY log_comment FORMAT CSVWithNames"
$data = ($csv -join "`n") | ConvertFrom-Csv
$data | ForEach-Object { $_ | Add-Member -NotePropertyName label -NotePropertyValue $Label -PassThru } |
  Export-Csv "$root\bench\workload.csv" -Append -NoTypeInformation
$data | Format-Table query_name, runs, median_ms, read_rows,
  @{n = 'read_MB'; e = { [math]::Round([double]$_.read_bytes / 1MB) } },
  @{n = 'peak_MB'; e = { [math]::Round([double]$_.peak_mem / 1MB) } } -AutoSize
